"""WebDAV file server -- mounts instance workspaces and extra paths via wsgidav.

Paths served when ``dav.enabled = true``:

    /dav/instances/{instance_id}/workspace/...   (rw)
    /dav/instances/{instance_id}/output/...      (rw)
    /dav/{extra_name}/...                        (rw/ro, config-driven)

The wsgidav WSGI app is wrapped with ``PooledWSGIApp`` (a multi-threaded ASGI
bridge) and mounted on the Starlette app via ``starlette.routing.Mount``.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any

from wsgidav.dav_error import DAVError, HTTP_FORBIDDEN, HTTP_METHOD_NOT_ALLOWED
from wsgidav.dav_provider import DAVCollection, DAVProvider
from wsgidav.fs_dav_provider import FileResource as _FileResource
from wsgidav.fs_dav_provider import FolderResource

if TYPE_CHECKING:
    from collections.abc import Callable

    from ns_hpc.config import Config

    from starlette.types import Receive, Scope, Send


logger = logging.getLogger("ns-hpc")

FileResource = _FileResource  # re-export


def _build_environ(scope: Scope, body: bytes) -> dict[str, Any]:
    """Build a WSGI environ dict from an ASGI scope and request body."""
    script_name = scope.get("root_path", "").encode("utf8").decode("latin1")
    path_info = scope["path"].encode("utf8").decode("latin1")
    if path_info.startswith(script_name):
        path_info = path_info[len(script_name):]

    environ: dict[str, Any] = {
        "REQUEST_METHOD": scope["method"],
        "SCRIPT_NAME": script_name,
        "PATH_INFO": path_info,
        "QUERY_STRING": scope["query_string"].decode("ascii"),
        "SERVER_PROTOCOL": f"HTTP/{scope['http_version']}",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": scope.get("scheme", "http"),
        "wsgi.input": io.BytesIO(body),
        "wsgi.errors": io.StringIO(),
        "wsgi.multithread": True,
        "wsgi.multiprocess": True,
        "wsgi.run_once": False,
    }

    server = scope.get("server") or ("localhost", 80)
    environ["SERVER_NAME"] = server[0]
    environ["SERVER_PORT"] = str(server[1])

    if scope.get("client"):
        environ["REMOTE_ADDR"] = scope["client"][0]

    for name, value in scope.get("headers", []):
        name = name.decode("latin1")
        if name == "content-length":
            environ["CONTENT_LENGTH"] = value.decode("latin1")
        elif name == "content-type":
            environ["CONTENT_TYPE"] = value.decode("latin1")
        else:
            key = f"HTTP_{name}".upper().replace("-", "_")
            existing = environ.get(key)
            val = value.decode("latin1")
            environ[key] = f"{existing},{val}" if existing else val

    return environ


class PooledWSGIApp:
    """ASGI wrapper around a WSGI app with a dedicated thread pool.

    Unlike ``asgiref.wsgi.WsgiToAsgi`` (which uses a ``thread_sensitive``
    single-thread executor), this wrapper uses a configurable thread pool so
    that concurrent requests — e.g. multiple WebDAV file transfers — don't
    stall waiting for a slow I/O operation.

    Additionally, PROPFIND Depth defaults to ``"1"`` instead of
    ``"infinity"`` (RFC 4918) to avoid accidentally walking deep
    directory trees on NFS-backed mounts.
    """

    def __init__(self, wsgi_app: Callable[..., Any], max_workers: int = 10) -> None:
        self._wsgi_app = wsgi_app
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        body = b""
        more_body = True
        while more_body:
            message = await receive()
            body += message.get("body", b"")
            more_body = message.get("more_body", False)

        environ = _build_environ(scope, body)
        # Cap PROPFIND depth to 1 instead of infinity (RFC 4918 §9.1)
        if environ.get("REQUEST_METHOD") == "PROPFIND":
            environ.setdefault("HTTP_DEPTH", "1")

        loop = asyncio.get_running_loop()
        start_event = asyncio.Event()
        resp: dict[str, Any] = {}

        def start_response(
            status: str, headers: list[tuple[str, str]], exc_info: Any = None
        ) -> None:
            if resp:
                return
            resp["status"] = int(status.split()[0])
            resp["headers"] = [
                (k.encode("latin1"), v.encode("latin1")) for k, v in headers
            ]
            loop.call_soon_threadsafe(start_event.set)

        chunks_future = loop.run_in_executor(
            self._executor,
            lambda: list(self._wsgi_app(environ, start_response)),
        )

        await start_event.wait()
        await send({
            "type": "http.response.start",
            "status": resp["status"],
            "headers": resp["headers"],
        })

        for chunk in await chunks_future:
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
        await send({"type": "http.response.body", "body": b""})


def _validate_within_root(target_path: Path, root_path: Path) -> None:
    """Raise RuntimeError if *target_path* escapes *root_path*.

    For non-existent paths (e.g. during PUT/CREATE), validates against the
    closest existing parent, then reconstructs the resolved target.
    """
    check_path = target_path
    while not check_path.exists() and check_path != check_path.parent:
        check_path = check_path.parent

    if target_path.exists():
        resolved_target = target_path.resolve()
    else:
        resolved_target = check_path.resolve() / target_path.relative_to(check_path)

    resolved_root = root_path.resolve()
    root_prefix = str(resolved_root) + os.sep
    if not str(resolved_target).startswith(root_prefix) and resolved_target != resolved_root:
        raise RuntimeError(
            f"Security: path {target_path} resolves outside root {root_path}"
        )


class SandboxDavProvider(DAVProvider):
    """DAV provider that routes requests to instance workspaces or extra mounts.

    URL path structure (relative to the /dav/ mount point)::

        /instances/{instance_id}/workspace/...     -> instance workspace (rw)
        /instances/{instance_id}/output/...        -> instance output dir (rw)
        /{extra_name}/...                          -> config dav.extras entry

    Instance directories that don't exist or are archived return 404.
    """

    def __init__(self, config: Config) -> None:
        super().__init__()
        self._cfg = config
        self._instances_dir = config.resolve_instances_dir()
        self._output_dir = self._instances_dir / "output"
        self._extras = config.dav.extras
        # wsgidav.fs_dav_provider checks self.provider.readonly; we handle
        # per-mount readonly in get_resource_inst, so mark provider as rw
        self.readonly = False

    def __repr__(self) -> str:
        return f"SandboxDavProvider(instances={self._instances_dir})"

    def _resolve(self, path: str, environ: dict) -> tuple[Path, bool] | None:
        """Resolve a provider-relative path to ``(host_path, readonly)``.

        Returns ``None`` when the path doesn't map to anything (404).

        ``get_resource_inst`` handles virtual directories (root, instance listing,
        mount listing) before this is called.
        """
        parts = path.strip("/").split("/")
        if not parts or parts[0] == "":
            return None

        if parts[0] == "instances":
            if len(parts) < 2:
                return None
            return self._resolve_instance_path(parts)

        return self._resolve_extra_path(parts)

    def _resolve_instance_path(self, parts: list[str]) -> tuple[Path, bool] | None:
        """Resolve ``/instances/{id}/{mount_name}/...``."""
        if len(parts) < 3:
            return None

        instance_id = parts[1]
        mount_name = parts[2]
        relative = "/".join(parts[3:]) if len(parts) > 3 else ""

        inst_base = self._instances_dir / instance_id
        meta_path = inst_base / "metadata.json"
        if not meta_path.exists():
            return None

        try:
            meta = json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        if meta.get("archived"):
            return None

        if mount_name == "workspace":
            root = inst_base / "workspace"
        elif mount_name == "output":
            root = self._output_dir / instance_id
        else:
            return None

        target = root / relative if relative else root
        return target, False  # rw

    def _resolve_extra_path(self, parts: list[str]) -> tuple[Path, bool] | None:
        """Resolve ``/{extra_name}/...`` against configured extras."""
        name = parts[0]
        extra = self._extras.get(name)
        if extra is None:
            return None

        root = Path(os.path.expanduser(extra.path)).resolve()
        relative = "/".join(parts[1:]) if len(parts) > 1 else ""
        target = root / relative if relative else root
        return target, extra.ro

    def _loc_to_file_path(self, path: str, environ: dict | None = None) -> str:
        """Map a provider-relative path to an absolute filesystem path.

        Required by wsgidav.fs_dav_provider for PUT/MKCOL/create_empty_resource
        which call the provider directly rather than going through
        get_resource_inst.
        """
        resolved = self._resolve(path, environ or {})
        if resolved is None:
            # For create operations, the target path goes through _resolve
            # which validates the instance exists. If it returns None,
            # the path is invalid.
            raise RuntimeError(f"Invalid path for DAV create: {path}")
        return str(resolved[0])

    def get_resource_inst(self, path: str, environ: dict):
        """Return a DAVNonCollection or DAVCollection for *path*."""
        parts = path.strip("/").split("/")

        # Virtual root directory listing at /dav/
        if not parts or parts[0] == "":
            return VirtualRootCollection(path, environ, self._cfg)

        # Virtual instance listing at /dav/instances/
        if parts[0] == "instances":
            if len(parts) == 1:
                return VirtualInstanceListCollection(
                    path, environ, self._cfg, self._instances_dir, self._output_dir,
                )
            if len(parts) == 2:
                inst = _load_instance(parts[1], self._cfg)
                if inst is None or inst.is_archived():
                    return None
                return VirtualInstanceMountCollection(
                    path, environ, self._instances_dir, self._output_dir,
                )
            # Fall through to real path resolution for deeper paths

        resolved = self._resolve(path, environ)
        if resolved is None:
            return None

        host_path, readonly = resolved

        parts = path.strip("/").split("/")
        if parts[0] == "instances" and len(parts) >= 3:
            instance_id = parts[1]
            mount_name = parts[2]
            if mount_name == "workspace":
                root_path = self._instances_dir / instance_id / "workspace"
            elif mount_name == "output":
                root_path = self._output_dir / instance_id
            else:
                return None
        elif parts[0] in self._extras:
            extra = self._extras[parts[0]]
            root_path = Path(os.path.expanduser(extra.path)).resolve()
        else:
            return None

        _validate_within_root(host_path, root_path)

        method = environ.get("REQUEST_METHOD", "GET").upper()
        if readonly and method not in ("GET", "HEAD", "OPTIONS", "PROPFIND"):
            raise DAVError(HTTP_METHOD_NOT_ALLOWED, f"read-only mount: {path}")

        if not host_path.exists():
            return None

        # Prevent DELETE, MOVE, COPY on mount roots (workspace, output, extra root)
        is_mount_root = (parts[0] == "instances" and len(parts) == 3) or (
            parts[0] in self._extras and len(parts) == 1
        )

        # Audit writes to instance audit log
        if parts[0] == "instances" and method not in (
            "GET", "HEAD", "OPTIONS", "PROPFIND",
        ):
            inst = _load_instance(parts[1], self._cfg)
            if inst is not None:
                inst.audit("dav.write", method=method, path=path)

        if host_path.is_dir():
            cls: Any = ProtectedFolderResource if is_mount_root else FolderResource
            res = cls(path, environ, str(host_path))
        else:
            res = FileResource(path, environ, str(host_path))
        # _FileResource.__init__ overrides name with os.path.basename(_file_path);
        # restore the name from the virtual URL path so directory listings show
        # the correct display name (e.g. "output" not "explore-01").
        res.name = path.strip("/").split("/")[-1]
        return res


def _load_instance(instance_id: str, config: Config):
    """Load an instance, returning None if not found. Avoids top-level import."""
    from ns_hpc.instance import Instance
    return Instance.load(instance_id, config)


def _list_active_instances(config: Config) -> list:
    """List active (non-archived) instances. Avoids top-level import."""
    from ns_hpc.instance import Instance
    return [i for i in Instance.list_instances(config) if not i.is_archived()]


# -- Protected mount root resource --------------------------------------------


class ProtectedFolderResource(FolderResource):
    """A FolderResource that rejects DELETE, MOVE, and COPY.

    Used for mount roots (workspace, output, extra roots) that should
    not be deleted, renamed, or copied via WebDAV. Subpath operations
    (e.g. deleting a file inside the workspace) are unaffected.
    """

    def handle_delete(self):
        raise DAVError(
            HTTP_FORBIDDEN, f"Cannot delete mount point: {self.path}"
        )

    def handle_move(self, dest_path):
        raise DAVError(
            HTTP_FORBIDDEN, f"Cannot rename mount point: {self.path}"
        )

    def handle_copy(self, dest_path, *, depth_infinity):
        raise DAVError(
            HTTP_FORBIDDEN, f"Cannot copy mount point: {self.path}"
        )

    def delete(self):
        raise DAVError(
            HTTP_FORBIDDEN, f"Cannot delete mount point: {self.path}"
        )


# -- Virtual DAV collections for directory listing ----------------------------


class VirtualRootCollection(DAVCollection):
    """Virtual root directory at ``/`` listing instances/ and configured extras."""

    def __init__(self, path: str, environ: dict, config: Config) -> None:
        super().__init__(path, environ)
        self._cfg = config

    def get_member_names(self) -> list[str]:
        names = ["instances"]
        names.extend(self._cfg.dav.extras)
        return sorted(names)

    def get_member(self, name: str):
        from wsgidav import util
        child_path = util.join_uri(self.path, name)
        return self.provider.get_resource_inst(child_path, self.environ)

    def get_displayname(self) -> str:
        return "ns-hpc"

    def get_creationdate(self):
        return time.time()

    def get_last_modified(self):
        return time.time()

    def handle_delete(self):
        raise DAVError(HTTP_FORBIDDEN, "Cannot delete root directory")

    def handle_move(self, dest_path):
        raise DAVError(HTTP_FORBIDDEN, "Cannot rename root directory")

    def handle_copy(self, dest_path, *, depth_infinity):
        raise DAVError(HTTP_FORBIDDEN, "Cannot copy root directory")


class VirtualInstanceListCollection(DAVCollection):
    """Virtual directory at ``/instances/`` listing all active (non-archived) instances."""

    def __init__(
        self, path: str, environ: dict, config: Config,
        instances_dir: Path, output_dir: Path,
    ) -> None:
        super().__init__(path, environ)
        self._cfg = config
        self._instances_dir = instances_dir
        self._output_dir = output_dir

    def get_member_names(self) -> list[str]:
        instances = _list_active_instances(self._cfg)
        return sorted(i.id for i in instances)

    def get_member(self, name: str):
        from wsgidav import util
        child_path = util.join_uri(self.path, name)
        return self.provider.get_resource_inst(child_path, self.environ)

    def get_displayname(self) -> str:
        return "instances"

    def get_creationdate(self):
        return time.time()

    def get_last_modified(self):
        return time.time()

    def handle_delete(self):
        raise DAVError(HTTP_FORBIDDEN, "Cannot delete instance list")

    def handle_move(self, dest_path):
        raise DAVError(HTTP_FORBIDDEN, "Cannot rename instance list")

    def handle_copy(self, dest_path, *, depth_infinity):
        raise DAVError(HTTP_FORBIDDEN, "Cannot copy instance list")


class VirtualInstanceMountCollection(DAVCollection):
    """Virtual directory at ``/instances/{id}/`` listing workspace/ and output/."""

    def __init__(
        self, path: str, environ: dict,
        instances_dir: Path, output_dir: Path,
    ) -> None:
        super().__init__(path, environ)
        self._instance_id = path.strip("/").split("/")[-1]
        self._instances_dir = instances_dir
        self._output_dir = output_dir

    def get_member_names(self) -> list[str]:
        names = []
        if (self._instances_dir / self._instance_id / "workspace").is_dir():
            names.append("workspace")
        if (self._output_dir / self._instance_id).is_dir():
            names.append("output")
        return names

    def get_member(self, name: str):
        from wsgidav import util
        child_path = util.join_uri(self.path, name)
        return self.provider.get_resource_inst(child_path, self.environ)

    def get_displayname(self) -> str:
        return self._instance_id

    def get_creationdate(self):
        return time.time()

    def get_last_modified(self):
        return time.time()

    def handle_delete(self):
        raise DAVError(
            HTTP_FORBIDDEN,
            "Cannot delete instance mount point; use the archive_instance tool instead",
        )

    def handle_move(self, dest_path):
        raise DAVError(
            HTTP_FORBIDDEN,
            "Cannot rename instance mount point; instance ID is managed",
        )

    def handle_copy(self, dest_path, *, depth_infinity):
        raise DAVError(HTTP_FORBIDDEN, "Cannot copy instance mount point")
