"""WebDAV file server — mounts instance workspaces and extra paths via wsgidav.

Paths served when ``dav.enabled = true``:

    /dav/instances/{instance_id}/workspace/...   (rw)
    /dav/instances/{instance_id}/output/...      (rw)
    /dav/{extra_name}/...                        (rw/ro, config-driven)

The wsgidav WSGI app is wrapped with ``asgiref.wsgi.WsgiToAsgi`` and mounted
on the Starlette app via ``starlette.routing.Mount``.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from wsgidav.dav_error import DAVError, HTTP_METHOD_NOT_ALLOWED
from wsgidav.dav_provider import DAVProvider
from wsgidav.fs_dav_provider import FileResource as _FileResource
from wsgidav.fs_dav_provider import FolderResource

if TYPE_CHECKING:
    from ns_hpc.config import Config


logger = logging.getLogger("ns-hpc")

FileResource = _FileResource  # re-export


def _validate_within_root(target_path, root_path):
    """Raise RuntimeError if *target_path* escapes *root_path*.

    For non-existent paths (e.g. during PUT/CREATE), validates against the
    closest existing parent, then reconstructs the resolved target.
    """
    from pathlib import Path
    check_path = target_path
    while not check_path.exists() and check_path != check_path.parent:
        check_path = check_path.parent

    if target_path.exists():
        resolved_target = target_path.resolve()
    else:
        resolved_target = check_path.resolve() / target_path.relative_to(check_path)

    resolved_root = root_path.resolve()
    root_prefix = str(resolved_root) + "/"
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
        self._ws_mount = config.namespace.workspace_mount.lstrip("/")
        self._out_mount = config.namespace.output_mount.lstrip("/")

    def __repr__(self) -> str:
        return f"SandboxDavProvider(instances={self._instances_dir})"

    def _resolve(self, path: str, environ: dict) -> tuple[Path, bool] | None:
        """Resolve a provider-relative path to ``(host_path, readonly)``.

        Returns ``None`` when the path doesn't map to anything (404).
        """
        parts = path.strip("/").split("/")
        if len(parts) < 2:
            return None

        if parts[0] == "instances":
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

        if mount_name == self._ws_mount:
            root = inst_base / "workspace"
        elif mount_name == self._out_mount:
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

    def get_resource_inst(self, path: str, environ: dict):
        """Return a DAVNonCollection or DAVCollection for *path*."""
        resolved = self._resolve(path, environ)
        if resolved is None:
            return None

        host_path, readonly = resolved

        parts = path.strip("/").split("/")
        if parts[0] == "instances" and len(parts) >= 3:
            instance_id = parts[1]
            mount_name = parts[2]
            if mount_name == self._ws_mount:
                root_path = self._instances_dir / instance_id / "workspace"
            elif mount_name == self._out_mount:
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

        if host_path.is_dir():
            return FolderResource(path, environ, str(host_path))
        return FileResource(path, environ, str(host_path))
