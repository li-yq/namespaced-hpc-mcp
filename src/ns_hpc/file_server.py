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
import shutil
import stat
import tempfile
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any, IO

from asgiref.sync import AsyncToSync
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


def _build_environ(scope: Scope, body: Any) -> dict[str, Any]:
    """Build a WSGI environ dict from an ASGI scope and request body."""
    if isinstance(body, bytes):
        body = io.BytesIO(body)
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
        "wsgi.input": body,
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


class _StoppableInput:
    """WSGI input wrapper that aborts blocking upload work after disconnect."""

    def __init__(self, stream: IO[bytes], stop_event: threading.Event) -> None:
        self._stream = stream
        self._stop_event = stop_event

    def _check_stopped(self) -> None:
        if self._stop_event.is_set():
            raise ConnectionAbortedError("ASGI request stopped")

    def read(self, size: int = -1) -> bytes:
        self._check_stopped()
        return self._stream.read(size)

    def readline(self, size: int = -1) -> bytes:
        self._check_stopped()
        return self._stream.readline(size)

    def readlines(self, hint: int = -1) -> list[bytes]:
        self._check_stopped()
        return self._stream.readlines(hint)

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)


class PooledWSGIApp:
    """ASGI wrapper around a WSGI app with a dedicated thread pool.

    Unlike ``asgiref.wsgi.WsgiToAsgi`` (which uses a ``thread_sensitive``
    single-thread executor), this wrapper uses a configurable thread pool so
    that concurrent requests — e.g. multiple WebDAV file transfers — don't
    stall waiting for a slow I/O operation. Request bodies spool to the
    explicitly configured disk directory after 64 KiB, and WSGI response
    chunks are forwarded immediately with ASGI backpressure.

    Additionally, PROPFIND Depth defaults to ``"1"`` instead of
    ``"infinity"`` (RFC 4918) to avoid accidentally walking deep
    directory trees on NFS-backed mounts.
    """

    def __init__(
        self,
        wsgi_app: Callable[..., Any],
        *,
        spool_dir: Path,
        max_workers: int = 10,
        max_inflight_requests: int | None = None,
        min_spool_free_bytes: int = 1024**3,
    ) -> None:
        spool_path = Path(spool_dir)
        if spool_path.is_symlink():
            raise RuntimeError(
                f"WebDAV spool directory must not be a symbolic link: {spool_path}"
            )
        if not spool_path.exists():
            raise RuntimeError(f"WebDAV spool directory does not exist: {spool_path}")
        if not spool_path.is_dir():
            raise RuntimeError(f"WebDAV spool path is not a directory: {spool_path}")
        spool_stat = spool_path.stat(follow_symlinks=False)
        if spool_stat.st_uid != os.geteuid():
            raise RuntimeError(
                f"WebDAV spool directory is not owned by the service user: {spool_path}"
            )
        if stat.S_IMODE(spool_stat.st_mode) & 0o077:
            raise RuntimeError(
                f"WebDAV spool directory permissions must be 0700: {spool_path}"
            )
        if not os.access(spool_path, os.W_OK | os.X_OK):
            raise RuntimeError(
                f"WebDAV spool directory is not writable or traversable: {spool_path}"
            )
        if min_spool_free_bytes < 0:
            raise ValueError("min_spool_free_bytes must be non-negative")
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        if max_inflight_requests is None:
            max_inflight_requests = 2 * max_workers
        if max_inflight_requests < 1:
            raise ValueError("max_inflight_requests must be positive")
        try:
            spool_fd = os.open(
                spool_path,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            )
        except OSError as exc:
            raise RuntimeError(
                f"Cannot securely open WebDAV spool directory: {spool_path}"
            ) from exc
        try:
            opened_stat = os.fstat(spool_fd)
            if opened_stat.st_uid != os.geteuid():
                raise RuntimeError(
                    f"WebDAV spool directory is not owned by the service user: {spool_path}"
                )
            if stat.S_IMODE(opened_stat.st_mode) & 0o077:
                raise RuntimeError(
                    f"WebDAV spool directory permissions must be 0700: {spool_path}"
                )
        except BaseException:
            os.close(spool_fd)
            raise
        executor: ThreadPoolExecutor | None = None
        try:
            executor = ThreadPoolExecutor(max_workers=max_workers)
            spool_fd_finalizer = weakref.finalize(self, os.close, spool_fd)
        except BaseException:
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
            os.close(spool_fd)
            raise
        assert executor is not None
        self._wsgi_app = wsgi_app
        self._executor = executor
        self._spool_fd = spool_fd
        self._spool_fd_finalizer = spool_fd_finalizer
        self._spool_dir = f"/proc/self/fd/{spool_fd}"
        self._min_spool_free_bytes = min_spool_free_bytes
        self._spool_reservation_lock = threading.Lock()
        self._reserved_spool_bytes = 0
        self._admission_lock = threading.Lock()
        self._max_inflight_requests = max_inflight_requests
        self._inflight_requests = 0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not self._try_admit_request():
            await self._send_plain_error(send, 503, b"WebDAV server is busy")
            return
        try:
            await self._handle_request(scope, receive, send)
        finally:
            self._release_request_admission()

    async def _handle_request(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        request_reserved_bytes = 0
        declared_length = self._get_content_length(scope)
        with tempfile.SpooledTemporaryFile(
            max_size=64 * 1024,
            mode="w+b",
            dir=self._spool_dir,
        ) as body:
            try:
                if declared_length:
                    initial_reservation = 2 * declared_length
                    if not self._reserve_spool_bytes(initial_reservation):
                        await self._send_insufficient_storage(send)
                        return
                    request_reserved_bytes = initial_reservation

                received_bytes = 0
                more_body = True
                while more_body:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    if message["type"] != "http.request":
                        raise ValueError(
                            f"WSGI bridge received unexpected ASGI message: {message['type']}"
                        )
                    chunk = message.get("body", b"")
                    if chunk:
                        if (
                            declared_length is not None
                            and received_bytes + len(chunk) > declared_length
                        ):
                            await self._send_plain_error(
                                send, 400, b"Request body exceeds Content-Length"
                            )
                            return
                        if declared_length is None:
                            chunk_reservation = 2 * len(chunk)
                            if not self._reserve_spool_bytes(chunk_reservation):
                                await self._send_insufficient_storage(send)
                                return
                            request_reserved_bytes += chunk_reservation
                        try:
                            body.write(chunk)
                        except BaseException:
                            if declared_length is None:
                                self._release_spool_bytes(2 * len(chunk))
                                request_reserved_bytes -= 2 * len(chunk)
                            raise
                        self._release_spool_bytes(len(chunk))
                        request_reserved_bytes -= len(chunk)
                        received_bytes += len(chunk)
                    more_body = message.get("more_body", False)
                body.seek(0)

                stop_event = threading.Event()
                environ = _build_environ(scope, _StoppableInput(body, stop_event))
                # Cap PROPFIND depth to 1 instead of infinity (RFC 4918 §9.1)
                if environ.get("REQUEST_METHOD") == "PROPFIND":
                    environ.setdefault("HTTP_DEPTH", "1")

                loop = asyncio.get_running_loop()
                sync_send = AsyncToSync(send)
                worker_future = loop.run_in_executor(
                    self._executor,
                    self._run_wsgi_app,
                    environ,
                    sync_send,
                    stop_event,
                )
                disconnect_task = asyncio.create_task(
                    self._wait_for_disconnect(receive, stop_event)
                )
                try:
                    done, _ = await asyncio.wait(
                        (worker_future, disconnect_task),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if disconnect_task in done:
                        try:
                            await disconnect_task
                        finally:
                            stop_event.set()
                            was_cancelled = await self._drain_worker(worker_future)
                        if was_cancelled:
                            raise asyncio.CancelledError
                        return
                    await asyncio.shield(worker_future)
                except asyncio.CancelledError:
                    stop_event.set()
                    try:
                        await self._drain_worker(worker_future)
                    finally:
                        raise
                finally:
                    disconnect_task.cancel()
                    await asyncio.gather(disconnect_task, return_exceptions=True)
            finally:
                if request_reserved_bytes:
                    self._release_spool_bytes(request_reserved_bytes)

    def _try_admit_request(self) -> bool:
        with self._admission_lock:
            if self._inflight_requests >= self._max_inflight_requests:
                return False
            self._inflight_requests += 1
            return True

    def _release_request_admission(self) -> None:
        with self._admission_lock:
            self._inflight_requests -= 1

    @staticmethod
    async def _drain_worker(worker_future: asyncio.Future[Any]) -> bool:
        """Wait for a worker despite repeated cancellation of the ASGI task."""
        was_cancelled = False
        while not worker_future.done():
            try:
                await asyncio.shield(worker_future)
            except asyncio.CancelledError:
                was_cancelled = True

        if worker_future.cancelled():
            return was_cancelled
        try:
            worker_future.result()
        except Exception:
            if not was_cancelled:
                raise
            logger.debug("WSGI worker failed while its ASGI task was cancelled", exc_info=True)
        return was_cancelled

    @staticmethod
    def _get_content_length(scope: Scope) -> int | None:
        for name, value in scope.get("headers", []):
            if name.lower() == b"content-length":
                try:
                    length = int(value)
                except ValueError:
                    return None
                return length if length >= 0 else None
        return None

    def _reserve_spool_bytes(self, size: int) -> bool:
        """Atomically reserve bytes while preserving the configured free floor."""
        with self._spool_reservation_lock:
            free = shutil.disk_usage(self._spool_dir).free
            required = (
                self._reserved_spool_bytes
                + size
                + self._min_spool_free_bytes
            )
            if free < required:
                return False
            self._reserved_spool_bytes += size
            return True

    def _release_spool_bytes(self, size: int) -> None:
        with self._spool_reservation_lock:
            self._reserved_spool_bytes -= size

    @staticmethod
    async def _send_insufficient_storage(send: Send) -> None:
        await PooledWSGIApp._send_plain_error(
            send, 507, b"Insufficient storage for WebDAV upload"
        )

    @staticmethod
    async def _send_plain_error(send: Send, status: int, body: bytes) -> None:
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        })
        await send({"type": "http.response.body", "body": body})

    @staticmethod
    async def _wait_for_disconnect(
        receive: Receive, stop_event: threading.Event
    ) -> None:
        """Wait for the ASGI server to report that the response client left."""
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                stop_event.set()
                return
            if message["type"] != "http.request":
                raise ValueError(
                    f"WSGI bridge received unexpected ASGI message: {message['type']}"
                )

    def _run_wsgi_app(
        self,
        environ: dict[str, Any],
        sync_send: Callable[[dict[str, Any]], Any],
        stop_event: threading.Event,
    ) -> None:
        """Run WSGI in a worker, forwarding each response chunk immediately."""
        response_start: dict[str, Any] | None = None
        response_started = False

        def start_response(
            status: str, headers: list[tuple[str, str]], exc_info: Any = None
        ) -> Callable[[bytes], None]:
            nonlocal response_start
            if exc_info is not None and response_started:
                error = exc_info[1]
                raise error.with_traceback(exc_info[2])
            if response_start is not None and exc_info is None:
                raise RuntimeError("start_response called twice without exc_info")
            response_start = {
                "type": "http.response.start",
                "status": int(status.split()[0]),
                "headers": [
                    (key.encode("latin1"), value.encode("latin1"))
                    for key, value in headers
                ],
            }
            return write

        def write(chunk: bytes) -> None:
            nonlocal response_started
            if stop_event.is_set():
                raise ConnectionAbortedError("ASGI request stopped")
            if response_start is None:
                raise RuntimeError("write called before start_response")
            if not response_started:
                sync_send(response_start)
                response_started = True
            sync_send({
                "type": "http.response.body",
                "body": chunk,
                "more_body": True,
            })

        iterator = None
        try:
            if stop_event.is_set():
                return
            iterator = iter(self._wsgi_app(environ, start_response))
            while not stop_event.is_set():
                try:
                    chunk = next(iterator)
                except StopIteration:
                    break
                if stop_event.is_set():
                    break
                if response_start is None:
                    raise RuntimeError("WSGI app yielded before calling start_response")
                write(chunk)

            if stop_event.is_set():
                return
            if response_start is None:
                raise RuntimeError("WSGI app returned without calling start_response")
            if not response_started:
                sync_send(response_start)
            sync_send({"type": "http.response.body", "body": b""})
        except ConnectionAbortedError:
            if not stop_event.is_set():
                raise
        finally:
            close = getattr(iterator, "close", None) if iterator is not None else None
            if close is not None:
                close()


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
        for name, extra in self._extras.items():
            root = Path(os.path.expanduser(extra.path)).resolve()
            if not root.exists():
                raise RuntimeError(
                    f"WebDAV extra mount {name!r} does not exist: {root}"
                )
            if not root.is_dir():
                raise RuntimeError(
                    f"WebDAV extra mount {name!r} is not a directory: {root}"
                )
            if not os.access(root, os.R_OK | os.X_OK):
                raise RuntimeError(
                    f"WebDAV extra mount {name!r} is not readable or traversable: {root}"
                )
        # wsgidav.fs_dav_provider resources expect these FilesystemProvider
        # attributes even though this provider routes virtual mount points.
        self.fs_opts = {"follow_symlinks": False}
        self.shadow_map = {}
        # wsgidav.fs_dav_provider checks self.provider.readonly; we handle
        # per-mount readonly in get_resource_inst, so mark provider as rw.
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
