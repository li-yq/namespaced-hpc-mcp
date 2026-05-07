import json
import os
import subprocess
from dataclasses import dataclass
from typing import IO

from ns_hpc.config import Config, load_config


@dataclass
class SandboxResult:
    exit_code: int
    stdout: str
    stderr: str
    sandbox_ok: bool


def build_bwrap_args(
    command: list[str],
    workspace_host_path: str,
    workspace_mount: str | None = None,
    working_dir: str | None = None,
    extra_ro_binds: list[tuple[str, str]] | None = None,
    extra_rw_binds: list[tuple[str, str]] | None = None,
    extra_bwrap_flags: list[str] | None = None,
    config: Config | None = None,
) -> list[str]:
    if config is None:
        config = load_config()

    args = ["bwrap"]
    args.extend(config.namespace_defaults.flags)

    for host_path in config.namespace_defaults.bind_ro:
        args.extend(["--ro-bind", host_path, host_path])

    if extra_ro_binds:
        for host, dest in extra_ro_binds:
            args.extend(["--ro-bind", host, dest])

    workspace_mount = workspace_mount or config.namespace_defaults.workspace_mount
    args.extend(["--bind", workspace_host_path, workspace_mount])

    if extra_rw_binds:
        for host, dest in extra_rw_binds:
            args.extend(["--bind", host, dest])

    if extra_bwrap_flags:
        args.extend(extra_bwrap_flags)

    working_dir = working_dir or workspace_mount
    args.extend(["--chdir", working_dir])

    args.append("--")
    args.extend(command)
    return args


def run_in_sandbox(
    command: list[str],
    workspace_host_path: str,
    workspace_mount: str | None = None,
    working_dir: str | None = None,
    extra_ro_binds: list[tuple[str, str]] | None = None,
    extra_rw_binds: list[tuple[str, str]] | None = None,
    extra_bwrap_flags: list[str] | None = None,
    stdin: str | None = None,
    timeout: float | None = None,
    config: Config | None = None,
) -> SandboxResult:
    if config is None:
        config = load_config()

    args = build_bwrap_args(
        command=command,
        workspace_host_path=workspace_host_path,
        workspace_mount=workspace_mount,
        working_dir=working_dir,
        extra_ro_binds=extra_ro_binds,
        extra_rw_binds=extra_rw_binds,
        extra_bwrap_flags=extra_bwrap_flags,
        config=config,
    )

    r_fd, w_fd = os.pipe()
    try:
        # Insert --json-status-fd with the write fd before "--"
        dash_pos = args.index("--")
        args[dash_pos:dash_pos] = ["--json-status-fd", str(w_fd)]

        proc = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(w_fd,),
        )
        # Close w_fd in parent BEFORE communicate() to avoid deadlock
        os.close(w_fd)

        try:
            stdout_bytes, stderr_bytes = proc.communicate(
                input=stdin.encode() if stdin else None,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            # bwrap runs as PID 1 in its own PID namespace and ignores
            # SIGTERM; SIGKILL is required.  The inner command may still
            # hold the pipe fds open, so don't wait for communicate to
            # finish — just reap and return what we have.
            proc.kill()
            try:
                proc.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                proc.wait()
            try:
                os.close(r_fd)
            except OSError:
                pass
            return SandboxResult(exit_code=-1, stdout="", stderr="", sandbox_ok=False)

        # Read JSON status from the pipe
        try:
            raw_status = os.read(r_fd, 4096)
        except OSError:
            raw_status = b""
        finally:
            os.close(r_fd)

        exit_code = proc.returncode
        sandbox_ok = True

        lines = [l for l in raw_status.splitlines() if l.strip()]
        if lines:
            try:
                status = json.loads(lines[-1])
                exit_code = status.get("exit-code", exit_code)
            except (json.JSONDecodeError, KeyError):
                sandbox_ok = False

        return SandboxResult(
            exit_code=exit_code,
            stdout=stdout_bytes.decode() if stdout_bytes else "",
            stderr=stderr_bytes.decode() if stderr_bytes else "",
            sandbox_ok=sandbox_ok,
        )
    except OSError:
        try:
            os.close(w_fd)
        except OSError:
            pass
        try:
            os.close(r_fd)
        except OSError:
            pass
        return SandboxResult(exit_code=-1, stdout="", stderr="", sandbox_ok=False)
