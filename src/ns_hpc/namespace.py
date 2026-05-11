import os
import subprocess
from dataclasses import dataclass

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
    args.extend(["tini", "--"])
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

    try:
        proc = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            stdout_bytes, stderr_bytes = proc.communicate(
                input=stdin.encode() if stdin else None,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                proc.wait()
            return SandboxResult(exit_code=-1, stdout="", stderr="", sandbox_ok=False)

        return SandboxResult(
            exit_code=proc.returncode,
            stdout=stdout_bytes.decode() if stdout_bytes else "",
            stderr=stderr_bytes.decode() if stderr_bytes else "",
            sandbox_ok=True,
        )
    except OSError:
        return SandboxResult(exit_code=-1, stdout="", stderr="", sandbox_ok=False)
