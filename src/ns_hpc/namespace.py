"""Build bwrap command-line arguments for sandbox construction."""

import shlex

from ns_hpc.config import Config, load_config


def build_bwrap_args(
    command: list[str],
    workspace_host_path: str,
    workspace_mount: str | None = None,
    working_dir: str | None = None,
    extra_ro_binds: list[tuple[str, str]] | None = None,
    extra_rw_binds: list[tuple[str, str]] | None = None,
    extra_tmpfs: list[str] | None = None,
    extra_bwrap_flags: list[str] | None = None,
    config: Config | None = None,
) -> list[str]:
    if config is None:
        config = load_config()

    # Start from the base bwrap_command defined in config
    args = list(config.namespace.bwrap_command)

    # Sandbox tmpfs before any binds so explicit binds can overlay
    if extra_tmpfs:
        for mount_point in extra_tmpfs:
            args.extend(["--tmpfs", mount_point])

    # Workspace: either bind the host workspace dir or create a tmpfs
    ws_mount = workspace_mount or config.namespace.workspace_mount
    if workspace_host_path:
        args.extend(["--bind", workspace_host_path, ws_mount])
    else:
        args.extend(["--tmpfs", ws_mount])

    # Extra read-write binds
    if extra_rw_binds:
        for host, dest in extra_rw_binds:
            args.extend(["--bind", host, dest])

    # Extra read-only binds
    if extra_ro_binds:
        for host, dest in extra_ro_binds:
            args.extend(["--ro-bind", host, dest])

    # Extra bwrap flags (e.g. --json-status-fd)
    if extra_bwrap_flags:
        args.extend(extra_bwrap_flags)

    # Chdir into workspace
    working_dir = working_dir or ws_mount
    args.extend(["--chdir", working_dir])

    # Separator and command
    args.append("--")
    args.extend(command)
    return args
