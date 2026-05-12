from ns_hpc.config import Config, load_config


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
