"""Tests for the bwrap namespace / argument builder."""

import tempfile

from ns_hpc.config import Config
from ns_hpc.namespace import build_bwrap_args


def default_config() -> Config:
    return Config(
        namespace={
            "bwrap_command": [
                "bwrap",
                "--unshare-all", "--share-net",
                "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
                "--ro-bind", "/usr", "/usr",
                "--ro-bind", "/lib", "/lib",
                "--ro-bind", "/lib64", "/lib64",
                "--ro-bind", "/bin", "/bin",
                "--ro-bind", "/sbin", "/sbin",
                "--ro-bind", "/etc", "/etc",
            ],
        },
        jobs={
            "local": {
                "use_cgroups": False,
                "cgroups_command": ["systemd-run", "--user", "--scope", "--"],
            },
            "slurm": {
                "sbatch_command": ["sbatch"],
                "limit": {},
            },
        },
        proxied_mcps={},
    )


def test_build_bwrap_args_basic():
    cfg = default_config()
    with tempfile.TemporaryDirectory() as tmpdir:
        args = build_bwrap_args(
            command=["echo", "hello"],
            workspace_host_path=tmpdir,
            config=cfg,
        )

    assert args[0] == "bwrap"
    assert "--unshare-all" in args
    assert "--share-net" in args
    assert "--proc" in args
    assert "/proc" in args
    assert "--tmpfs" in args
    assert "/tmp" in args
    assert "--ro-bind" in args
    # 6 default ro-binds
    assert args.count("--ro-bind") == 6
    assert "--bind" in args
    assert tmpdir in args
    assert "/workspace" in args
    assert "--chdir" in args
    assert args[args.index("--chdir") + 1] == "/workspace"
    assert "--" in args
    assert args[args.index("--") + 1:] == ["echo", "hello"]


def test_build_bwrap_args_extra_binds():
    cfg = default_config()
    extra_ro = [("/host/ro", "/container/ro")]
    extra_rw = [("/host/rw", "/container/rw")]

    with tempfile.TemporaryDirectory() as tmpdir:
        args = build_bwrap_args(
            command=["true"],
            workspace_host_path=tmpdir,
            extra_ro_binds=extra_ro,
            extra_rw_binds=extra_rw,
            config=cfg,
        )

    ro_idx = [i for i, a in enumerate(args) if a == "--ro-bind"]
    # Default ro-binds (from bwrap_command) + extra
    assert len(ro_idx) == 6 + 1  # 6 default + 1 extra
    # Find the extra bind — it's appended after the workspace bind
    # So last --ro-bind entry should be our extra
    assert args[ro_idx[-1] + 1] == "/host/ro"
    assert args[ro_idx[-1] + 2] == "/container/ro"

    # Find all --bind entries (workspace bind + extra rw bind)
    rw_idx = [i for i, a in enumerate(args) if a == "--bind"]
    # Last --bind should be the extra_rw_bind
    assert args[rw_idx[-1] + 1] == "/host/rw"
    assert args[rw_idx[-1] + 2] == "/container/rw"
