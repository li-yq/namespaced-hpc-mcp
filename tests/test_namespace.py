import os
import tempfile
from pathlib import Path

from ns_hpc.config import Config, NamespaceDefaults, ResourceDefaults
from ns_hpc.namespace import build_bwrap_args, run_in_sandbox


def default_config() -> Config:
    return Config(
        namespace_defaults=NamespaceDefaults(
            bind_ro=["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc"],
            workspace_mount="/workspace",
            flags=["--unshare-all", "--share-net", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"],
        ),
        proxied_mcps={},
        resource_defaults=ResourceDefaults(
            context_dirs=["context"],
            resource_patterns=["*.md"],
        ),
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
    assert args.count("--ro-bind") == len(cfg.namespace_defaults.bind_ro)
    assert "--bind" in args
    assert tmpdir in args
    assert "/workspace" in args
    assert "--chdir" in args
    assert args[args.index("--chdir") + 1] == "/workspace"
    assert "--" in args
    assert args[args.index("--") + 1:] == ["tini", "--", "echo", "hello"]


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
    # Default ro-binds + extra
    assert len(ro_idx) == len(cfg.namespace_defaults.bind_ro) + 1
    assert args[ro_idx[-1] + 1] == "/host/ro"
    assert args[ro_idx[-1] + 2] == "/container/ro"

    rw_idx = [i for i, a in enumerate(args) if a == "--bind"]
    assert args[rw_idx[-1] + 1] == "/host/rw"
    assert args[rw_idx[-1] + 2] == "/container/rw"


def test_run_in_sandbox_basic():
    with tempfile.TemporaryDirectory() as tmpdir:
        result = run_in_sandbox(
            command=["echo", "hello sandbox"],
            workspace_host_path=tmpdir,
        )
    assert result.sandbox_ok
    assert result.exit_code == 0
    assert "hello sandbox" in result.stdout


def test_run_in_sandbox_exit_code():
    with tempfile.TemporaryDirectory() as tmpdir:
        result = run_in_sandbox(
            command=["sh", "-c", "exit 42"],
            workspace_host_path=tmpdir,
        )
    assert result.sandbox_ok
    assert result.exit_code == 42


def test_run_in_sandbox_stdin():
    with tempfile.TemporaryDirectory() as tmpdir:
        result = run_in_sandbox(
            command=["sh", "-c", "read x; echo $x"],
            workspace_host_path=tmpdir,
            stdin="hello from stdin",
        )
    assert result.sandbox_ok
    assert result.exit_code == 0
    assert "hello from stdin" in result.stdout


def test_run_in_sandbox_isolation():
    """Verify /tmp inside the sandbox does not expose host files."""
    marker_fd, marker_path = tempfile.mkstemp(dir="/tmp", prefix="bwrap-test-marker-")
    os.close(marker_fd)

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_in_sandbox(
                command=["sh", "-c", f"test -f '{marker_path}' && echo UNSAFE || echo SAFE"],
                workspace_host_path=tmpdir,
            )
        assert result.sandbox_ok
        assert result.exit_code == 0
        assert result.stdout.strip() == "SAFE"
    finally:
        try:
            os.unlink(marker_path)
        except FileNotFoundError:
            pass


def test_run_in_sandbox_timeout():
    with tempfile.TemporaryDirectory() as tmpdir:
        result = run_in_sandbox(
            command=["sleep", "10"],
            workspace_host_path=tmpdir,
            timeout=2,
        )
    assert not result.sandbox_ok
    assert result.exit_code == -1
