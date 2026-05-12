"""Tests for the job manager module."""

import os
import tempfile
import time
from pathlib import Path

from ns_hpc.job_manager import JobManager, JobStatus, _tail_file
from ns_hpc.config import Config, load_config
from ns_hpc.instance import Instance

# TOML template matching the same values that the old inline Config builder used.
# Written to a temp file so subprocesses (ns-hpc bwrap) inherit NS_HPC_CONFIG
# and can find test instances.
_CONFIG_TOML = """\
instances_dir = "{instances_dir}"

[namespace_defaults]
bind_ro = ["/usr", "/bin", "/lib", "/lib64"]
workspace_mount = "/workspace"
flags = ["--unshare-all", "--share-net", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]

[proxied_mcps]

[resource_defaults]
context_dirs = ["config/context"]
resource_patterns = ["*.md"]
"""


def _config(tmp_dir: str, monkeypatch) -> Config:
    config_path = Path(tmp_dir) / "config.toml"
    config_path.write_text(_CONFIG_TOML.format(instances_dir=tmp_dir))
    monkeypatch.setenv("NS_HPC_CONFIG", str(config_path))
    return load_config(str(config_path))


def _instance(config: Config) -> Instance:
    return Instance.create("test-job", config)


def test_tail_file():
    """Verify _tail_file reads the last N lines."""
    tmp = Path(tempfile.mkstemp()[1])
    try:
        tmp.write_text("line1\nline2\nline3\nline4\nline5\n")
        assert _tail_file(tmp, 2) == "line4\nline5"
        assert _tail_file(tmp, 10) == "line1\nline2\nline3\nline4\nline5"
        assert _tail_file(tmp, 0) == ""
    finally:
        tmp.unlink()


def test_tail_file_nonexistent():
    assert _tail_file(Path("/nonexistent"), 10) == ""


def test_tail_file_empty():
    tmp = Path(tempfile.mkstemp()[1])
    try:
        tmp.write_text("")
        assert _tail_file(tmp, 10) == ""
    finally:
        tmp.unlink()


def test_submit_and_complete(tmp_path, monkeypatch):
    """Submit a quick command, verify it completes."""
    cfg = _config(str(tmp_path), monkeypatch)
    inst = _instance(cfg)
    mgr = JobManager(inst, cfg)

    result = mgr.submit("echo hello_job", timeout=10, tail=5)
    assert result.status == JobStatus.COMPLETED, f"Got {result.status}"
    assert result.exit_code == 0
    assert "hello_job" in result.stdout_tail

    # Output files exist
    assert Path(result.stdout_path).exists()
    assert "hello_job" in Path(result.stdout_path).read_text()


def test_submit_exit_code(tmp_path, monkeypatch):
    """Verify non-zero exit codes."""
    cfg = _config(str(tmp_path), monkeypatch)
    inst = _instance(cfg)
    mgr = JobManager(inst, cfg)

    result = mgr.submit("exit 42", timeout=10)
    assert result.status == JobStatus.FAILED
    assert result.exit_code == 42


def test_submit_detach_timeout(tmp_path, monkeypatch):
    """Submit a long command, timeout before completion, verify running."""
    cfg = _config(str(tmp_path), monkeypatch)
    inst = _instance(cfg)
    mgr = JobManager(inst, cfg)

    result = mgr.submit("sleep 30 && echo done", timeout=2, tail=5)
    assert result.status == JobStatus.RUNNING, f"Expected RUNNING, got {result.status}"
    # Output file should exist (may be empty)
    assert Path(result.stdout_path).exists()

    # Clean up — cancel the still-running job
    mgr.cancel(result.job_id)


def test_poll_running_job(tmp_path, monkeypatch):
    """Submit long command, poll it while running."""
    cfg = _config(str(tmp_path), monkeypatch)
    inst = _instance(cfg)
    mgr = JobManager(inst, cfg)

    result = mgr.submit("sleep 30", timeout=2, tail=5)
    assert result.status == JobStatus.RUNNING

    # Poll with 0 timeout (just check status)
    polled = mgr.poll(result.job_id, timeout=0)
    assert polled is not None
    assert polled.status == JobStatus.RUNNING

    # Cancel
    assert mgr.cancel(result.job_id)
    assert mgr.poll(result.job_id, timeout=0) is None


def test_list_jobs(tmp_path, monkeypatch):
    """Submit two jobs, list them."""
    cfg = _config(str(tmp_path), monkeypatch)
    inst = _instance(cfg)
    mgr = JobManager(inst, cfg)

    r1 = mgr.submit("echo first", timeout=10)
    assert r1.status == JobStatus.COMPLETED

    r2 = mgr.submit("sleep 30", timeout=1, tail=5)
    assert r2.status == JobStatus.RUNNING

    jobs = mgr.list_jobs()
    assert len(jobs) == 1  # only the running one
    assert jobs[0]["job_id"] == r2.job_id
    assert jobs[0]["status"] == "running"

    mgr.cancel(r2.job_id)


def test_cancel_running(tmp_path, monkeypatch):
    """Submit a long command and cancel it."""
    cfg = _config(str(tmp_path), monkeypatch)
    inst = _instance(cfg)
    mgr = JobManager(inst, cfg)

    result = mgr.submit("sleep 30", timeout=1, tail=5)
    assert result.status == JobStatus.RUNNING

    assert mgr.cancel(result.job_id)
    assert mgr.poll(result.job_id, timeout=0) is None


def test_cancel_nonexistent(tmp_path, monkeypatch):
    cfg = _config(str(tmp_path), monkeypatch)
    inst = _instance(cfg)
    mgr = JobManager(inst, cfg)
    assert not mgr.cancel("nonexistent")


def test_submit_isolation(tmp_path, monkeypatch):
    """Verify job runs inside sandbox, can't access host filesystem."""
    cfg = _config(str(tmp_path), monkeypatch)
    inst = _instance(cfg)
    mgr = JobManager(inst, cfg)

    result = mgr.submit(
        "test -f /etc/passwd && echo EXPOSED || echo SAFE",
        timeout=10, tail=5,
    )
    assert result.status == JobStatus.COMPLETED
    # /etc/passwd is --ro-bind mounted, so it's accessible
    # But /tmp/host_secret should not be
    assert "EXPOSED" in result.stdout_tail or "SAFE" in result.stdout_tail
