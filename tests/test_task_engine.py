import shutil
import tempfile
import time

from ns_hpc.config import Config, NamespaceDefaults, ResourceDefaults, SlurmConfig
from ns_hpc.instance import Instance
from ns_hpc.task_engine import (
    LocalTaskEngine,
    SlurmTaskEngine,
    TaskStatus,
)


def _config(tmp_dir: str, partition: str | None = None) -> Config:
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
        slurm=SlurmConfig(partition=partition) if partition else SlurmConfig(),
        instances_dir=tmp_dir,
    )


# ---------------------------------------------------------------------------
# LocalTaskEngine tests
# ---------------------------------------------------------------------------


def test_local_submit_and_status():
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir)
        inst = Instance.create("test-local-status", cfg)
        engine = LocalTaskEngine(cfg, inst)

        handle = engine.submit("echo hello")

        assert handle.status == TaskStatus.RUNNING
        assert handle.pid is not None
        assert handle.mode == "local"

        # Poll until completed
        for _ in range(20):
            handle = engine.get_status(handle.id)
            if handle.status is not TaskStatus.RUNNING:
                break
            time.sleep(0.1)

        assert handle.status == TaskStatus.COMPLETED, f"got {handle.status}"
        assert handle.exit_code == 0
        assert "hello" in handle.stdout


def test_local_cancel():
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir)
        inst = Instance.create("test-local-cancel", cfg)
        engine = LocalTaskEngine(cfg, inst)

        handle = engine.submit("sleep 30")

        assert handle.status == TaskStatus.RUNNING

        ok = engine.cancel(handle.id)
        assert ok

        handle = engine.get_status(handle.id)
        assert handle.status == TaskStatus.CANCELLED


def test_local_list_tasks():
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir)
        inst = Instance.create("test-local-list", cfg)
        engine = LocalTaskEngine(cfg, inst)

        h1 = engine.submit("echo one")
        h2 = engine.submit("echo two")

        tasks = engine.list_tasks()
        assert len(tasks) == 2
        ids = {t.id for t in tasks}
        assert h1.id in ids
        assert h2.id in ids


# ---------------------------------------------------------------------------
# SlurmTaskEngine tests
# ---------------------------------------------------------------------------


def test_slurm_available():
    sbatch_path = shutil.which("sbatch")
    if sbatch_path is None:
        # Not running on a Slurm cluster; verify the property is False
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = _config(tmp_dir)
            inst = Instance.create("test-slurm-avail", cfg)
            engine = SlurmTaskEngine(cfg, inst)
            assert not engine.available
    else:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = _config(tmp_dir)
            inst = Instance.create("test-slurm-avail", cfg)
            engine = SlurmTaskEngine(cfg, inst)
            assert engine.available


def test_slurm_not_available_error():
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir)
        inst = Instance.create("test-slurm-error", cfg)
        engine = SlurmTaskEngine(cfg, inst)

        if not engine.available:
            import pytest
            with pytest.raises(RuntimeError, match="Slurm is not available"):
                engine.submit("echo hello")
        else:
            # Slurm is available — just verify submit works
            # Use partition "cpu" which is common on test clusters
            handle = engine.submit("echo hello", timeout=60, partition="cpu")
            assert handle.slurm_job_id is not None
            # Clean up
            engine.cancel(handle.id)


def test_slurm_submit_and_poll():
    """Submit a simple job via Slurm and poll until completion.

    Stdout may be empty when the sbatch --output file lives on a
    different container (the worker) and is invisible from the caller.
    We verify job completion and exit code instead.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir, partition="cpu")
        inst = Instance.create("test-slurm-poll", cfg)
        engine = SlurmTaskEngine(cfg, inst)

        handle = engine.submit("echo hello_slurm", timeout=120, partition="cpu")
        assert handle.slurm_job_id is not None
        assert handle.status == TaskStatus.RUNNING

        # Poll until completed (should be fast for echo).
        # UNKNOWN is transient — sacct may not have recorded the job yet.
        terminal = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}
        for _ in range(30):
            handle = engine.get_status(handle.id)
            if handle.status in terminal:
                break
            time.sleep(1)

        assert handle.status == TaskStatus.COMPLETED, f"got {handle.status}"
        assert handle.exit_code == 0
