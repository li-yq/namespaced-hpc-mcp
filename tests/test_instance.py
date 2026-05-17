"""Tests for the instance lifecycle module."""

import json
import os
from pathlib import Path

from ns_hpc.instance import Instance
from ns_hpc.config import Config, NamespaceDefaults, ResourceDefaults


def _config(tmp_dir: str) -> Config:
    return Config(
        namespace_defaults=NamespaceDefaults(
            bind_ro=["/usr", "/bin"],
            workspace_mount="/workspace",
            flags=["--unshare-all", "--share-net"],
        ),
        proxied_mcps={},
        resource_defaults=ResourceDefaults(),
        instances_dir=tmp_dir,
    )


def test_create_instance(tmp_path):
    cfg = _config(str(tmp_path))
    inst = Instance.create("test-001", cfg)
    assert inst.exists
    assert inst.workspace_dir.exists()
    assert inst.output_path.exists()
    assert inst.metadata_path.exists()
    metadata = json.loads(inst.metadata_path.read_text())
    assert metadata["id"] == "test-001"
    assert "created_at" in metadata
    # Shared output directory structure
    shared_root = cfg.resolve_instances_dir() / "output"
    assert shared_root.exists()
    assert (shared_root / "test-001").exists()


def test_create_duplicate(tmp_path):
    cfg = _config(str(tmp_path))
    Instance.create("test-dup", cfg)
    import pytest
    with pytest.raises(FileExistsError):
        Instance.create("test-dup", cfg)


def test_create_invalid_instance_id(tmp_path):
    cfg = _config(str(tmp_path))
    import pytest
    with pytest.raises(ValueError, match="Invalid instance_id"):
        Instance.create("../evil", cfg)


def test_load_instance(tmp_path):
    cfg = _config(str(tmp_path))
    Instance.create("test-002", cfg)
    inst = Instance.load("test-002", cfg)
    assert inst is not None
    assert inst.id == "test-002"


def test_load_nonexistent(tmp_path):
    cfg = _config(str(tmp_path))
    inst = Instance.load("does-not-exist", cfg)
    assert inst is None


def test_audit(tmp_path):
    """audit() appends a JSONL line with event type and extra fields."""
    cfg = _config(str(tmp_path))
    inst = Instance.create("test-003", cfg)
    inst.audit("job.completed", job_id="abc123", exit_code=0, command="echo hello")
    inst.audit("job.failed", job_id="def456", exit_code=1, command="false")

    lines = inst.audit_log_path.read_text().strip().splitlines()
    assert len(lines) == 2

    e1 = json.loads(lines[0])
    assert e1["event"] == "job.completed"
    assert e1["job_id"] == "abc123"
    assert e1["exit_code"] == 0
    assert "timestamp" in e1

    e2 = json.loads(lines[1])
    assert e2["event"] == "job.failed"
    assert e2["job_id"] == "def456"
    assert e2["exit_code"] == 1


def test_audit_arbitrary_fields(tmp_path):
    """audit() accepts arbitrary keyword arguments."""
    cfg = _config(str(tmp_path))
    inst = Instance.create("test-007", cfg)

    inst.audit("custom.event", foo="bar", count=42, nested={"key": "val"})
    line = inst.audit_log_path.read_text().strip()
    entry = json.loads(line)

    assert entry["event"] == "custom.event"
    assert entry["foo"] == "bar"
    assert entry["count"] == 42
    assert entry["nested"] == {"key": "val"}


def test_destroy_instance(tmp_path):
    cfg = _config(str(tmp_path))
    inst = Instance.create("test-005", cfg)
    assert inst.exists
    assert Instance.destroy("test-005", cfg)
    assert not inst.exists


def test_destroy_nonexistent(tmp_path):
    cfg = _config(str(tmp_path))
    assert not Instance.destroy("does-not-exist", cfg)


def test_list_instances(tmp_path):
    cfg = _config(str(tmp_path))
    Instance.create("list-a", cfg)
    Instance.create("list-b", cfg)
    instances = Instance.list_instances(cfg)
    ids = [i.id for i in instances]
    assert "list-a" in ids
    assert "list-b" in ids


def test_get_set_description(tmp_path):
    """get_description returns empty string when unset; set_description updates it."""
    cfg = _config(str(tmp_path))
    inst = Instance.create("test-desc", cfg)

    # Default is empty
    assert inst.get_description() == ""

    # Set and verify
    inst.set_description("my test instance")
    assert inst.get_description() == "my test instance"

    # Update
    inst.set_description("updated description")
    assert inst.get_description() == "updated description"

    # Re-load from disk and verify persistence
    loaded = Instance.load("test-desc", cfg)
    assert loaded is not None
    assert loaded.get_description() == "updated description"


# ── Archive tests ──────────────────────────────────────────────────────────


def test_archive_instance(tmp_path):
    """Archive marks metadata and moves directory to .archived/<id>__<ts>/."""
    cfg = _config(str(tmp_path))
    inst = Instance.create("test-arch", cfg)
    assert inst.exists

    result = inst.archive(cfg)
    assert result is True

    # Directory moved away from original path
    assert not inst.base_dir.exists()

    # Metadata has archived fields
    assert inst.is_archived()

    # Dir exists under .archived/ with timestamp suffix
    archived_root = cfg.resolve_instances_dir() / ".archived"
    assert archived_root.exists()
    entries = sorted(archived_root.iterdir())
    assert len(entries) == 1
    child = entries[0]
    assert child.name.startswith("test-arch__")
    assert child.is_dir()
    assert (child / "metadata.json").exists()


def test_archive_twice(tmp_path):
    """Second archive call returns False."""
    cfg = _config(str(tmp_path))
    inst = Instance.create("test-twice", cfg)
    assert inst.archive(cfg) is True
    assert inst.archive(cfg) is False


def test_archive_instance_not_found_by_load(tmp_path):
    """After archive, Instance.load() returns None."""
    cfg = _config(str(tmp_path))
    Instance.create("test-gone", cfg)
    Instance.load("test-gone", cfg).archive(cfg)
    assert Instance.load("test-gone", cfg) is None


def test_archive_not_listed(tmp_path):
    """Archived instance does not appear in list_instances()."""
    cfg = _config(str(tmp_path))
    Instance.create("arch-visible", cfg)
    Instance.create("arch-hidden", cfg).archive(cfg)

    ids = [i.id for i in Instance.list_instances(cfg)]
    assert "arch-visible" in ids
    assert "arch-hidden" not in ids


def test_archive_then_recreate_same_name(tmp_path):
    """After archiving, a new instance can be created with the same name."""
    cfg = _config(str(tmp_path))
    inst = Instance.create("test-reuse", cfg)
    inst.archive(cfg)

    # Should succeed — original dir was moved
    inst2 = Instance.create("test-reuse", cfg)
    assert inst2.exists
    assert inst2.id == "test-reuse"


def test_list_archived_instances(tmp_path):
    """list_archived_instances() returns all archived instances."""
    cfg = _config(str(tmp_path))
    Instance.create("arch-a", cfg).archive(cfg)
    Instance.create("arch-b", cfg).archive(cfg)

    archived = Instance.list_archived_instances(cfg)
    assert len(archived) == 2
    ids = [e["instance_id"] for e in archived]
    assert "arch-a" in ids
    assert "arch-b" in ids


def test_list_archived_empty(tmp_path):
    """list_archived_instances() returns [] when none exist."""
    cfg = _config(str(tmp_path))
    assert Instance.list_archived_instances(cfg) == []


def test_archive_duplicate_name_no_clash(tmp_path):
    """Archiving the same name twice creates two separate timestamped dirs."""
    cfg = _config(str(tmp_path))

    # First instance
    inst1 = Instance.create("test-clash", cfg)
    inst1.archive(cfg)

    # Second instance (same name, fresh creation)
    inst2 = Instance.create("test-clash", cfg)
    inst2.archive(cfg)

    # Both archived dirs exist with different timestamps
    archived_root = cfg.resolve_instances_dir() / ".archived"
    entries = sorted(archived_root.iterdir())
    assert len(entries) == 2
    for child in entries:
        assert child.name.startswith("test-clash__")
    assert entries[0].name != entries[1].name


def test_archive_shared_output_moved(tmp_path):
    """Archiving moves the shared output directory too."""
    cfg = _config(str(tmp_path))
    inst = Instance.create("test-output", cfg)

    # Verify shared output dir exists before archive
    shared_root = cfg.resolve_instances_dir() / "output"
    output_before = shared_root / "test-output"
    assert output_before.is_dir()

    inst.archive(cfg)

    # Original output dir is gone
    assert not output_before.exists()

    # Output dir exists under timestamped name
    output_entries = sorted(shared_root.iterdir())
    assert len(output_entries) == 1
    assert output_entries[0].name.startswith("test-output__")


def test_archive_recreate_shared_output(tmp_path):
    """Creating a new instance with same name creates a fresh shared output dir."""
    cfg = _config(str(tmp_path))
    Instance.create("test-reout", cfg).archive(cfg)

    # New instance with same name
    inst2 = Instance.create("test-reout", cfg)
    shared_root = cfg.resolve_instances_dir() / "output"
    assert (shared_root / "test-reout").is_dir()

    # Archive again — both new and old output dirs coexist
    inst2.archive(cfg)
    entries = sorted(shared_root.iterdir())
    assert len(entries) == 2
    assert all(e.name.startswith("test-reout__") for e in entries)


import shutil

import pytest

from ns_hpc.job_manager import JobStatus

_skip_no_bwrap = pytest.mark.skipif(
    not shutil.which("bwrap"), reason="bwrap not available"
)


@_skip_no_bwrap
@pytest.mark.asyncio
async def test_archive_with_running_job_blocked(tmp_path, monkeypatch):
    """Archiving an instance with running jobs raises RuntimeError."""
    from ns_hpc.job_manager import JobManager
    from ns_hpc.config import load_config

    config_path = Path(tmp_path) / "config.toml"
    config_path.write_text(f"""
instances_dir = "{tmp_path}"

[namespace_defaults]
bind_ro = ["/usr", "/bin", "/lib", "/lib64"]
workspace_mount = "/workspace"
flags = ["--unshare-all", "--share-net", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]

[resources]
use_systemd = false

[proxied_mcps]

[resource_defaults]
context_dirs = ["config/context"]
resource_patterns = ["*.md"]
""")
    monkeypatch.setenv("NS_HPC_CONFIG", str(config_path))
    cfg = load_config(str(config_path))

    inst = Instance.create("test-block", cfg)
    mgr = JobManager(inst, cfg)
    result = await mgr.submit("sleep 30", timeout=1, tail=5)
    assert result.status == JobStatus.RUNNING

    with pytest.raises(RuntimeError, match="still running"):
        inst.archive(cfg)

    # Cancel so cleanup doesn't leave a runaway
    await mgr.cancel(result.job_id)

    # Now archive should succeed
    assert inst.archive(cfg) is True
