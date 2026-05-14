"""Tests for the instance lifecycle module."""

import json
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
