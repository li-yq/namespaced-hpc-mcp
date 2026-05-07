"""Tests for the instance lifecycle module."""

import json
import os
import tempfile
from pathlib import Path

from ns_hpc.instance import Instance
from ns_hpc.config import Config, NamespaceDefaults, ResourceDefaults


def _config(tmp_dir: str | None = None) -> Config:
    return Config(
        namespace_defaults=NamespaceDefaults(
            bind_ro=["/usr", "/bin"],
            workspace_mount="/workspace",
            flags=["--unshare-all", "--share-net"],
        ),
        proxied_mcps={},
        resource_defaults=ResourceDefaults(),
        instances_dir=tmp_dir or tempfile.mkdtemp(),
    )


def test_create_instance():
    cfg = _config()
    inst = Instance.create("test-001", cfg)
    assert inst.exists
    assert inst.workspace_dir.exists()
    assert inst.output_dir.exists()
    assert inst.metadata_path.exists()
    metadata = json.loads(inst.metadata_path.read_text())
    assert metadata["id"] == "test-001"
    assert "created_at" in metadata


def test_create_duplicate():
    cfg = _config()
    Instance.create("test-dup", cfg)
    import pytest
    with pytest.raises(FileExistsError):
        Instance.create("test-dup", cfg)


def test_load_instance():
    cfg = _config()
    Instance.create("test-002", cfg)
    inst = Instance.load("test-002", cfg)
    assert inst is not None
    assert inst.id == "test-002"


def test_load_nonexistent():
    cfg = _config()
    inst = Instance.load("does-not-exist", cfg)
    assert inst is None


def test_audit():
    cfg = _config()
    inst = Instance.create("test-003", cfg)
    task_id = inst.audit("echo hello", 0, stdout="hello\n", stderr="")
    assert task_id is not None
    assert len(task_id) == 12
    # Output files created
    out_file = inst.output_dir / f"{task_id}.out"
    err_file = inst.output_dir / f"{task_id}.err"
    assert out_file.read_text() == "hello\n"
    assert err_file.read_text() == ""
    # Audit log written
    log = inst.audit_log_path.read_text()
    entry = json.loads(log.strip())
    assert entry["task_id"] == task_id
    assert entry["command"] == "echo hello"
    assert entry["exit_code"] == 0
    assert entry["stdout_len"] == 6


def test_legacy_write_audit():
    cfg = _config()
    inst = Instance.create("test-004", cfg)
    inst.write_audit("ls -la", {"exit_code": 0, "stdout": "file1", "stderr": ""})
    log = inst.audit_log_path.read_text()
    lines = log.strip().split("\n")
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["command"] == "ls -la"
    assert entry["exit_code"] == 0


def test_destroy_instance():
    cfg = _config()
    inst = Instance.create("test-005", cfg)
    assert inst.exists
    assert Instance.destroy("test-005", cfg)
    assert not inst.exists


def test_destroy_nonexistent():
    cfg = _config()
    assert not Instance.destroy("does-not-exist", cfg)


def test_list_instances():
    cfg = _config()
    Instance.create("list-a", cfg)
    Instance.create("list-b", cfg)
    instances = Instance.list_instances(cfg)
    ids = [i.id for i in instances]
    assert "list-a" in ids
    assert "list-b" in ids
