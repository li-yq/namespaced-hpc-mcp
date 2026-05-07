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


def test_audit_start_finish():
    cfg = _config()
    inst = Instance.create("test-003", cfg)
    stdout_path, stderr_path = inst.audit_start("task-1", "echo hello")
    assert stdout_path.parent == inst.output_dir
    assert stdout_path.name == "task-1.out"
    assert stderr_path.name == "task-1.err"
    inst.audit_finish("task-1", 0)

    log = inst.audit_log_path.read_text()
    lines = log.strip().split("\n")
    assert len(lines) == 2
    start = json.loads(lines[0])
    finish = json.loads(lines[1])
    assert start["event"] == "start"
    assert start["task_id"] == "task-1"
    assert start["command"] == "echo hello"
    assert finish["event"] == "finish"
    assert finish["task_id"] == "task-1"
    assert finish["exit_code"] == 0


def test_legacy_write_audit():
    cfg = _config()
    inst = Instance.create("test-004", cfg)
    inst.write_audit("ls -la", {"exit_code": 0, "stdout": "file1", "stderr": ""})
    log = inst.audit_log_path.read_text()
    lines = log.strip().split("\n")
    assert len(lines) == 2  # start + finish


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
