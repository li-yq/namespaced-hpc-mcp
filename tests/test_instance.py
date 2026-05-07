import json
import tempfile
from pathlib import Path

from ns_hpc.config import Config, NamespaceDefaults, ResourceDefaults
from ns_hpc.instance import Instance


def _config(tmp_dir: str) -> Config:
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
        instances_dir=tmp_dir,
    )


def test_create_instance():
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir)
        inst = Instance.create("test-create", cfg)

        assert inst.id == "test-create"
        assert inst.base_dir.exists()
        assert inst.workspace_dir.exists()
        assert inst.metadata_path.exists()

        meta = json.loads(inst.metadata_path.read_text())
        assert meta["id"] == "test-create"
        assert "created_at" in meta
        assert "workspace" in meta
        assert "hostname" in meta


def test_load_instance():
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir)
        created = Instance.create("test-load", cfg)
        assert created.exists

        loaded = Instance.load("test-load", cfg)
        assert loaded is not None
        assert loaded.id == "test-load"
        assert loaded.base_dir == created.base_dir
        assert loaded.metadata_path.exists()

        # Loading a nonexistent instance returns None
        missing = Instance.load("does-not-exist", cfg)
        assert missing is None


def test_audit_log():
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir)
        inst = Instance.create("test-audit", cfg)

        inst.write_audit("echo hello", {"exit_code": 0, "stdout": "hello\n", "stderr": ""})
        inst.write_audit("exit 1", {"exit_code": 1, "stdout": "", "stderr": "error"})

        lines = inst.audit_log_path.read_text().strip().splitlines()
        assert len(lines) == 2

        first = json.loads(lines[0])
        assert first["command"] == "echo hello"
        assert first["exit_code"] == 0
        assert first["stdout_len"] == 6
        assert first["stderr_len"] == 0
        assert "timestamp" in first

        second = json.loads(lines[1])
        assert second["command"] == "exit 1"
        assert second["exit_code"] == 1
        assert second["stdout_len"] == 0
        assert second["stderr_len"] == 5


def test_destroy_instance():
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir)
        Instance.create("test-destroy", cfg)
        assert (Path(tmp_dir) / "test-destroy").exists()

        result = Instance.destroy("test-destroy", cfg)
        assert result
        assert not (Path(tmp_dir) / "test-destroy").exists()

        # Destroying nonexistent returns False
        result = Instance.destroy("test-destroy", cfg)
        assert not result


def test_list_instances():
    with tempfile.TemporaryDirectory() as tmp_dir:
        cfg = _config(tmp_dir)

        Instance.create("alpha", cfg)
        Instance.create("beta", cfg)

        instances = Instance.list_instances(cfg)
        ids = sorted(i.id for i in instances)
        assert ids == ["alpha", "beta"]

        # Directory with no instances dir exists returns empty list
        cfg2 = _config(tempfile.mkdtemp())
        assert Instance.list_instances(cfg2) == []
