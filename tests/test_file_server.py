"""Tests for ns_hpc.file_server — SandboxDavProvider and path validation."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

from ns_hpc.config import (
    Config,
    DavConfig,
    DavExtraMount,
    JobLocal,
    JobSlurm,
    JobSlurmResource,
    JobsConfig,
    Namespace,
    ResourceConfig,
)
from ns_hpc.file_server import SandboxDavProvider, _validate_within_root


def _make_config(instances_dir: Path, *, dav_enabled: bool = True, extras: dict | None = None) -> Config:
    return Config(
        namespace=Namespace(
            instances_dir=str(instances_dir),
            bwrap_command=["bwrap"],
            workspace_mount="/workspace",
            output_mount="/output",
            shared_output_mount="/shared-output",
        ),
        jobs=JobsConfig(
            local=JobLocal(cgroups_command=["echo"]),
            slurm=JobSlurm(
                sbatch_command=["echo"],
                limit={"cpus": JobSlurmResource(default=1, max=4)},
            ),
        ),
        resource=ResourceConfig(),
        dav=DavConfig(enabled=dav_enabled, extras=extras or {}),
        proxied_mcps={},
    )


def _make_provider(cfg: Config) -> SandboxDavProvider:
    return SandboxDavProvider(cfg)


def _make_environ(provider: SandboxDavProvider, method: str = "GET") -> dict:
    return {"REQUEST_METHOD": method, "wsgidav.provider": provider}


# ── _validate_within_root ─────────────────────────────────────────────────


def test_validate_inside_root(tmp_path):
    (tmp_path / "sub").mkdir()
    _validate_within_root(tmp_path / "sub" / "foo.txt", tmp_path)  # should not raise


def test_validate_symlink_escape(tmp_path):
    (tmp_path / "sub").mkdir()
    os.symlink("/etc/passwd", str(tmp_path / "sub" / "escape"))
    with pytest.raises(RuntimeError, match="resolves outside root"):
        _validate_within_root(tmp_path / "sub" / "escape", tmp_path)


def test_validate_dotdot(tmp_path):
    (tmp_path / "sub").mkdir()
    # sub/.. stays inside root; need to go above tmp_path to actually escape
    with pytest.raises(RuntimeError, match="resolves outside root"):
        _validate_within_root(tmp_path / "../outside.txt", tmp_path)


def test_validate_nonexistent_file_inside_root(tmp_path):
    """Non-existent files inside root should validate (PUT scenario)."""
    (tmp_path / "sub").mkdir()
    _validate_within_root(tmp_path / "sub" / "new.txt", tmp_path)


def test_validate_nonexistent_file_outside_root(tmp_path):
    """Non-existent files outside root should raise."""
    (tmp_path / "sub").mkdir()
    with pytest.raises(RuntimeError):
        _validate_within_root(tmp_path / ".." / "outside.txt", tmp_path)


def test_validate_target_is_root(tmp_path):
    _validate_within_root(tmp_path, tmp_path)


# ── SandboxDavProvider._resolve ────────────────────────────────────────────


def test_resolve_workspace_file(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    inst_dir = instances_dir / "my-inst"
    (inst_dir / "workspace").mkdir(parents=True)
    (inst_dir / "metadata.json").write_text(json.dumps({"id": "my-inst", "created_at": "2024-01-01"}))

    (inst_dir / "workspace" / "hello.txt").write_text("hi")

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    host_path, readonly = prov._resolve("/instances/my-inst/workspace/hello.txt", environ)
    assert host_path == inst_dir / "workspace" / "hello.txt"
    assert readonly is False


def test_resolve_output_file(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output" / "my-inst").mkdir(parents=True)
    inst_dir = instances_dir / "my-inst"
    (inst_dir / "workspace").mkdir(parents=True)
    (inst_dir / "metadata.json").write_text(json.dumps({"id": "my-inst", "created_at": "2024-01-01"}))

    (instances_dir / "output" / "my-inst" / "result.txt").write_text("done")

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    host_path, readonly = prov._resolve("/instances/my-inst/output/result.txt", environ)
    assert host_path == instances_dir / "output" / "my-inst" / "result.txt"
    assert readonly is False


def test_resolve_nonexistent_instance(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    assert prov._resolve("/instances/nope/workspace/file.txt", environ) is None


def test_resolve_archived_instance(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    inst_dir = instances_dir / "archived-inst"
    (inst_dir / "workspace").mkdir(parents=True)
    (inst_dir / "metadata.json").write_text(
        json.dumps({"id": "archived-inst", "created_at": "2024-01-01", "archived": True})
    )

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    assert prov._resolve("/instances/archived-inst/workspace/file.txt", environ) is None


def test_resolve_bare_instances(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    assert prov._resolve("/instances/", environ) is None
    assert prov._resolve("/instances", environ) is None


def test_resolve_extra_mount(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)

    extra_dir = tmp_path / "extra-data"
    extra_dir.mkdir()
    (extra_dir / "data.csv").write_text("a,b,c")

    extras = {"data": DavExtraMount(path=str(extra_dir), ro=True)}
    cfg = _make_config(instances_dir, extras=extras)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    host_path, readonly = prov._resolve("/data/data.csv", environ)
    assert host_path == extra_dir / "data.csv"
    assert readonly is True


def test_resolve_unknown_extra_mount(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    assert prov._resolve("/unknown/data.csv", environ) is None


# ── get_resource_inst ─────────────────────────────────────────────────────


def test_get_resource_inst_file(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    inst_dir = instances_dir / "my-inst"
    (inst_dir / "workspace").mkdir(parents=True)
    (inst_dir / "metadata.json").write_text(json.dumps({"id": "my-inst", "created_at": "2024-01-01"}))
    (inst_dir / "workspace" / "readme.md").write_text("# Hello")

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    resource = prov.get_resource_inst("/instances/my-inst/workspace/readme.md", environ)
    assert resource is not None
    from wsgidav.fs_dav_provider import FileResource
    assert isinstance(resource, FileResource)


def test_get_resource_inst_directory(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    inst_dir = instances_dir / "my-inst"
    (inst_dir / "workspace").mkdir(parents=True)
    (inst_dir / "metadata.json").write_text(json.dumps({"id": "my-inst", "created_at": "2024-01-01"}))

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    resource = prov.get_resource_inst("/instances/my-inst/workspace/", environ)
    assert resource is not None
    from wsgidav.fs_dav_provider import FolderResource
    assert isinstance(resource, FolderResource)


def test_get_resource_inst_not_found(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    inst_dir = instances_dir / "my-inst"
    (inst_dir / "workspace").mkdir(parents=True)
    (inst_dir / "metadata.json").write_text(json.dumps({"id": "my-inst", "created_at": "2024-01-01"}))

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    # unresolved path
    assert prov.get_resource_inst("/instances/nope/workspace/f", environ) is None
    # resolved but file doesn't exist
    assert prov.get_resource_inst("/instances/my-inst/workspace/nonexistent.txt", environ) is None


def test_get_resource_inst_readonly_mount_blocks_write(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)

    extra_dir = tmp_path / "extra-data"
    extra_dir.mkdir()
    (extra_dir / "protected.txt").write_text("keep")

    extras = {"protected": DavExtraMount(path=str(extra_dir), ro=True)}
    cfg = _make_config(instances_dir, extras=extras)
    prov = _make_provider(cfg)
    environ = _make_environ(prov, method="PUT")

    from wsgidav.dav_error import DAVError
    with pytest.raises(DAVError, match="read-only"):
        prov.get_resource_inst("/protected/protected.txt", environ)


def test_get_resource_inst_symlink_escape_blocked(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    inst_dir = instances_dir / "my-inst"
    (inst_dir / "workspace").mkdir(parents=True)
    (inst_dir / "metadata.json").write_text(json.dumps({"id": "my-inst", "created_at": "2024-01-01"}))
    os.symlink("/etc/passwd", str(inst_dir / "workspace" / "leak"))

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    with pytest.raises(RuntimeError, match="resolves outside root"):
        prov.get_resource_inst("/instances/my-inst/workspace/leak", environ)


def test_get_resource_inst_dotdot_blocked(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    inst_dir = instances_dir / "my-inst"
    (inst_dir / "workspace").mkdir(parents=True)
    (inst_dir / "metadata.json").write_text(json.dumps({"id": "my-inst", "created_at": "2024-01-01"}))

    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    environ = _make_environ(prov)

    with pytest.raises(RuntimeError, match="resolves outside root"):
        prov.get_resource_inst("/instances/my-inst/workspace/../metadata.json", environ)


# ── Config integration ────────────────────────────────────────────────────


def test_config_default_dav_disabled():
    cfg = _make_config(Path("/nonexistent"), dav_enabled=False)
    assert cfg.dav.enabled is False
    assert cfg.dav.extras == {}


def test_config_dav_with_extras():
    extras = {"data": DavExtraMount(path="/tmp/data", ro=True)}
    cfg = _make_config(Path("/tmp"), dav_enabled=True, extras=extras)
    assert cfg.dav.enabled is True
    assert cfg.dav.extras["data"].path == "/tmp/data"
    assert cfg.dav.extras["data"].ro is True
