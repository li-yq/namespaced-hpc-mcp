"""Tests for ns_hpc.file_server."""
from __future__ import annotations

import io as _io
import json
import os
from pathlib import Path

import pytest

from ns_hpc.config import (
    Config, DavConfig, DavExtraMount, JobLocal, JobSlurm,
    JobSlurmResource, JobsConfig, Namespace, ResourceConfig,
)
from ns_hpc.file_server import SandboxDavProvider, _validate_within_root


def _make_config(instances_dir, *, dav_enabled=True, extras=None):
    return Config(
        namespace=Namespace(
            instances_dir=str(instances_dir), bwrap_command=["bwrap"],
            workspace_mount="/workspace", output_mount="/output",
            shared_output_mount="/shared-output",
        ),
        jobs=JobsConfig(
            local=JobLocal(cgroups_command=["echo"]),
            slurm=JobSlurm(sbatch_command=["echo"],
                limit={"cpus": JobSlurmResource(default=1, max=4)}),
        ),
        resource=ResourceConfig(),
        dav=DavConfig(enabled=dav_enabled, extras=extras or {}),
        proxied_mcps={},
    )


def _make_provider(cfg):
    return SandboxDavProvider(cfg)


def _make_environ(provider, method="GET"):
    return {"REQUEST_METHOD": method, "wsgidav.provider": provider}


def _setup_instance(instances_dir, instance_id="my-inst"):
    (instances_dir / "output" / instance_id).mkdir(parents=True, exist_ok=True)
    inst_dir = instances_dir / instance_id
    (inst_dir / "workspace").mkdir(parents=True)
    (inst_dir / "metadata.json").write_text(
        json.dumps({"id": instance_id, "created_at": "2024-01-01T00:00:00Z"})
    )
    return inst_dir


def _make_dav_app(instances_dir, **dav_kw):
    from wsgidav.wsgidav_app import WsgiDAVApp, DEFAULT_CONFIG
    extras = dav_kw.pop("extras", None)
    cfg = _make_config(instances_dir, **dav_kw, extras=extras)
    provider = SandboxDavProvider(cfg)
    dav_cfg = dict(DEFAULT_CONFIG)
    dav_cfg.update({
        "provider_mapping": {"/": provider},
        "simple_dc": {"user_mapping": {"*": True}},
        "verbose": 1,
        "mount_path": "/dav",
    })
    return WsgiDAVApp(dav_cfg)


def _wsgi_call(app, method, path, body=None):
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "SCRIPT_NAME": "/dav",
        "SERVER_NAME": "localhost",
        "SERVER_PORT": "80",
        "wsgi.url_scheme": "http",
        "wsgi.input": _io.BytesIO(body or b""),
        "wsgi.errors": _io.StringIO(),
    }
    status = []
    headers = []
    def start_response(s, hdrs, exc_info=None):
        status.append(s)
        headers.extend(hdrs)
    body_bytes = b"".join(app(environ, start_response))
    status_code = int(status[0].split()[0]) if status else 500
    return status_code, headers, body_bytes


# -- _validate_within_root

def test_validate_inside_root(tmp_path):
    (tmp_path / "sub").mkdir()
    _validate_within_root(tmp_path / "sub" / "foo.txt", tmp_path)

def test_validate_symlink_escape(tmp_path):
    (tmp_path / "sub").mkdir()
    os.symlink("/etc/passwd", str(tmp_path / "sub" / "escape"))
    with pytest.raises(RuntimeError, match="resolves outside root"):
        _validate_within_root(tmp_path / "sub" / "escape", tmp_path)

def test_validate_dotdot(tmp_path):
    (tmp_path / "sub").mkdir()
    with pytest.raises(RuntimeError, match="resolves outside root"):
        _validate_within_root(tmp_path / "../outside.txt", tmp_path)

def test_validate_nonexistent_file_inside_root(tmp_path):
    (tmp_path / "sub").mkdir()
    _validate_within_root(tmp_path / "sub" / "new.txt", tmp_path)

def test_validate_nonexistent_file_outside_root(tmp_path):
    (tmp_path / "sub").mkdir()
    with pytest.raises(RuntimeError):
        _validate_within_root(tmp_path / "../outside.txt", tmp_path)

def test_validate_target_is_root(tmp_path):
    _validate_within_root(tmp_path, tmp_path)


# -- SandboxDavProvider._resolve

def test_resolve_workspace_file(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    (instances_dir / "my-inst" / "workspace" / "hello.txt").write_text("hi")
    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    host_path, readonly = prov._resolve("/instances/my-inst/workspace/hello.txt", _make_environ(prov))
    assert host_path == instances_dir / "my-inst" / "workspace" / "hello.txt"
    assert readonly is False

def test_resolve_output_file(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    (instances_dir / "output" / "my-inst" / "result.txt").write_text("done")
    cfg = _make_config(instances_dir)
    prov = _make_provider(cfg)
    host_path, readonly = prov._resolve("/instances/my-inst/output/result.txt", _make_environ(prov))
    assert host_path == instances_dir / "output" / "my-inst" / "result.txt"
    assert readonly is False

def test_resolve_nonexistent_instance(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    prov = _make_provider(_make_config(instances_dir))
    assert prov._resolve("/instances/nope/workspace/file.txt", _make_environ(prov)) is None

def test_resolve_archived_instance(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir, "archived-inst")
    meta = json.loads((instances_dir / "archived-inst" / "metadata.json").read_text())
    meta["archived"] = True
    (instances_dir / "archived-inst" / "metadata.json").write_text(json.dumps(meta))
    prov = _make_provider(_make_config(instances_dir))
    assert prov._resolve("/instances/archived-inst/workspace/file.txt", _make_environ(prov)) is None

def test_resolve_bare_instances(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    prov = _make_provider(_make_config(instances_dir))
    environ = _make_environ(prov)
    assert prov._resolve("/instances/", environ) is None
    assert prov._resolve("/instances", environ) is None

def test_resolve_extra_mount(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    extra_dir = tmp_path / "extra-data"
    extra_dir.mkdir()
    (extra_dir / "data.csv").write_text("a,b,c")
    cfg = _make_config(instances_dir, extras={"data": DavExtraMount(path=str(extra_dir), ro=True)})
    prov = _make_provider(cfg)
    host_path, readonly = prov._resolve("/data/data.csv", _make_environ(prov))
    assert host_path == extra_dir / "data.csv"
    assert readonly is True

def test_resolve_unknown_extra_mount(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    prov = _make_provider(_make_config(instances_dir))
    assert prov._resolve("/unknown/data.csv", _make_environ(prov)) is None


# -- get_resource_inst

def test_get_resource_inst_file(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    (instances_dir / "my-inst" / "workspace" / "readme.md").write_text("# Hello")
    prov = _make_provider(_make_config(instances_dir))
    resource = prov.get_resource_inst("/instances/my-inst/workspace/readme.md", _make_environ(prov))
    assert resource is not None
    from wsgidav.fs_dav_provider import FileResource
    assert isinstance(resource, FileResource)

def test_get_resource_inst_directory(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    prov = _make_provider(_make_config(instances_dir))
    resource = prov.get_resource_inst("/instances/my-inst/workspace/", _make_environ(prov))
    assert resource is not None
    from wsgidav.fs_dav_provider import FolderResource
    assert isinstance(resource, FolderResource)

def test_get_resource_inst_not_found(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    prov = _make_provider(_make_config(instances_dir))
    assert prov.get_resource_inst("/instances/nope/workspace/f", _make_environ(prov)) is None
    assert prov.get_resource_inst("/instances/my-inst/workspace/nonexistent.txt", _make_environ(prov)) is None

def test_get_resource_inst_readonly_mount_blocks_write(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    extra_dir = tmp_path / "extra-data"
    extra_dir.mkdir()
    (extra_dir / "protected.txt").write_text("keep")
    cfg = _make_config(instances_dir, extras={"protected": DavExtraMount(path=str(extra_dir), ro=True)})
    prov = _make_provider(cfg)
    from wsgidav.dav_error import DAVError
    with pytest.raises(DAVError, match="read-only"):
        prov.get_resource_inst("/protected/protected.txt", _make_environ(prov, method="PUT"))

def test_get_resource_inst_symlink_escape_blocked(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    os.symlink("/etc/passwd", str(instances_dir / "my-inst" / "workspace" / "leak"))
    prov = _make_provider(_make_config(instances_dir))
    with pytest.raises(RuntimeError, match="resolves outside root"):
        prov.get_resource_inst("/instances/my-inst/workspace/leak", _make_environ(prov))

def test_get_resource_inst_dotdot_blocked(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    prov = _make_provider(_make_config(instances_dir))
    with pytest.raises(RuntimeError, match="resolves outside root"):
        prov.get_resource_inst("/instances/my-inst/workspace/../metadata.json", _make_environ(prov))


# -- Config

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


# -- WebDAV WSGI integration

def test_dav_get_file(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    (instances_dir / "my-inst" / "workspace" / "greeting.txt").write_text("hello world")
    app = _make_dav_app(instances_dir)
    code, _, body = _wsgi_call(app, "GET", "/instances/my-inst/workspace/greeting.txt")
    assert code == 200
    assert body.decode() == "hello world"

def test_dav_put_and_delete_file(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    app = _make_dav_app(instances_dir)
    code, _, _ = _wsgi_call(app, "PUT", "/instances/my-inst/workspace/new.txt", b"created")
    assert code in (200, 201, 204)
    code, _, body = _wsgi_call(app, "GET", "/instances/my-inst/workspace/new.txt")
    assert code == 200
    assert body.decode() == "created"
    code, _, _ = _wsgi_call(app, "DELETE", "/instances/my-inst/workspace/new.txt")
    assert code in (200, 204)
    code, _, _ = _wsgi_call(app, "GET", "/instances/my-inst/workspace/new.txt")
    assert code == 404

def test_dav_put_audits_write(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    app = _make_dav_app(instances_dir)
    audit_path = instances_dir / "my-inst" / "audit.log"
    assert not audit_path.exists()
    _wsgi_call(app, "PUT", "/instances/my-inst/workspace/data.bin", b"binary")
    assert audit_path.exists()
    events = [json.loads(line) for line in audit_path.read_text().strip().split("\n") if line]
    write_events = [e for e in events if e["event"] == "dav.write"]
    assert len(write_events) >= 1
    assert write_events[0]["method"] == "PUT"

def test_dav_archived_instance_404(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir, "archived-inst")
    (instances_dir / "archived-inst" / "workspace" / "secret.txt").write_text("secret")
    meta = json.loads((instances_dir / "archived-inst" / "metadata.json").read_text())
    meta["archived"] = True
    (instances_dir / "archived-inst" / "metadata.json").write_text(json.dumps(meta))
    app = _make_dav_app(instances_dir)
    code, _, _ = _wsgi_call(app, "GET", "/instances/archived-inst/workspace/secret.txt")
    assert code == 404

def test_dav_readonly_extra_mount_rejects_write(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    extra_dir = tmp_path / "readonly-data"
    extra_dir.mkdir()
    (extra_dir / "locked.txt").write_text("immutable")
    app = _make_dav_app(instances_dir, extras={"readonly": DavExtraMount(path=str(extra_dir), ro=True)})
    code, _, _ = _wsgi_call(app, "PUT", "/readonly/locked.txt", b"hacked")
    assert code == 405

def test_dav_mkcol_creates_directory(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    app = _make_dav_app(instances_dir)
    code, _, _ = _wsgi_call(app, "MKCOL", "/instances/my-inst/workspace/subdir")
    assert code in (200, 201)
    assert (instances_dir / "my-inst" / "workspace" / "subdir").is_dir()
