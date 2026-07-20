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


def test_provider_rejects_missing_extra_mount_at_start(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    missing = tmp_path / "missing-data"
    cfg = _make_config(
        instances_dir,
        extras={"external-data": DavExtraMount(path=str(missing), ro=True)},
    )

    with pytest.raises(RuntimeError, match="external-data.*does not exist"):
        _make_provider(cfg)


def test_provider_rejects_extra_mount_that_is_not_directory(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    extra_file = tmp_path / "data.txt"
    extra_file.write_text("not a directory")
    cfg = _make_config(
        instances_dir,
        extras={"external-data": DavExtraMount(path=str(extra_file), ro=True)},
    )

    with pytest.raises(RuntimeError, match="external-data.*not a directory"):
        _make_provider(cfg)


def test_provider_rejects_unreadable_extra_mount_at_start(tmp_path, monkeypatch):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    extra_dir = tmp_path / "extra-data"
    extra_dir.mkdir()
    cfg = _make_config(
        instances_dir,
        extras={"external-data": DavExtraMount(path=str(extra_dir), ro=True)},
    )
    real_access = os.access
    monkeypatch.setattr(
        os,
        "access",
        lambda path, mode: False if Path(path) == extra_dir else real_access(path, mode),
    )

    with pytest.raises(RuntimeError, match="external-data.*not readable or traversable"):
        _make_provider(cfg)

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


def test_provider_has_wsgidav_filesystem_compat_options(tmp_path):
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    prov = _make_provider(_make_config(instances_dir))
    assert prov.fs_opts["follow_symlinks"] is False
    assert prov.shadow_map == {}


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


def test_dav_get_directory_browser_listing(tmp_path):
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    (instances_dir / "my-inst" / "workspace" / "hello.txt").write_text("hello world")
    app = _make_dav_app(instances_dir)
    code, _, body = _wsgi_call(app, "GET", "/instances/my-inst/workspace/")
    assert code == 200
    assert "hello.txt" in body.decode()


# -- WebDAV end-to-end via Starlette Mount + WsgiToAsgi (simulates production) --


def _make_starlette_app(instances_dir, **dav_kw):
    """Create a Starlette app with DAV mounted at /dav, matching production setup."""
    import copy
    from wsgidav.wsgidav_app import WsgiDAVApp, DEFAULT_CONFIG
    from starlette.applications import Starlette
    from starlette.routing import Mount
    from ns_hpc.file_server import PooledWSGIApp

    extras = dav_kw.pop("extras", None)
    cfg = _make_config(instances_dir, **dav_kw, extras=extras)
    provider = SandboxDavProvider(cfg)
    dav_cfg = copy.deepcopy(DEFAULT_CONFIG)
    dav_cfg.update({
        "provider_mapping": {"/": provider},
        "simple_dc": {"user_mapping": {"*": True}},
        "verbose": 1,
        "dir_browser": {"enable": True},
        "http_authenticator": {
            "domain_controller": None,
            "accept_basic": False,
            "accept_digest": False,
        },
        "mount_path": "/dav",
    })
    dav_app = WsgiDAVApp(dav_cfg)
    asgi_app = PooledWSGIApp(
        dav_app,
        max_workers=10,
        spool_dir=instances_dir.parent,
        min_spool_free_bytes=0,
    )
    routes = [Mount("/dav", app=asgi_app, name="dav")]
    return Starlette(routes=routes)


@pytest.fixture
def starlette_client(tmp_path):
    """Create a Starlette TestClient with DAV mounted at /dav."""
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    (instances_dir / "my-inst" / "workspace" / "hello.txt").write_text("hello world")
    (instances_dir / "my-inst" / "workspace" / "subdir").mkdir()
    (instances_dir / "output" / "my-inst" / "result.log").write_text("done")
    app = _make_starlette_app(instances_dir)
    return TestClient(app, raise_server_exceptions=False), instances_dir


def test_starlette_dav_get_file(starlette_client):
    client, instances_dir = starlette_client
    resp = client.get("/dav/instances/my-inst/workspace/hello.txt")
    assert resp.status_code == 200
    assert resp.content == b"hello world"


def test_starlette_dav_get_nonexistent(starlette_client):
    client, instances_dir = starlette_client
    resp = client.get("/dav/instances/my-inst/workspace/nope.txt")
    assert resp.status_code == 404


def test_starlette_dav_put_and_get(starlette_client):
    client, instances_dir = starlette_client
    resp = client.put("/dav/instances/my-inst/workspace/new.txt", content=b"fresh")
    assert resp.status_code in (200, 201, 204)
    resp = client.get("/dav/instances/my-inst/workspace/new.txt")
    assert resp.status_code == 200
    assert resp.content == b"fresh"


def test_starlette_dav_delete(starlette_client):
    client, instances_dir = starlette_client
    client.put("/dav/instances/my-inst/workspace/todelete.txt", content=b"x")
    resp = client.delete("/dav/instances/my-inst/workspace/todelete.txt")
    assert resp.status_code in (200, 204)
    resp = client.get("/dav/instances/my-inst/workspace/todelete.txt")
    assert resp.status_code == 404


def test_starlette_dav_mkcol(starlette_client):
    client, instances_dir = starlette_client
    resp = client.request("MKCOL", "/dav/instances/my-inst/workspace/newdir")
    assert resp.status_code in (200, 201)
    assert (instances_dir / "my-inst" / "workspace" / "newdir").is_dir()


def test_starlette_dav_propfind_directory(starlette_client):
    client, instances_dir = starlette_client
    resp = client.request("PROPFIND", "/dav/instances/my-inst/workspace/")
    assert resp.status_code == 207
    body = resp.content.decode()
    assert "hello.txt" in body


def test_starlette_dav_propfind_file(starlette_client):
    client, instances_dir = starlette_client
    resp = client.request("PROPFIND", "/dav/instances/my-inst/workspace/hello.txt")
    assert resp.status_code == 207
    body = resp.content.decode()
    assert "hello.txt" in body


def test_starlette_dav_options(starlette_client):
    client, instances_dir = starlette_client
    resp = client.options("/dav/instances/my-inst/workspace/")
    assert resp.status_code == 200
    # wsgidav returns dav: 1,2 and ms-author-via: DAV headers
    assert resp.headers.get("dav", "") == "1,2"
    assert resp.headers.get("ms-author-via", "") == "DAV"


def test_starlette_dav_nonexistent_instance(starlette_client):
    client, instances_dir = starlette_client
    resp = client.get("/dav/instances/nope/workspace/file.txt")
    assert resp.status_code == 404


def test_starlette_dav_archived_instance(tmp_path):
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir, "archived-inst")
    (instances_dir / "archived-inst" / "workspace" / "x.txt").write_text("x")
    meta = json.loads((instances_dir / "archived-inst" / "metadata.json").read_text())
    meta["archived"] = True
    (instances_dir / "archived-inst" / "metadata.json").write_text(json.dumps(meta))
    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/dav/instances/archived-inst/workspace/x.txt")
    assert resp.status_code == 404


def test_starlette_dav_output_file(starlette_client):
    client, instances_dir = starlette_client
    resp = client.get("/dav/instances/my-inst/output/result.log")
    assert resp.status_code == 200
    assert resp.content == b"done"


def test_starlette_dav_output_put(starlette_client):
    client, instances_dir = starlette_client
    resp = client.put("/dav/instances/my-inst/output/out.csv", content=b"a,b")
    assert resp.status_code in (200, 201, 204)
    assert (instances_dir / "output" / "my-inst" / "out.csv").read_text() == "a,b"


def test_starlette_dav_readonly_extra_mount(tmp_path):
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    extra_dir = tmp_path / "readonly"
    extra_dir.mkdir()
    (extra_dir / "locked.txt").write_text("secret")
    app = _make_starlette_app(instances_dir,
                              extras={"readonly": DavExtraMount(path=str(extra_dir), ro=True)})
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/dav/readonly/locked.txt")
    assert resp.status_code == 200
    resp = client.put("/dav/readonly/locked.txt", content=b"hack")
    assert resp.status_code == 405


def test_starlette_dav_symlink_escape_blocked(tmp_path):
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    os.symlink("/etc/passwd", str(instances_dir / "my-inst" / "workspace" / "leak"))
    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/dav/instances/my-inst/workspace/leak")
    assert resp.status_code == 500  # RuntimeError from validation


def test_starlette_dav_dotdot_blocked(tmp_path):
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)
    # ASGI layer normalizes ../ before it reaches WSGI; gets 404 (no match)
    # or 500 if it reaches the provider's security check
    resp = client.get("/dav/instances/my-inst/workspace/../metadata.json")
    assert resp.status_code in (404, 500)


def test_starlette_dav_head_file(starlette_client):
    client, instances_dir = starlette_client
    resp = client.head("/dav/instances/my-inst/workspace/hello.txt")
    assert resp.status_code == 200
    assert int(resp.headers.get("content-length", "0")) == len("hello world")


def test_starlette_dav_binary_roundtrip(starlette_client):
    client, instances_dir = starlette_client
    binary_data = bytes(range(256))
    resp = client.put("/dav/instances/my-inst/workspace/binary.bin", content=binary_data)
    assert resp.status_code in (200, 201, 204)
    resp = client.get("/dav/instances/my-inst/workspace/binary.bin")
    assert resp.status_code == 200
    assert resp.content == binary_data


def test_starlette_dav_empty_file(starlette_client):
    client, instances_dir = starlette_client
    resp = client.put("/dav/instances/my-inst/workspace/empty.txt", content=b"")
    assert resp.status_code in (200, 201, 204)
    resp = client.get("/dav/instances/my-inst/workspace/empty.txt")
    assert resp.status_code == 200
    assert resp.content == b""


def test_starlette_dav_large_file(starlette_client):
    client, instances_dir = starlette_client
    large_data = b"x" * 1_000_000
    resp = client.put("/dav/instances/my-inst/workspace/large.bin", content=large_data)
    assert resp.status_code in (200, 201, 204)
    resp = client.get("/dav/instances/my-inst/workspace/large.bin")
    assert resp.status_code == 200
    assert len(resp.content) == 1_000_000


def test_starlette_dav_overwrite_file(starlette_client):
    client, instances_dir = starlette_client
    client.put("/dav/instances/my-inst/workspace/overwrite.txt", content=b"v1")
    resp = client.put("/dav/instances/my-inst/workspace/overwrite.txt", content=b"v2")
    assert resp.status_code in (200, 201, 204)
    resp = client.get("/dav/instances/my-inst/workspace/overwrite.txt")
    assert resp.content == b"v2"


def test_starlette_dav_directory_listing_empty(starlette_client):
    client, instances_dir = starlette_client
    empty_dir = instances_dir / "my-inst" / "workspace" / "empty-dir"
    empty_dir.mkdir()
    resp = client.request("PROPFIND", "/dav/instances/my-inst/workspace/empty-dir/")
    assert resp.status_code == 207


def test_starlette_dav_instance_dir_listing(starlette_client):
    client, instances_dir = starlette_client
    resp = client.request("PROPFIND", "/dav/instances/my-inst/workspace/")
    assert resp.status_code == 207
    body = resp.content.decode()
    assert "subdir" in body


def test_starlette_dav_parent_dir_traversal(starlette_client):
    client, instances_dir = starlette_client
    resp = client.get("/dav/instances/my-inst/workspace/subdir/../../hello.txt")
    # ASGI layer normalizes the path; traversal is blocked either way
    assert resp.status_code in (404, 500)


def test_starlette_dav_put_creates_dirs(tmp_path):
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.put("/dav/instances/my-inst/workspace/a/b/c/file.txt", content=b"deep")
    # wsgidav doesn't auto-create intermediate dirs; expect 404 or 409
    assert resp.status_code in (200, 201, 204, 404, 409)


def test_starlette_dav_utf8_filename(starlette_client):
    client, instances_dir = starlette_client
    resp = client.put("/dav/instances/my-inst/workspace/%E4%B8%AD%E6%96%87.txt",
                      content=b"unicode")
    assert resp.status_code in (200, 201, 204)
    resp = client.get("/dav/instances/my-inst/workspace/%E4%B8%AD%E6%96%87.txt")
    assert resp.status_code == 200
    assert resp.content == b"unicode"


def test_starlette_dav_special_chars_filename(starlette_client):
    client, instances_dir = starlette_client
    name = "file (1) - copy.txt"
    from urllib.parse import quote
    url = "/dav/instances/my-inst/workspace/" + quote(name)
    resp = client.put(url, content=b"special")
    assert resp.status_code in (200, 201, 204)
    resp = client.get(url)
    assert resp.status_code == 200
    assert resp.content == b"special"


def test_starlette_dav_audit_log_on_write(starlette_client):
    client, instances_dir = starlette_client
    audit_path = instances_dir / "my-inst" / "audit.log"
    resp = client.put("/dav/instances/my-inst/workspace/audited.txt", content=b"data")
    assert resp.status_code in (200, 201, 204)
    assert audit_path.exists()
    events = [json.loads(line) for line in audit_path.read_text().strip().split("\n") if line]
    write_events = [e for e in events if e["event"] == "dav.write"]
    assert len(write_events) >= 1
    assert write_events[0]["method"] == "PUT"


# -- Config-driven DAV gating --


def test_config_dav_disabled_by_default():
    cfg = _make_config(Path("/tmp"), dav_enabled=False)
    assert cfg.dav.enabled is False


def test_config_dav_enabled_explicitly():
    cfg = _make_config(Path("/tmp"), dav_enabled=True)
    assert cfg.dav.enabled is True


# -- Virtual directory listings --


def test_starlette_dav_root_listing(tmp_path):
    """PROPFIND on /dav/ lists instances/ and extra mounts."""
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    extra_dir = tmp_path / "data"
    extra_dir.mkdir()
    extras = {"data": DavExtraMount(path=str(extra_dir), ro=True)}
    app = _make_starlette_app(instances_dir, extras=extras)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.request("PROPFIND", "/dav/")
    assert resp.status_code == 207
    body = resp.content.decode()
    assert "instances" in body
    assert "data" in body


def test_starlette_dav_instances_listing(tmp_path):
    """PROPFIND on /dav/instances/ lists active instances."""
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir, "inst-a")
    _setup_instance(instances_dir, "inst-b")
    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.request("PROPFIND", "/dav/instances/")
    assert resp.status_code == 207
    body = resp.content.decode()
    assert "inst-a" in body
    assert "inst-b" in body


def test_starlette_dav_instances_omits_archived(tmp_path):
    """Archived instances should not appear in /dav/instances/ listing."""
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir, "active-inst")
    _setup_instance(instances_dir, "archived-inst")
    meta = json.loads((instances_dir / "archived-inst" / "metadata.json").read_text())
    meta["archived"] = True
    (instances_dir / "archived-inst" / "metadata.json").write_text(json.dumps(meta))
    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.request("PROPFIND", "/dav/instances/")
    assert resp.status_code == 207
    body = resp.content.decode()
    assert "active-inst" in body
    assert "archived-inst" not in body


def test_starlette_dav_instance_mount_listing(tmp_path):
    """PROPFIND on /dav/instances/{id}/ lists workspace and output."""
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    (instances_dir / "my-inst" / "workspace" / "f.txt").write_text("x")
    (instances_dir / "output" / "my-inst").mkdir(parents=True, exist_ok=True)
    (instances_dir / "output" / "my-inst" / "out.log").write_text("done")
    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.request("PROPFIND", "/dav/instances/my-inst/")
    assert resp.status_code == 207
    body = resp.content.decode()
    assert "workspace" in body
    assert "output" in body


def test_starlette_dav_instance_mount_nonexistent(tmp_path):
    """GET on /dav/instances/nope/ returns 404."""
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    (instances_dir / "output").mkdir(parents=True)
    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/dav/instances/nope/")
    assert resp.status_code == 404


def test_starlette_dav_instance_mount_archived(tmp_path):
    """Archived instance mount should return 404."""
    from starlette.testclient import TestClient
    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir, "archived-inst")
    meta = json.loads((instances_dir / "archived-inst" / "metadata.json").read_text())
    meta["archived"] = True
    (instances_dir / "archived-inst" / "metadata.json").write_text(json.dumps(meta))
    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/dav/instances/archived-inst/")
    assert resp.status_code == 404


def test_build_environ_basic():
    """Verify _build_environ produces a correct WSGI environ."""
    from ns_hpc.file_server import _build_environ

    scope: dict[str, Any] = {
        "type": "http",
        "method": "PROPFIND",
        "path": "/instances/my-inst/workspace/",
        "root_path": "/dav",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "client": ("127.0.0.1", 54321),
        "headers": [
            (b"host", b"localhost:8000"),
            (b"depth", b"1"),
        ],
    }
    env = _build_environ(scope, b"")
    assert env["REQUEST_METHOD"] == "PROPFIND"
    assert env["SCRIPT_NAME"] == "/dav"
    assert env["PATH_INFO"] == "/instances/my-inst/workspace/"
    assert env["HTTP_HOST"] == "localhost:8000"
    assert env["HTTP_DEPTH"] == "1"
    assert env["wsgi.multithread"] is True


def test_build_environ_no_root_path():
    """Verify SCRIPT_NAME is empty when scope has no root_path."""
    from ns_hpc.file_server import _build_environ

    scope: dict[str, Any] = {
        "type": "http",
        "method": "GET",
        "path": "/file.txt",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [],
    }
    env = _build_environ(scope, b"")
    assert env["SCRIPT_NAME"] == ""
    assert env["PATH_INFO"] == "/file.txt"


def test_build_environ_content_headers():
    """Verify content-type and content-length are mapped to WSGI keys."""
    from ns_hpc.file_server import _build_environ

    scope: dict[str, Any] = {
        "type": "http",
        "method": "PUT",
        "path": "/file.txt",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [
            (b"content-type", b"text/plain"),
            (b"content-length", b"5"),
        ],
    }
    env = _build_environ(scope, b"hello")
    assert env["CONTENT_TYPE"] == "text/plain"
    assert env["CONTENT_LENGTH"] == "5"
    assert env["wsgi.input"].read() == b"hello"


def test_pooled_wsgi_app_depth_defaults_to_one(tmp_path):
    """Verify PooledWSGIApp defaults PROPFIND Depth to 1."""
    from ns_hpc.file_server import PooledWSGIApp
    from starlette.testclient import TestClient
    from starlette.applications import Starlette
    from starlette.routing import Mount

    instances_dir = tmp_path / "instances"
    _setup_instance(instances_dir)
    (instances_dir / "my-inst" / "workspace" / "a.txt").write_text("a")
    (instances_dir / "my-inst" / "workspace" / "sub" / "b.txt").parent.mkdir(exist_ok=True)
    (instances_dir / "my-inst" / "workspace" / "sub" / "b.txt").write_text("b")

    app = _make_starlette_app(instances_dir)
    client = TestClient(app, raise_server_exceptions=False)

    # PROPFIND without Depth header should NOT return sub/b.txt (depth=1 cap)
    resp = client.request("PROPFIND", "/dav/instances/my-inst/workspace/")
    assert resp.status_code == 207
    body = resp.content.decode()
    assert "a.txt" in body
    # subdir entry itself is listed as an immediate child
    assert "sub" in body
    # nested b.txt should NOT appear (depth=1, not infinity)
    assert "sub/b.txt" not in body


@pytest.mark.asyncio
async def test_pooled_wsgi_app_get_response():
    """Verify PooledWSGIApp returns correct content for a GET."""
    from ns_hpc.file_server import PooledWSGIApp
    from wsgidav.wsgidav_app import WsgiDAVApp, DEFAULT_CONFIG

    instances_dir = Path(tmp_path := __import__("tempfile").mkdtemp())
    _setup_instance(instances_dir)
    (instances_dir / "my-inst" / "workspace" / "test.txt").write_text("hello pool")

    cfg = _make_config(instances_dir)
    provider = SandboxDavProvider(cfg)
    dav_cfg = dict(DEFAULT_CONFIG)
    dav_cfg.update({
        "provider_mapping": {"/": provider},
        "simple_dc": {"user_mapping": {"*": True}},
        "verbose": 1,
        "mount_path": None,
    })
    wsgi_app = WsgiDAVApp(dav_cfg)
    asgi_app = PooledWSGIApp(
        wsgi_app,
        max_workers=2,
        spool_dir=instances_dir,
        min_spool_free_bytes=0,
    )

    # Use Starlette TestClient for the ASGI interface
    from starlette.testclient import TestClient
    from starlette.applications import Starlette
    from starlette.routing import Mount
    app = Starlette(routes=[Mount("/dav", app=asgi_app, name="dav")])
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get("/dav/instances/my-inst/workspace/test.txt")
    assert resp.status_code == 200
    assert resp.content == b"hello pool"

    # Cleanup temp dir
    import shutil
    shutil.rmtree(tmp_path)


@pytest.mark.asyncio
async def test_pooled_wsgi_app_streams_each_response_chunk_immediately(tmp_path):
    """The first WSGI chunk reaches ASGI before the iterator finishes."""
    import asyncio
    import threading

    from ns_hpc.file_server import PooledWSGIApp

    allow_second_chunk = threading.Event()
    first_chunk_sent = asyncio.Event()
    sent_messages = []
    released_before_first_chunk = False

    def wsgi_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "application/octet-stream")])
        yield b"first"
        assert allow_second_chunk.wait(timeout=2)
        yield b"second"

    request_delivered = False

    async def receive():
        nonlocal request_delivered
        if not request_delivered:
            request_delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return await asyncio.Future()

    async def send(message):
        sent_messages.append(message)
        if message["type"] == "http.response.body" and message.get("body") == b"first":
            first_chunk_sent.set()

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/file.bin",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [],
    }
    app = PooledWSGIApp(wsgi_app, max_workers=1, spool_dir=tmp_path, min_spool_free_bytes=0)
    task = asyncio.create_task(app(scope, receive, send))

    try:
        await asyncio.wait_for(first_chunk_sent.wait(), timeout=1.0)
    except TimeoutError:
        released_before_first_chunk = True
    finally:
        allow_second_chunk.set()
        await task

    assert released_before_first_chunk is False
    bodies = [m.get("body") for m in sent_messages if m["type"] == "http.response.body"]
    assert bodies == [b"first", b"second", b""]


@pytest.mark.asyncio
async def test_pooled_wsgi_app_applies_backpressure_before_reading_next_chunk(tmp_path):
    """A blocked ASGI send prevents the WSGI iterator advancing."""
    import asyncio
    import threading

    from ns_hpc.file_server import PooledWSGIApp

    first_send_started = asyncio.Event()
    release_first_send = asyncio.Event()
    second_chunk_requested = threading.Event()

    def wsgi_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "application/octet-stream")])
        yield b"first"
        second_chunk_requested.set()
        yield b"second"

    request_delivered = False

    async def receive():
        nonlocal request_delivered
        if not request_delivered:
            request_delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return await asyncio.Future()

    async def send(message):
        if message["type"] == "http.response.body" and message.get("body") == b"first":
            first_send_started.set()
            await release_first_send.wait()

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/file.bin",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [],
    }
    app = PooledWSGIApp(
        wsgi_app,
        max_workers=1,
        spool_dir=tmp_path,
        min_spool_free_bytes=0,
    )
    task = asyncio.create_task(app(scope, receive, send))
    await asyncio.wait_for(first_send_started.wait(), timeout=1)

    assert second_chunk_requested.is_set() is False
    release_first_send.set()
    await task
    assert second_chunk_requested.is_set() is True


@pytest.mark.asyncio
async def test_pooled_wsgi_app_spools_large_request_body_to_configured_disk(tmp_path):
    """Large request bodies use the configured disk-backed WSGI input."""
    import asyncio

    from ns_hpc.file_server import PooledWSGIApp

    chunk = b"x" * (32 * 1024)
    chunks = [chunk, chunk, chunk]
    observed = {}

    def wsgi_app(environ, start_response):
        stream = environ["wsgi.input"]
        observed["rolled"] = getattr(stream, "_rolled", False)
        observed["fd_path"] = os.readlink(f"/proc/self/fd/{stream.fileno()}")
        observed["body"] = stream.read()
        start_response("204 No Content", [("Content-Length", "0")])
        return []

    async def receive():
        if not chunks:
            return await asyncio.Future()
        body = chunks.pop(0)
        return {
            "type": "http.request",
            "body": body,
            "more_body": bool(chunks),
        }

    sent_messages = []

    async def send(message):
        sent_messages.append(message)

    scope = {
        "type": "http",
        "method": "PUT",
        "path": "/upload.bin",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [(b"content-length", str(3 * len(chunk)).encode())],
    }
    app = PooledWSGIApp(wsgi_app, max_workers=1, spool_dir=tmp_path, min_spool_free_bytes=0)
    await app(scope, receive, send)

    assert observed["rolled"] is True
    assert observed["fd_path"].startswith(str(tmp_path))
    assert observed["body"] == chunk * 3
    assert sent_messages[-1] == {"type": "http.response.body", "body": b""}
    assert app._reserved_spool_bytes == 0


@pytest.mark.asyncio
async def test_pooled_wsgi_app_stops_when_client_disconnects_during_upload(tmp_path):
    """A disconnected upload must not invoke the WSGI application."""
    from ns_hpc.file_server import PooledWSGIApp

    wsgi_called = False
    sent_messages = []

    def wsgi_app(environ, start_response):
        nonlocal wsgi_called
        wsgi_called = True
        start_response("200 OK", [])
        return [b"unexpected"]

    async def receive():
        return {"type": "http.disconnect"}

    async def send(message):
        sent_messages.append(message)

    scope = {
        "type": "http",
        "method": "PUT",
        "path": "/upload.bin",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [],
    }
    app = PooledWSGIApp(wsgi_app, max_workers=1, spool_dir=tmp_path, min_spool_free_bytes=0)
    await app(scope, receive, send)

    assert wsgi_called is False
    assert sent_messages == []


def test_pooled_wsgi_app_requires_explicit_spool_directory():
    """The bridge must never silently fall back to tmpfs for large uploads."""
    from ns_hpc.file_server import PooledWSGIApp

    with pytest.raises(TypeError):
        PooledWSGIApp(lambda environ, start_response: [])


def test_pooled_wsgi_app_rejects_missing_spool_directory(tmp_path):
    """Invalid spool storage fails at startup instead of during an upload."""
    from ns_hpc.file_server import PooledWSGIApp

    with pytest.raises(RuntimeError, match="spool directory does not exist"):
        PooledWSGIApp(
            lambda environ, start_response: [],
            spool_dir=tmp_path / "missing",
        )


def test_pooled_wsgi_app_rejects_symlink_spool_directory(tmp_path):
    """The spool path cannot redirect uploads outside managed storage."""
    from ns_hpc.file_server import PooledWSGIApp

    target = tmp_path / "target"
    target.mkdir()
    spool_link = tmp_path / "spool-link"
    spool_link.symlink_to(target, target_is_directory=True)

    with pytest.raises(RuntimeError, match="must not be a symbolic link"):
        PooledWSGIApp(
            lambda environ, start_response: [],
            spool_dir=spool_link,
        )


def test_pooled_wsgi_app_invalid_worker_count_does_not_leak_spool_fd(tmp_path):
    """Constructor validation happens before retaining a spool directory fd."""
    before = set(os.listdir("/proc/self/fd"))

    from ns_hpc.file_server import PooledWSGIApp

    with pytest.raises(ValueError, match="max_workers must be positive"):
        PooledWSGIApp(
            lambda environ, start_response: [],
            max_workers=0,
            max_inflight_requests=1,
            spool_dir=tmp_path,
        )

    assert set(os.listdir("/proc/self/fd")) == before


def test_pooled_wsgi_app_executor_failure_closes_spool_fd(tmp_path, monkeypatch):
    """A post-open constructor failure closes the retained directory descriptor."""
    import ns_hpc.file_server as file_server

    before = set(os.listdir("/proc/self/fd"))

    def fail_executor(*args, **kwargs):
        raise RuntimeError("executor unavailable")

    monkeypatch.setattr(file_server, "ThreadPoolExecutor", fail_executor)
    with pytest.raises(RuntimeError, match="executor unavailable"):
        file_server.PooledWSGIApp(
            lambda environ, start_response: [],
            max_workers=1,
            spool_dir=tmp_path,
        )

    assert set(os.listdir("/proc/self/fd")) == before


@pytest.mark.asyncio
async def test_pooled_wsgi_app_preserves_spool_free_space(tmp_path, monkeypatch):
    """Uploads are rejected before spool plus destination copies fill storage."""
    import types

    from ns_hpc.file_server import PooledWSGIApp

    monkeypatch.setattr(
        "ns_hpc.file_server.shutil.disk_usage",
        lambda path: types.SimpleNamespace(free=119),
    )
    wsgi_called = False

    def wsgi_app(environ, start_response):
        nonlocal wsgi_called
        wsgi_called = True
        start_response("204 No Content", [])
        return []

    request_delivered = False

    async def receive():
        nonlocal request_delivered
        assert not request_delivered
        request_delivered = True
        return {"type": "http.request", "body": b"0123456789", "more_body": False}

    messages = []

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "method": "PUT",
        "path": "/upload.bin",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [(b"content-length", b"10")],
    }
    app = PooledWSGIApp(
        wsgi_app,
        max_workers=1,
        spool_dir=tmp_path,
        min_spool_free_bytes=100,
    )
    await app(scope, receive, send)

    assert wsgi_called is False
    assert messages[0]["status"] == 507
    assert app._reserved_spool_bytes == 0


@pytest.mark.asyncio
async def test_pooled_wsgi_app_closes_iterator_when_client_send_fails(tmp_path):
    """A failed ASGI send closes the WSGI iterator and its file resources."""
    import asyncio
    import threading

    from ns_hpc.file_server import PooledWSGIApp

    iterator_closed = threading.Event()

    def wsgi_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "application/octet-stream")])
        try:
            yield b"first"
            yield b"must-not-be-read"
        finally:
            iterator_closed.set()

    request_delivered = False

    async def receive():
        nonlocal request_delivered
        if not request_delivered:
            request_delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return await asyncio.Future()

    async def send(message):
        if message["type"] == "http.response.body" and message.get("body"):
            raise ConnectionError("client disconnected")

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/file.bin",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [],
    }
    app = PooledWSGIApp(wsgi_app, max_workers=1, spool_dir=tmp_path, min_spool_free_bytes=0)

    with pytest.raises(ConnectionError, match="client disconnected"):
        await app(scope, receive, send)

    assert iterator_closed.is_set()


@pytest.mark.asyncio
async def test_pooled_wsgi_app_reraises_exc_info_after_response_started(tmp_path):
    """WSGI cannot replace headers after the first response chunk was sent."""
    import asyncio
    import sys

    from ns_hpc.file_server import PooledWSGIApp

    def wsgi_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        yield b"started"
        try:
            raise ValueError("late WSGI failure")
        except ValueError:
            start_response("500 Internal Server Error", [], sys.exc_info())

    request_delivered = False

    async def receive():
        nonlocal request_delivered
        if not request_delivered:
            request_delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return await asyncio.Future()

    async def send(message):
        return None

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/file.txt",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [],
    }
    app = PooledWSGIApp(wsgi_app, max_workers=1, spool_dir=tmp_path, min_spool_free_bytes=0)

    with pytest.raises(ValueError, match="late WSGI failure"):
        await app(scope, receive, send)


@pytest.mark.asyncio
async def test_pooled_wsgi_app_supports_wsgi_write_callable(tmp_path):
    """The start_response return value implements PEP 3333 write()."""
    import asyncio

    from ns_hpc.file_server import PooledWSGIApp

    def wsgi_app(environ, start_response):
        write = start_response("200 OK", [("Content-Type", "text/plain")])
        write(b"written")
        return [b"yielded"]

    request_delivered = False

    async def receive():
        nonlocal request_delivered
        if not request_delivered:
            request_delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return await asyncio.Future()

    messages = []

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/write-style",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [],
    }
    app = PooledWSGIApp(wsgi_app, max_workers=1, spool_dir=tmp_path, min_spool_free_bytes=0)
    await app(scope, receive, send)

    bodies = [m.get("body") for m in messages if m["type"] == "http.response.body"]
    assert bodies == [b"written", b"yielded", b""]


@pytest.mark.asyncio
async def test_pooled_wsgi_app_cancellation_stops_worker_before_next_chunk(tmp_path):
    """Cancelling the ASGI task waits for the WSGI iterator to close."""
    import asyncio
    import threading

    from ns_hpc.file_server import PooledWSGIApp

    first_sent = asyncio.Event()
    release_second = threading.Event()
    iterator_closed = threading.Event()
    bodies = []

    def wsgi_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "application/octet-stream")])
        try:
            yield b"first"
            assert release_second.wait(timeout=2)
            yield b"second"
        finally:
            iterator_closed.set()

    request_delivered = False

    async def receive():
        nonlocal request_delivered
        if not request_delivered:
            request_delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return await asyncio.Future()

    async def send(message):
        if message["type"] == "http.response.body" and message.get("body"):
            bodies.append(message["body"])
            if message["body"] == b"first":
                first_sent.set()

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/cancel",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [],
    }
    app = PooledWSGIApp(wsgi_app, max_workers=1, spool_dir=tmp_path, min_spool_free_bytes=0)
    task = asyncio.create_task(app(scope, receive, send))
    await asyncio.wait_for(first_sent.wait(), timeout=1)

    task.cancel()
    await asyncio.sleep(0)
    release_second.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await asyncio.to_thread(iterator_closed.wait, 1)
    assert bodies == [b"first"]


@pytest.mark.asyncio
async def test_pooled_wsgi_app_stops_on_response_disconnect_without_send_error(tmp_path):
    """An http.disconnect stops iteration even when ASGI send does not fail."""
    import asyncio
    import threading

    from ns_hpc.file_server import PooledWSGIApp

    first_sent = asyncio.Event()
    disconnect_allowed = asyncio.Event()
    disconnect_consumed = asyncio.Event()
    release_second = threading.Event()
    bodies = []
    receive_count = 0

    def wsgi_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "application/octet-stream")])
        yield b"first"
        assert release_second.wait(timeout=2)
        yield b"second"

    async def receive():
        nonlocal receive_count
        receive_count += 1
        if receive_count == 1:
            return {"type": "http.request", "body": b"", "more_body": False}
        await disconnect_allowed.wait()
        disconnect_consumed.set()
        return {"type": "http.disconnect"}

    async def send(message):
        if message["type"] == "http.response.body" and message.get("body"):
            bodies.append(message["body"])
            if message["body"] == b"first":
                first_sent.set()

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/disconnect",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [],
    }
    app = PooledWSGIApp(wsgi_app, max_workers=1, spool_dir=tmp_path, min_spool_free_bytes=0)
    task = asyncio.create_task(app(scope, receive, send))
    await asyncio.wait_for(first_sent.wait(), timeout=1)

    disconnect_allowed.set()
    disconnect_was_observed = True
    try:
        await asyncio.wait_for(disconnect_consumed.wait(), timeout=0.2)
    except TimeoutError:
        disconnect_was_observed = False
    finally:
        release_second.set()
        await task

    assert disconnect_was_observed is True
    assert bodies == [b"first"]


@pytest.mark.asyncio
async def test_pooled_wsgi_app_disconnect_interrupts_wsgi_input_reads(tmp_path):
    """Disconnects stop WSGI upload processing before a response starts."""
    import asyncio
    import threading

    from ns_hpc.file_server import PooledWSGIApp

    first_read = threading.Event()
    release_second_read = threading.Event()
    disconnect_allowed = asyncio.Event()
    disconnect_consumed = asyncio.Event()
    second_read_happened = False
    receive_count = 0

    def wsgi_app(environ, start_response):
        nonlocal second_read_happened
        assert environ["wsgi.input"].read(5) == b"first"
        first_read.set()
        assert release_second_read.wait(timeout=2)
        environ["wsgi.input"].read(6)
        second_read_happened = True
        start_response("204 No Content", [])
        return []

    async def receive():
        nonlocal receive_count
        receive_count += 1
        if receive_count == 1:
            return {
                "type": "http.request",
                "body": b"firstsecond",
                "more_body": False,
            }
        await disconnect_allowed.wait()
        disconnect_consumed.set()
        return {"type": "http.disconnect"}

    async def send(message):
        return None

    scope = {
        "type": "http",
        "method": "PUT",
        "path": "/upload.bin",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [(b"content-length", b"11")],
    }
    app = PooledWSGIApp(
        wsgi_app,
        max_workers=1,
        spool_dir=tmp_path,
        min_spool_free_bytes=0,
    )
    task = asyncio.create_task(app(scope, receive, send))
    assert await asyncio.to_thread(first_read.wait, 1)

    disconnect_allowed.set()
    disconnect_was_observed = True
    try:
        await asyncio.wait_for(disconnect_consumed.wait(), timeout=0.2)
        await asyncio.sleep(0)
    except TimeoutError:
        disconnect_was_observed = False
    finally:
        release_second_read.set()
        await task

    assert disconnect_was_observed is True
    assert second_read_happened is False


@pytest.mark.asyncio
async def test_pooled_wsgi_app_rejects_requests_above_admission_limit(tmp_path):
    """The bridge rejects excess requests instead of building an unbounded queue."""
    import asyncio
    import threading

    from ns_hpc.file_server import PooledWSGIApp

    first_started = threading.Event()
    release_first = threading.Event()
    calls = []

    def wsgi_app(environ, start_response):
        calls.append(environ["PATH_INFO"])
        first_started.set()
        assert release_first.wait(timeout=2)
        start_response("204 No Content", [])
        return []

    app = PooledWSGIApp(
        wsgi_app,
        max_workers=1,
        max_inflight_requests=1,
        spool_dir=tmp_path,
        min_spool_free_bytes=0,
    )

    def make_receive():
        request_delivered = False

        async def receive():
            nonlocal request_delivered
            if not request_delivered:
                request_delivered = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return await asyncio.Future()

        return receive

    def make_scope(path):
        return {
            "type": "http",
            "method": "GET",
            "path": path,
            "root_path": "",
            "query_string": b"",
            "http_version": "1.1",
            "scheme": "http",
            "server": ("localhost", 8000),
            "headers": [],
        }

    async def first_send(message):
        return None

    first_task = asyncio.create_task(
        app(make_scope("/first"), make_receive(), first_send)
    )
    assert await asyncio.to_thread(first_started.wait, 1)

    second_messages = []

    async def second_send(message):
        second_messages.append(message)

    await app(make_scope("/second"), make_receive(), second_send)
    release_first.set()
    await first_task

    assert calls == ["/first"]
    assert second_messages[0]["status"] == 503


@pytest.mark.asyncio
async def test_pooled_wsgi_app_cancelled_queued_request_never_calls_wsgi(tmp_path):
    """A queued worker checks cancellation before invoking application code."""
    import asyncio
    import threading

    from ns_hpc.file_server import PooledWSGIApp

    first_started = threading.Event()
    release_first = threading.Event()
    second_called = threading.Event()

    def wsgi_app(environ, start_response):
        if environ["PATH_INFO"] == "/first":
            first_started.set()
            assert release_first.wait(timeout=2)
        else:
            second_called.set()
        start_response("204 No Content", [])
        return []

    app = PooledWSGIApp(
        wsgi_app,
        max_workers=1,
        max_inflight_requests=2,
        spool_dir=tmp_path,
        min_spool_free_bytes=0,
    )

    def make_receive():
        request_delivered = False

        async def receive():
            nonlocal request_delivered
            if not request_delivered:
                request_delivered = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return await asyncio.Future()

        return receive

    def make_scope(path):
        return {
            "type": "http",
            "method": "GET",
            "path": path,
            "root_path": "",
            "query_string": b"",
            "http_version": "1.1",
            "scheme": "http",
            "server": ("localhost", 8000),
            "headers": [],
        }

    async def send(message):
        return None

    first_task = asyncio.create_task(app(make_scope("/first"), make_receive(), send))
    assert await asyncio.to_thread(first_started.wait, 1)
    second_task = asyncio.create_task(app(make_scope("/second"), make_receive(), send))
    for _ in range(100):
        if app._executor._work_queue.qsize() == 1:
            break
        await asyncio.sleep(0)
    assert app._executor._work_queue.qsize() == 1

    second_task.cancel()
    await asyncio.sleep(0)
    assert second_task.done() is False
    second_task.cancel()
    await asyncio.sleep(0)

    third_messages = []

    async def third_send(message):
        third_messages.append(message)

    await app(make_scope("/third"), make_receive(), third_send)
    assert third_messages[0]["status"] == 503
    assert app._inflight_requests == 2

    release_first.set()

    await first_task
    with pytest.raises(asyncio.CancelledError):
        await second_task
    assert second_called.is_set() is False


@pytest.mark.asyncio
async def test_pooled_wsgi_app_worker_drain_resists_repeated_cancellation():
    """Cleanup does not finish until the worker completes, despite repeated cancel()."""
    import asyncio

    from ns_hpc.file_server import PooledWSGIApp

    loop = asyncio.get_running_loop()
    worker_future = loop.create_future()
    drain_task = asyncio.create_task(PooledWSGIApp._drain_worker(worker_future))
    await asyncio.sleep(0)

    drain_task.cancel()
    await asyncio.sleep(0)
    assert drain_task.done() is False
    drain_task.cancel()
    await asyncio.sleep(0)
    assert drain_task.done() is False

    worker_future.set_result(None)
    assert await drain_task is True


@pytest.mark.asyncio
async def test_pooled_wsgi_app_uses_opened_spool_directory_after_path_replacement(tmp_path):
    """Temporary files stay in the validated directory after pathname replacement."""
    import asyncio

    from ns_hpc.file_server import PooledWSGIApp

    spool = tmp_path / "spool"
    spool.mkdir(mode=0o700)
    attacker = tmp_path / "attacker"
    attacker.mkdir(mode=0o700)
    trusted = tmp_path / "trusted-renamed"
    observed = {}

    def wsgi_app(environ, start_response):
        stream = environ["wsgi.input"]
        observed["fd_path"] = os.readlink(f"/proc/self/fd/{stream.fileno()}")
        start_response("204 No Content", [])
        return []

    app = PooledWSGIApp(
        wsgi_app,
        max_workers=1,
        spool_dir=spool,
        min_spool_free_bytes=0,
    )
    spool.rename(trusted)
    spool.symlink_to(attacker, target_is_directory=True)

    delivered = False

    async def receive():
        nonlocal delivered
        if not delivered:
            delivered = True
            return {
                "type": "http.request",
                "body": b"x" * (96 * 1024),
                "more_body": False,
            }
        return await asyncio.Future()

    async def send(message):
        return None

    scope = {
        "type": "http",
        "method": "PUT",
        "path": "/upload.bin",
        "root_path": "",
        "query_string": b"",
        "http_version": "1.1",
        "scheme": "http",
        "server": ("localhost", 8000),
        "headers": [(b"content-length", str(96 * 1024).encode())],
    }
    await app(scope, receive, send)

    assert observed["fd_path"].startswith(str(trusted))
    assert not observed["fd_path"].startswith(str(attacker))


@pytest.mark.asyncio
async def test_pooled_wsgi_app_keeps_concurrent_requests_independent(tmp_path):
    """A blocked WSGI request must not prevent another worker responding."""
    import asyncio
    import threading

    from ns_hpc.file_server import PooledWSGIApp

    slow_started = threading.Event()
    release_slow = threading.Event()

    def wsgi_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        if environ["PATH_INFO"] == "/slow":
            slow_started.set()
            assert release_slow.wait(timeout=2)
        return [environ["PATH_INFO"].encode()]

    app = PooledWSGIApp(
        wsgi_app,
        max_workers=2,
        spool_dir=tmp_path,
        min_spool_free_bytes=0,
    )

    async def invoke(path):
        request_delivered = False
        messages = []

        async def receive():
            nonlocal request_delivered
            if not request_delivered:
                request_delivered = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return await asyncio.Future()

        async def send(message):
            messages.append(message)

        scope = {
            "type": "http",
            "method": "GET",
            "path": path,
            "root_path": "",
            "query_string": b"",
            "http_version": "1.1",
            "scheme": "http",
            "server": ("localhost", 8000),
            "headers": [],
        }
        await app(scope, receive, send)
        return b"".join(
            message.get("body", b"")
            for message in messages
            if message["type"] == "http.response.body"
        )

    slow_task = asyncio.create_task(invoke("/slow"))
    assert await asyncio.to_thread(slow_started.wait, 1)
    try:
        assert await asyncio.wait_for(invoke("/fast"), timeout=1.0) == b"/fast"
    finally:
        release_slow.set()
        await slow_task
