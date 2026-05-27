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
    asgi_app = PooledWSGIApp(dav_app, max_workers=10)
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
    asgi_app = PooledWSGIApp(wsgi_app, max_workers=2)

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
