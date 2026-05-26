"""Tests for the MCP proxy module.

These tests verify tool discovery and FunctionTool wrapping.
Discovery always runs inside bwrap, so these tests require bwrap.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

from ns_hpc.config import Config, ProxiedMCP
from ns_hpc.instance import Instance
from ns_hpc.proxy import discover_tools
import json

# Path to the test MCP server script
_TEST_SERVER = Path(__file__).parent / "test_proxy_server.py"
_PROJECT_ROOT = _TEST_SERVER.parent.parent.resolve()
_VENV_ROOT = Path(sys.prefix)

_skip_no_bwrap = pytest.mark.skipif(
    not shutil.which("bwrap"), reason="bwrap not available"
)


@pytest.fixture
def proxy_cfg() -> ProxiedMCP:
    return ProxiedMCP(
        command=sys.executable,
        args=[str(_TEST_SERVER)],
    )


@pytest.fixture
def bwrap_config(tmp_path: Path) -> Config:
    """Minimal config with venv and project root bound for bwrap."""
    return Config(
        namespace={
            "instances_dir": str(tmp_path),
            "bwrap_command": [
                "bwrap",
                "--unshare-all", "--share-net",
                "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
                "--ro-bind", "/usr", "/usr",
                "--ro-bind", "/bin", "/bin",
                "--ro-bind", "/lib", "/lib64",
                "--ro-bind", str(_VENV_ROOT), str(_VENV_ROOT),
                "--ro-bind", str(_PROJECT_ROOT), str(_PROJECT_ROOT),
            ],
        },
        jobs={
            "local": {
                "use_cgroups": False,
                "cgroups_command": ["sh", "-c"],
            },
            "slurm": {
                "sbatch_command": ["sbatch"],
                "limit": {},
            },
        },
        proxied_mcps={},
    )


@pytest.mark.asyncio
@_skip_no_bwrap
async def test_discover_tools(proxy_cfg: ProxiedMCP, bwrap_config: Config):
    """Discovery finds echo, add, and failing tools with correct schemas."""
    tools = await discover_tools(proxy_cfg, bwrap_config)
    names = {t.name for t in tools}
    assert "echo" in names
    assert "add" in names
    assert "failing" in names

    # Check tool schemas
    by_name = {t.name: t for t in tools}
    echo_schema = by_name["echo"].inputSchema
    assert "message" in echo_schema.get("properties", {})
    assert echo_schema["properties"]["message"]["type"] == "string"

    add_schema = by_name["add"].inputSchema
    assert "a" in add_schema.get("properties", {})
    assert "b" in add_schema.get("properties", {})
    assert add_schema["properties"]["a"]["type"] == "integer"


@pytest.mark.asyncio
async def test_discover_tools_empty_config(bwrap_config: Config):
    """A config with an invalid command returns an empty list."""
    cfg = ProxiedMCP(command="nonexistent-command-xyz")
    tools = await discover_tools(cfg, bwrap_config)
    assert tools == []


@pytest.mark.asyncio
@_skip_no_bwrap
async def test_discover_tools_creates_wrapped_tool_schema(
    proxy_cfg: ProxiedMCP, bwrap_config: Config,
):
    """Verify the wrapped FunctionTool schema has instance_id prepended."""
    from fastmcp.tools import FunctionTool

    tools = await discover_tools(proxy_cfg, bwrap_config)
    echo_tool = [t for t in tools if t.name == "echo"][0]

    wrapped_schema = dict(echo_tool.inputSchema)
    wrapped_schema["properties"] = {
        "instance_id": {"type": "string", "description": "Instance ID"},
        **echo_tool.inputSchema.get("properties", {}),
    }
    wrapped_schema["required"] = ["instance_id"] + list(
        echo_tool.inputSchema.get("required", [])
    )

    # Create a FunctionTool with the wrapped schema
    async def handler(**kwargs):
        return ""

    ft = FunctionTool(
        fn=handler,
        name="test__echo",
        description=echo_tool.description or "",
        parameters=wrapped_schema,
    )

    assert ft.name == "test__echo"
    assert "instance_id" in ft.parameters.get("properties", {})
    assert "message" in ft.parameters.get("properties", {})
    assert ft.parameters["required"] == ["instance_id", "message"]


# ── Helpers for audit tests ──────────────────────────────────────────────────


def _config(tmp_dir: str) -> Config:
    return Config(
        namespace={
            "instances_dir": tmp_dir,
            "bwrap_command": [
                "bwrap",
                "--unshare-all", "--share-net",
                "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
                "--ro-bind", "/usr", "/usr",
                "--ro-bind", "/bin", "/bin",
                "--ro-bind", "/lib", "/lib",
                "--ro-bind", "/lib64", "/lib64",
            ],
        },
        jobs={
            "local": {
                "use_cgroups": False,
                "cgroups_command": ["sh", "-c"],
            },
            "slurm": {
                "sbatch_command": ["sbatch"],
                "limit": {},
            },
        },
        proxied_mcps={},
    )


# ── Audit logging tests ────────────────────────────────────────────────────


class _FakeClient:
    """Fake ProxiedMCPClient that records calls and returns canned results."""

    def __init__(self, result_content: str = "", should_fail: bool = False) -> None:
        self.result_content = result_content
        self.should_fail = should_fail
        self._client = None  # simulate not-connected-yet
        self.connect_calls: int = 0
        self.call_tool_calls: list[tuple[str, dict]] = []

    @property
    def is_connected(self) -> bool:
        return self._client is not None

    async def ensure_connected(self):
        if self._client is not None:
            return  # already connected
        self.connect_calls += 1
        if self.should_fail:
            raise RuntimeError("connect failed")
        self._client = object()  # mark as connected

    async def call_tool(self, name: str, arguments: dict):
        self.call_tool_calls.append((name, arguments))
        if self.should_fail:
            raise RuntimeError("tool call failed")
        from mcp.types import TextContent
        return type("_R", (), {
            "content": [TextContent(type="text", text=self.result_content)]
        })()


class _FakePM:
    """Fake ProxyManager that returns a pre-configured FakeClient."""

    def __init__(self, client: _FakeClient) -> None:
        self.client = client

    def get_or_start(self, proxy_name: str, instance_id: str, cfg, config):
        return self.client


@pytest.mark.asyncio
async def test_proxy_handler_audit_connected_and_completed(tmp_path, monkeypatch):
    """Calling a proxied tool audits proxy.connected and proxy.call.completed."""
    from ns_hpc.server import _make_proxy_handler, ServerContext
    from ns_hpc.config import ProxiedMCP

    cfg = _config(str(tmp_path))
    inst = Instance.create("proxy-audit", cfg)
    monkeypatch.setattr("ns_hpc.server.Instance.load", lambda iid, c: inst)

    fake_client = _FakeClient(result_content="hello from proxy")
    fake_pm = _FakePM(fake_client)

    proxy_cfg = ProxiedMCP(command="echo", args=["hello"])
    handler = _make_proxy_handler(fake_pm, "testproxy", proxy_cfg, "echo_cmd", cfg)

    result = await handler(instance_id="proxy-audit", message="world")
    assert result == "hello from proxy"

    # Check audit log
    lines = inst.audit_log_path.read_text().strip().splitlines()
    events = [json.loads(line) for line in lines]
    event_types = [e["event"] for e in events]

    assert "proxy.connected" in event_types
    assert "proxy.call.started" in event_types
    assert "proxy.call.completed" in event_types

    # Verify proxy.connected
    conn = events[event_types.index("proxy.connected")]
    assert conn["proxy_name"] == "testproxy"
    assert conn["command"] == "echo"

    # Verify proxy.call.started
    started = events[event_types.index("proxy.call.started")]
    assert started["proxy_name"] == "testproxy"
    assert started["tool_name"] == "echo_cmd"
    assert started["arguments"] == {"message": "world"}

    # Verify proxy.call.completed
    completed = events[event_types.index("proxy.call.completed")]
    assert completed["proxy_name"] == "testproxy"
    assert completed["tool_name"] == "echo_cmd"


@pytest.mark.asyncio
async def test_proxy_handler_audit_connection_failed(tmp_path, monkeypatch):
    """A failed connection audits proxy.connection.failed and raises."""
    from ns_hpc.server import _make_proxy_handler
    from ns_hpc.config import ProxiedMCP

    cfg = _config(str(tmp_path))
    inst = Instance.create("proxy-connfail", cfg)
    monkeypatch.setattr("ns_hpc.server.Instance.load", lambda iid, c: inst)

    fake_client = _FakeClient(should_fail=True)
    fake_pm = _FakePM(fake_client)

    proxy_cfg = ProxiedMCP(command="nonexistent")
    handler = _make_proxy_handler(fake_pm, "testproxy", proxy_cfg, "bad_cmd", cfg)

    with pytest.raises(RuntimeError, match="connect failed"):
        await handler(instance_id="proxy-connfail")

    lines = inst.audit_log_path.read_text().strip().splitlines()
    events = [json.loads(line) for line in lines]
    event_types = [e["event"] for e in events]

    assert "proxy.connection.failed" in event_types
    # No call.started or call.completed because connect failed
    assert "proxy.call.started" not in event_types
    assert "proxy.call.completed" not in event_types

    fail = events[event_types.index("proxy.connection.failed")]
    assert fail["proxy_name"] == "testproxy"
    assert "connect failed" in fail["error"]


@pytest.mark.asyncio
async def test_proxy_handler_audit_call_failed(tmp_path, monkeypatch):
    """A failed call audits proxy.call.started + proxy.call.failed, no completed."""
    from ns_hpc.server import _make_proxy_handler
    from ns_hpc.config import ProxiedMCP

    cfg = _config(str(tmp_path))
    inst = Instance.create("proxy-callfail", cfg)
    monkeypatch.setattr("ns_hpc.server.Instance.load", lambda iid, c: inst)

    fake_client = _FakeClient(should_fail=True)
    # Mark as already-connected so ensure_connected succeeds but call_tool fails
    fake_client._client = object()
    fake_pm = _FakePM(fake_client)

    proxy_cfg = ProxiedMCP(command="echo")
    handler = _make_proxy_handler(fake_pm, "testproxy", proxy_cfg, "failing_cmd", cfg)

    with pytest.raises(RuntimeError, match="tool call failed"):
        await handler(instance_id="proxy-callfail")

    lines = inst.audit_log_path.read_text().strip().splitlines()
    events = [json.loads(line) for line in lines]
    event_types = [e["event"] for e in events]

    assert "proxy.call.started" in event_types
    assert "proxy.call.failed" in event_types
    assert "proxy.call.completed" not in event_types

    fail = events[event_types.index("proxy.call.failed")]
    assert fail["proxy_name"] == "testproxy"
    assert fail["tool_name"] == "failing_cmd"
    assert "tool call failed" in fail["error"]


@pytest.mark.asyncio
async def test_proxy_handler_audit_no_connected_on_reuse(tmp_path, monkeypatch):
    """Second call to the same proxy does NOT re-audit proxy.connected."""
    from ns_hpc.server import _make_proxy_handler
    from ns_hpc.config import ProxiedMCP

    cfg = _config(str(tmp_path))
    inst = Instance.create("proxy-reuse", cfg)
    monkeypatch.setattr("ns_hpc.server.Instance.load", lambda iid, c: inst)

    fake_client = _FakeClient(result_content="ok")
    fake_pm = _FakePM(fake_client)

    proxy_cfg = ProxiedMCP(command="echo")
    handler = _make_proxy_handler(fake_pm, "testproxy", proxy_cfg, "test_cmd", cfg)

    await handler(instance_id="proxy-reuse")
    await handler(instance_id="proxy-reuse")

    lines = inst.audit_log_path.read_text().strip().splitlines()
    events = [json.loads(line) for line in lines]
    event_types = [e["event"] for e in events]

    # proxy.connected appears exactly once
    assert event_types.count("proxy.connected") == 1
    # proxy.call.started and completed appear twice each
    assert event_types.count("proxy.call.started") == 2
    assert event_types.count("proxy.call.completed") == 2


# ── Annotation pass-through test ────────────────────────────────────────────


def test_proxy_functiontool_passes_annotations():
    """FunctionTool wrapping preserves the remote tool's annotations."""
    from mcp.types import Tool, ToolAnnotations
    from fastmcp.tools import FunctionTool

    remote = Tool(
        name="read",
        description="Read a file",
        inputSchema={"type": "object", "properties": {}, "required": []},
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
        ),
    )

    combined = {
        "type": "object",
        "properties": {
            "instance_id": {"type": "string", "description": "Instance ID"},
            **dict(remote.inputSchema.get("properties", {})),
        },
        "required": ["instance_id"] + list(remote.inputSchema.get("required", [])),
    }

    async def handler(**kwargs):
        return ""

    ft = FunctionTool(
        fn=handler,
        name="filesystem__read",
        description=remote.description or "",
        parameters=combined,
        annotations=remote.annotations,
    )

    assert ft.annotations is not None
    assert ft.annotations.readOnlyHint is True
    assert ft.annotations.destructiveHint is False
    assert ft.annotations.idempotentHint is True


# ── Tool include/exclude filter tests ──────────────────────────────────────


def _make_tool(name: str) -> object:
    """Create a minimal tool-like object with just a name attribute."""
    from mcp.types import Tool
    return Tool(
        name=name,
        description=f"Tool {name}",
        inputSchema={"type": "object", "properties": {}, "required": []},
    )


def test_filter_tools_empty_lists():
    """Both include and exclude empty → all tools pass through."""
    from ns_hpc.server import _filter_tools
    from ns_hpc.config import ProxiedMCP

    cfg = ProxiedMCP(command="test", include=[], exclude=[])
    tools = [_make_tool("read"), _make_tool("write"), _make_tool("delete")]
    result = _filter_tools("test", cfg, tools)
    assert [t.name for t in result] == ["read", "write", "delete"]


def test_filter_tools_include_only():
    """include is non-empty → only matching tools kept."""
    from ns_hpc.server import _filter_tools
    from ns_hpc.config import ProxiedMCP

    cfg = ProxiedMCP(command="test", include=["read", "list_*"])
    tools = [_make_tool("read"), _make_tool("write"), _make_tool("list_files"), _make_tool("list_dirs")]
    result = _filter_tools("test", cfg, tools)
    names = {t.name for t in result}
    assert names == {"read", "list_files", "list_dirs"}
    assert "write" not in names


def test_filter_tools_exclude_only():
    """exclude is non-empty → matching tools removed."""
    from ns_hpc.server import _filter_tools
    from ns_hpc.config import ProxiedMCP

    cfg = ProxiedMCP(command="test", exclude=["delete_*", "write"])
    tools = [_make_tool("read"), _make_tool("write"), _make_tool("delete_file"), _make_tool("delete_dir")]
    result = _filter_tools("test", cfg, tools)
    names = {t.name for t in result}
    assert names == {"read"}


def test_filter_tools_include_and_exclude():
    """Both set → must match include AND not match exclude."""
    from ns_hpc.server import _filter_tools
    from ns_hpc.config import ProxiedMCP

    cfg = ProxiedMCP(command="test", include=["file_*"], exclude=["*_dangerous"])
    tools = [
        _make_tool("file_read"),
        _make_tool("file_write"),
        _make_tool("file_dangerous"),
        _make_tool("dir_list"),
    ]
    result = _filter_tools("test", cfg, tools)
    names = {t.name for t in result}
    assert names == {"file_read", "file_write"}


def test_filter_tools_exclude_no_match():
    """exclude patterns that match nothing keep all tools."""
    from ns_hpc.server import _filter_tools
    from ns_hpc.config import ProxiedMCP

    cfg = ProxiedMCP(command="test", exclude=["nonexistent"])
    tools = [_make_tool("read"), _make_tool("write")]
    result = _filter_tools("test", cfg, tools)
    assert [t.name for t in result] == ["read", "write"]


def test_filter_tools_include_glob():
    """Glob patterns in include list work correctly."""
    from ns_hpc.server import _filter_tools
    from ns_hpc.config import ProxiedMCP

    cfg = ProxiedMCP(command="test", include=["file_*"])
    tools = [_make_tool("file_read"), _make_tool("file_write"), _make_tool("dir_list")]
    result = _filter_tools("test", cfg, tools)
    names = {t.name for t in result}
    assert names == {"file_read", "file_write"}


def test_filter_tools_include_returns_empty():
    """If include matches nothing, result is empty."""
    from ns_hpc.server import _filter_tools
    from ns_hpc.config import ProxiedMCP

    cfg = ProxiedMCP(command="test", include=["nonexistent"])
    tools = [_make_tool("read"), _make_tool("write")]
    result = _filter_tools("test", cfg, tools)
    assert result == []
