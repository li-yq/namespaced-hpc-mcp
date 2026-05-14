"""Tests for the MCP proxy module.

These tests verify tool discovery and FunctionTool wrapping.
Discovery always runs inside bwrap, so these tests require bwrap.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

from ns_hpc.config import Config, NamespaceDefaults, ProxiedMCP, ResourceDefaults
from ns_hpc.proxy import discover_tools

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
        namespace_defaults=NamespaceDefaults(
            bind_ro=[
                "/usr", "/bin", "/lib", "/lib64",
                str(_VENV_ROOT),
                str(_PROJECT_ROOT),
            ],
            workspace_mount="/workspace",
            flags=[
                "--unshare-all", "--share-net", "--die-with-parent",
                "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
            ],
        ),
        proxied_mcps={},
        resource_defaults=ResourceDefaults(),
        instances_dir=str(tmp_path),
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
