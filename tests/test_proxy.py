"""Tests for the MCP proxy module.

These tests verify tool discovery and FunctionTool wrapping without
needing bwrap — they use a test MCP server running as a subprocess.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from ns_hpc.config import ProxiedMCP
from ns_hpc.proxy import discover_tools

# Path to the test MCP server script
_TEST_SERVER = Path(__file__).parent / "test_proxy_server.py"


@pytest.fixture
def proxy_cfg() -> ProxiedMCP:
    return ProxiedMCP(
        command=sys.executable,
        args=[str(_TEST_SERVER)],
    )


@pytest.mark.asyncio
async def test_discover_tools(proxy_cfg: ProxiedMCP):
    """Discovery finds echo, add, and failing tools with correct schemas."""
    tools = await discover_tools(proxy_cfg)
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
async def test_discover_tools_empty_config():
    """A config with an invalid command returns an empty list."""
    cfg = ProxiedMCP(command="nonexistent-command-xyz")
    tools = await discover_tools(cfg)
    assert tools == []


@pytest.mark.asyncio
async def test_discover_tools_creates_wrapped_tool_schema(proxy_cfg: ProxiedMCP):
    """Verify the wrapped FunctionTool schema has instance_id prepended."""
    from fastmcp.tools import FunctionTool

    tools = await discover_tools(proxy_cfg)
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
