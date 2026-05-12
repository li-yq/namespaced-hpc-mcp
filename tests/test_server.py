"""Tests for the MCP server module."""

import pytest

from ns_hpc.server import mcp


@pytest.mark.asyncio
async def test_server_imports():
    """Verify the server module loads and has tools registered."""
    assert mcp is not None
    tools = await mcp.list_tools()
    tool_names = {t.name for t in tools}
    assert "submit_job" in tool_names, f"Expected submit_job in {tool_names}"
    assert "poll_job" in tool_names, f"Expected poll_job in {tool_names}"
    assert "list_jobs" in tool_names, f"Expected list_jobs in {tool_names}"
    assert "cancel_job" in tool_names, f"Expected cancel_job in {tool_names}"
    assert "run_command" not in tool_names, "run_command should be replaced by submit_job"
