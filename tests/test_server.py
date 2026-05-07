"""Tests for the MCP server module."""

from ns_hpc.server import mcp


def test_server_imports():
    """Verify the server module loads and has tools registered."""
    assert mcp is not None
    tool_names = {t.name for t in mcp._tool_manager.list_tools()}
    assert "run_command" in tool_names, f"Expected run_command in {tool_names}"
    assert "read_file" in tool_names, f"Expected read_file in {tool_names}"
    assert "write_file" in tool_names, f"Expected write_file in {tool_names}"
    assert "list_directory" in tool_names, f"Expected list_directory in {tool_names}"
