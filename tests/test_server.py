"""Tests for the MCP server module."""

from ns_hpc.server import mcp


def test_server_imports():
    """Verify the server module loads and has tools registered."""
    assert mcp is not None
    tool_names = {t.name for t in mcp._tool_manager.list_tools()}
    assert "run_command" in tool_names, f"Expected run_command in {tool_names}"
    assert "create_instance" in tool_names, f"Expected create_instance in {tool_names}"
    assert "list_instances" in tool_names, f"Expected list_instances in {tool_names}"
    assert "destroy_instance" in tool_names, f"Expected destroy_instance in {tool_names}"
    assert "read_file" not in tool_names, "read_file should have been removed"
    assert "write_file" not in tool_names, "write_file should have been removed"
    assert "list_directory" not in tool_names, "list_directory should have been removed"
