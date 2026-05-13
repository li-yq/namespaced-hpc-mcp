"""A minimal MCP server used as a test target for the proxy module.

Run as a subprocess over stdio:

    uv run python tests/test_proxy_server.py
"""
from __future__ import annotations

from fastmcp import FastMCP

server = FastMCP("ns-hpc-test-proxy")


@server.tool()
def echo(message: str) -> str:
    """Echo a message back."""
    return message


@server.tool()
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


@server.tool()
def failing() -> str:
    """Always fails."""
    raise RuntimeError("this tool always fails")


if __name__ == "__main__":
    server.run(transport="stdio")
