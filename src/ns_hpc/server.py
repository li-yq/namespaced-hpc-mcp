"""MCP server for ns-hpc — sandboxed file ops and command execution."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

from ns_hpc.config import Config, load_config
from ns_hpc.instance import Instance
from ns_hpc.namespace import run_in_sandbox


@dataclass
class ServerContext:
    """Lifespan context shared across MCP tools."""
    config: Config
    instance: Instance


@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[ServerContext]:
    """Initialize server context — creates a default sandbox instance."""
    config_path = os.environ.get("NS_HPC_CONFIG")
    config = load_config(config_path)

    instance = Instance.create("default", config)

    try:
        yield ServerContext(config=config, instance=instance)
    finally:
        instance.write_audit("server_shutdown", {
            "exit_code": 0, "stdout": "", "stderr": "",
        })


# Create the MCP server with lifespan
mcp = FastMCP(
    name="ns-hpc",
    instructions="HPC sandboxing via bubblewrap — execute commands and manage files in isolated bwrap containers.",
    lifespan=server_lifespan,
)


# ── Command execution ────────────────────────────────────────────────────


class RunCommandInput(BaseModel):
    """Input for the run_command tool."""
    command: str = Field(
        ...,
        description="Shell command to run inside the bwrap sandbox",
    )
    instance_id: str = Field(
        default="default",
        description="Instance ID (created on demand if it doesn't exist)",
    )
    timeout: int = Field(
        default=60,
        description="Max execution time in seconds",
        ge=1,
        le=3600,
    )


@mcp.tool()
async def run_command(input: RunCommandInput) -> str:
    """Execute a shell command inside a bwrap-sandboxed environment.

    The command runs in a fresh sandbox with:
    - Read-only system paths (/usr, /lib, /bin, /etc)
    - Read-write workspace directory
    - Network access (--share-net)
    - No access to host filesystem outside workspace
    - /tmp as tmpfs, /proc and /dev available
    """
    ctx = mcp.get_context()
    context: ServerContext = ctx.lifespan_context
    cfg = context.config

    # Load or create instance
    instance = Instance.load(input.instance_id, cfg)
    if instance is None:
        instance = Instance.create(input.instance_id, cfg)

    result = run_in_sandbox(
        command=["/bin/sh", "-c", input.command],
        workspace_host_path=str(instance.workspace_dir),
        timeout=input.timeout,
        config=cfg,
    )

    # Audit from host side — critical security boundary
    instance.write_audit(input.command, {
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
    })

    output = f"Exit code: {result.exit_code}\n"
    if result.stdout:
        output += f"STDOUT:\n{result.stdout}\n"
    if result.stderr:
        output += f"STDERR:\n{result.stderr}\n"

    return output


# ── File operations ──────────────────────────────────────────────────────


class ReadFileInput(BaseModel):
    """Input for the read_file tool."""
    path: str = Field(
        ...,
        description="Path to read, relative to workspace root",
    )
    instance_id: str = Field(
        default="default",
        description="Instance ID",
    )


@mcp.tool()
async def read_file(input: ReadFileInput) -> str:
    """Read a file from the workspace directory.

    Path is relative to the workspace root. Path traversal outside the
    workspace is blocked.
    """
    ctx = mcp.get_context()
    context: ServerContext = ctx.lifespan_context
    cfg = context.config

    instance = Instance.load(input.instance_id, cfg)
    if instance is None:
        instance = Instance.create(input.instance_id, cfg)

    safe_path = (instance.workspace_dir / input.path.lstrip("/")).resolve()
    workspace_root = instance.workspace_dir.resolve()

    if not str(safe_path).startswith(str(workspace_root)):
        return "Error: Path escapes workspace"

    if not safe_path.exists():
        return f"Error: {input.path} not found"

    if safe_path.is_dir():
        return f"Error: {input.path} is a directory"

    return safe_path.read_text(errors="replace")


class WriteFileInput(BaseModel):
    """Input for the write_file tool."""
    path: str = Field(
        ...,
        description="Path to write, relative to workspace root",
    )
    content: str = Field(
        ...,
        description="Text content to write to the file",
    )
    instance_id: str = Field(
        default="default",
        description="Instance ID",
    )


@mcp.tool()
async def write_file(input: WriteFileInput) -> str:
    """Write content to a file inside the workspace.

    Path is relative to the workspace root. Creates parent directories
    automatically.
    """
    ctx = mcp.get_context()
    context: ServerContext = ctx.lifespan_context
    cfg = context.config

    instance = Instance.load(input.instance_id, cfg)
    if instance is None:
        instance = Instance.create(input.instance_id, cfg)

    safe_path = (instance.workspace_dir / input.path.lstrip("/")).resolve()
    workspace_root = instance.workspace_dir.resolve()

    if not str(safe_path).startswith(str(workspace_root)):
        return "Error: Path escapes workspace"

    safe_path.parent.mkdir(parents=True, exist_ok=True)
    safe_path.write_text(input.content)

    instance.write_audit(f"write_file {input.path}", {
        "exit_code": 0,
        "stdout": f"Wrote {len(input.content)} bytes",
        "stderr": "",
    })

    return f"Wrote {len(input.content)} bytes to {input.path}"


class ListDirectoryInput(BaseModel):
    """Input for the list_directory tool."""
    path: str = Field(
        default=".",
        description="Directory path relative to workspace root",
    )
    instance_id: str = Field(
        default="default",
        description="Instance ID",
    )


@mcp.tool()
async def list_directory(input: ListDirectoryInput) -> str:
    """List the contents of a directory inside the workspace."""
    ctx = mcp.get_context()
    context: ServerContext = ctx.lifespan_context
    cfg = context.config

    instance = Instance.load(input.instance_id, cfg)
    if instance is None:
        instance = Instance.create(input.instance_id, cfg)

    safe_path = (instance.workspace_dir / input.path.lstrip("/")).resolve()
    workspace_root = instance.workspace_dir.resolve()

    if not str(safe_path).startswith(str(workspace_root)):
        return "Error: Path escapes workspace"

    if not safe_path.exists():
        return f"Error: {input.path} not found"

    if not safe_path.is_dir():
        return f"Error: {input.path} is not a directory"

    entries: list[str] = []
    for entry in sorted(safe_path.iterdir()):
        suffix = "/" if entry.is_dir() else ""
        entries.append(f"{entry.name}{suffix}")

    return "\n".join(entries) if entries else "(empty)"


# ── Entry point ──────────────────────────────────────────────────────────


def run_server(config_path: str | None = None, port: int = 8001) -> None:
    """Start the MCP server over stdio.

    This is the entry point called from the CLI.

    Args:
        config_path: Optional override for the config.toml path.
        port: Ignored in stdio mode; kept for API compatibility.
    """
    mcp.run(transport="stdio")
