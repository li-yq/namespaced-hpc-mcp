"""MCP server for ns-hpc — sandboxed command execution and instance management."""
from __future__ import annotations

import json
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


@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[ServerContext]:
    """Initialize server context — no default instances created at startup."""
    config_path = os.environ.get("NS_HPC_CONFIG")
    config = load_config(config_path)

    try:
        yield ServerContext(config=config)
    finally:
        pass


# Create the MCP server with lifespan
mcp = FastMCP(
    name="ns-hpc",
    instructions="HPC sandboxing via bubblewrap — manage instances and execute commands in isolated bwrap containers.",
    lifespan=server_lifespan,
)


# ── Instance management ────────────────────────────────────────────────────


class CreateInstanceInput(BaseModel):
    """Input for the create_instance tool."""
    instance_id: str = Field(
        ...,
        description="Unique identifier for the new instance",
    )


@mcp.tool()
async def create_instance(input: CreateInstanceInput) -> str:
    """Create a new sandbox instance with a persistent workspace directory."""
    ctx = mcp.get_context()
    context: ServerContext = ctx.lifespan_context

    try:
        instance = Instance.create(input.instance_id, context.config)
    except FileExistsError:
        return f"Error: Instance '{input.instance_id}' already exists"

    return (
        f"Instance '{input.instance_id}' created.\n"
        f"Workspace: {instance.workspace_dir}"
    )


class ListInstancesInput(BaseModel):
    """Input for the list_instances tool."""


@mcp.tool()
async def list_instances(input: ListInstancesInput) -> str:
    """List all existing sandbox instances."""
    ctx = mcp.get_context()
    context: ServerContext = ctx.lifespan_context

    instances = Instance.list_instances(context.config)
    if not instances:
        return "No instances found."

    lines = []
    for inst in instances:
        try:
            meta = json.loads(inst.metadata_path.read_text())
            created = meta.get("created_at", "unknown")[:19]
        except Exception:
            created = "unknown"
        lines.append(f"{inst.id:20s}  created: {created}")

    return "\n".join(lines)


class DestroyInstanceInput(BaseModel):
    """Input for the destroy_instance tool."""
    instance_id: str = Field(
        ...,
        description="ID of the instance to destroy",
    )


@mcp.tool()
async def destroy_instance(input: DestroyInstanceInput) -> str:
    """Destroy a sandbox instance and remove its workspace directory."""
    ctx = mcp.get_context()
    context: ServerContext = ctx.lifespan_context

    if not Instance.load(input.instance_id, context.config):
        return f"Error: Instance '{input.instance_id}' not found"

    Instance.destroy(input.instance_id, context.config)
    return f"Instance '{input.instance_id}' destroyed."


# ── Command execution ──────────────────────────────────────────────────────


class RunCommandInput(BaseModel):
    """Input for the run_command tool."""
    command: str = Field(
        ...,
        description="Shell command to run inside the bwrap sandbox",
    )
    instance_id: str = Field(
        ...,
        description="Instance ID to run the command in",
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

    instance = Instance.load(input.instance_id, cfg)
    if instance is None:
        return f"Error: Instance '{input.instance_id}' not found"

    result = run_in_sandbox(
        command=["/bin/sh", "-c", input.command],
        workspace_host_path=str(instance.workspace_dir),
        timeout=input.timeout,
        config=cfg,
    )

    instance.audit(input.command, result.exit_code,
                   stdout=result.stdout, stderr=result.stderr)

    output = f"Exit code: {result.exit_code}\n"
    if result.stdout:
        output += f"STDOUT:\n{result.stdout}\n"
    if result.stderr:
        output += f"STDERR:\n{result.stderr}\n"

    return output


# ── Entry point ────────────────────────────────────────────────────────────


def run_server(config_path: str | None = None, port: int = 8001) -> None:
    """Start the MCP server over stdio.

    This is the entry point called from the CLI.

    Args:
        config_path: Optional override for the config.toml path.
        port: Ignored in stdio mode; kept for API compatibility.
    """
    mcp.run(transport="stdio")
