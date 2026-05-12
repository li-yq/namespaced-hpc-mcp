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
from ns_hpc.job_manager import JobManager, JobStatus


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


# ── Job execution ─────────────────────────────────────────────────────────


class SubmitJobInput(BaseModel):
    instance_id: str = Field(..., description="Existing instance ID")
    command: str = Field(..., description="Shell command to run")
    mode: str = Field(
        default="local",
        description="Execution mode: 'local' (bwrap) or 'slurm' (sbatch)",
    )
    timeout: int = Field(
        default=60,
        description="Max seconds to wait for completion",
        ge=1,
        le=86400,
    )
    detach: bool = Field(
        default=False,
        description="If True, keep job running past timeout instead of killing",
    )
    tail: int = Field(
        default=50,
        description="Number of tail lines to return from output",
        ge=0,
        le=1000,
    )


@mcp.tool()
async def submit_job(input: SubmitJobInput) -> str:
    """Submit a command as an async job.

    The job runs inside a bwrap sandbox.  stdout/stderr are written
    directly to disk files.  The tool waits up to ``timeout`` seconds,
    then either returns the result (completed) or a running state.

    When ``detach=False`` (default): if the job exceeds timeout, it is
    killed and partial output is returned.

    When ``detach=True``: if the job exceeds timeout, it keeps running
    in the background.  Use ``poll_job`` to check on it later.
    """
    ctx = mcp.get_context()
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        return json.dumps({"error": f"Instance '{input.instance_id}' not found"})

    mgr = JobManager(instance, config)
    result = mgr.submit(
        input.command,
        mode=input.mode,
        timeout=input.timeout,
        tail=input.tail,
    )

    # Handle detach: if still running after timeout, keep it running
    if result.status == JobStatus.RUNNING and not input.detach:
        mgr.cancel_and_tail(result, input.tail)

    return json.dumps(result.to_dict(), indent=2)


class PollJobInput(BaseModel):
    instance_id: str = Field(..., description="Instance ID")
    job_id: str = Field(..., description="Job ID to poll")
    timeout: int = Field(
        default=0,
        description="Seconds to wait for completion (0 = just peek)",
        ge=0,
        le=3600,
    )
    detach: bool = Field(
        default=False,
        description="If True and job still running after timeout, keep it",
    )
    tail: int = Field(
        default=50,
        description="Number of tail lines to return",
        ge=0,
        le=1000,
    )


@mcp.tool()
async def poll_job(input: PollJobInput) -> str:
    """Poll a running job.  Optionally wait for completion.

    Same timeout/detach semantics as submit_job.
    """
    ctx = mcp.get_context()
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        return json.dumps({"error": f"Instance '{input.instance_id}' not found"})

    mgr = JobManager(instance, config)
    result = mgr.poll(input.job_id, timeout=input.timeout, tail=input.tail)

    if result is None:
        return json.dumps({"error": f"Job '{input.job_id}' not found or already finished"})

    if result.status == JobStatus.RUNNING and not input.detach:
        mgr.cancel_and_tail(result, input.tail)

    return json.dumps(result.to_dict(), indent=2)


class ListJobsInput(BaseModel):
    instance_id: str = Field(..., description="Instance ID")


@mcp.tool()
async def list_jobs(input: ListJobsInput) -> str:
    """List all tracked jobs for an instance."""
    ctx = mcp.get_context()
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        return json.dumps({"error": f"Instance '{input.instance_id}' not found"})

    mgr = JobManager(instance, config)
    jobs = mgr.list_jobs()
    if not jobs:
        return json.dumps([])
    return json.dumps(jobs, indent=2)


class CancelJobInput(BaseModel):
    instance_id: str = Field(..., description="Instance ID")
    job_id: str = Field(..., description="Job ID to cancel")


@mcp.tool()
async def cancel_job(input: CancelJobInput) -> str:
    """Cancel a running job."""
    ctx = mcp.get_context()
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        return json.dumps({"error": f"Instance '{input.instance_id}' not found"})

    mgr = JobManager(instance, config)
    ok = mgr.cancel(input.job_id)
    return json.dumps({"job_id": input.job_id, "cancelled": ok})


# ── Entry point ────────────────────────────────────────────────────────────


def run_server(config_path: str | None = None, port: int = 8001) -> None:
    """Start the MCP server over stdio.

    This is the entry point called from the CLI.

    Args:
        config_path: Optional override for the config.toml path.
        port: Ignored in stdio mode; kept for API compatibility.
    """
    mcp.run(transport="stdio")
