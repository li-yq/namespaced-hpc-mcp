"""MCP server for ns-hpc — sandboxed command execution and instance management."""
from __future__ import annotations

import fnmatch
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from fastmcp.resources import FileResource
from fastmcp.tools import FunctionTool
from mcp.types import TextContent
from pydantic import BaseModel, Field

from ns_hpc.config import Config, ProxiedMCP, load_config
from ns_hpc.instance import Instance
from ns_hpc.job_manager import JobManager, JobStatus
from ns_hpc.proxy import ProxyManager, discover_tools



@dataclass
class ServerContext:
    """Lifespan context shared across MCP tools."""
    config: Config
    config_path: str | None = None
    job_managers: dict[str, JobManager] = field(default_factory=dict)
    proxy_manager: ProxyManager = field(default_factory=ProxyManager)


def _get_manager(ctx: Context, instance: Instance) -> JobManager:
    """Get or create a cached JobManager for the given instance."""
    context: ServerContext = ctx.lifespan_context
    mgr = context.job_managers.get(instance.id)
    if mgr is None:
        mgr = JobManager(instance, context.config)
        context.job_managers[instance.id] = mgr
    return mgr


def _register_context_resources(server: FastMCP, config: Config, config_path: str | None = None) -> None:
    """Scan context directories and register matching files as static resources.

    Relative context dirs are resolved from the config file's parent directory
    so that the config is self-contained regardless of CWD.
    """
    config_dir = Path(config_path).resolve().parent if config_path else Path.cwd()
    patterns = config.resource_defaults.resource_patterns
    for raw_dir in config.resource_defaults.context_dirs:
        d = Path(raw_dir).expanduser()
        if not d.is_absolute():
            d = config_dir / d
        if not d.exists():
            continue
        for file_path in sorted(d.iterdir()):
            if not file_path.is_file():
                continue
            for pat in patterns:
                if fnmatch.fnmatch(file_path.name, pat):
                    uri = f"resource://ns-hpc/context/{file_path.name}"
                    resource = FileResource(
                        uri=uri,
                        name=file_path.name,
                        path=file_path,
                    )
                    server.add_resource(resource)
                    break


def _make_proxy_handler(
    pm: ProxyManager, proxy_name: str, cfg: ProxiedMCP, remote_name: str,
    config: Config,
) -> Callable[..., Awaitable[str]]:
    """Create a **kwargs handler that routes to the proxied MCP inside an instance."""
    async def handler(**kwargs: Any) -> str:
        instance_id = kwargs.pop("instance_id")
        if not Instance.load(instance_id, config):
            raise ToolError(f"Instance '{instance_id}' not found")
        client = pm.get_or_start(proxy_name, instance_id, cfg)
        await client.ensure_connected()
        result = await client.call_tool(remote_name, kwargs)
        texts = [
            c.text for c in (result.content or [])
            if isinstance(c, TextContent)
        ]
        return "\n".join(texts) if texts else str(result)
    return handler


async def _register_proxied_tools(server: FastMCP, config: Config) -> ProxyManager:
    """Discover tools from each proxied MCP and register wrapped FunctionTools."""
    pm = ProxyManager()

    for proxy_name, proxy_cfg in config.proxied_mcps.items():
        remote_tools = await discover_tools(proxy_cfg, config)
        if not remote_tools:
            logger = __import__("logging").getLogger("ns-hpc")
            logger.warning("no tools discovered for proxied MCP %r, skipping", proxy_name)
            continue

        for remote_tool in remote_tools:
            orig_props = dict(remote_tool.inputSchema.get("properties", {}))
            orig_required = list(remote_tool.inputSchema.get("required", []))

            combined_schema = {
                "type": "object",
                "properties": {
                    "instance_id": {"type": "string", "description": "Instance ID"},
                    **orig_props,
                },
                "required": ["instance_id"] + orig_required,
            }

            tool_name = f"{proxy_name}__{remote_tool.name}"
            handler = _make_proxy_handler(pm, proxy_name, proxy_cfg, remote_tool.name, config)

            ft = FunctionTool(
                fn=handler,
                name=tool_name,
                description=remote_tool.description or "",
                parameters=combined_schema,
            )
            server.add_tool(ft)

    return pm


@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[ServerContext]:
    """Initialize server context and register context resources."""
    config_path = os.environ.get("NS_HPC_CONFIG")
    config = load_config(config_path)

    _register_context_resources(server, config, config_path)
    proxy_manager = await _register_proxied_tools(server, config)

    context = ServerContext(
        config=config, config_path=config_path,
        proxy_manager=proxy_manager,
    )
    try:
        yield context
    finally:
        await context.proxy_manager.close_all()


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
    description: str = Field(
        "",
        description="Optional human-readable description for the instance",
    )


@mcp.tool()
async def create_instance(input: CreateInstanceInput, ctx: Context) -> str:
    """Create a new sandbox instance with a persistent workspace directory."""
    context: ServerContext = ctx.lifespan_context

    try:
        instance = Instance.create(input.instance_id, context.config, input.description)
    except FileExistsError:
        raise ToolError(f"Instance '{input.instance_id}' already exists")

    return f"Instance '{input.instance_id}' created."


class ListInstancesInput(BaseModel):
    """Input for the list_instances tool."""


@mcp.tool(annotations={"readOnlyHint": True})
async def list_instances(input: ListInstancesInput, ctx: Context) -> str:
    """List all existing sandbox instances."""
    context: ServerContext = ctx.lifespan_context

    instances = Instance.list_instances(context.config)
    if not instances:
        return "No instances found."

    lines = []
    for inst in instances:
        try:
            meta = json.loads(inst.metadata_path.read_text())
            created = meta.get("created_at", "unknown")[:19]
            desc = meta.get("description", "")
            label = f"{inst.id:20s}  created: {created}"
            if desc:
                label += f"  [{desc[:50]}]"
            lines.append(label)
        except Exception:
            lines.append(f"{inst.id:20s}  created: unknown")

    return "\n".join(lines)


class DestroyInstanceInput(BaseModel):
    """Input for the destroy_instance tool."""
    instance_id: str = Field(
        ...,
        description="ID of the instance to destroy",
    )


@mcp.tool(annotations={"destructiveHint": True})
async def destroy_instance(input: DestroyInstanceInput, ctx: Context) -> str:
    """Destroy a sandbox instance and remove its workspace directory."""
    context: ServerContext = ctx.lifespan_context

    if not Instance.load(input.instance_id, context.config):
        raise ToolError(f"Instance '{input.instance_id}' not found")

    context.job_managers.pop(input.instance_id, None)
    await context.proxy_manager.stop_all(input.instance_id)
    Instance.destroy(input.instance_id, context.config)
    return f"Instance '{input.instance_id}' destroyed."


class UpdateInstanceInput(BaseModel):
    """Input for the update_instance tool."""
    instance_id: str = Field(
        ...,
        description="ID of the instance to update",
    )
    description: str | None = Field(
        default=None,
        description="New description for the instance (omit to leave unchanged)",
    )


@mcp.tool()
async def update_instance(input: UpdateInstanceInput, ctx: Context) -> str:
    """Update an instance's metadata (e.g. description)."""
    context: ServerContext = ctx.lifespan_context

    instance = Instance.load(input.instance_id, context.config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    if input.description is not None:
        instance.set_description(input.description)

    desc = instance.get_description()
    return f"Instance '{input.instance_id}': description='{desc}'"


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
        default=True,
        description="If True, keep job running past timeout instead of killing",
    )
    tail: int = Field(
        default=50,
        description="Number of tail lines to return from output",
        ge=0,
        le=1000,
    )
    slurm_resources: dict[str, int | str] | None = Field(
        default=None,
        description="Per-job resource overrides for Slurm (e.g. {'cpus': 4})",
    )


@mcp.tool()
async def submit_job(input: SubmitJobInput, ctx: Context) -> dict:
    """Submit a command as an async job.

    The job runs inside a bwrap sandbox.  stdout/stderr are written
    directly to disk files.  The tool waits up to ``timeout`` seconds,
    then either returns the result (completed) or a running state.

    When ``detach=False`` (default): if the job exceeds timeout, it is
    killed and partial output is returned.

    When ``detach=True``: if the job exceeds timeout, it keeps running
    in the background.  Use ``poll_job`` to check on it later.
    """
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    mgr = _get_manager(ctx, instance)
    instance.audit("job.submitted", command=input.command, mode=input.mode,
                   timeout=input.timeout)
    result = mgr.submit(
        input.command,
        mode=input.mode,
        timeout=input.timeout,
        tail=input.tail,
        slurm_resources=input.slurm_resources,
    )

    # Handle detach: if still running after timeout, keep it running
    if result.status == JobStatus.RUNNING and not input.detach:
        mgr.cancel_and_tail(result, input.tail)

    # Audit outcome
    if result.status == JobStatus.RUNNING:
        instance.audit("job.running", job_id=result.job_id,
                       command=input.command, mode=input.mode,
                       detached=input.detach, timeout=input.timeout,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)
    else:
        instance.audit(f"job.{result.status.value}", job_id=result.job_id,
                       exit_code=result.exit_code, command=input.command,
                       mode=input.mode,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)

    return result.to_dict()


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
        default=True,
        description="If True and job still running after timeout, keep it",
    )
    tail: int = Field(
        default=50,
        description="Number of tail lines to return",
        ge=0,
        le=1000,
    )


@mcp.tool()
async def poll_job(input: PollJobInput, ctx: Context) -> dict:
    """Poll a running job.  Optionally wait for completion.

    Same timeout/detach semantics as submit_job.
    """
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    mgr = _get_manager(ctx, instance)
    result = mgr.poll(input.job_id, timeout=input.timeout, tail=input.tail)

    if result is None:
        raise ToolError(f"Job '{input.job_id}' not found or already finished")

    if result.status == JobStatus.RUNNING and not input.detach:
        mgr.cancel_and_tail(result, input.tail)

    # Audit outcome
    if result.status == JobStatus.RUNNING:
        instance.audit("job.running", job_id=result.job_id,
                       detached=input.detach, poll_timeout=input.timeout,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)
    elif result.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        instance.audit(f"job.{result.status.value}", job_id=result.job_id,
                       exit_code=result.exit_code,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)

    return result.to_dict()


class ListJobsInput(BaseModel):
    instance_id: str = Field(..., description="Instance ID")


@mcp.tool(annotations={"readOnlyHint": True})
async def list_jobs(input: ListJobsInput, ctx: Context) -> list:
    """List all tracked jobs for an instance."""
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    mgr = _get_manager(ctx, instance)
    return mgr.list_jobs()


class CancelJobInput(BaseModel):
    instance_id: str = Field(..., description="Instance ID")
    job_id: str = Field(..., description="Job ID to cancel")
    tail: int = Field(
        default=50,
        description="Number of tail lines to return from output",
        ge=0,
        le=1000,
    )


@mcp.tool()
async def cancel_job(input: CancelJobInput, ctx: Context) -> dict:
    """Cancel a running job and return its final status and output tail."""
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    mgr = _get_manager(ctx, instance)
    ok = mgr.cancel(input.job_id)
    if ok:
        instance.audit("job.cancelled", job_id=input.job_id)

    # Poll after cancel to capture final exit code and tail output
    result = mgr.poll(input.job_id, tail=input.tail)
    if result is not None:
        return result.to_dict()
    return {"job_id": input.job_id, "cancelled": ok}


# ── Entry point ────────────────────────────────────────────────────────────


def run_server(config_path: str | None = None) -> None:
    """Start the MCP server over stdio.

    This is the entry point called from the CLI.

    Args:
        config_path: Optional override for the config.toml path.
    """
    mcp.run(transport="stdio")
