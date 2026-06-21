"""MCP server for ns-hpc — sandboxed command execution and instance management."""
from __future__ import annotations

import asyncio
import fnmatch
import json
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable

import logging
from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from fastmcp.resources import FileResource
from fastmcp.tools import FunctionTool
from fastmcp.tools.base import ToolResult
from mcp.types import TextContent
from pydantic import BaseModel, Field

from ns_hpc import _enable_debug_logging
from ns_hpc.config import Config, HostCommand, ProxiedMCP, load_config
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


def _tool_result(config: Config, text: str, structured: dict[str, Any]) -> ToolResult:
    """Format first-party tool results according to the MCP result config."""
    if config.mcp.result_type == "structured":
        return ToolResult(content=[], structured_content=structured)
    if config.mcp.result_type == "both":
        return ToolResult(content=text, structured_content=structured)
    return ToolResult(content=text)


def _register_context_resources(server: FastMCP, config: Config, config_path: str | None = None) -> None:
    """Scan context directories and register matching files as static resources.

    Relative context dirs are resolved from the config file's parent directory
    so that the config is self-contained regardless of CWD.
    """
    config_dir = Path(config_path).resolve().parent if config_path else Path.cwd()
    patterns = config.resource.resource_patterns
    for raw_dir in config.resource.context_dirs:
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
        instance = Instance.load(instance_id, config)
        if instance is None:
            raise ToolError(f"Instance '{instance_id}' not found")

        client = pm.get_or_start(proxy_name, instance_id, cfg, config)
        is_new_connection = not client.is_connected

        # Connect (if not already connected) and audit
        try:
            await client.ensure_connected()
        except Exception as e:
            instance.audit(
                "proxy.connection.failed",
                proxy_name=proxy_name,
                command=cfg.command,
                error=str(e),
            )
            raise

        if is_new_connection:
            instance.audit(
                "proxy.connected",
                proxy_name=proxy_name,
                command=cfg.command,
            )

        # Audit call start
        instance.audit(
            "proxy.call.started",
            proxy_name=proxy_name,
            tool_name=remote_name,
            arguments=kwargs,
        )

        # Execute the proxied call
        try:
            result = await client.call_tool(remote_name, kwargs)
        except Exception as e:
            instance.audit(
                "proxy.call.failed",
                proxy_name=proxy_name,
                tool_name=remote_name,
                error=str(e),
            )
            raise

        instance.audit(
            "proxy.call.completed",
            proxy_name=proxy_name,
            tool_name=remote_name,
        )

        texts = [
            c.text for c in (result.content or [])
            if isinstance(c, TextContent)
        ]
        return "\n".join(texts) if texts else str(result)
    return handler


def _filter_tools(
    proxy_name: str,
    cfg: ProxiedMCP,
    tools: list,
    logger: Any = None,
) -> list:
    """Apply include/exclude glob patterns to a list of discovered MCP tools.

    - If *include* is non-empty, only tools matching at least one include
      pattern are kept.
    - If *exclude* is non-empty, tools matching any exclude pattern are
      removed.
    - If both are set, a tool must match an include pattern AND not match
      any exclude pattern.
    - If both are empty, all tools pass through.
    """
    if cfg.include or cfg.exclude:
        before = {t.name for t in tools}

    kept = tools
    if cfg.include:
        kept = [t for t in kept if any(
            fnmatch.fnmatch(t.name, pat) for pat in cfg.include
        )]
    if cfg.exclude:
        kept = [t for t in kept if not any(
            fnmatch.fnmatch(t.name, pat) for pat in cfg.exclude
        )]

    if cfg.include or cfg.exclude:
        after = {t.name for t in kept}
        dropped = before - after
        if dropped and logger:
            logger.info(
                "proxied MCP %r filtered: dropped %s, kept %s "
                "(include=%s, exclude=%s)",
                proxy_name,
                sorted(dropped),
                sorted(after),
                cfg.include,
                cfg.exclude,
            )

    return kept


async def _register_proxied_tools(server: FastMCP, config: Config) -> ProxyManager:
    """Discover tools from each proxied MCP and register wrapped FunctionTools."""
    pm = ProxyManager()

    for proxy_name, proxy_cfg in config.proxied_mcps.items():
        remote_tools = await discover_tools(proxy_cfg, config)
        if not remote_tools:
            logger = __import__("logging").getLogger("ns-hpc")
            logger.warning("no tools discovered for proxied MCP %r, skipping", proxy_name)
            continue

        # Apply allow/deny filtering
        remote_tools = _filter_tools(
            proxy_name, proxy_cfg, remote_tools,
            logger=__import__("logging").getLogger("ns-hpc"),
        )
        if not remote_tools:
            continue

        for remote_tool in remote_tools:
            orig_props = dict(remote_tool.inputSchema.get("properties", {}))
            orig_required = list(remote_tool.inputSchema.get("required", []))

            combined_schema = {
                "type": "object",
                "properties": {
                    "instance_id": {"type": "string", "description": "Sandbox instance ID"},
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
                output_schema=None,
                annotations=remote_tool.annotations,
            )
            server.add_tool(ft)

    return pm



def _mount_dav(server: FastMCP, config: Config) -> None:
    """Mount a WebDAV app at /dav/ for direct file access."""
    import copy
    from wsgidav.wsgidav_app import WsgiDAVApp, DEFAULT_CONFIG
    from starlette.routing import Mount
    from ns_hpc.file_server import SandboxDavProvider, PooledWSGIApp

    provider = SandboxDavProvider(config)
    dav_cfg = copy.deepcopy(DEFAULT_CONFIG)
    dav_cfg.update({
        "provider_mapping": {"/": provider},
        "simple_dc": {"user_mapping": {"*": True}},
        "verbose": 1,
        "dir_browser": {"enable": True},
        "http_authenticator": {
            "domain_controller": None,
            "accept_basic": False,
            "accept_digest": False,
        },
        "mount_path": "/dav",
    })
    dav_app = WsgiDAVApp(dav_cfg)
    asgi_app = PooledWSGIApp(dav_app, max_workers=10)
    server._additional_http_routes.append(Mount("/dav", app=asgi_app, name="dav"))
    logger = logging.getLogger("ns-hpc")
    logger.info("WebDAV mounted at /dav/")

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
    instructions="HPC sandboxing via bubblewrap — manage instances and execute commands in isolated bwrap namespaces. Detailed usage instructions are available as resources.",
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


@mcp.tool(output_schema=None)
async def create_instance(input: CreateInstanceInput, ctx: Context) -> ToolResult:
    """Create a new sandbox instance with a persistent workspace directory."""
    context: ServerContext = ctx.lifespan_context

    try:
        instance = Instance.create(input.instance_id, context.config, input.description)
    except FileExistsError:
        raise ToolError(f"Instance '{input.instance_id}' already exists")

    return _tool_result(
        context.config,
        f"Instance '{input.instance_id}' created.",
        {
            "instance_id": instance.id,
            "created": True,
            "description": input.description,
        },
    )


class ListInstancesInput(BaseModel):
    """Input for the list_instances tool."""


@mcp.tool(annotations={"readOnlyHint": True}, output_schema=None)
async def list_instances(input: ListInstancesInput, ctx: Context) -> ToolResult:
    """List all active (non-archived) sandbox instances."""
    context: ServerContext = ctx.lifespan_context

    instances = Instance.list_instances(context.config)
    if not instances:
        return _tool_result(
            context.config,
            "No instances found.",
            {"total": 0, "instances": []},
        )

    lines = []
    structured_instances = []
    for inst in instances:
        try:
            meta = json.loads(inst.metadata_path.read_text())
            created = meta.get("created_at", "unknown")[:19]
            desc = meta.get("description", "")
            label = f"{inst.id:20s}  created: {created}"
            if desc:
                label += f"  [{desc[:50]}]"
            lines.append(label)
            structured_instances.append({
                "instance_id": inst.id,
                "created_at": meta.get("created_at"),
                "description": desc,
            })
        except Exception:
            lines.append(f"{inst.id:20s}  created: unknown")
            structured_instances.append({
                "instance_id": inst.id,
                "created_at": None,
                "description": "",
            })

    return _tool_result(
        context.config,
        "\n".join(lines),
        {"total": len(structured_instances), "instances": structured_instances},
    )


class ListArchivedInstancesInput(BaseModel):
    """Input for the list_archived_instances tool."""


@mcp.tool(annotations={"readOnlyHint": True}, output_schema=None)
async def list_archived_instances(input: ListArchivedInstancesInput, ctx: Context) -> ToolResult:
    """List all archived sandbox instances."""
    context: ServerContext = ctx.lifespan_context

    archived = Instance.list_archived_instances(context.config)
    if not archived:
        return _tool_result(
            context.config,
            "No archived instances found.",
            {"total": 0, "instances": []},
        )

    lines = []
    for entry in archived:
        label = f"{entry['instance_id']:20s}"
        if entry["archived_at"]:
            label += f"  archived: {entry['archived_at'][:19]}"
        if entry["created_at"]:
            label += f"  created: {entry['created_at'][:19]}"
        if entry["description"]:
            label += f"  [{entry['description'][:50]}]"
        lines.append(label)

    return _tool_result(
        context.config,
        "\n".join(lines),
        {"total": len(archived), "instances": archived},
    )


class ArchiveInstanceInput(BaseModel):
    """Input for the archive_instance tool."""
    instance_id: str = Field(
        ...,
        description="ID of the instance to archive",
    )


@mcp.tool(output_schema=None)
async def archive_instance(input: ArchiveInstanceInput, ctx: Context) -> ToolResult:
    """Archive a sandbox instance, disabling new job submissions."""
    context: ServerContext = ctx.lifespan_context

    instance = Instance.load(input.instance_id, context.config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    context.job_managers.pop(input.instance_id, None)
    await context.proxy_manager.stop_all(input.instance_id)
    try:
        instance.archive(context.config)
    except RuntimeError as e:
        raise ToolError(str(e))
    return _tool_result(
        context.config,
        f"Instance '{input.instance_id}' archived.",
        {"instance_id": input.instance_id, "archived": True},
    )


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


@mcp.tool(output_schema=None)
async def update_instance(input: UpdateInstanceInput, ctx: Context) -> ToolResult:
    """Update or get an instance's metadata (currently only description)."""
    context: ServerContext = ctx.lifespan_context

    instance = Instance.load(input.instance_id, context.config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    if input.description is not None:
        instance.set_description(input.description)

    desc = instance.get_description()
    return _tool_result(
        context.config,
        f"Instance '{input.instance_id}': description='{desc}'",
        {"instance_id": input.instance_id, "description": desc},
    )


# ── Job execution ─────────────────────────────────────────────────────────


class SubmitJobInput(BaseModel):
    instance_id: str = Field(..., description="Existing instance ID")
    command: str = Field(..., description="Shell command to run inside the bwrap sandbox")
    mode: str = Field(
        default="local",
        description="Execution mode: 'local' (run at HPC login node) or 'slurm' (submitted via sbatch)",
    )
    timeout: int = Field(
        default=60,
        description="Max seconds to wait for completion",
        ge=1,
        le=86400,
    )
    detach: bool = Field(
        default=True,
        description="If True, keep job running past timeout instead of killing. Use ``poll_job`` to check on it later.",
    )
    tail: int = Field(
        default=50,
        description="Number of tail lines to return from output",
        ge=0,
        le=1000,
    )
    slurm_resources: dict[str, int] | None = Field(
        default=None,
        description="Per-job resource overrides for Slurm (e.g. {'cpus': 4, 'memory': 8192}) — integers only. See resource document for cluster-specific required fields.",
    )


def _cap_timeout(timeout: int, config: Config) -> tuple[int, str]:
    """Cap *timeout* at ``config.jobs.max_timeout`` and return ``(capped, msg)``."""
    cap = config.jobs.max_timeout
    if timeout > cap:
        return cap, f"timeout capped to {cap}s by server max_timeout"
    return timeout, ""


@mcp.tool(output_schema=None)
async def submit_job(input: SubmitJobInput, ctx: Context) -> ToolResult:
    """Submit a command as an async (non-blocking) job."""
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    timeout, cap_msg = _cap_timeout(input.timeout, config)

    mgr = _get_manager(ctx, instance)
    instance.audit("job.submitted", command=input.command, mode=input.mode,
                   timeout=timeout)

    # Use the async API so the event loop stays responsive
    result = await mgr.submit(
        input.command,
        mode=input.mode,
        timeout=timeout,
        tail=input.tail,
        slurm_resources=input.slurm_resources,
    )

    # Handle detach: if still running after timeout, kill it
    if result.status == JobStatus.RUNNING and not input.detach:
        await mgr.cancel(result.job_id)
        result = await mgr.poll(result.job_id, tail=input.tail)

    # Audit outcome
    if result.status == JobStatus.RUNNING:
        instance.audit("job.running", job_id=result.job_id,
                       command=input.command, mode=input.mode,
                       detached=input.detach, timeout=timeout,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)
    else:
        instance.audit(f"job.{result.status.value}", job_id=result.job_id,
                       exit_code=result.exit_code, command=input.command,
                       mode=input.mode,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)

    d = result.to_dict()
    if cap_msg:
        d["timeout_capped"] = True
        existing = d.get("message", "")
        d["message"] = f"{cap_msg}. {existing}".strip() if existing else cap_msg

    summary = f"Job {d['job_id']}: {d['status']}"
    if d.get("exit_code") is not None:
        summary += f" (exit={d['exit_code']})"
    if d.get("duration"):
        summary += f" in {d['duration']}s"
    if d.get("message"):
        summary += f" — {d['message']}"
    return _tool_result(config, summary, d)


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


@mcp.tool(output_schema=None)
async def poll_job(input: PollJobInput, ctx: Context) -> ToolResult:
    """Poll a running job.  Optionally wait for completion."""
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    timeout, cap_msg = _cap_timeout(input.timeout, config)

    mgr = _get_manager(ctx, instance)
    result = await mgr.poll(input.job_id, timeout=timeout, tail=input.tail)

    if result is None:
        raise ToolError(f"Job '{input.job_id}' not found or already finished")

    if result.status == JobStatus.RUNNING and not input.detach:
        await mgr.cancel(input.job_id)
        result = await mgr.poll(input.job_id, tail=input.tail)

    # Audit outcome
    if result.status == JobStatus.RUNNING:
        instance.audit("job.running", job_id=result.job_id,
                       detached=input.detach, poll_timeout=timeout,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)
    elif result.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        instance.audit(f"job.{result.status.value}", job_id=result.job_id,
                       exit_code=result.exit_code,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)

    d = result.to_dict()
    if cap_msg:
        d["timeout_capped"] = True
        existing = d.get("message", "")
        d["message"] = f"{cap_msg}. {existing}".strip() if existing else cap_msg

    summary = f"Job {d['job_id']}: {d['status']}"
    if d.get("exit_code") is not None:
        summary += f" (exit={d['exit_code']})"
    if d.get("duration"):
        summary += f" in {d['duration']}s"
    if d.get("message"):
        summary += f" — {d['message']}"
    return _tool_result(config, summary, d)


class ListJobsInput(BaseModel):
    instance_id: str = Field(..., description="Instance ID")
    limit: int = Field(default=15, description="Max jobs to return", ge=1, le=500)
    offset: int = Field(default=0, description="Jobs to skip", ge=0)


@mcp.tool(annotations={"readOnlyHint": True}, output_schema=None)
async def list_jobs(input: ListJobsInput, ctx: Context) -> ToolResult:
    """List tracked jobs for an instance, newest first, with pagination."""
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    mgr = _get_manager(ctx, instance)
    all_jobs = mgr.list_jobs()
    if not all_jobs:
        return _tool_result(
            config,
            "No jobs found for this instance.",
            {"total": 0, "jobs": []},
        )

    # Sort newest first (created_at may be missing for legacy entries)
    all_jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)

    total = len(all_jobs)
    page = all_jobs[input.offset : input.offset + input.limit]
    first = input.offset + 1
    last = min(input.offset + len(page), total)

    lines = [f"Jobs {first}-{last} of {total} (limit={input.limit}, offset={input.offset})"]
    for job in page:
        line = f"{job['job_id']}: {job['status']}"
        if job.get("mode"):
            line += f" [{job['mode']}]"
        if job.get("created_at"):
            line += f" created: {job['created_at']}"
        if job.get("command"):
            line += f" — {job['command']}"
        lines.append(line)
    return _tool_result(config, "\n".join(lines), {"total": total, "jobs": page})


class CancelJobInput(BaseModel):
    instance_id: str = Field(..., description="Instance ID")
    job_id: str = Field(..., description="Job ID to cancel")
    tail: int = Field(
        default=50,
        description="Number of tail lines to return from output",
        ge=0,
        le=1000,
    )


@mcp.tool(output_schema=None)
async def cancel_job(input: CancelJobInput, ctx: Context) -> ToolResult:
    """Cancel a running job and return its final status and output tail."""
    config: Config = ctx.lifespan_context.config

    instance = Instance.load(input.instance_id, config)
    if instance is None:
        raise ToolError(f"Instance '{input.instance_id}' not found")

    mgr = _get_manager(ctx, instance)
    ok = await mgr.cancel(input.job_id)
    if ok:
        instance.audit("job.cancelled", job_id=input.job_id)

    # Poll after cancel to capture final exit code and tail output
    result = await mgr.poll(input.job_id, tail=input.tail)
    if result is not None:
        d = result.to_dict()
        summary = f"Job {d['job_id']} cancelled: {d['status']}"
        if d.get("exit_code") is not None:
            summary += f" (exit={d['exit_code']})"
        return _tool_result(config, summary, d)
    return _tool_result(
        config,
        f"Job {input.job_id} cancellation requested (cancelled={'yes' if ok else 'no'}).",
        {"job_id": input.job_id, "cancelled": ok},
    )



# ── Host command execution ─────────────────────────────────────────────────


class HostExecInput(BaseModel):
    command: str | None = Field(
        default=None,
        description="Config key of the host command to run. Omit to list available commands.",
    )


async def _run_host_cmd(cfg: HostCommand) -> dict[str, Any]:
    """Run a configured host command and return stdout/stderr/exit/elapsed."""
    start = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        "sh", "-c", cfg.command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=cfg.timeout,
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise ToolError(
            f"Host command timed out after {cfg.timeout}s: {cfg.command!r}"
        )
    elapsed = round(time.monotonic() - start, 2)
    return {
        "stdout": stdout_bytes.decode(errors="replace").strip(),
        "stderr": stderr_bytes.decode(errors="replace").strip(),
        "exit_code": proc.returncode,
        "elapsed": elapsed,
    }


@mcp.tool(output_schema=None)
async def host_exec(input: HostExecInput, ctx: Context) -> ToolResult:
    """Run a pre-configured host command or list available commands.

    With no argument, returns the list of configured host commands and
    their descriptions.  With a ``command`` key, runs the matching
    host command outside any sandbox (directly on the host).
    """
    config: Config = ctx.lifespan_context.config
    cmds = config.host_commands

    if input.command is None:
        if not cmds:
            return _tool_result(
                config,
                "No host commands configured.",
                {"commands": {}},
            )
        lines = ["Available host commands:"]
        for key, cfg in sorted(cmds.items()):
            desc = f" — {cfg.description}" if cfg.description else ""
            lines.append(f"  {key}{desc}")
        return _tool_result(
            config,
            "\n".join(lines),
            {
                "commands": {
                    key: {"description": cfg.description, "command": cfg.command}
                    for key, cfg in cmds.items()
                },
            },
        )

    cfg = cmds.get(input.command)
    if cfg is None:
        available = ", ".join(sorted(cmds.keys())) if cmds else "(none configured)"
        raise ToolError(
            f"Unknown host command {input.command!r}. "
            f"Available: {available}"
        )

    result = await _run_host_cmd(cfg)
    summary = f"host:{input.command} exit={result['exit_code']} in {result['elapsed']}s"
    if result["stdout"]:
        summary += f"\n{result['stdout']}"
    if result["stderr"]:
        summary += f"\n[stderr] {result['stderr']}"
    return _tool_result(config, summary, result)


# ── Entry point ────────────────────────────────────────────────────────────




def run_server(
    config_path: str | None = None,
    transport: str = "stdio",
    host: str = "127.0.0.1",
    port: int = 8000,
    uds: str | None = None,
    path: str = "/mcp",
) -> None:
    """Start the MCP server.

    This is the entry point called from the CLI.

    Args:
        config_path: Optional override for the config.toml path.
        transport: Transport protocol — "stdio", "streamable-http", or "sse".
        host: HTTP host (ignored for stdio and UDS).
        port: HTTP port (ignored for stdio and UDS).
        uds: Unix Domain Socket path — overrides host/port when set.
        path: HTTP endpoint path.
    """
    _enable_debug_logging()

    if transport not in ("stdio", "streamable-http", "sse"):
        raise ValueError(
            f"Unknown transport: {transport!r}. "
            f"Must be one of: stdio, streamable-http, sse"
        )

    if transport != "stdio":
        # Load the config before creating the HTTP app so we can register
        # additional routes (e.g. DAV) before FastMCP snapshots the route table.
        cfg = load_config(config_path)
        if cfg.dav.enabled:
            _mount_dav(mcp, cfg)

        mcp.run(
            transport=transport,
            host=host,
            port=port,
            path=path,
            uvicorn_config={"uds": uds} if uds else None,
        )
    else:
        mcp.run(transport="stdio")
