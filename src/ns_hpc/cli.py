import asyncio
import json
import os
import shutil
import sys

import typer

from ns_hpc import _enable_debug_logging
from ns_hpc.cli_impl import clean_instances, run_doctor
from ns_hpc.config import load_config
from ns_hpc.instance import Instance
from ns_hpc.job_manager import JobManager, JobStatus
from ns_hpc.namespace import build_bwrap_args

app = typer.Typer()
instance_app = typer.Typer()
app.add_typer(instance_app, name="instance", help="Manage sandbox instances.")


def _cap_timeout(timeout: int) -> tuple[int, str]:
    """Cap *timeout* at config.jobs.max_timeout and return ``(capped, msg)``."""
    cfg = load_config()
    cap = cfg.jobs.max_timeout
    if timeout > cap:
        return cap, f"timeout capped to {cap}s by server max_timeout"
    return timeout, ""


@app.callback()
def main(
    config: str | None = typer.Option(
        None, "--config", "-c",
        envvar="NS_HPC_CONFIG",
        help="Path to TOML config file.  Also read from NS_HPC_CONFIG env var.",
    ),
) -> None:
    """ns-hpc — HPC sandboxing via bubblewrap."""
    _enable_debug_logging()
    if config:
        os.environ["NS_HPC_CONFIG"] = config


# ── Top-level commands ────────────────────────────────────────────────────


@app.command()
def run(
    transport: str = typer.Option(
        "stdio", "--transport", "-t",
        help="Transport: stdio, streamable-http, or sse",
    ),
    host: str = typer.Option(
        "127.0.0.1", "--host", "-H",
        help="HTTP host (ignored for stdio and UDS)",
    ),
    port: int = typer.Option(
        8000, "--port", "-p",
        help="HTTP port (ignored for stdio and UDS)",
    ),
    uds: str | None = typer.Option(
        None, "--uds", "-u",
        help="Unix Domain Socket path; overrides host/port",
    ),
    path: str = typer.Option(
        "/mcp", "--path",
        help="HTTP endpoint path",
    ),
):
    """Start the MCP server.

    Supports three transports:

    * **stdio** — Standard I/O (default).  Use with SSH or pipe-based MCP
      clients.

    * **streamable-http** — Streamable HTTP transport.  The recommended
      transport for HTTP-based clients.  Supports SSE polling/resumability
      and both stateful and stateless operation.

    * **sse** — Server-Sent Events transport.  Legacy compatibility;
      prefer streamable-http for new deployments.

    **Unix Domain Sockets**: When --uds is specified, the server binds to
    a Unix socket instead of TCP.  Host and port are ignored.

    Examples:

        ns-hpc run                                         # stdio
        ns-hpc run --transport streamable-http              # HTTP :8000/mcp
        ns-hpc run -t streamable-http -p 9000               # HTTP :9000/mcp
        ns-hpc run -t streamable-http --uds /tmp/mcp.sock   # UDS
        ns-hpc run -t sse --host 0.0.0.0 --port 8080        # SSE
    """
    from ns_hpc.server import run_server

    run_server(
        transport=transport,
        host=host,
        port=port,
        uds=uds,
        path=path,
    )


@app.command()
def doctor():
    """Run system diagnostics."""
    run_doctor()


@app.command()
def clean(
    days: int = typer.Option(7, "--days", "-d", help="Remove instances older than N days"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
):
    """Remove stale instances."""
    clean_instances(days, force)


@app.command()
def bwrap(
    instance_id: str = typer.Argument(
        help="Instance whose workspace will be mounted as the sandbox working directory.",
    ),
    command: list[str] = typer.Argument(
        ...,
        help="Command and arguments to run inside the bwrap sandbox. Use -- to separate ns-hpc args from the command.",
    ),
):
    """Run a command inside a bwrap sandbox with no output redirection or job tracking.

    This is the primitive that backs both local and Slurm job submission.
    The command runs directly inside the sandbox.  The sandbox namespace
    is torn down by the kernel when the outer bwrap process exits.

    Use shell redirect for output capture:

        ns-hpc bwrap my-instance -- ls -la > output.txt

    No --timeout, --slurm, --detach, or --tail options -
    this is a direct pass-through to bwrap.
    """
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    bwrap_path = shutil.which("bwrap")
    if not bwrap_path:
        print("Error: 'bwrap' not found on PATH. Is bubblewrap installed?", file=sys.stderr)
        raise typer.Exit(code=1)

    fd = cfg.namespace.status_fd
    shared_output_root = cfg.resolve_instances_dir() / "output"
    argv = build_bwrap_args(
        command=list(command),
        workspace_host_path=str(inst.workspace_dir),
        config=cfg,
        extra_rw_binds=[(str(inst.output_path), cfg.namespace.output_mount)],
        extra_ro_binds=[(str(shared_output_root), cfg.namespace.shared_output_mount)],
        extra_bwrap_flags=["--json-status-fd", str(fd)],
    )
    os.execvp(bwrap_path, argv)


# ── Instance subcommands ─────────────────────────────────────────────────


@instance_app.command(name="list")
def list_cmd():
    """List all sandbox instances."""
    cfg = load_config()
    instances = Instance.list_instances(cfg)

    if not instances:
        print("No instances found.")
        raise typer.Exit()

    for inst in instances:
        meta = json.loads(inst.metadata_path.read_text())
        created = meta.get("created_at", "unknown")
        print(f"{inst.id:20s}  created: {created}")


@instance_app.command(name="list-archived")
def list_archived_cmd():
    """List all archived instances."""
    cfg = load_config()
    archived = Instance.list_archived_instances(cfg)
    if not archived:
        print("No archived instances found.")
        raise typer.Exit()

    for entry in archived:
        label = entry["instance_id"]
        if entry["archived_at"]:
            label += f"  archived: {entry['archived_at'][:19]}"
        if entry["created_at"]:
            label += f"  created: {entry['created_at'][:19]}"
        print(label)


@instance_app.command()
def create(
    instance_id: str = typer.Argument(help="Unique instance identifier"),
):
    """Create a new sandbox instance."""
    cfg = load_config()
    try:
        inst = Instance.create(instance_id, cfg)
    except FileExistsError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise typer.Exit(code=1)

    print(f"Created instance '{inst.id}' at {inst.base_dir}")


@instance_app.command()
def describe(
    instance_id: str = typer.Argument(help="Instance ID"),
):
    """Show instance metadata."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    meta = json.loads(inst.metadata_path.read_text())
    print(f"ID:          {inst.id}")
    print(f"Created at:  {meta.get('created_at', 'unknown')}")
    print(f"Hostname:    {meta.get('hostname', 'unknown')}")
    print(f"Description: {inst.get_description()}")
    print(f"Workspace:   {inst.workspace_dir}")


@instance_app.command()
def update(
    instance_id: str = typer.Argument(help="Instance ID"),
    description: str = typer.Option("", "--description", "-d", help="New description"),
):
    """Update instance metadata."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    if description:
        inst.set_description(description)

    print(f"Updated instance '{instance_id}'.")
    print(f"Description: {inst.get_description()}")


@instance_app.command()
def run(
    instance_id: str = typer.Argument(help="Instance ID"),
    command: list[str] = typer.Argument(help="Command and arguments"),
    detach: bool = typer.Option(True, "--detach/--no-detach", help="Keep running past timeout"),
    slurm: bool = typer.Option(False, "--slurm", help="Submit via sbatch"),
    timeout: int = typer.Option(60, "--timeout", "-t", help="Max wait in seconds"),
    tail: int = typer.Option(50, "--tail", help="Tail lines to show"),
    slurm_resource: list[str] = typer.Option(
        [], "--slurm-resource", "-r",
        help="Slurm resource (key=value), repeatable (e.g. -r cpus=4 -r memory=8192)",
    ),
):
    """Run a command as an async job. Waits up to --timeout seconds.

    By default (with --detach), if the command exceeds --timeout it keeps
    running and you can poll later with 'ns-hpc instance status'.
    Use --no-detach to kill the job on timeout instead.
    """
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    run_timeout, cap_msg = _cap_timeout(timeout)

    cmd_str = " ".join(command)
    mode_str = "slurm" if slurm else "local"

    # Parse --slurm-resource key=value pairs into dict (integers only)
    parsed_resources: dict[str, int] = {}
    for r in slurm_resource:
        k, _, v = r.partition("=")
        if not k or not v:
            print(f"Error: invalid resource spec '{r}' (use key=value)", file=sys.stderr)
            raise typer.Exit(code=1)
        try:
            parsed_resources[k] = int(v)
        except ValueError:
            print(f"Error: slurm resource values must be integers, got '{v}'", file=sys.stderr)
            raise typer.Exit(code=1)

    async def _do_run():
        mgr = JobManager(inst, cfg)
        result = await mgr.submit(
            cmd_str,
            mode=mode_str,
            timeout=run_timeout,
            tail=tail,
            slurm_resources=parsed_resources or None,
        )

        # Handle detach
        if result.status == JobStatus.RUNNING and not detach:
            await mgr.cancel(result.job_id)
            result = await mgr.poll(result.job_id, tail=tail)

        # Audit outcome
        if result.status == JobStatus.RUNNING:
            inst.audit("job.running", job_id=result.job_id, command=cmd_str,
                       mode=mode_str, detached=True, timeout=run_timeout,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)
        else:
            inst.audit(f"job.{result.status.value}", job_id=result.job_id,
                       exit_code=result.exit_code, command=cmd_str, mode=mode_str,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)

        print(f"Job {result.job_id}: {result.status.value}")
        if cap_msg:
            print(f"Note: {cap_msg}")
        if result.exit_code is not None:
            print(f"Exit code: {result.exit_code}")
        print(f"stdout: {result.stdout_path}")
        print(f"stderr: {result.stderr_path}")

        if result.status == JobStatus.FAILED:
            raise typer.Exit(code=result.exit_code or 1)
        return result

    asyncio.run(_do_run())


@instance_app.command()
def status(
    instance_id: str = typer.Argument(help="Instance ID"),
    job_id: str = typer.Argument(help="Job ID"),
    timeout: int = typer.Option(0, "--timeout", "-t", help="Seconds to wait"),
    detach: bool = typer.Option(True, "--detach/--no-detach", help="Keep running past timeout"),
    tail: int = typer.Option(50, "--tail", help="Tail lines to show"),
):
    """Check the status of a running job."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    poll_timeout, cap_msg = _cap_timeout(timeout)

    async def _do_status():
        mgr = JobManager(inst, cfg)
        result = await mgr.poll(job_id, timeout=poll_timeout, tail=tail)

        if result is None:
            print(f"Job '{job_id}' not found or already finished.")
            raise typer.Exit(code=1)

        if result.status == JobStatus.RUNNING and not detach and poll_timeout > 0:
            await mgr.cancel(result.job_id)
            result = await mgr.poll(result.job_id, tail=tail)

        # Audit outcome
        if result.status == JobStatus.RUNNING:
            inst.audit("job.running", job_id=result.job_id,
                       detached=bool(detach), poll_timeout=poll_timeout,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)
        elif result.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            inst.audit(f"job.{result.status.value}", job_id=result.job_id,
                       exit_code=result.exit_code,
                       stdout_path=result.stdout_path, stderr_path=result.stderr_path)

        print(f"Job {result.job_id}: {result.status.value}")
        if cap_msg:
            print(f"Note: {cap_msg}")
        if result.exit_code is not None:
            print(f"Exit code: {result.exit_code}")
        print(f"stdout: {result.stdout_path}")
        print(f"stderr: {result.stderr_path}")
        return result

    asyncio.run(_do_status())


@instance_app.command()
def jobs_list(
    instance_id: str = typer.Argument(help="Instance ID"),
):
    """List all tracked jobs for an instance."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    async def _do_list():
        mgr = JobManager(inst, cfg)
        jobs = mgr.list_jobs()
        if not jobs:
            print("No running jobs.")
            return
        for j in jobs:
            print(f"{j['job_id']:20s}  {j['status']:12s}  {j['command'][:60]}")

    asyncio.run(_do_list())


@instance_app.command()
def cancel(
    instance_id: str = typer.Argument(help="Instance ID"),
    job_id: str = typer.Argument(help="Job ID"),
):
    """Cancel a running job."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    async def _do_cancel():
        mgr = JobManager(inst, cfg)
        ok = await mgr.cancel(job_id)
        if ok:
            inst.audit("job.cancelled", job_id=job_id)
            result = await mgr.poll(job_id, tail=0)
            print(f"Job {job_id}: cancelled")
            if result:
                print(f"stdout: {result.stdout_path}")
                print(f"stderr: {result.stderr_path}")
        else:
            print(f"Job '{job_id}' not found.")
        return ok

    asyncio.run(_do_cancel())


@instance_app.command()
def archive(
    instance_id: str = typer.Argument(help="Instance ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Archive an instance, disabling new job submissions."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)

    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    if not force:
        confirm = input(f"Archive instance '{instance_id}' and disable new jobs? [y/N] ")
        if confirm.lower() not in ("y", "yes"):
            print("Aborted.")
            return

    try:
        inst.archive(cfg)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise typer.Exit(code=1)

    print(f"Archived instance '{instance_id}'.")


@instance_app.command()
def enter(
    instance_id: str = typer.Argument(help="Instance ID"),
):
    """Start an interactive bash shell inside a sandbox instance."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)

    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    shared_output_root = cfg.resolve_instances_dir() / "output"
    argv = build_bwrap_args(
        command=["/bin/bash", "-i"],
        workspace_host_path=str(inst.workspace_dir),
        config=cfg,
        extra_rw_binds=[(str(inst.output_path), cfg.namespace.output_mount)],
        extra_ro_binds=[(str(shared_output_root), cfg.namespace.shared_output_mount)],
    )
    os.execvp("bwrap", argv)
