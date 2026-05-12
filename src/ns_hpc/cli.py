import json
import os
import sys

import typer

from ns_hpc.cli_impl import clean_instances, run_doctor
from ns_hpc.config import load_config
from ns_hpc.instance import Instance
from ns_hpc.job_manager import JobManager, JobStatus
from ns_hpc.namespace import build_bwrap_args

app = typer.Typer()
instance_app = typer.Typer()
app.add_typer(instance_app, name="instance", help="Manage sandbox instances.")


@app.callback()
def main(
    config: str | None = typer.Option(
        None, "--config", "-c",
        envvar="NS_HPC_CONFIG",
        help="Path to TOML config file.  Also read from NS_HPC_CONFIG env var.",
    ),
) -> None:
    """ns-hpc — HPC sandboxing via bubblewrap."""
    if config:
        os.environ["NS_HPC_CONFIG"] = config


# ── Top-level commands ────────────────────────────────────────────────────


@app.command()
def run():
    """Start the MCP server (stdio)."""
    from ns_hpc.server import run_server

    run_server()


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
    The command runs inside the named instance's workspace, with tini(1) as PID 1
    so that signals are forwarded correctly.

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

    argv = build_bwrap_args(
        command=list(command),
        workspace_host_path=str(inst.workspace_dir),
        config=cfg,
    )
    os.execvp("bwrap", argv)


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
def run(
    instance_id: str = typer.Argument(help="Instance ID"),
    command: list[str] = typer.Argument(help="Command and arguments"),
    detach: bool = typer.Option(False, "--detach", help="Keep running past timeout"),
    slurm: bool = typer.Option(False, "--slurm", help="Submit via sbatch"),
    timeout: int = typer.Option(60, "--timeout", "-t", help="Max wait in seconds"),
    tail: int = typer.Option(50, "--tail", help="Tail lines to show"),
):
    """Run a command as an async job. Waits up to --timeout seconds.

    By default (without --detach), if the command exceeds --timeout
    it is killed.  With --detach, it keeps running and you can poll
    later with 'ns-hpc instance status'.
    """
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    mgr = JobManager(inst, cfg)
    cmd_str = " ".join(command)
    mode_str = "slurm" if slurm else "local"
    inst.audit("job.submitted", command=cmd_str, mode=mode_str, timeout=timeout)
    result = mgr.submit(
        cmd_str,
        mode=mode_str,
        timeout=timeout,
        tail=tail,
    )

    # Handle detach
    if result.status == "running" and not detach:
        mgr.cancel_and_tail(result, tail)

    # Audit outcome
    if result.status == JobStatus.RUNNING:
        inst.audit("job.running", job_id=result.job_id, command=cmd_str,
                   mode=mode_str, detached=True, timeout=timeout,
                   stdout_path=result.stdout_path, stderr_path=result.stderr_path)
    else:
        inst.audit(f"job.{result.status.value}", job_id=result.job_id,
                   exit_code=result.exit_code, command=cmd_str, mode=mode_str,
                   stdout_path=result.stdout_path, stderr_path=result.stderr_path)

    print(f"Job {result.job_id}: {result.status.value}")
    if result.exit_code is not None:
        print(f"Exit code: {result.exit_code}")
    if result.stdout_tail:
        print("--- stdout ---")
        sys.stdout.write(result.stdout_tail)
    if result.stderr_tail:
        print("--- stderr ---")
        sys.stderr.write(result.stderr_tail)
    print(f"---\nstdout: {result.stdout_path}\nstderr: {result.stderr_path}")

    if result.status in ("failed", "timeout"):
        raise typer.Exit(code=result.exit_code or 1)


@instance_app.command()
def status(
    instance_id: str = typer.Argument(help="Instance ID"),
    job_id: str = typer.Argument(help="Job ID"),
    timeout: int = typer.Option(0, "--timeout", "-t", help="Seconds to wait"),
    detach: bool = typer.Option(False, "--detach", help="Keep running past timeout"),
    tail: int = typer.Option(50, "--tail", help="Tail lines to show"),
):
    """Check the status of a running job."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    mgr = JobManager(inst, cfg)
    result = mgr.poll(job_id, timeout=timeout, tail=tail)

    if result is None:
        print(f"Job '{job_id}' not found or already finished.")
        raise typer.Exit(code=1)

    if result.status == "running" and not detach and timeout > 0:
        mgr.cancel_and_tail(result, tail)

    # Audit outcome
    if result.status == JobStatus.RUNNING:
        inst.audit("job.running", job_id=result.job_id,
                   detached=bool(detach), poll_timeout=timeout,
                   stdout_path=result.stdout_path, stderr_path=result.stderr_path)
    elif result.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.TIMEOUT):
        inst.audit(f"job.{result.status.value}", job_id=result.job_id,
                   exit_code=result.exit_code,
                   stdout_path=result.stdout_path, stderr_path=result.stderr_path)

    print(f"Job {result.job_id}: {result.status.value}")
    if result.exit_code is not None:
        print(f"Exit code: {result.exit_code}")
    if result.stdout_tail:
        print("--- stdout ---")
        sys.stdout.write(result.stdout_tail)
    if result.stderr_tail:
        print("--- stderr ---")
        sys.stderr.write(result.stderr_tail)


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

    mgr = JobManager(inst, cfg)
    jobs = mgr.list_jobs()
    if not jobs:
        print("No running jobs.")
        return
    for j in jobs:
        print(f"{j['job_id']:20s}  {j['status']:12s}  {j['command'][:60]}")


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

    mgr = JobManager(inst, cfg)
    ok = mgr.cancel(job_id)
    if ok:
        inst.audit("job.cancelled", job_id=job_id)
        print(f"Cancelled job {job_id}.")
    else:
        print(f"Job '{job_id}' not found.")

@instance_app.command()
def destroy(
    instance_id: str = typer.Argument(help="Instance ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
):
    """Remove an instance and its workspace."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)

    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

    if not force:
        confirm = input(f"Destroy instance '{instance_id}' and all its data? [y/N] ")
        if confirm.lower() not in ("y", "yes"):
            print("Aborted.")
            return

    Instance.destroy(instance_id, cfg)
    print(f"Destroyed instance '{instance_id}'.")


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

    argv = build_bwrap_args(
        command=["/bin/bash", "-i"],
        workspace_host_path=str(inst.workspace_dir),
        config=cfg,
    )
    os.execvp("bwrap", argv)
