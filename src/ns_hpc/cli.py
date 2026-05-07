import os
import sys

import typer

from ns_hpc.cli_impl import clean_instances, run_doctor
from ns_hpc.config import load_config
from ns_hpc.instance import Instance
from ns_hpc.namespace import build_bwrap_args, run_in_sandbox

app = typer.Typer()


@app.command()
def run(
    port: int = typer.Option(8000, "--port", "-p", help="Port for the MCP server"),
):
    """Start the MCP server."""
    from ns_hpc.server import run_server

    run_server(port=port)


@app.command()
def enter(
    instance_id: str = typer.Argument(help="Instance ID to enter"),
):
    """Start an interactive bash shell inside a sandbox instance."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg) or Instance.create(instance_id, cfg)

    argv = build_bwrap_args(
        command=["/bin/bash", "-i"],
        workspace_host_path=str(inst.workspace_dir),
        config=cfg,
    )
    os.execvp("bwrap", argv)


@app.command()
def exec(
    instance_id: str = typer.Argument(help="Instance ID"),
    command: list[str] = typer.Argument(help="Command and arguments to run"),
):
    """Run a command in a sandbox instance."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg) or Instance.create(instance_id, cfg)

    result = run_in_sandbox(
        command=command,
        workspace_host_path=str(inst.workspace_dir),
        config=cfg,
    )

    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)

    inst.write_audit(" ".join(command), {
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
    })

    raise typer.Exit(code=result.exit_code)


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
