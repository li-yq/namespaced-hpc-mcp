import json
import os
import sys

import typer

from ns_hpc.cli_impl import clean_instances, run_doctor
from ns_hpc.config import load_config
from ns_hpc.instance import Instance
from ns_hpc.namespace import build_bwrap_args, run_in_sandbox

app = typer.Typer()
instance_app = typer.Typer()
app.add_typer(instance_app, name="instance", help="Manage sandbox instances.")


# ── Top-level commands ────────────────────────────────────────────────────


@app.command()
def run(
    port: int = typer.Option(8000, "--port", "-p", help="Port for the MCP server"),
):
    """Start the MCP server."""
    from ns_hpc.server import run_server

    run_server(port=port)


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
def exec(
    instance_id: str = typer.Argument(help="Instance ID"),
    command: list[str] = typer.Argument(help="Command and arguments to run"),
):
    """Run a command in an existing sandbox instance."""
    cfg = load_config()
    inst = Instance.load(instance_id, cfg)

    if inst is None:
        print(f"Error: instance '{instance_id}' not found.", file=sys.stderr)
        raise typer.Exit(code=1)

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
