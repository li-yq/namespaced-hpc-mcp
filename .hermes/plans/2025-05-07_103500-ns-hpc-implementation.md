# ns-hpc MCP Server — Implementation Plan

> **For Hermes:** Use subagent-driven-development to implement this plan task-by-task.

**Goal:** Build a production-grade MCP server that sandboxes code execution via bwrap, with Slurm support for HPC environments.

**Architecture:** Single-shot bwrap model (no persistent namespaces). MCP server exposes tools for file operations and command execution, all wrapped in bwrap sandboxes. Slurm integration wraps bwrap inside sbatch submissions. MCP proxy (gateway) deferred to v2.

**Tech Stack:** Python 3.14, `uv`, `mcp[cli]` SDK v2 (MCPServer + Lifespan + @mcp.tool()), Typer for CLI, tomli for TOML parsing, Pydantic for config validation

---

## Environment & Prerequisites

- Dev machine: Fedora 44, kernel 6.19, bwrap 0.11.0, podman 5.8.1 (rootless)
- User namespaces: OK (max_user_namespaces=15393, /etc/subuid exists)
- Slurm: not available natively — test via `giovtorres/slurm-docker-cluster` under podman
- No Docker — use podman + podman-compose for the Slurm cluster

---

## Phase 0: Project Scaffolding

### Task 0.1: Initialize project structure

**Objective:** Create the project skeleton with uv, install dependencies, set up directory tree.

**Files:**
- Create: `pyproject.toml` (uv-managed)
- Create: `src/ns_hpc/__init__.py`
- Create: `src/ns_hpc/__main__.py`
- Create: `src/ns_hpc/config.py`
- Create: `src/ns_hpc/cli.py`
- Create: `src/ns_hpc/namespace.py`
- Create: `src/ns_hpc/instance.py`
- Create: `src/ns_hpc/task_engine.py`
- Create: `src/ns_hpc/server.py`
- Create: `context/README.md`
- Create: `config.toml`

**Step 1: uv init**

```bash
cd /home/liyq/workspace/ns-hpc-mcp
uv init --lib --name ns-hpc
```

**Step 2: Add dependencies**

```bash
uv add "mcp[cli]" typer tomli pydantic
uv add --dev pytest pytest-asyncio
```

**Step 3: Create directory tree**

```bash
mkdir -p src/ns_hpc context tests
touch src/ns_hpc/__init__.py
touch src/ns_hpc/__main__.py
touch src/ns_hpc/config.py
touch src/ns_hpc/cli.py
touch src/ns_hpc/namespace.py
touch src/ns_hpc/instance.py
touch src/ns_hpc/task_engine.py
touch src/ns_hpc/server.py
touch context/README.md
```

**Step 4: Verify import**

```bash
uv run python -c "import ns_hpc; print('OK')"
```

---

### Task 0.2: bwrap smoke test — JSON status FD

**Objective:** Verify that bwrap's `--json-status-fd` works correctly for capturing exit codes from inside the sandbox. This is the critical low-level primitive.

**Files:**
- Create: `tests/test_bwrap_primitive.py`

**Step 1: Write smoke test**

```python
"""Test bwrap JSON status FD primitive."""
import subprocess
import json
import os
import tempfile

def test_bwrap_json_status_fd():
    """Verify bwrap --json-status-fd returns correct exit code."""
    with tempfile.TemporaryDirectory() as tmpdir:
        r_fd, w_fd = os.pipe()
        try:
            proc = subprocess.Popen(
                [
                    "bwrap",
                    "--ro-bind", "/usr", "/usr",
                    "--ro-bind", "/lib", "/lib",
                    "--ro-bind", "/lib64", "/lib64",
                    "--ro-bind", "/bin", "/bin",
                    "--ro-bind", "/sbin", "/sbin",
                    "--proc", "/proc",
                    "--dev", "/dev",
                    "--tmpfs", "/tmp",
                    "--bind", tmpdir, "/workspace",
                    "--unshare-all",
                    "--share-net",
                    "--json-status-fd", str(w_fd),
                    "--", "/bin/sh", "-c", "exit 42",
                ],
                pass_fds=(w_fd,),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            os.close(w_fd)
            stdout, stderr = proc.communicate()
            os.close(r_fd)
            json_data = os.read(r_fd, 4096)
            status = json.loads(json_data)
            assert proc.returncode == 0, f"bwrap exited {proc.returncode}: {stderr.decode()}"
            assert status["exit-code"] == 42, f"Expected exit-code 42, got {status}"
        finally:
            # Clean up pipe fds if still open
            try:
                os.close(r_fd)
            except OSError:
                pass
            try:
                os.close(w_fd)
            except OSError:
                pass

def test_bwrap_workspace_isolation():
    """Verify bwrap sandbox cannot access files outside workspace."""
    outer_file = "/tmp/ns_hpc_secret_test"
    try:
        with open(outer_file, "w") as f:
            f.write("should_not_be_seen")

        with tempfile.TemporaryDirectory() as tmpdir:
            proc = subprocess.Popen(
                [
                    "bwrap", "--ro-bind", "/usr", "/usr",
                    "--ro-bind", "/bin", "/bin",
                    "--ro-bind", "/lib", "/lib",
                    "--ro-bind", "/lib64", "/lib64",
                    "--bind", tmpdir, "/workspace",
                    "--proc", "/proc", "--dev", "/dev",
                    "--tmpfs", "/tmp",
                    "--unshare-all", "--share-net",
                    "--json-status-fd", str(w_fd),
                    "--", "/bin/sh", "-c",
                    f"test -f /tmp/ns_hpc_secret_test && echo FOUND || echo SAFE",
                ],
                pass_fds=(w_fd,),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            os.close(w_fd)
            stdout, _ = proc.communicate()
            os.close(r_fd)
            json_data = os.read(r_fd, 4096)
            assert stdout.decode().strip() == "SAFE", "Sandbox breached!"
    finally:
        os.unlink(outer_file)
```

**Step 3: Run test**

```bash
uv run pytest tests/test_bwrap_primitive.py -v
```

Expected: both tests pass. If not, adjust bwrap flags (some distros need --disable-userns or different path bindings).

---

## Phase 1: Configuration System

### Task 1.1: Config data model and loader

**Objective:** Define the TOML config schema and implement a Pydantic-based loader.

**Files:**
- Create: `config.toml` (default config)
- Modify: `src/ns_hpc/config.py`

**Step 1: Write default config.toml**

```toml
# ns-hpc Configuration
# Paths are resolved relative to this file's directory unless absolute.

[namespace_defaults]
# System paths to bind ro (auto-detected if empty)
bind_ro = ["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc"]
# Workspace mount point inside the sandbox
workspace_mount = "/workspace"
# Default bwrap flags
flags = [
    "--unshare-all",
    "--share-net",
    "--proc", "/proc",
    "--dev", "/dev",
    "--tmpfs", "/tmp",
]

[proxied_mcps]
# v2: MCP proxy servers to spawn inside sandbox instances
# [proxied_mcps.filesystem]
# command = "mcp-server-filesystem"
# args = ["/workspace"]

[resource_defaults]
# Directories to scan for context/ resource files
context_dirs = ["context"]
# Glob patterns for resource discovery
resource_patterns = ["*.md"]
```

**Step 2: Implement Config loader**

In `src/ns_hpc/config.py`:

```python
"""Configuration loader for ns-hpc."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class NamespaceDefaults(BaseModel):
    bind_ro: list[str] = Field(
        default_factory=lambda: ["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc"]
    )
    workspace_mount: str = "/workspace"
    flags: list[str] = Field(
        default_factory=lambda: [
            "--unshare-all",
            "--share-net",
            "--proc", "/proc",
            "--dev", "/dev",
            "--tmpfs", "/tmp",
        ]
    )


class ProxiedMCP(BaseModel):
    command: str
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)


class ResourceDefaults(BaseModel):
    context_dirs: list[str] = Field(default_factory=lambda: ["context"])
    resource_patterns: list[str] = Field(default_factory=lambda: ["*.md"])


class Config(BaseModel):
    namespace_defaults: NamespaceDefaults = Field(default_factory=NamespaceDefaults)
    proxied_mcps: dict[str, ProxiedMCP] = Field(default_factory=dict)
    resource_defaults: ResourceDefaults = Field(default_factory=ResourceDefaults)

    # Top-level: instance storage path
    instances_dir: str = Field(default="${HOME}/mcp_instances")

    def resolve_instances_dir(self) -> Path:
        expanded = os.path.expandvars(self.instances_dir)
        return Path(expanded).expanduser().resolve()


def load_config(path: Optional[str] = None) -> Config:
    """Load config from TOML file. Falls back to env var or default path."""
    from tomli import load

    if path is None:
        path = os.environ.get("NS_HPC_CONFIG", "config.toml")

    config_path = Path(path)
    if not config_path.exists():
        return Config()

    with open(config_path, "rb") as f:
        data = load(f)

    return Config.model_validate(data)
```

**Step 3: Write tests**

```python
# tests/test_config.py
from ns_hpc.config import load_config, Config
import tempfile
from pathlib import Path

def test_default_config():
    cfg = load_config("/nonexistent/path.toml")
    assert isinstance(cfg, Config)
    assert "/usr" in cfg.namespace_defaults.bind_ro
    assert cfg.namespace_defaults.workspace_mount == "/workspace"

def test_config_from_toml():
    toml_content = """
[namespace_defaults]
bind_ro = ["/usr", "/custom"]
workspace_mount = "/sandbox"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        f.write(toml_content)
        tmppath = f.name
    try:
        cfg = load_config(tmppath)
        assert cfg.namespace_defaults.workspace_mount == "/sandbox"
        assert "/custom" in cfg.namespace_defaults.bind_ro
    finally:
        Path(tmppath).unlink()
```

**Step 4: Run tests**

```bash
uv run pytest tests/test_config.py -v
```

---

## Phase 2: Core Namespace Engine

### Task 2.1: bwrap argument builder

**Objective:** Build the complete bwrap argv from config + command + workspace path.

**Files:**
- Modify: `src/ns_hpc/namespace.py`

**Implementation:**

```python
"""bwrap namespace engine — argument builder and process runner."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Optional

from ns_hpc.config import Config


def build_bwrap_args(
    config: Config,
    workspace_host_path: Path,
    command: list[str],
    *,
    extra_ro_binds: list[tuple[str, str]] | None = None,
    extra_rw_binds: list[tuple[str, str]] | None = None,
    extra_bwrap_flags: list[str] | None = None,
    working_dir: str | None = None,
) -> list[str]:
    """Build bwrap argv that creates a sandbox for the given command.

    Every invocation creates a fresh sandbox — no persistent namespaces.

    Args:
        config: Loaded ns-hpc configuration
        workspace_host_path: Host path to the instance workspace
        command: The command to run inside the sandbox
        extra_ro_binds: Additional --ro-bind pairs (host, guest)
        extra_rw_binds: Additional --bind pairs (host, guest)
        extra_bwrap_flags: Additional bwrap flags
        working_dir: Working directory inside the sandbox (default: workspace_mount)

    Returns:
        argv list ready for subprocess.Popen
    """
    argv = ["bwrap"]

    # Default flags from config
    argv.extend(config.namespace_defaults.flags)

    # Read-only system paths
    for syspath in config.namespace_defaults.bind_ro:
        argv.extend(["--ro-bind", syspath, syspath])

    # Extra read-only binds
    if extra_ro_binds:
        for host, guest in extra_ro_binds:
            argv.extend(["--ro-bind", host, guest])

    # Workspace bind (read-write)
    ws_mount = config.namespace_defaults.workspace_mount
    argv.extend(["--bind", str(workspace_host_path), ws_mount])

    # Extra read-write binds
    if extra_rw_binds:
        for host, guest in extra_rw_binds:
            argv.extend(["--bind", host, guest])

    # Extra flags
    if extra_bwrap_flags:
        argv.extend(extra_bwrap_flags)

    # Working directory
    if working_dir:
        argv.extend(["--chdir", working_dir])
    else:
        argv.extend(["--chdir", ws_mount])

    # The command
    argv.append("--")
    argv.extend(command)

    return argv
```

**Write tests:**

```python
# tests/test_namespace.py
from ns_hpc.namespace import build_bwrap_args
from ns_hpc.config import Config
from pathlib import Path

def test_build_bwrap_args_basic():
    cfg = Config()
    ws = Path("/tmp/test_workspace")
    args = build_bwrap_args(cfg, ws, ["/bin/sh", "-c", "echo hi"])
    assert args[0] == "bwrap"
    assert "--unshare-all" in args
    assert "--share-net" in args
    assert "--ro-bind" in args
    assert "--bind" in args
    assert str(ws) in args
    assert "--chdir" in args
    assert "/workspace" in args  # default workspace_mount
    assert args[-3:] == ["--", "/bin/sh", "-c", "echo hi"] or args[-2:] == ["--", "/bin/sh", "-c", "echo hi"]

def test_build_bwrap_args_extra_binds():
    cfg = Config()
    ws = Path("/tmp/test_ws")
    args = build_bwrap_args(cfg, ws, ["ls"], extra_ro_binds=[("/data", "/data")])
    assert "/data" in args
    assert args.count("--ro-bind") > cfg.namespace_defaults.bind_ro.count("/usr") + 1  # +1 for /data
```

---

### Task 2.2: bwrap process runner with JSON status FD

**Objective:** Implement `run_in_sandbox()` that spawns bwrap, captures output, parses JSON status FD, and returns structured results.

**Files:**
- Modify: `src/ns_hpc/namespace.py`

**Implementation (add to namespace.py):**

```python
from dataclasses import dataclass


@dataclass
class SandboxResult:
    """Result of a sandboxed command execution."""
    exit_code: int
    stdout: str
    stderr: str
    sandbox_ok: bool  # True if bwrap itself finished cleanly


def run_in_sandbox(
    argv: list[str],
    *,
    timeout: float | None = None,
    stdin: str | None = None,
    env: dict[str, str] | None = None,
) -> SandboxResult:
    """Run a command inside a bwrap sandbox.

    Uses --json-status-fd to reliably capture the inner command's exit code.
    The JSON status FD is the authoritative source: bwrap itself should always
    return 0 when its inner process runs to completion.

    Args:
        argv: Complete bwrap argv (from build_bwrap_args)
        timeout: Kill the sandbox after this many seconds
        stdin: Text to pipe to the inner command's stdin
        env: Environment variables for bwrap (not the inner process)

    Returns:
        SandboxResult with exit code, stdout, stderr
    """
    r_fd, w_fd = os.pipe()

    try:
        proc = subprocess.Popen(
            argv,
            pass_fds=(w_fd,),
            stdin=subprocess.PIPE if stdin is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        os.close(w_fd)  # Parent closes write end

        stdout_bytes, stderr_bytes = proc.communicate(
            input=stdin.encode() if stdin else None,
            timeout=timeout,
        )

        # Read JSON status from pipe
        json_data = os.read(r_fd, 4096)
        os.close(r_fd)

        status = json.loads(json_data)
        inner_exit_code = status.get("exit-code", -1)
        sandbox_ok = proc.returncode == 0

    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        os.close(r_fd)
        return SandboxResult(
            exit_code=-1,
            stdout="",
            stderr="[ns-hpc] Command timed out after {timeout}s",
            sandbox_ok=False,
        )
    except (OSError, json.JSONDecodeError) as e:
        try:
            os.close(r_fd)
        except OSError:
            pass
        return SandboxResult(
            exit_code=-1,
            stdout="",
            stderr=f"[ns-hpc] bwrap runtime error: {e}",
            sandbox_ok=False,
        )

    return SandboxResult(
        exit_code=inner_exit_code,
        stdout=stdout_bytes.decode(errors="replace"),
        stderr=stderr_bytes.decode(errors="replace"),
        sandbox_ok=sandbox_ok,
    )
```

**Step 2: Write integration test**

```python
# tests/test_namespace.py (append)
import tempfile
from ns_hpc.namespace import build_bwrap_args, run_in_sandbox

def test_run_in_sandbox_basic():
    cfg = Config()
    with tempfile.TemporaryDirectory() as tmpdir:
        args = build_bwrap_args(cfg, Path(tmpdir), ["/bin/sh", "-c", "echo hello_from_sandbox"])
        result = run_in_sandbox(args)
        assert result.sandbox_ok, f"bwrap failed: {result.stderr}"
        assert result.exit_code == 0
        assert "hello_from_sandbox" in result.stdout

def test_run_in_sandbox_exit_code():
    cfg = Config()
    with tempfile.TemporaryDirectory() as tmpdir:
        args = build_bwrap_args(cfg, Path(tmpdir), ["/bin/sh", "-c", "exit 42"])
        result = run_in_sandbox(args)
        assert result.sandbox_ok
        assert result.exit_code == 42

def test_run_in_sandbox_stdin():
    cfg = Config()
    with tempfile.TemporaryDirectory() as tmpdir:
        args = build_bwrap_args(cfg, Path(tmpdir), ["/bin/sh", "-c", "read x; echo $x"])
        result = run_in_sandbox(args, stdin="hello_stdin")
        assert result.sandbox_ok
        assert "hello_stdin" in result.stdout

def test_run_in_sandbox_isolation():
    """Verify sandbox cannot touch host filesystem."""
    outer_file = "/tmp/ns_hpc_secret_test"
    try:
        with open(outer_file, "w") as f:
            f.write("secret_data")
        cfg = Config()
        with tempfile.TemporaryDirectory() as tmpdir:
            args = build_bwrap_args(cfg, Path(tmpdir), ["/bin/sh", "-c", "test -f /tmp/ns_hpc_secret_test && echo EXPOSED || echo SAFE"])
            result = run_in_sandbox(args)
            assert "SAFE" in result.stdout, f"SANDBOX BREACH: {result.stdout}"
    finally:
        Path(outer_file).unlink(missing_ok=True)
```

**Step 3: Run tests**

```bash
uv run pytest tests/test_namespace.py -v
```

---

### Task 2.3: Instance lifecycle manager

**Objective:** Create, track, and manage sandbox instances. Each instance is a directory with `workspace/`, `audit.log`, and `metadata.json`.

**Files:**
- Modify: `src/ns_hpc/instance.py`

**Implementation:**

```python
"""Instance lifecycle — directory management, metadata, audit logging."""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ns_hpc.config import Config


class Instance:
    """A sandbox instance tied to a persistent workspace directory.

    Instances are stateless in the bwrap sense (no persistent namespace) but
    maintain a workspace directory, audit log, and metadata on the host.
    """

    def __init__(self, instance_id: str, base_dir: Path):
        self.id = instance_id
        self.base_dir = base_dir
        self.workspace_dir = base_dir / "workspace"
        self.audit_log_path = base_dir / "audit.log"
        self.metadata_path = base_dir / "metadata.json"

    @property
    def exists(self) -> bool:
        return self.base_dir.exists()

    @staticmethod
    def create(instance_id: str, config: Config) -> "Instance":
        """Create a new instance directory structure.

        Args:
            instance_id: Unique instance identifier (auto-generated if empty)
            config: Loaded ns-hpc configuration

        Returns:
            New Instance object
        """
        if not instance_id:
            instance_id = uuid.uuid4().hex[:12]

        instances_dir = config.resolve_instances_dir()
        base_dir = instances_dir / instance_id

        if base_dir.exists():
            raise FileExistsError(f"Instance {instance_id} already exists at {base_dir}")

        base_dir.mkdir(parents=True, exist_ok=True)
        workspace_dir = base_dir / "workspace"
        workspace_dir.mkdir(parents=True, exist_ok=True)

        metadata = {
            "id": instance_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "workspace": str(workspace_dir),
            "hostname": os.uname().nodename,
        }
        metadata_path = base_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        return Instance(instance_id, base_dir)

    @staticmethod
    def load(instance_id: str, config: Config) -> Optional["Instance"]:
        """Load an existing instance by ID."""
        instances_dir = config.resolve_instances_dir()
        base_dir = instances_dir / instance_id
        if not base_dir.exists():
            return None
        return Instance(instance_id, base_dir)

    def write_audit(self, command: str, result: dict) -> None:
        """Write an audit entry from the HOST side.

        CRITICAL: This method runs on the host, never inside the sandbox.
        The audit log path is not bind-mounted into the sandbox.
        """
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "command": command,
            "exit_code": result.get("exit_code"),
            "stdout_len": len(result.get("stdout", "")),
            "stderr_len": len(result.get("stderr", "")),
        }
        with open(self.audit_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    @staticmethod
    def list_instances(config: Config) -> list["Instance"]:
        """List all existing instances."""
        instances_dir = config.resolve_instances_dir()
        if not instances_dir.exists():
            return []
        return [
            Instance(d.name, instances_dir / d.name)
            for d in sorted(instances_dir.iterdir())
            if d.is_dir() and (d / "metadata.json").exists()
        ]

    @staticmethod
    def destroy(instance_id: str, config: Config) -> bool:
        """Remove an instance directory and all its contents."""
        instances_dir = config.resolve_instances_dir()
        base_dir = instances_dir / instance_id
        if not base_dir.exists():
            return False
        import shutil
        shutil.rmtree(base_dir)
        return True
```

**Write tests:**

```python
# tests/test_instance.py
from ns_hpc.instance import Instance
from ns_hpc.config import Config
from pathlib import Path
import tempfile

def test_create_instance():
    cfg = Config(instances_dir=tempfile.mkdtemp())
    inst = Instance.create("test-001", cfg)
    assert inst.exists
    assert inst.workspace_dir.exists()
    assert inst.metadata_path.exists()
    metadata = json.loads(inst.metadata_path.read_text())
    assert metadata["id"] == "test-001"
    assert "created_at" in metadata

def test_load_instance():
    cfg = Config(instances_dir=tempfile.mkdtemp())
    Instance.create("test-002", cfg)
    inst = Instance.load("test-002", cfg)
    assert inst is not None
    assert inst.id == "test-002"

def test_audit_log():
    cfg = Config(instances_dir=tempfile.mkdtemp())
    inst = Instance.create("test-003", cfg)
    inst.write_audit("echo hello", {"exit_code": 0, "stdout": "hello", "stderr": ""})
    log = inst.audit_log_path.read_text()
    assert "echo hello" in log
    assert '"exit_code": 0' in log

def test_destroy_instance():
    cfg = Config(instances_dir=tempfile.mkdtemp())
    inst = Instance.create("test-004", cfg)
    assert inst.exists
    assert Instance.destroy("test-004", cfg)
    assert not inst.exists

def test_list_instances():
    cfg = Config(instances_dir=tempfile.mkdtemp())
    Instance.create("list-a", cfg)
    Instance.create("list-b", cfg)
    instances = Instance.list_instances(cfg)
    assert len(instances) == 2
```

---

## Phase 3: CLI Interface

### Task 3.1: CLI skeleton with all subcommands

**Objective:** Build the Typer CLI with all subcommands registered (bodies delegated to later tasks).

**Files:**
- Modify: `src/ns_hpc/cli.py`
- Modify: `src/ns_hpc/__main__.py`

**Implementation:**

```python
# src/ns_hpc/cli.py
from __future__ import annotations

import sys
import typer
from pathlib import Path
from typing import Optional

from ns_hpc.config import load_config

app = typer.Typer(
    name="ns-hpc",
    help="HPC sandboxing via bubblewrap — MCP server and CLI tools.",
    no_args_is_help=True,
)


@app.callback()
def main(
    config: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Path to config.toml"
    ),
) -> None:
    """Load configuration into context."""
    ctx = typer.Context
    # Store config path in app state for subcommands
    app.config_path = config


@app.command()
def run(
    port: int = typer.Option(8001, "--port", "-p", help="MCP server port"),
) -> None:
    """Start the MCP server (stdio by default, HTTP with --port)."""
    from ns_hpc.server import run_server
    run_server(config_path=getattr(app, "config_path", None), port=port)


@app.command()
def enter(
    instance_id: str = typer.Argument(..., help="Instance ID to enter"),
) -> None:
    """Start an interactive shell inside a sandbox instance."""
    from ns_hpc.namespace import build_bwrap_args, run_in_sandbox
    from ns_hpc.instance import Instance
    from ns_hpc.config import load_config
    import subprocess
    import os

    cfg = load_config(str(getattr(app, "config_path", "")) if getattr(app, "config_path", None) else None)
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        typer.echo(f"Error: instance {instance_id} not found", err=True)
        raise typer.Exit(1)

    argv = build_bwrap_args(cfg, inst.workspace_dir, ["/bin/bash", "-i"])
    # Interactive: replace current process with bwrap
    os.execvp("bwrap", argv)


@app.command()
def exec(
    instance_id: str = typer.Argument(..., help="Instance ID"),
    command: list[str] = typer.Argument(
        ..., help="Command to execute (e.g., 'ls -la')"
    ),
) -> None:
    """Run a command inside a sandbox instance (non-interactive)."""
    from ns_hpc.namespace import build_bwrap_args, run_in_sandbox
    from ns_hpc.instance import Instance
    from ns_hpc.config import load_config

    cfg = load_config(str(getattr(app, "config_path", "")) if getattr(app, "config_path", None) else None)
    inst = Instance.load(instance_id, cfg)
    if inst is None:
        typer.echo(f"Error: instance {instance_id} not found", err=True)
        raise typer.Exit(1)

    argv = build_bwrap_args(cfg, inst.workspace_dir, command)
    result = run_in_sandbox(argv)
    typer.echo(result.stdout)
    if result.stderr:
        typer.echo(result.stderr, err=True)
    inst.write_audit(" ".join(command), {
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
    })
    raise typer.Exit(result.exit_code)


@app.command()
def doctor() -> None:
    """Diagnose system prerequisites for ns-hpc."""
    from ns_hpc.cli_impl import run_doctor
    run_doctor()


@app.command()
def clean(
    days: int = typer.Option(7, "--days", "-d", help="Remove instances older than N days"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
) -> None:
    """Remove stale instance directories."""
    from ns_hpc.cli_impl import clean_instances
    clean_instances(days, force)


if __name__ == "__main__":
    app()
```

```python
# src/ns_hpc/__main__.py
from ns_hpc.cli import app

app()
```

---

### Task 3.2: `ns-hpc doctor` implementation

**Objective:** Check bwrap, user namespaces, kernel settings, Slurm binaries.

**Files:**
- Create: `src/ns_hpc/cli_impl.py`

**Implementation:**

```python
# src/ns_hpc/cli_impl.py
"""CLI command implementations."""
from __future__ import annotations

import shutil
import subprocess
import sys
import typer
from pathlib import Path


def run_doctor() -> None:
    """Run all diagnostic checks."""
    results: list[tuple[str, bool, str]] = []

    # 1. bwrap exists
    bwrap_path = shutil.which("bwrap")
    if bwrap_path:
        ver = subprocess.run([bwrap_path, "--version"], capture_output=True, text=True)
        results.append(("bwrap", True, ver.stdout.strip() or bwrap_path))
    else:
        results.append(("bwrap", False, "NOT FOUND — install bubblewrap"))

    # 2. User namespace: unshare -r
    ns_ok = subprocess.run(
        ["unshare", "-r", "--", "true"],
        capture_output=True,
        timeout=5,
    ).returncode == 0
    results.append(("User namespaces (unshare -r)", ns_ok, "OK" if ns_ok else "FAIL"))

    # 3. max_user_namespaces
    try:
        max_ns = Path("/proc/sys/user/max_user_namespaces").read_text().strip()
        enough = int(max_ns) > 0
        results.append(("max_user_namespaces", enough, max_ns))
    except (FileNotFoundError, ValueError):
        results.append(("max_user_namespaces", False, "NOT FOUND"))

    # 4. /etc/subuid exists and has current user
    import pwd
    user = pwd.getpwuid(os.getuid()).pw_name
    subuid_path = Path("/etc/subuid")
    subuid_ok = subuid_path.exists() and user in subuid_path.read_text()
    results.append(("/etc/subuid", subuid_ok, "OK" if subuid_ok else f"MISSING entry for {user}"))

    # 5. Slurm binaries
    for bin_name in ["sbatch", "squeue", "sacct", "scancel"]:
        found = shutil.which(bin_name) is not None
        results.append((f"Slurm: {bin_name}", found, shutil.which(bin_name) or "not found"))

    # 6. Temporary directory supports bwrap
    tmpdir = Path("/tmp")
    writable = os.access(tmpdir, os.W_OK)
    results.append(("Temp dir (/tmp) writable", writable, "OK" if writable else "READ-ONLY"))

    # Print results
    all_ok = True
    typer.echo("\nns-hpc diagnostics:\n")
    for name, ok, detail in results:
        icon = "✓" if ok else "✗"
        typer.echo(f"  {icon} {name}: {detail}")
        if not ok:
            all_ok = False

    # Also test bwrap JSON status FD pipe
    typer.echo()
    r_fd, w_fd = os.pipe()
    try:
        proc = subprocess.Popen(
            ["bwrap", "--ro-bind", "/usr", "/usr", "--ro-bind", "/bin", "/bin",
             "--ro-bind", "/lib", "/lib", "--ro-bind", "/lib64", "/lib64",
             "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
             "--unshare-all", "--share-net",
             "--json-status-fd", str(w_fd),
             "--", "/bin/sh", "-c", "exit 0"],
            pass_fds=(w_fd,),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        os.close(w_fd)
        proc.communicate(timeout=10)
        json_data = os.read(r_fd, 4096)
        import json
        status = json.loads(json_data)
        bwrap_works = status.get("exit-code") == 0
        typer.echo(f"  {'✓' if bwrap_works else '✗'} bwrap JSON status FD: {'OK' if bwrap_works else 'FAILED'}")
        if not bwrap_works:
            all_ok = False
    except Exception as e:
        typer.echo(f"  ✗ bwrap JSON status FD: FAILED — {e}")
        all_ok = False
    finally:
        try:
            os.close(r_fd)
        except OSError:
            pass

    typer.echo()
    if all_ok:
        typer.echo("All checks passed.")
    else:
        typer.echo("Some checks failed — ns-hpc may not work correctly.", err=True)
        sys.exit(1)
```

---

### Task 3.3: `ns-hpc clean` implementation

**Objective:** Remove stale instances.

**Implementation (add to cli_impl.py):**

```python
def clean_instances(days: int = 7, force: bool = False) -> None:
    """Remove instances older than `days`."""
    from datetime import datetime, timezone, timedelta
    from ns_hpc.config import load_config
    from ns_hpc.instance import Instance

    cfg = load_config()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    instances = Instance.list_instances(cfg)

    stale = []
    for inst in instances:
        try:
            meta = json.loads(inst.metadata_path.read_text())
            created = datetime.fromisoformat(meta["created_at"])
            if created < cutoff:
                stale.append(inst)
        except (json.JSONDecodeError, KeyError, ValueError):
            stale.append(inst)  # Corrupt metadata => clean up

    if not stale:
        typer.echo("No stale instances found.")
        return

    typer.echo(f"Found {len(stale)} stale instance(s):")
    for inst in stale:
        age = datetime.now(timezone.utc) - datetime.fromisoformat(
            json.loads(inst.metadata_path.read_text()).get("created_at", "2000-01-01")
        )
        typer.echo(f"  {inst.id} ({age.days}d old)")

    if not force:
        typer.confirm("Remove these instances?", abort=True)

    for inst in stale:
        Instance.destroy(inst.id, cfg)
        typer.echo(f"  Removed {inst.id}")

    typer.echo("Done.")
```

---

## Phase 4: MCP Server

### Task 4.1: MCP server skeleton with Lifespan

**Objective:** Stand up the MCP server that connects to config, manages lifecycle, and registers tools.

**Files:**
- Modify: `src/ns_hpc/server.py`

**Implementation:**

```python
# src/ns_hpc/server.py
"""MCP server for ns-hpc — sandboxed file ops and command execution."""
from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Optional

from mcp.server import MCPServer
from mcp.server.session import ServerSession
from mcp.types import Resource

from ns_hpc.config import Config, load_config
from ns_hpc.instance import Instance
from ns_hpc.namespace import build_bwrap_args, run_in_sandbox


@dataclass
class ServerContext:
    """Lifespan context shared across MCP tools."""
    config: Config
    instance: Instance


@asynccontextmanager
async def server_lifespan(server: MCPServer) -> AsyncIterator[ServerContext]:
    """Initialize and teardown server context.

    Creates a default instance on startup tied to the server's lifetime.
    """
    config_path = os.environ.get("NS_HPC_CONFIG")
    config = load_config(config_path)

    instance = Instance.create("default", config)

    try:
        yield ServerContext(config=config, instance=instance)
    finally:
        # Clean shutdown — audit final state
        instance.write_audit("server_shutdown", {"exit_code": 0, "stdout": "", "stderr": ""})


# Create the MCP server
server = MCPServer(lifespan=server_lifespan)
```

---

### Task 4.2: `ns-hpc run_command` MCP tool

**Objective:** Expose a `run_command` tool that executes shell commands inside a bwrap sandbox.

**Files:**
- Modify: `src/ns_hpc/server.py`

**Implementation (append to server.py):**

```python
from mcp.types import TextContent, Tool
from pydantic import BaseModel, Field


class RunCommandInput(BaseModel):
    command: str = Field(..., description="Shell command to run inside the sandbox")
    instance_id: str = Field(default="default", description="Instance ID (creates if not exists)")
    timeout: int = Field(default=60, description="Max execution time in seconds", ge=1, le=3600)
    working_dir: str = Field(default=None, description="Working dir inside sandbox (default: workspace)")


@server.tool()
async def run_command(ctx: ServerSession, input: RunCommandInput) -> list[TextContent]:
    """Execute a shell command inside a bwrap-sandboxed environment.

    The command runs in a fresh sandbox with:
    - Read-only system paths (/usr, /lib, /bin, /etc)
    - Read-write workspace directory
    - Network access (--share-net)
    - No access to host filesystem outside workspace
    - /tmp as tmpfs, /proc and /dev available

    Results are audited to the instance's audit.log on the host side.
    """
    context: ServerContext = ctx.request_context.lifespan_context
    cfg = context.config

    # Load or create the instance
    instance = Instance.load(input.instance_id, cfg)
    if instance is None:
        instance = Instance.create(input.instance_id, cfg)

    # Build command as list (shell=True via /bin/sh -c)
    command_list = ["/bin/sh", "-c", input.command]

    argv = build_bwrap_args(
        cfg,
        instance.workspace_dir,
        command_list,
        working_dir=input.working_dir or None,
    )

    result = run_in_sandbox(argv, timeout=input.timeout)

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

    return [TextContent(type="text", text=output)]
```

---

### Task 4.3: File operation MCP tools

**Objective:** Expose `read_file`, `write_file`, `list_directory` tools that operate inside the sandbox workspace.

**Files:**
- Modify: `src/ns_hpc/server.py`

```python
class ReadFileInput(BaseModel):
    path: str = Field(..., description="Path relative to workspace root")
    instance_id: str = Field(default="default", description="Instance ID")


@server.tool()
async def read_file(ctx: ServerSession, input: ReadFileInput) -> list[TextContent]:
    """Read a file from the workspace. Path is relative to workspace root."""
    context: ServerContext = ctx.request_context.lifespan_context
    cfg = context.config
    instance = Instance.load(input.instance_id, cfg) or Instance.create(input.instance_id, cfg)

    safe_path = instance.workspace_dir / input.path.lstrip("/")
    safe_path = safe_path.resolve()
    if not str(safe_path).startswith(str(instance.workspace_dir.resolve())):
        return [TextContent(type="text", text="Error: Path escapes workspace")]

    if not safe_path.exists():
        return [TextContent(type="text", text=f"Error: {input.path} not found")]

    content = safe_path.read_text(errors="replace")
    return [TextContent(type="text", text=content)]


class WriteFileInput(BaseModel):
    path: str = Field(..., description="Path relative to workspace root")
    content: str = Field(..., description="File content to write")
    instance_id: str = Field(default="default", description="Instance ID")


@server.tool()
async def write_file(ctx: ServerSession, input: WriteFileInput) -> list[TextContent]:
    """Write a file inside the workspace. Path is relative to workspace root."""
    context: ServerContext = ctx.request_context.lifespan_context
    cfg = context.config
    instance = Instance.load(input.instance_id, cfg) or Instance.create(input.instance_id, cfg)

    safe_path = instance.workspace_dir / input.path.lstrip("/")
    safe_path = safe_path.resolve()
    if not str(safe_path).startswith(str(instance.workspace_dir.resolve())):
        return [TextContent(type="text", text="Error: Path escapes workspace")]

    safe_path.parent.mkdir(parents=True, exist_ok=True)
    safe_path.write_text(input.content)

    instance.write_audit(f"write_file {input.path}", {
        "exit_code": 0,
        "stdout": f"Wrote {len(input.content)} bytes",
        "stderr": "",
    })

    return [TextContent(type="text", text=f"Wrote {len(input.content)} bytes to {input.path}")]


class ListDirectoryInput(BaseModel):
    path: str = Field(default=".", description="Directory path relative to workspace root")
    instance_id: str = Field(default="default", description="Instance ID")


@server.tool()
async def list_directory(ctx: ServerSession, input: ListDirectoryInput) -> list[TextContent]:
    """List contents of a directory inside the workspace."""
    context: ServerContext = ctx.request_context.lifespan_context
    cfg = context.config
    instance = Instance.load(input.instance_id, cfg) or Instance.create(input.instance_id, cfg)

    safe_path = instance.workspace_dir / input.path.lstrip("/")
    safe_path = safe_path.resolve()
    if not str(safe_path).startswith(str(instance.workspace_dir.resolve())):
        return [TextContent(type="text", text="Error: Path escapes workspace")]

    if not safe_path.exists():
        return [TextContent(type="text", text=f"Error: {input.path} not found")]
    if not safe_path.is_dir():
        return [TextContent(type="text", text=f"Error: {input.path} is not a directory")]

    entries = []
    for entry in sorted(safe_path.iterdir()):
        suffix = "/" if entry.is_dir() else ""
        entries.append(f"{entry.name}{suffix}")

    return [TextContent(type="text", text="\n".join(entries) if entries else "(empty)")]
```

---

### Task 4.4: MCP resources (context docs)

**Objective:** Expose Markdown files from the `context/` directory as MCP resources.

**Files:**
- Modify: `src/ns_hpc/server.py`

```python
@server.resource("ns-hpc://context/{filename}")
async def context_resource(filename: str) -> str | bytes:
    """Read a context/resource document from the context directory."""
    cfg = load_config(os.environ.get("NS_HPC_CONFIG"))
    context_dirs = cfg.resource_defaults.context_dirs

    for ctx_dir in context_dirs:
        ctx_path = Path(ctx_dir)
        if not ctx_path.exists():
            continue
        target = (ctx_path / filename).resolve()
        if target.exists() and str(target).startswith(str(ctx_path.resolve())):
            return target.read_text(errors="replace")

    return f"Context document '{filename}' not found"
```

---

## Phase 5: Task Engine

### Task 5.1: Local task engine

**Objective:** Wrap the namespace runner into a task engine that returns `task_handle` for async tracking.

**Files:**
- Modify: `src/ns_hpc/task_engine.py`

```python
"""Task engine — local and Slurm execution modes."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskHandle:
    """Handle for tracking an async task."""
    id: str
    status: TaskStatus
    mode: str  # "local" or "slurm"
    pid: Optional[int] = None
    slurm_job_id: Optional[int] = None
    exit_code: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: Optional[str] = None


class LocalTaskEngine:
    """Run commands locally via bwrap sandbox, tracking as tasks."""

    def __init__(self, config, instance):
        self.config = config
        self.instance = instance
        self._tasks: dict[str, TaskHandle] = {}

    def submit(self, command: str, timeout: int = 300) -> TaskHandle:
        """Submit a command for execution. Returns immediately with a task handle."""
        from ns_hpc.namespace import build_bwrap_args, run_in_sandbox

        task_id = uuid.uuid4().hex[:12]
        handle = TaskHandle(id=task_id, status=TaskStatus.PENDING, mode="local")
        self._tasks[task_id] = handle

        argv = build_bwrap_args(
            self.config,
            self.instance.workspace_dir,
            ["/bin/sh", "-c", command],
        )

        handle.status = TaskStatus.RUNNING

        try:
            result = run_in_sandbox(argv, timeout=timeout)
            handle.status = TaskStatus.COMPLETED if result.exit_code == 0 else TaskStatus.FAILED
            handle.exit_code = result.exit_code
            handle.stdout = result.stdout
            handle.stderr = result.stderr

            # Audit from host side
            self.instance.write_audit(f"[task:{task_id}] {command}", {
                "exit_code": result.exit_code,
                "stdout": result.stdout,
                "stderr": result.stderr,
            })
        except Exception as e:
            handle.status = TaskStatus.FAILED
            handle.stderr = str(e)

        handle.completed_at = datetime.now(timezone.utc).isoformat()
        return handle

    def get_status(self, task_id: str) -> Optional[TaskHandle]:
        return self._tasks.get(task_id)

    def cancel(self, task_id: str) -> bool:
        handle = self._tasks.get(task_id)
        if handle is None or handle.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
            return False
        handle.status = TaskStatus.CANCELLED
        handle.completed_at = datetime.now(timezone.utc).isoformat()
        return True
```

---

### Task 5.2: Slurm task engine

**Objective:** Submit bwrap-wrapped commands to Slurm. Graceful degradation when Slurm unavailable.

**Files:**
- Modify: `src/ns_hpc/task_engine.py`

```python
import shutil


class SlurmTaskEngine:
    """Submit bwrap-wrapped commands to Slurm.

    Strategy: sbatch wraps a script that calls bwrap internally.
    The bwrap sandbox is constructed from config + instance workspace.

    Slurm binaries are checked at construction time — missing Slurm means
    all submissions fail immediately with a clear message.
    """

    def __init__(self, config, instance):
        self.config = config
        self.instance = instance
        self._check_slurm()

    def _check_slurm(self):
        missing = [bin for bin in ["sbatch", "squeue", "scancel"] if not shutil.which(bin)]
        if missing:
            self._available = False
            self._missing = missing
        else:
            self._available = True
            self._missing = []

    @property
    def available(self) -> bool:
        return self._available

    def submit(self, command: str, timeout: int = 3600,
               cpus: int = 1, memory_gb: int = 4,
               partition: str = "debug") -> TaskHandle:
        """Submit a bwrap-wrapped command to Slurm.

        The sbatch script:
        1. Creates the sandbox via bwrap
        2. Runs the command inside
        3. Writes result to a JSON file for polling
        """
        if not self._available:
            raise RuntimeError(
                f"Slurm not available — missing binaries: {', '.join(self._missing)}"
            )

        task_id = uuid.uuid4().hex[:12]
        handle = TaskHandle(id=task_id, status=TaskStatus.PENDING, mode="slurm")
        self._tasks[task_id] = handle

        from ns_hpc.namespace import build_bwrap_args
        ws_mount = self.config.namespace_defaults.workspace_mount
        result_path = f"{ws_mount}/.ns_hpc_task_{task_id}.json"

        bwrap_cmd = build_bwrap_args(
            self.config,
            self.instance.workspace_dir,
            ["/bin/sh", "-c", command],
        )

        # Escape for embedding in sbatch script
        bwrap_line = " ".join(shlex.quote(a) for a in bwrap_cmd)

        script = f"""#!/bin/bash
#SBATCH --job-name=ns-hpc-{task_id[:8]}
#SBATCH --output={ws_mount}/.ns_hpc_{task_id}.out
#SBATCH --error={ws_mount}/.ns_hpc_{task_id}.err
#SBATCH --time={timeout // 60}:00
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={memory_gb}G
#SBATCH --partition={partition}
#SBATCH --export=ALL

set -e
{bwrap_line}
EXIT_CODE=$?
echo '{{"exit_code": {EXIT_CODE}, "task_id": "{task_id}"}}' > {result_path}
exit $EXIT_CODE
"""

        # Write script to instance workspace (host-side) + submit
        script_host_path = self.instance.workspace_dir / f".ns_hpc_slurm_{task_id}.sh"
        script_host_path.write_text(script)

        proc = subprocess.run(
            ["sbatch", str(script_host_path)],
            capture_output=True, text=True, timeout=30,
        )

        if proc.returncode != 0:
            handle.status = TaskStatus.FAILED
            handle.stderr = f"sbatch failed: {proc.stderr}"
            return handle

        # Parse "Submitted batch job 12345"
        import re
        match = re.search(r"Submitted batch job (\d+)", proc.stdout)
        if match:
            handle.slurm_job_id = int(match.group(1))
            handle.status = TaskStatus.RUNNING
        else:
            handle.status = TaskStatus.FAILED
            handle.stderr = f"Unexpected sbatch output: {proc.stdout}"

        return handle

    def get_status(self, task_id: str) -> Optional[TaskHandle]:
        handle = self._tasks.get(task_id)
        if handle is None:
            return None

        if handle.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            return handle

        if handle.slurm_job_id:
            # Poll via sacct
            proc = subprocess.run(
                ["sacct", "-j", str(handle.slurm_job_id),
                 "--format", "State,ExitCode", "--noheader", "-P"],
                capture_output=True, text=True, timeout=15,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                # Try to read result file
                result_path = self.instance.workspace_dir / f".ns_hpc_task_{task_id}.json"
                if result_path.exists():
                    try:
                        data = json.loads(result_path.read_text())
                        handle.exit_code = data["exit_code"]
                        handle.status = TaskStatus.COMPLETED if data["exit_code"] == 0 else TaskStatus.FAILED
                        handle.completed_at = datetime.now(timezone.utc).isoformat()
                    except (json.JSONDecodeError, KeyError):
                        pass

        return handle

    def cancel(self, task_id: str) -> bool:
        handle = self._tasks.get(task_id)
        if handle is None or handle.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
            return False
        if handle.slurm_job_id:
            subprocess.run(["scancel", str(handle.slurm_job_id)], timeout=15)
        handle.status = TaskStatus.CANCELLED
        handle.completed_at = datetime.now(timezone.utc).isoformat()
        return True
```

---

## Phase 6: Integration & Documentation

### Task 6.1: Template context/ docs

**Objective:** Create sample HPC usage documentation in `context/`.

**Files:**
- Create: `context/README.md`
- Create: `context/slurm-basics.md`
- Create: `context/python-env.md`

context/README.md:

```markdown
# ns-hpc Context Documents

This directory contains Markdown documentation about HPC usage that
the MCP server exposes as resources via `ns-hpc://context/{filename}`.

Add any reference material the LLM agent should know about this HPC
environment, such as:
- Module availability
- Filesystem layout
- Job submission patterns
- Software stack documentation
```

context/slurm-basics.md:

```markdown
# Slurm Basics for ns-hpc

## Partitions

- `debug` — 10 min limit, interactive testing
- `compute` — 48h limit, production jobs
- `gpu` — 24h limit, GPU nodes

## Common Commands

- `sinfo` — partition/node status
- `squeue -u $USER` — your running jobs
- `scancel <jobid>` — cancel a job
- `sacct -j <jobid> --format=JobID,State,ExitCode` — job history
```

context/python-env.md:

```markdown
# Python Environment on HPC

Available via modules:
- `module load python/3.11`
- `module load cuda/12.4` (for GPU nodes)

Use `uv` for package management (pre-installed in workspace).
For heavy dependencies, use `uv sync` with the project's pyproject.toml.
```

---

### Task 6.2: README and final wiring

**Objective:** Write README, wire up main entry point, final testing pass.

**Files:**
- Create: `README.md`

README.md:

```markdown
# ns-hpc MCP Server

HPC sandboxing via bubblewrap — an MCP server that executes code inside
bwrap containers with read-only system paths and an isolated workspace.

## Quick Start

```bash
# Install
uv sync

# Create an instance and run a command
ns-hpc exec my-instance -- ls -la

# Start the MCP server
ns-hpc run

# Diagnostics
ns-hpc doctor
```

## Configuration

See `config.toml` for namespace defaults, workspace mount points, and
resource directory settings.

## Architecture

- **bwrap model**: Stateless, single-shot. Every command creates a fresh
  sandbox with `--unshare-all --share-net`.
- **Instance**: A directory `{instances_dir}/{id}/` containing
  `workspace/`, `audit.log`, and `metadata.json`.
- **Audit**: Written on the host side, never inside the sandbox.
- **Slurm**: Optionally wraps bwrap inside sbatch submissions.

## Security

- All commands run via `bwrap --unshare-all` (user, PID, mount, IPC, UTS,
  CGROUP namespaces).
- System paths are read-only (`--ro-bind`). /tmp is fresh tmpfs.
- Workspace is the only writable path inside the sandbox.
- Audit log is written by the host process, never bind-mounted.
```

---

## Phase 7: Slurm Test Environment (infrastructure)

### Task 7.1: Set up Slurm Docker cluster via podman

**Objective:** Clone `giovtorres/slurm-docker-cluster` and get it running under podman.

```bash
cd /home/liyq/workspace
git clone https://github.com/giovtorres/slurm-docker-cluster.git
cd slurm-docker-cluster
```

**Note:** This repo uses `docker-compose.yml`. Since we have podman 5.8.1, we need:
- Install `podman-compose` (via pip) or
- Use `podman play kube` with a converted manifest, or
- Install docker-compose and use its docker socket with podman

After the cluster is running, test:
```bash
docker exec slurmctld sinfo  # or podman exec
```

Expected: nodes should show "idle".

---

## Implementation Order

| Phase | Tasks | Depends On | Est. Time |
|-------|-------|------------|-----------|
| 0     | 0.1, 0.2 | — | 15 min |
| 1     | 1.1 | 0.1 | 15 min |
| 2     | 2.1, 2.2, 2.3 | 1.1 | 30 min |
| 3     | 3.1, 3.2, 3.3 | 2.3 | 25 min |
| 4     | 4.1, 4.2, 4.3, 4.4 | 2.2, 2.3 | 35 min |
| 5     | 5.1, 5.2 | 2.2, 2.3 | 20 min |
| 6     | 6.1, 6.2 | 4.4 | 15 min |
| 7     | 7.1 | — | 20 min (parallel) |
| **Total** | **21 tasks** | | **~2.5h** |

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| bwrap `--json-status-fd` behaves differently on HPC kernels | Smoke test in Task 0.2 catches this before any real logic is built |
| bwrap without overlayfs on HPC filesystem | doctor checks /tmp writability; workspace should use local filesystem |
| Slurm Docker cluster doesn't work under podman | If it fails, Slurm engine gets built with unit tests only, integration tests marked `@pytest.mark.skip` |
| bwrap needs more paths bound on different distros | All bind paths are configurable; doctor can validate available dirs |
| Pipe FD exhaustion with concurrent tasks | Task engine is single-threaded for v1; each bwrap invocation uses exactly one pipe |
| Path traversal via symlinks in file tools | `resolve()` + `startswith()` check in read/write/list tools |
| pyproject.toml conflicts with uv | Let uv write it first, add deps via `uv add`, never hand-edit |

---

## Verification Checklist

- [ ] `ns-hpc doctor` passes all checks on dev machine
- [ ] `ns-hpc exec test-01 "echo hello"` returns "hello"
- [ ] `ns-hpc exec test-01 "touch /tmp/outside.txt && ls /tmp"` shows empty
- [ ] `ns-hpc enter test-01` gives interactive bash in sandbox
- [ ] MCP server starts with `ns-hpc run` (stdio mode)
- [ ] MCP tools return correct results via direct invocation
- [ ] audit.log is written host-side and not accessible from sandbox
- [ ] `ns-hpc clean` correctly removes stale instances
- [ ] Slurm test cluster reports nodes idle
