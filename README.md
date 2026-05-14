# ns-hpc MCP Server

HPC sandboxing via bubblewrap — an MCP server that executes code inside
bwrap containers with read-only system paths and an isolated workspace.

```bash
pip install ns-hpc   # or: uv add ns-hpc
```

## Quick Start

```bash
# Run diagnostics
ns-hpc doctor

# Create an instance and run a command
ns-hpc bwrap my-instance -- ls -la
ns-hpc bwrap my-instance -- python -c "print('hello from sandbox')"

# Interactive shell
ns-hpc instance enter my-instance

# Start the MCP server
ns-hpc run

# Clean up old instances
ns-hpc clean --days 7
```

## Configuration

Create `config.toml` in the project root:

```toml
[namespace_defaults]
bind_ro = ["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc"]
workspace_mount = "/workspace"
flags = ["--unshare-all", "--share-net", "--proc", "/proc",
         "--dev", "/dev", "--tmpfs", "/tmp"]

[resource_defaults]
context_dirs = ["context"]
resource_patterns = ["*.md"]
```

See `config.toml` for the full default configuration.

## Architecture

```
┌─────────────────────────────────────────────┐
│              MCP Client (LLM)               │
└──────────────┬──────────────────────────────┘
               │ SSH / stdio
┌──────────────▼──────────────────────────────┐
│          ns-hpc MCP Server                  │
│                                             │
│  ┌─────────────┐  ┌──────────────────────┐  │
│  │ run_command  │  │ read/write_file      │  │
│  │ (bwrap exec) │  │ list_directory       │  │
│  └──────┬──────┘  └──────────┬───────────┘  │
│         │                     │              │
│  ┌──────▼─────────────────────▼───────────┐  │
│  │        bwrap sandbox                   │  │
│  │  ┌─────────────────────────────────┐   │  │
│  │  │ /workspace (rw)                 │   │  │
│  │  │ /usr /lib /bin /etc (ro)        │   │  │
│  │  │ /proc /dev (namespace)          │   │  │
│  │  │ /tmp (tmpfs)                    │   │  │
│  │  └─────────────────────────────────┘   │  │
│  └────────────────────────────────────────┘  │
│                                             │
│  ┌──────────────────────────────────────┐   │
│  │  Instance: ~/mcp_instances/{id}/     │   │
│  │  ├── workspace/  (host-side bind)    │   │
│  │  ├── audit.log   (host-side only)    │   │
│  │  └── metadata.json                   │   │
│  └──────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
```

### Key Design Decisions

- **bwrap model**: Stateless, single-shot. Every command creates a fresh
  sandbox. No persistent namespaces needed.
- **enter/exec are identical**: Both rebuild the same sandbox from scratch.
- **Audit log**: Written on the host side, never bind-mounted into the
  sandbox — the sandbox cannot tamper with its own audit trail.
- **File path protection**: All file tools use `resolve()` + `startswith()`
  to block path traversal attacks.
- **MCP Proxy**: Deferred to v2. See `config.toml`'s `[proxied_mcps]` for the placeholder.

## CLI Reference

| Command | Description |
|---------|-------------|
| `ns-hpc doctor` | Diagnose system prerequisites |
| `ns-hpc bwrap <id> -- <cmd>` | Run command in bwrap sandbox (raw) |
| `ns-hpc instance create <id>` | Create a new sandbox instance |
| `ns-hpc instance run <id> -- <cmd>` | Run command as an async job |
| `ns-hpc instance status <id> <job-id>` | Check job status |
| `ns-hpc instance jobs <id>` | List tracked jobs |
| `ns-hpc instance cancel <id> <job-id>` | Cancel a running job |
| `ns-hpc instance enter <id>` | Interactive bash in sandbox |
| `ns-hpc instance describe <id>` | Show instance metadata |
| `ns-hpc instance update <id> -d <desc>` | Update instance description |
| `ns-hpc instance archive <id>` | Archive an instance, disabling new job submissions |
| `ns-hpc run` | Start MCP server (stdio) |
| `ns-hpc clean --days 7` | Remove stale instances |

## MCP Tools

| Tool | Description |
|------|-------------|
| `create_instance` | Create a new sandbox instance |
| `list_instances` | List all instances |
| `update_instance` | Update instance metadata (description) |
| `archive_instance` | Archive an instance, disabling new job submissions |
| `list_archived_instances` | List all archived instances |
| `submit_job` | Submit a command as an async job |
| `poll_job` | Poll a running job (optionally wait for completion) |
| `list_jobs` | List all tracked jobs for an instance |
| `cancel_job` | Cancel a running job and return final output |

All commands run inside isolated bwrap containers with read-only system paths.

## Security

- All commands run via `bwrap --unshare-all` (user, PID, mount, IPC, UTS,
  CGROUP namespaces)
- System paths are read-only (`--ro-bind`)
- `/tmp` is a fresh tmpfs (no host files visible)
- Workspace is the only writable bind mount
- Audit log is written by the host, never bind-mounted
- Path traversal is blocked in all file tools
- `ns-hpc doctor` validates all prerequisites before use

## Development

```bash
# Setup
uv sync

# Run tests
uv run pytest

# Run diagnostics
uv run python -m ns_hpc doctor

# Run a command in sandbox
uv run python -m ns_hpc bwrap test-instance -- echo "hello"

# Start MCP server
uv run python -m ns_hpc run
```

## Requirements

- Linux with user namespaces enabled
- bwrap (bubblewrap) 0.11+
- Python 3.12+ (developed on 3.14)
- (Optional) Slurm: sbatch, squeue, sacct

## License

MIT
