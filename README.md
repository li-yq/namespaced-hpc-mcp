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
ns-hpc exec my-instance -- ls -la
ns-hpc exec my-instance -- python -c "print('hello from sandbox')"

# Interactive shell
ns-hpc enter my-instance

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
| `ns-hpc exec <id> -- <cmd>` | Run command in sandbox |
| `ns-hpc enter <id>` | Interactive bash in sandbox |
| `ns-hpc run` | Start MCP server (stdio) |
| `ns-hpc clean --days 7` | Remove stale instances |

## MCP Tools

| Tool | Description |
|------|-------------|
| `run_command` | Execute shell command in bwrap sandbox |
| `read_file` | Read file from workspace (path traversal protected) |
| `write_file` | Write file to workspace |
| `list_directory` | List workspace directory contents |

All tools accept `instance_id` (default: `"default"`) for multi-instance isolation.

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
uv run python -m ns_hpc exec test-instance -- echo "hello"

# Start MCP server
uv run python -m ns_hpc run
```

## Requirements

- Linux with user namespaces enabled
- bwrap (bubblewrap) 0.11+
- Python 3.14+
- (Optional) Slurm: sbatch, squeue, sacct

## License

MIT
