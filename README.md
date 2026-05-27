# ns-hpc MCP Server

HPC sandboxing via bubblewrap — an MCP server that manages sandbox instances
and executes commands inside isolated bwrap containers.

```bash
pip install git+https://github.com/li-yq/namespaced-hpc-mcp.git
```

## Quick Start

```bash
# Run diagnostics
ns-hpc doctor

# Create an instance and run a command
ns-hpc instance create my-inst
ns-hpc bwrap my-inst -- ls -la

# Interactive shell
ns-hpc instance enter my-inst

# Start the MCP server
ns-hpc run                                    # stdio (default)
ns-hpc run --transport streamable-http        # HTTP on :8000/mcp
ns-hpc run -t streamable-http --uds /tmp/ns-hpc.sock  # Unix socket
```

## Architecture

```
┌─────────────────────────────────────────────┐
│              MCP Client (LLM)               │
└──────────────┬──────────────────────────────┘
               │ stdio / streamable-http / SSE
┌──────────────▼──────────────────────────────┐
│          ns-hpc MCP Server                  │
│                                             │
│  ┌──────────────────────┐  ┌─────────────┐  │
│  │ submit_job / poll_job│  │ WebDAV /dav │  │
│  │ (bwrap exec)         │  │ (GET/PUT)   │  │
│  └──────────┬───────────┘  └──────┬──────┘  │
│             │                      │        │
│  ┌──────────▼──────────────────────▼───────┐ │
│  │         bwrap sandbox                  │ │
│  │  ┌──────────────────────────────────┐  │ │
│  │  │ /workspace  (rw, bind-mounted)   │  │ │
│  │  │ /output     (rw, bind-mounted)   │  │ │
│  │  │ /usr /lib /bin /etc  (ro)        │  │ │
│  │  │ /proc /dev  (virtual)            │  │ │
│  │  │ /tmp        (tmpfs)              │  │ │
│  │  └──────────────────────────────────┘  │ │
│  └────────────────────────────────────────┘ │
│                                             │
│  ┌──────────────────────────────────────┐   │
│  │  Instance: ~/.local/share/ns-hpc/    │   │
│  │            instances/{id}/            │   │
│  │  ├── workspace/  (rw host-side bind) │   │
│  │  ├── .ns_hpc_output/  (job outputs)  │   │
│  │  ├── .ns_hpc_jobs/    (job state)    │   │
│  │  ├── status/          (bwrap fd)     │   │
│  │  ├── metadata.json                   │   │
│  │  └── audit.log     (host-side only)  │   │
│  └──────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
```

### Key Design Decisions

- **Stateless bwrap**: Every command creates a fresh sandbox. No persistent
  Linux namespaces. The kernel tears down the sandbox when the outer bwrap
  process exits.
- **Audit log on host**: Written outside the sandbox — the sandbox cannot
  tamper with its own audit trail.
- **Shared network**: `--share-net` overrides `--unshare-all`, so processes
  inside bwrap share the host network namespace. This enables WebDAV and
  proxied MCP servers to bind ports reachable from the host.

## Configuration

Configuration merges three layers (highest priority last):

1. Built-in defaults
2. `~/.config/ns-hpc/config.toml` (XDG)
3. `NS_HPC_CONFIG` env var or `--config` CLI flag

See `config/config.toml` for the full reference.

```toml
# ~/.config/ns-hpc/config.toml

[namespace]
bwrap_command = [
    "bwrap",
    "--unshare-all", "--share-net",
    "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
    "--ro-bind", "/usr", "/usr",
    "--ro-bind", "/lib", "/lib",
    "--ro-bind", "/lib64", "/lib64",
    "--ro-bind", "/bin", "/bin",
    "--ro-bind", "/sbin", "/sbin",
    "--ro-bind", "/etc", "/etc",
]

[jobs]
max_timeout = 3600

[jobs.local]
use_cgroups = true
cgroups_command = [
    "systemd-run", "--user", "--scope",
    "-p", "CPUQuota=400%",
    "-p", "MemoryMax=8G",
    "--",
]

[jobs.slurm]
sbatch_command = [
    "sbatch",
    "--partition", "cpu",
    "--cpus-per-task={cpus}",
    "--mem={memory}M",
]

[jobs.slurm.limit]
cpus = { default = 1, max = 8 }
memory = { default = 4096, max = 32768 }

# WebDAV file access (default: disabled)
[dav]
enabled = true

[dav.extras.external-data]
path = "/public5/home/t6s001890/data"
ro = true

[proxied_mcps.filesystem]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/"]
# include = ["read_*", "list_*"]
# exclude = ["*_dangerous"]
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `ns-hpc doctor` | Diagnose system prerequisites |
| `ns-hpc bwrap <id> -- <cmd>` | Run command in raw bwrap sandbox |
| `ns-hpc run` | Start MCP server (stdio, streamable-http, sse, UDS) |
| `ns-hpc clean --days 7` | Remove stale instances |
| `ns-hpc instance create <id>` | Create a new sandbox instance |
| `ns-hpc instance list` | List all instances |
| `ns-hpc instance list-archived` | List archived instances |
| `ns-hpc instance describe <id>` | Show instance metadata |
| `ns-hpc instance update <id> -d <desc>` | Update description |
| `ns-hpc instance enter <id>` | Interactive bash in sandbox |
| `ns-hpc instance run <id> -- <cmd>` | Run command as an async job |
| `ns-hpc instance status <id> <job>` | Check job status |
| `ns-hpc instance jobs <id>` | List tracked jobs |
| `ns-hpc instance cancel <id> <job>` | Cancel a running job |
| `ns-hpc instance archive <id>` | Archive instance (disables new jobs) |

## MCP Tools

### Instance Management

| Tool | Description |
|------|-------------|
| `create_instance` | Create a new sandbox instance |
| `list_instances` | List all active instances |
| `list_archived_instances` | List all archived instances |
| `update_instance` | Update instance metadata (description) |
| `archive_instance` | Archive an instance, disabling new jobs |

### Job Execution

| Tool | Description |
|------|-------------|
| `submit_job` | Submit a command as an async job (local or Slurm) |
| `poll_job` | Poll a running job, optionally wait for completion |
| `list_jobs` | List all tracked jobs for an instance |
| `cancel_job` | Cancel a running job and return final output |

### File Access

| Tool | Description |
|------|-------------|
| `filesystem__read_text_file` | Read a file from the sandbox workspace |
| `filesystem__write_file` | Write a file to the sandbox workspace |
| `filesystem__list_directory` | List directory contents |
| `filesystem__*` | Additional proxied filesystem tools (search, move, etc.) |

> The `filesystem__*` tools are proxied from the
> `@modelcontextprotocol/server-filesystem` MCP server running inside bwrap.
> They can be filtered with `include`/`exclude` patterns in config.

## WebDAV File Transfer

When `[dav].enabled = true` (and the server runs in HTTP mode), the WebDAV
endpoint at `/dav/` provides direct file access from Finder, Windows
Explorer, rclone, or curl.

```
/dav/instances/{id}/workspace/...   (read-write)
/dav/instances/{id}/output/...      (read-write)
/dav/{extra_name}/...               (config-controlled, defaults to ro)
```

```bash
# Mount in Finder: ⌘K → http://127.0.0.1:8000/dav/
# Or with curl:
curl http://127.0.0.1:8000/dav/instances/my-inst/workspace/file.txt
curl -T data.csv http://127.0.0.1:8000/dav/instances/my-inst/workspace/data.csv
curl -X DELETE http://127.0.0.1:8000/dav/instances/my-inst/workspace/old.txt
```

- **Writes** (PUT, DELETE, MKCOL) are audited to the instance `audit.log`.
- Archived instances return 404.
- Path traversal (symlinks, `..`) is blocked.
- Read-only extra mounts reject writes.

## Remote HPC Setup

### 1. Install on the HPC node

```bash
pip install git+https://github.com/li-yq/namespaced-hpc-mcp.git
ns-hpc doctor
```

### 2. Configure for your cluster

```toml
# ~/.config/ns-hpc/config.toml
[namespace]
bwrap_command = [
    "bwrap",
    "--unshare-all", "--share-net",
    "--uid", "1000", "--gid", "1000",
    "--ro-bind", "/home/user/.local/share/ns-hpc/rootfs", "/",
    "--ro-bind", "/home/user/.local/share/ns-hpc/agent-tools", "/opt/agent-tools",
    "--proc", "/proc", "--dev", "/dev",
    "--tmpfs", "/run", "--tmpfs", "/tmp",
]
workspace_mount = "/home/agent"
output_mount = "/mnt/output"
shared_output_mount = "/mnt/shared-output"

[jobs.slurm]
sbatch_command = ["sbatch", "--partition", "compute", "--cpus-per-task={cpus}", "--mem={memory}M"]

[jobs.slurm.limit]
cpus = { default = 1, max = 32 }
memory = { default = 4096, max = 131072 }
```

### 3. Start the server

```bash
# stdio (for SSH-based MCP clients)
ns-hpc run

# Streamable HTTP (recommended for direct HTTP)
ns-hpc run --transport streamable-http --port 8000

# With WebDAV
ns-hpc run --transport streamable-http --port 8000  # set [dav].enabled=true
```

### 4. MCP client config

```json
{
  "mcpServers": {
    "ns-hpc": {
      "command": "ssh",
      "args": ["user@hpc-login", "ns-hpc", "run"]
    }
  }
}
```

Or for HTTP:

```json
{
  "mcpServers": {
    "ns-hpc": {
      "url": "http://hpc-login:8000/mcp"
    }
  }
}
```

## Development

```bash
uv sync
uv run pytest                          # Full test suite
uv run python -m ns_hpc doctor         # Diagnostics
uv run python -m ns_hpc run            # Start server
```

**Tests by tier:**

| Tier | Command |
|------|---------|
| Pure unit (no bwrap) | `uv run pytest tests/test_{config,instance,namespace,proxy,proxy_server,server,file_server}.py -v` |
| Unit + bwrap | `uv run pytest tests/test_{job_manager,bwrap_primitive}.py -v` |
| Full Slurm integration | `cd slurm && bash setup.sh && bash test_session.sh` |

## Security

- All commands run via `bwrap --unshare-all` (user, PID, mount, IPC, UTS,
  CGROUP namespaces)
- System paths are read-only (`--ro-bind`)
- `/tmp` is a fresh tmpfs
- Workspace is the only writable bind mount
- Audit log written host-side, never exposed to sandbox
- Path traversal blocked in filesystem and WebDAV tools
- `ns-hpc doctor` validates prerequisites

## Requirements

- Linux with user namespaces enabled
- bwrap (bubblewrap) 0.11+
- Python 3.12+
- (Optional) Slurm: sbatch, squeue, sacct
- (Optional for WebDAV) network access for Finder/rclone/curl clients

## License

MIT
