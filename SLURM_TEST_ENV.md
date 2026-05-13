# ns-hpc Slurm Test Environment

## Overview

A local Slurm cluster for testing ns-hpc's Slurm task engine, running in
rootless podman containers from a docker-compose.yml in this repo.

## Quick Start

```bash
cd slurm

# Full refresh: tear down, start fresh, install ns-hpc
bash setup.sh

# Or just build and start the cluster manually:
podman-compose up -d
```

## Services

| Container | Image | Role |
|-----------|-------|------|
| slurm-mysql | mariadb:12 | Accounting database |
| slurm-slurmdbd | ns-hpc-slurm:latest | Slurm Database Daemon |
| slurm-slurmctld | ns-hpc-slurm:latest | Slurm Controller (privileged) |
| slurm-cpu-worker | ns-hpc-slurm:latest | Single compute node c1 (privileged) |

All services use a bridge network (`slurm-network`). podman's built-in DNS
resolves container names automatically (tested with rootless podman 5.8.1).

## Architecture

```
┌─────────────────────────────────────────────────┐
│ slurm-network (bridge)                           │
│                                                   │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │  mysql    │  │ slurmdbd │  │  slurmctld    │  │
│  │  :3306   │←─│  :6819   │←─│  :6817        │  │
│  └──────────┘  └──────────┘  └───────┬───────┘  │
│                                       │           │
│                                ┌──────▼───────┐  │
│                                │  cpu-worker  │  │
│                                │  c1 idle     │  │
│                                └──────────────┘  │
└─────────────────────────────────────────────────┘
```

## Image

The `ns-hpc-slurm` image is built from `giovtorres/slurm-docker-cluster:latest`
with the following additions:

- **tini** — installed via EPEL (`dnf install tini`)
- **uv** — installed from <https://astral.sh/uv> (v0.11.13)
- **bubblewrap** — already present in the base image
- **testuser** — unprivileged user (uid 2000, gid 2000) for non-root testing

Dockerfile: `slurm/Dockerfile`

## Shared Filesystem

Both slurmctld and cpu-worker bind-mount the project root at `/ns-hpc-mcp`
(read-only) and share the writable named volume `slurm_jobdir` as
`/home/testuser`.  This means all testuser state (ns-hpc install, config,
instances, job output) is visible from both containers.

## Setup

The single `setup.sh` script handles the full lifecycle:

```bash
cd slurm
bash setup.sh
```

This tears down the old cluster, removes the shared volume for a clean slate,
starts a fresh cluster, and bootstraps the testuser environment:

| Step | What it does |
|------|-------------|
| 1. Tear down | `podman-compose down` + remove `slurm_jobdir` volume |
| 2. Start cluster | `podman-compose up -d`, wait for all 4 services healthy |
| 3. Fix ownership | `chown 2000:2000 /home/testuser` on the fresh volume |
| 4. Install ns-hpc | Creates venv at `~/.local/ns-hpc/venv`, `pip install -e /ns-hpc-mcp` |
| 5. Create config | Writes `~/.local/ns-hpc/config.toml` with `instances_dir = /home/testuser/mcp_instances` |
| 6. Verify | Runs `ns-hpc doctor` inside the container |

Re-run `bash setup.sh` after any change to start from a clean slate.

## MCP Inspector

Once the cluster is up, inspect the MCP server:

```bash
npx @modelcontextprotocol/inspector \
  podman exec --user testuser -w /home/testuser -i slurm-slurmctld \
  ns-hpc run
```

Or use the full path (useful when `~/.local/ns-hpc/venv/bin` is not on PATH):

```bash
NS_HPC_CONFIG=/home/testuser/.local/ns-hpc/config.toml \
  podman exec --user testuser -w /home/testuser -i slurm-slurmctld \
  /home/testuser/.local/ns-hpc/venv/bin/ns-hpc run
```

The Inspector sends/receives JSON-RPC messages over stdio, which
`podman exec -i` pipes to `ns-hpc run` inside the container.

## Verification

```bash
# Cluster status
podman exec slurm-slurmctld sinfo

# DNS resolution
podman exec slurm-slurmctld getent hosts slurmdbd

# ns-hpc doctor inside container
podman exec --user testuser -w /home/testuser slurm-slurmctld \
  /home/testuser/.local/ns-hpc/venv/bin/ns-hpc doctor

# bwrap works inside sandbox
podman exec slurm-cpu-worker bwrap --ro-bind /usr /usr -- /bin/true

# tini works
podman exec slurm-slurmctld tini -- /bin/true

# Project source accessible
podman exec slurm-slurmctld ls /ns-hpc-mcp/src/ns_hpc
```

## Tear Down

```bash
cd slurm
podman-compose down
podman volume rm -f slurm_slurm_etc_munge slurm_slurm_etc_slurm \
  slurm_slurm_var_log_slurm slurm_slurm_var_lib_mysql
```

## Test Requirements

Different tests require different environments:

| Tests | Requirements | Command |
|-------|-------------|---------|
| `test_config.py`, `test_instance.py`, `test_namespace.py`, `test_proxy.py`, `test_server.py` | Pure unit — no special setup | `uv run pytest tests/ -v` |
| `test_job_manager.py` | bwrap + user namespaces on host | `uv run pytest tests/test_job_manager.py -v` |
| `test_bwrap_primitive.py` | bwrap + user namespaces on host | `uv run pytest tests/test_bwrap_primitive.py -v` |
| `session_test.py` (local scenarios) | bwrap + user namespaces on host | `uv run python tests/session_test.py` |
| `session_test.py` (slurm scenarios) | Podman Slurm cluster | `bash slurm/test_session.sh` |
| cgroup resource tests | cgroup v2 + systemd on host | Requires `systemd-run --user --scope` |

**Quick local-only test run:**
```bash
uv run pytest tests/test_config.py tests/test_instance.py tests/test_namespace.py tests/test_proxy.py tests/test_server.py tests/test_bwrap_primitive.py tests/test_job_manager.py -v
```

**Full integration (inside slurm cluster):**
```bash
bash slurm/test_session.sh
```
