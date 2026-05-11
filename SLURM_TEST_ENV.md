# ns-hpc Slurm Test Environment

## Overview

A local Slurm cluster for testing ns-hpc's Slurm task engine, running in
rootless podman containers from a docker-compose.yml in this repo.

## Quick Start

```bash
cd slurm

# Build and start the cluster
podman-compose up -d

# Check cluster status
podman exec slurm-slurmctld sinfo
# Output: cpu* idle 1 c1, debug idle 1 c1

# Run a test job
podman exec slurm-slurmctld sbatch --wrap="echo 'Hello from ns-hpc'"

# Check job status
podman exec slurm-slurmctld sacct --format=JobID,State,ExitCode,NodeList

# Stop the cluster
podman-compose down

# Full cleanup (removes volumes too)
podman-compose down && podman volume rm -f \
  slurm_slurm_etc_munge slurm_slurm_etc_slurm \
  slurm_slurm_var_log_slurm slurm_slurm_var_lib_mysql
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

To run as the test user inside a container:

```bash
podman exec --user testuser -w /data slurm-slurmctld bash
```

## Shared Filesystem

Both slurmctld and cpu-worker bind-mount the project root at `/ns-hpc-mcp` (read-only) and share a writable named volume at `/data` for job output and runtime data.

Slurm jobs should write output to `/data` (the writable shared volume) rather than the
read-only project tree.

## Configuration

Slurm configuration (slurm.conf, slurmdbd.conf) is baked into the base image.
Key settings:

- Partition `cpu`: default, infinite time limit
- Partition `debug`: infinite time limit, shares c1
- Node c1: 4 CPUs, ~9 GB RAM, feature=cpu
- Dynamic node registration via `slurmd -Z`

## Verification

```bash
# Cluster status
podman exec slurm-slurmctld sinfo

# DNS resolution
podman exec slurm-slurmctld getent hosts slurmdbd

# bwrap works
podman exec slurm-cpu-worker bwrap --ro-bind /usr /usr -- /bin/true

# tini works
podman exec slurm-slurmctld tini -- /bin/true

# Project source accessible
podman exec slurm-slurmctld ls /ns-hpc-mcp/src/ns_hpc
```

## Files

```
slurm/
├── Dockerfile              # Image build: add tini + uv
└── docker-compose.yml      # Service definitions
```

## Tear Down

```bash
cd slurm
podman-compose down
podman volume rm -f slurm_slurm_etc_munge slurm_slurm_etc_slurm \
  slurm_slurm_var_log_slurm slurm_slurm_var_lib_mysql
```
