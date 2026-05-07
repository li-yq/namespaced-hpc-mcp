# ns-hpc Slurm Test Environment

## Overview

A local Slurm cluster for testing ns-hpc's Slurm task engine, running in
rootful podman containers.

## Quick Start

```bash
cd /home/liyq/workspace/ns-hpc-mcp
./scripts/slurm-cluster.sh start   # Start the cluster
./scripts/slurm-cluster.sh stop    # Stop the cluster
./scripts/slurm-cluster.sh status  # Check status
```

## Current Status (May 7, 2026)

| Container | Status | Notes |
|-----------|--------|-------|
| mysql     | Up     | MariaDB 12, Slurm accounting DB |
| slurmdbd  | Up     | Slurm Database Daemon |
| slurmctld | Up     | Slurm Controller |
| cpu-worker| Up     | 1 compute node (`c1`, idle) |

The cluster uses rootful podman (`sudo podman`). The worker runs slurmd
as root inside the container to bypass cgroup v2 permission issues with
Slurm 25.11's systemd scope management.

## Usage

```bash
# Check cluster status
sudo podman exec slurmctld sinfo
# Output: cpu* idle 1 c1

# Submit a test job
sudo podman exec slurmctld sbatch --wrap="echo 'Hello from ns-hpc'"

# Check job status
sudo podman exec slurmctld sacct --format=JobID,State,ExitCode,NodeList

# Clean up
sudo podman exec slurmctld scancel -u root
```

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
│                                │  slurmd -Z   │  │
│                                │  "c1" idle   │  │
│                                └──────────────┘  │
└─────────────────────────────────────────────────┘
```

## Ports

| Port | Service | Host |
|------|---------|------|
| 3022 | SSH     | No (SSH_ENABLE=false) |
| 6817 | slurmctld | Container only |
| 6818 | slurmd | Container only |
| 6819 | slurmdbd | Container only |

## Implementation Note

The worker runs slurmd as root (not `gosu slurm`) because Slurm 25.11's
cgroup/v2 plugin needs to create cgroup directories under
`/sys/fs/cgroup/machine.slice/`, which requires root access even with
`--cgroupns=host` and writable cgroup bind mount.

Run command used:
```bash
--entrypoint /bin/bash -c '
  gosu munge /usr/sbin/munged
  # ... wait for slurmctld ...
  exec /usr/sbin/slurmd -Z -Dvvv --conf "Feature=cpu"
'
```

## Tear Down

```bash
sudo podman rm -f cpu-worker slurmctld slurmdbd mysql 2>/dev/null
sudo podman volume rm etc_munge etc_slurm var_log_slurm slurm_jobdir 2>/dev/null
sudo podman network rm slurm-network 2>/dev/null
```
