# ns-hpc Slurm Test Environment

## Overview

This document describes how to set up a Slurm test cluster for developing and
testing ns-hpc's Slurm task engine. The cluster runs in containers via podman.

## Running the Cluster

A setup script is provided to start the cluster:

```bash
cd /home/liyq/workspace/slurm-docker-cluster
bash setup-slurm-cluster.sh
```

## Current Status

### Running containers (May 7, 2026):

| Container | Status | Notes |
|-----------|--------|-------|
| mysql     | Up     | MariaDB 12, serves as Slurm accounting DB |
| slurmdbd  | Up     | Slurm Database Daemon |
| slurmctld | Up     | Slurm Controller (head node) |

### Worker node (cpu-worker)

Dynamic node registration (slurmd -Z) fails under rootless podman because
Slurm 25.11's cgroup/v2 plugin requires dbus/systemd for slurmstepd scope
management, which isn't available inside the container.

**Current workaround:** Run slurmd inside the slurmctld container (same PID
and cgroup namespace):

```bash
podman exec slurmctld bash -c '
  # Start munged if not running
  gosu munge /usr/sbin/munged

  # Add static node
  sed -i "/^NodeName=/d" /etc/slurm/slurm.conf
  echo "NodeName=localhost NodeAddr=127.0.0.1 CPUs=4 RealMemory=4000 State=UNKNOWN" >> /etc/slurm/slurm.conf
  scontrol reconfigure

  # Start slurmd
  gosu slurm /usr/sbin/slurmd -Z -Dvvv --conf "Feature=cpu"
'
```

## Usage

### Check cluster status

```bash
podman exec slurmctld sinfo
```

### Submit a test job

```bash
podman exec slurmctld sbatch --wrap="echo 'Hello from Slurm'"
podman exec slurmctld squeue
```

## Limitations

- Dynamic node registration doesn't work with rootless podman + cgroup v2
- Only one node (slurmctld itself) is available for job execution
- GPU workers not configured

## Tear Down

```bash
podman rm -f cpu-worker slurmctld slurmdbd mysql 2>/dev/null
podman volume rm etc_munge etc_slurm var_log_slurm slurm_jobdir 2>/dev/null
podman network rm slurm-network 2>/dev/null
```
