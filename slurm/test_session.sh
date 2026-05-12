#!/bin/bash
# Run integration tests inside the slurm cluster.
# Usage:  bash slurm/test_session.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HPC_HOME="${HPC_HOME:-/home/testuser/.local/ns-hpc}"
VENV="$HPC_HOME/venv"
NS_HPC_CONFIG="/home/testuser/.local/ns-hpc/config.toml"

echo "=== ns-hpc integration test (slurm) ==="
echo "Config: $NS_HPC_CONFIG"
echo

# Run the test script inside the slurmctld container as uid 2000 (testuser)
podman exec --user 2000 -w /home/testuser slurm-slurmctld \
    sh -c "cd /ns-hpc-mcp && NS_HPC_CONFIG=$NS_HPC_CONFIG $VENV/bin/python tests/test_session.py"
