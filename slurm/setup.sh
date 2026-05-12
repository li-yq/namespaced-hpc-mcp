#!/bin/bash
# Full refresh of the ns-hpc Slurm test cluster.
#
# Tears down the old cluster, starts a fresh one, and bootstraps
# the testuser environment with ns-hpc installed from the source
# mount.  The shared volume (slurm_jobdir) serves as /home/testuser
# so that both slurmctld and cpu-worker see the same home.
#
# Usage:
#   cd slurm && bash setup.sh
#
# After setup, ns-hpc is available on PATH everywhere.  Inspect the server:
#   npx @modelcontextprotocol/inspector \
#     podman exec --user testuser -w /home/testuser -i slurm-slurmctld \
#     ns-hpc run

set -euo pipefail
cd "$(dirname "$0")"  # slurm/

HPC_HOME="${HPC_HOME:-/home/testuser/.local/ns-hpc}"
VENV="$HPC_HOME/venv"
CONFIG="$HPC_HOME/config.toml"
NS_HPC_BIN="/usr/local/bin/ns-hpc"

# ── 1. Tear down ──────────────────────────────────────────────────────────
echo "=== Tearing down old cluster ==="
podman-compose down 2>/dev/null || true
# Nuke the shared home volume so we start clean
podman volume rm -f slurm_slurm_jobdir 2>/dev/null || true

# ── 2. Start fresh ────────────────────────────────────────────────────────
echo "=== Starting cluster ==="
podman-compose up -d

echo "=== Waiting for cluster to be healthy ==="
for svc in mysql slurmdbd slurmctld cpu-worker; do
    name="slurm-${svc}"
    echo -n "  $name "
    until podman container inspect "$name" --format '{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; do
        echo -n "."
        sleep 2
    done
    echo " healthy"
done

# ── 3. Fix ownership on the fresh volume ──────────────────────────────────
echo "=== Initializing testuser home ==="
# Fresh volume is owned by root; fix so testuser (uid 2000) can write.
podman exec slurm-slurmctld chown -R 2000:2000 /home/testuser

# ── 4. Bootstrap ns-hpc environment ──────────────────────────────────────
# Create venv and install ns-hpc in editable mode (source is read-only
# mounted, but the venv on the shared volume is writable).
UV="/usr/local/bin/uv"

echo "=== Installing ns-hpc ==="
# uv venv creates parent directories automatically
podman exec --user testuser -w /home/testuser slurm-slurmctld \
    "$UV" venv "$VENV" --clear

podman exec --user testuser -w /home/testuser slurm-slurmctld \
    sh -c "VIRTUAL_ENV=$VENV $UV pip install -e /ns-hpc-mcp --quiet"

podman exec --user testuser -w /home/testuser slurm-slurmctld \
    sh -c "VIRTUAL_ENV=$VENV $UV pip install pytest pytest-asyncio --quiet"

# Make ns-hpc available on PATH via system-wide symlink
podman exec slurm-slurmctld ln -sf "$VENV/bin/ns-hpc" "$NS_HPC_BIN"
podman exec slurm-cpu-worker ln -sf "$VENV/bin/ns-hpc" "$NS_HPC_BIN"

# ── 5. Create config.toml ─────────────────────────────────────────────────
echo "=== Creating config ==="
podman exec --user testuser -w /home/testuser slurm-slurmctld \
    mkdir -p "$HPC_HOME" "$HPC_HOME/context"
# Copy context markdown files into the slurm config's context directory
for f in config/context/*.md; do
    podman cp "$f" slurm-slurmctld:"$HPC_HOME/context/"
done
# Write config via host temp file (avoids quoting/escaping issues)
TMP_CONFIG="$(mktemp)"
cat > "$TMP_CONFIG" <<'TOML'
[namespace_defaults]
bind_ro = ["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc"]
workspace_mount = "/workspace"
flags = ["--unshare-all", "--share-net", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]

[proxied_mcps]

[resource_defaults]
context_dirs = ["/home/testuser/.local/ns-hpc/context"]
resource_patterns = ["*.md"]

[slurm]
partition = "cpu"
default_cpus = 1
default_memory_gb = 4
default_timeout = 3600

[resource_limits]
local_timeout = 300
slurm_timeout = 86400

instances_dir = "/home/testuser/mcp_instances"
TOML
podman cp "$TMP_CONFIG" slurm-slurmctld:"$CONFIG"
podman exec slurm-slurmctld chown 2000:2000 "$CONFIG"
rm "$TMP_CONFIG"

# ── 6. Create activation script ──────────────────────────────────────────
podman exec --user testuser -w /home/testuser slurm-slurmctld \
    sh -c "cat > $HPC_HOME/activate.sh <<SCRIPT
export NS_HPC_CONFIG=$CONFIG
export PATH=$VENV/bin:\$PATH
export PS1=\"(ns-hpc) \$PS1\"
SCRIPT"

# ── 7. Verify ─────────────────────────────────────────────────────────────
echo "=== Verifying ==="
podman exec --user testuser -w /home/testuser slurm-slurmctld \
    ns-hpc doctor 2>&1 | head -10 || true

echo ""
echo "=== ns-hpc Slurm cluster ready ==="
echo "Config:     $CONFIG"
echo "Venv:       $VENV"
echo "Instances:  /home/testuser/mcp_instances"
echo ""
echo "Activate:   source $HPC_HOME/activate.sh"
echo ""
echo "Inspect:    npx @modelcontextprotocol/inspector \\"
echo "              podman exec --user testuser -w /home/testuser -i slurm-slurmctld $NS_HPC_BIN run"
