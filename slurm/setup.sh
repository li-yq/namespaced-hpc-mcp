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
CONFIG="${XDG_CONFIG_HOME:-/home/testuser/.config}/ns-hpc/config.toml"
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

# ── 5. Install filesystem MCP proxy ──────────────────────────────────────
echo "=== Installing filesystem MCP ==="
# Pre-install globally so npx doesn't download on every discovery.
podman exec slurm-slurmctld \
    npm install -g @modelcontextprotocol/server-filesystem --quiet 2>/dev/null || true

# ── 6. Create config.toml ─────────────────────────────────────────────────
echo "=== Creating config ==="
podman exec slurm-slurmctld mkdir -p "$(dirname "$CONFIG")"
podman exec --user testuser -w /home/testuser slurm-slurmctld \
    mkdir -p "$HPC_HOME" "$HPC_HOME/context"
# Copy context markdown files into the slurm config's context directory
for f in ../config/context/*.md; do
    podman cp "$f" slurm-slurmctld:"$HPC_HOME/context/"
done
# Write config via host temp file (avoids quoting/escaping issues)
TMP_CONFIG="$(mktemp)"
cat > "$TMP_CONFIG" <<'TOML'
# ── Namespace sandbox defaults ───────────────────────────────────────────────
[namespace]
instances_dir = "/home/testuser/.local/share/ns-hpc/instances"
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
workspace_mount = "/workspace"
output_mount = "/output"
shared_output_mount = "/shared-output"

# ── Job execution ────────────────────────────────────────────────────────────
[jobs]
max_timeout = 3600

[jobs.local]
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

# ── Proxied MCP servers ──────────────────────────────────────────────────────
[proxied_mcps.filesystem]
command = "mcp-server-filesystem"
args = ["/"]

# ── Context resources ────────────────────────────────────────────────────────
[resource]
context_dirs = ["~/.local/ns-hpc/context"]
resource_patterns = ["*.md"]

# ── WebDAV file access ───────────────────────────────────────────────────────
[dav]
enabled = true
TOML
podman cp "$TMP_CONFIG" slurm-slurmctld:"$CONFIG"
podman exec slurm-slurmctld chown 2000:2000 "$CONFIG"
rm "$TMP_CONFIG"

# ── 7. Create activation script ──────────────────────────────────────────
podman exec --user testuser -w /home/testuser slurm-slurmctld \
    sh -c "cat > $HPC_HOME/activate.sh <<SCRIPT
export NS_HPC_CONFIG=$CONFIG
export PATH=$VENV/bin:\$PATH
export PS1=\"(ns-hpc) \$PS1\"
SCRIPT"

# ── 8. Verify ─────────────────────────────────────────────────────────────
echo "=== Verifying ==="
podman exec --user testuser -w /home/testuser slurm-slurmctld \
    ns-hpc doctor 2>&1 | head -10 || true

echo ""
echo "=== ns-hpc Slurm cluster ready ==="
echo "Config:     $CONFIG"
echo "Venv:       $VENV"
echo "Instances:  /home/testuser/.local/share/ns-hpc/instances"
echo ""
echo "Activate:   source $HPC_HOME/activate.sh"
echo "           (sets NS_HPC_CONFIG and adds ns-hpc to PATH)"
echo ""
echo "Inspect:    npx @modelcontextprotocol/inspector \\"
echo "              podman exec --user testuser -w /home/testuser -i slurm-slurmctld $NS_HPC_BIN run"
