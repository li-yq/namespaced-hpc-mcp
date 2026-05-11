#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# setup-hpc-user.sh — Bootstrap a realistic HPC user environment for ns-hpc.
#
# Installs the project into the user's home directory and creates a standard
# config layout at  $HOME/.local/ns-hpc/{config.toml,context/}.
#
# Usage:  ./setup-hpc-user.sh              (run as the target user)
#         sudo -u testuser ./setup-hpc-user.sh
#
# Environment variables:
#   NS_HPC_SRC   — path to the ns-hpc project source  (default: /ns-hpc-mcp)
#   NS_HPC_HOME  — config & venv root                 (default: $HOME/.local/ns-hpc)
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SRC="${NS_HPC_SRC:-/ns-hpc-mcp}"
HPC_HOME="${NS_HPC_HOME:-$HOME/.local/ns-hpc}"
VENV="$HPC_HOME/venv"
CONTEXT_DIR="$HPC_HOME/context"
CONFIG="$HPC_HOME/config.toml"

echo "==> Installing ns-hpc for user $(whoami)"
echo "    Source:   $SRC"
echo "    Prefix:   $HPC_HOME"

# ── 1. Verify source is readable ────────────────────────────────────────────
if [ ! -f "$SRC/pyproject.toml" ]; then
    echo "Error: ns-hpc source not found at $SRC" >&2
    exit 1
fi

# ── 2. Create directory structure ───────────────────────────────────────────
mkdir -p "$HPC_HOME" "$CONTEXT_DIR"

# ── 3. Create Python virtual environment and install ns-hpc ─────────────────
echo "==> Creating virtual environment at $VENV"
python3 -m venv --clear "$VENV"

echo "==> Installing ns-hpc and dependencies"
"$VENV/bin/pip" install --quiet -e "$SRC"
"$VENV/bin/pip" install --quiet pytest pytest-asyncio

echo "==> Installing uv into venv"
"$VENV/bin/pip" install --quiet uv

# ── 4. Write config.toml ────────────────────────────────────────────────────
cat > "$CONFIG" <<'CONFIG_EOF'
[namespace_defaults]
bind_ro = ["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc"]
workspace_mount = "/workspace"
flags = ["--unshare-all", "--share-net", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]

[proxied_mcps]

[resource_defaults]
context_dirs = ["context"]
resource_patterns = ["*.md"]

[slurm]
partition = "cpu"
default_cpus = 1
default_memory_gb = 4
default_timeout = 3600

[resource_limits]
local_timeout = 300
slurm_timeout = 86400

instances_dir = "${HOME}/mcp_instances"
CONFIG_EOF
# Expand $HOME in the heredoc output — we intentionally quoted 'CONFIG_EOF'
# to avoid early expansion, so substitute now.
sed -i "s|\${HOME}|$HOME|g" "$CONFIG"

# ── 5. Create a placeholder context document ─────────────────────────────────
if [ ! -f "$CONTEXT_DIR/README.md" ]; then
    cat > "$CONTEXT_DIR/README.md" <<'EOF'
# ns-hpc Context

This directory contains Markdown files that are exposed as MCP resources.
Add documentation, guidelines, or reference material here.
EOF
fi

# ── 6. Create convenience activation script ──────────────────────────────────
ACTIVATE="$HPC_HOME/activate.sh"
cat > "$ACTIVATE" <<SCRIPT
# Source this file to activate the ns-hpc environment:
#   source \$HOME/.local/ns-hpc/activate.sh
export NS_HPC_CONFIG="$CONFIG"
export PATH="$VENV/bin:\$PATH"
export PS1="(ns-hpc) \$PS1"
SCRIPT
chmod +x "$ACTIVATE"

# ── 7. Summary ──────────────────────────────────────────────────────────────
echo ""
echo "┌──────────────────────────────────────────────────────────┐"
echo "│  ns-hpc HPC user setup complete                          │"
echo "├──────────────────────────────────────────────────────────┤"
echo "│                                                          │"
echo "│  Activate with:                                          │"
echo "│    source $ACTIVATE     │"
echo "│                                                          │"
echo "│  Config:      $CONFIG  │"
echo "│  Instances:   $HOME/mcp_instances                    │"
echo "│  Context:     $CONTEXT_DIR                           │"
echo "│  Venv:        $VENV                               │"
echo "│                                                          │"
echo "│  Or run directly:                                        │"
echo "│    NS_HPC_CONFIG=$CONFIG \\"
echo "│      $VENV/bin/ns-hpc doctor            │"
echo "│                                                          │"
echo "└──────────────────────────────────────────────────────────┘"
