import os
import tempfile
from pathlib import Path

from ns_hpc.config import Config, load_config


def test_default_config_values():
    """Verify the built-in default values are sensible."""
    from ns_hpc.config import _default_config
    cfg = _default_config()
    assert cfg.namespace.workspace_mount == "/workspace"
    assert cfg.namespace.shared_output_mount == "/shared-output"
    assert "--share-net" in cfg.namespace.bwrap_command
    assert "filesystem" in cfg.proxied_mcps
    assert cfg.proxied_mcps["filesystem"].command == "npx"
    assert cfg.resource.context_dirs == ["context"]
    assert cfg.resource.resource_patterns == ["*.md"]
    assert cfg.jobs.slurm.limit["cpus"].default == 1
    assert cfg.jobs.slurm.limit["cpus"].max == 8
    assert cfg.jobs.slurm.limit["memory"].default == 4096
    assert cfg.jobs.slurm.limit["memory"].max == 32768
    assert cfg.jobs.max_timeout == 3600  # one hour


def test_config_fallback_nonexistent_returns_user_or_defaults():
    """When no config file is found, returns user-level config or built-in defaults."""
    cfg = load_config("/nonexistent/config.toml")
    assert isinstance(cfg, Config)
    # Always present regardless of fallback level
    assert cfg.namespace.workspace_mount
    assert isinstance(cfg.proxied_mcps, dict)


def test_config_from_toml():
    """A TOML file with custom values is loaded correctly."""
    toml_content = """
[namespace]
instances_dir = "~/.local/share/ns-hpc/instances"
bwrap_command = ["bwrap", "--unshare-all", "--share-net", "--proc", "/proc"]
workspace_mount = "/project"
output_mount = "/out"
shared_output_mount = "/shared-out"

[jobs]
max_timeout = 7200

[jobs.local]
use_cgroups = true
cgroups_command = ["systemd-run", "--user", "--scope", "-p", "CPUQuota=800%", "--"]
proc_check = true

[jobs.slurm]
sbatch_command = ["sbatch", "--partition", "gpu", "--cpus-per-task={cpus}", "--mem={memory}M"]

[jobs.slurm.limit]
cpus = {default = 2, max = 16}
memory = {default = 8192, max = 65536}
gpus = {default = 0, max = 8}

[proxied_mcps.my-mcp]
command = "uvx"
args = ["mcp-srv"]
env = {TOKEN = "sekret"}

[resource]
context_dirs = ["docs"]
resource_patterns = ["*.rst", "*.txt"]
"""
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".toml", delete=False) as f:
        f.write(toml_content.encode())
        tmp = f.name

    try:
        cfg = load_config(tmp)
        assert cfg.namespace.workspace_mount == "/project"
        assert cfg.namespace.output_mount == "/out"
        assert cfg.namespace.shared_output_mount == "/shared-out"
        assert cfg.namespace.bwrap_command == ["bwrap", "--unshare-all", "--share-net", "--proc", "/proc"]
        assert cfg.jobs.max_timeout == 7200
        assert cfg.jobs.local.use_cgroups is True
        assert cfg.jobs.local.cgroups_command == ["systemd-run", "--user", "--scope", "-p", "CPUQuota=800%", "--"]
        assert cfg.jobs.slurm.sbatch_command == ["sbatch", "--partition", "gpu", "--cpus-per-task={cpus}", "--mem={memory}M"]

        mcp = cfg.proxied_mcps["my-mcp"]
        assert mcp.command == "uvx"
        assert mcp.args == ["mcp-srv"]
        assert mcp.env == {"TOKEN": "sekret"}

        assert cfg.resource.context_dirs == ["docs"]
        assert cfg.resource.resource_patterns == ["*.rst", "*.txt"]
        assert cfg.jobs.slurm.limit["cpus"].default == 2
        assert cfg.jobs.slurm.limit["cpus"].max == 16
        assert cfg.jobs.slurm.limit["memory"].default == 8192
        assert cfg.jobs.slurm.limit["memory"].max == 65536
        assert cfg.jobs.slurm.limit["gpus"].default == 0
        assert cfg.jobs.slurm.limit["gpus"].max == 8
    finally:
        os.unlink(tmp)


def test_resolve_instances_dir():
    """instances_dir expands to ~/.local/share/ns-hpc/instances by default."""
    cfg = load_config("/nonexistent/config.toml")
    resolved = cfg.resolve_instances_dir()
    expected = Path.home() / ".local" / "share" / "ns-hpc" / "instances"
    assert resolved == expected.resolve()
    assert str(resolved).endswith("ns-hpc/instances")
