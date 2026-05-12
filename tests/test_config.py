import os
import tempfile
from pathlib import Path

from ns_hpc.config import Config, load_config


def test_default_config_values():
    """Verify the built-in default values are sensible."""
    # Import _default_config directly so we aren't affected by ~/.local/ns-hpc/config.toml
    from ns_hpc.config import _default_config
    cfg = _default_config()
    assert cfg.namespace_defaults.workspace_mount == "/workspace"
    assert "--share-net" in cfg.namespace_defaults.flags
    assert cfg.proxied_mcps == {}
    assert cfg.resource_defaults.context_dirs == ["config/context"]
    assert cfg.resource_defaults.resource_patterns == ["*.md"]
    assert cfg.slurm.partition == "debug"
    assert cfg.slurm.default_cpus == 1
    assert cfg.slurm.default_memory_gb == 4
    assert cfg.slurm.default_timeout == 3600
    assert cfg.resource_limits.local_timeout == 300
    assert cfg.resource_limits.slurm_timeout == 86400


def test_config_fallback_nonexistent_returns_user_or_defaults():
    """When no config file is found, returns user-level config or built-in defaults."""
    cfg = load_config("/nonexistent/config.toml")
    assert isinstance(cfg, Config)
    # Always present regardless of fallback level
    assert cfg.namespace_defaults.workspace_mount
    assert isinstance(cfg.proxied_mcps, dict)


def test_config_from_toml():
    """A TOML file with custom values is loaded correctly."""
    toml_content = """
[namespace_defaults]
bind_ro = ["/custom", "/paths"]
workspace_mount = "/project"
flags = ["--unshare-all"]

[proxied_mcps.my-mcp]
command = "uvx"
args = ["mcp-srv"]
env = {TOKEN = "sekret"}

[resource_defaults]
context_dirs = ["docs"]
resource_patterns = ["*.rst", "*.txt"]

[slurm]
partition = "gpu"
default_cpus = 2
default_memory_gb = 8

[resource_limits]
local_timeout = 600
slurm_timeout = 43200
"""
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".toml", delete=False) as f:
        f.write(toml_content.encode())
        tmp = f.name

    try:
        cfg = load_config(tmp)
        assert cfg.namespace_defaults.bind_ro == ["/custom", "/paths"]
        assert cfg.namespace_defaults.workspace_mount == "/project"
        assert cfg.namespace_defaults.flags == ["--unshare-all"]

        mcp = cfg.proxied_mcps["my-mcp"]
        assert mcp.command == "uvx"
        assert mcp.args == ["mcp-srv"]
        assert mcp.env == {"TOKEN": "sekret"}

        assert cfg.resource_defaults.context_dirs == ["docs"]
        assert cfg.resource_defaults.resource_patterns == ["*.rst", "*.txt"]
        assert cfg.slurm.partition == "gpu"
        assert cfg.slurm.default_cpus == 2
        assert cfg.slurm.default_memory_gb == 8
        assert cfg.resource_limits.local_timeout == 600
        assert cfg.resource_limits.slurm_timeout == 43200
    finally:
        os.unlink(tmp)


def test_resolve_instances_dir():
    """${HOME} in instances_dir is expanded to the real home directory."""
    cfg = load_config("/nonexistent/config.toml")
    resolved = cfg.resolve_instances_dir()
    home = Path.home().resolve()
    assert str(resolved).startswith(str(home))
