import os
import tempfile
from pathlib import Path

from ns_hpc.config import Config, load_config, parse_memory


def test_parse_memory():
    """parse_memory converts human-readable strings to bytes."""
    assert parse_memory("1K") == 1024
    assert parse_memory("1M") == 1024 ** 2
    assert parse_memory("1G") == 1024 ** 3
    assert parse_memory("512M") == 512 * 1024 ** 2
    assert parse_memory(4096) == 4096
    assert parse_memory("4096") == 4096


def test_default_config_values():
    """Verify the built-in default values are sensible."""
    # Import _default_config directly so we aren't affected by ~/.local/ns-hpc/config.toml
    from ns_hpc.config import _default_config
    cfg = _default_config()
    assert cfg.namespace_defaults.workspace_mount == "/workspace"
    assert "--share-net" in cfg.namespace_defaults.flags
    assert "filesystem" in cfg.proxied_mcps
    assert cfg.proxied_mcps["filesystem"].command == "npx"
    assert cfg.resource_defaults.context_dirs == ["context"]
    assert cfg.resource_defaults.resource_patterns == ["*.md"]
    assert cfg.slurm.partition == "debug"
    assert cfg.resources.cpus == 4
    assert cfg.resources.memory == "8G"
    assert cfg.slurm.resources["cpus"].default == 1
    assert cfg.slurm.resources["cpus"].max == 8
    assert cfg.slurm.resources["memory"].default == "4G"
    assert cfg.slurm.resources["memory"].max == "32G"
    assert cfg.job.max_timeout == 3600  # one hour


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

[resources]
cpus = 8
memory = "16G"

[slurm]
partition = "gpu"

[slurm.resources.cpus]
parameter = "--cpus-per-task={}"
default = 2
max = 16

[slurm.resources.memory]
parameter = "--mem={}"
default = "8G"
max = "64G"

[slurm.resources.gpus]
parameter = "--gres=gpu:{}"
default = 0
max = 8
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
        assert cfg.resources.cpus == 8
        assert cfg.resources.memory == "16G"
        assert cfg.slurm.resources["cpus"].default == 2
        assert cfg.slurm.resources["cpus"].max == 16
        assert cfg.slurm.resources["cpus"].parameter == "--cpus-per-task={}"
        assert cfg.slurm.resources["memory"].default == "8G"
        assert cfg.slurm.resources["memory"].max == "64G"
        assert cfg.slurm.resources["gpus"].default == 0
        assert cfg.slurm.resources["gpus"].max == 8
        assert cfg.slurm.resources["gpus"].parameter == "--gres=gpu:{}"
    finally:
        os.unlink(tmp)


def test_resolve_instances_dir():
    """instances_dir expands to ~/.local/share/ns-hpc/instances by default."""
    cfg = load_config("/nonexistent/config.toml")
    resolved = cfg.resolve_instances_dir()
    expected = Path.home() / ".local" / "share" / "ns-hpc" / "instances"
    assert resolved == expected.resolve()
    assert str(resolved).endswith("ns-hpc/instances")
