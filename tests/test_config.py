import os
import tempfile
from pathlib import Path

from ns_hpc.config import Config, NamespaceDefaults, ResourceDefaults, load_config


def test_default_config():
    """Loading from a nonexistent path returns sensible defaults."""
    cfg = load_config("/nonexistent/config.toml")
    assert isinstance(cfg, Config)
    assert cfg.namespace_defaults.bind_ro == [
        "/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc"
    ]
    assert cfg.namespace_defaults.workspace_mount == "/workspace"
    assert "--share-net" in cfg.namespace_defaults.flags
    assert cfg.proxied_mcps == {}
    assert cfg.resource_defaults.context_dirs == ["config/context"]
    assert cfg.resource_defaults.resource_patterns == ["*.md"]


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
    finally:
        os.unlink(tmp)


def test_resolve_instances_dir():
    """${HOME} in instances_dir is expanded to the real home directory."""
    cfg = load_config("/nonexistent/config.toml")
    resolved = cfg.resolve_instances_dir()
    home = Path.home().resolve()
    assert resolved == home / "mcp_instances"
