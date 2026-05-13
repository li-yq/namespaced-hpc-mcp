import logging
import os
from pathlib import Path

from pydantic import BaseModel
import tomli


logger = logging.getLogger("ns-hpc")


class NamespaceDefaults(BaseModel):
    bind_ro: list[str]
    workspace_mount: str
    flags: list[str]
    status_fd: int = 3


class ProxiedMCP(BaseModel):
    command: str
    args: list[str] | None = None
    env: dict[str, str] | None = None


class ResourceDefaults(BaseModel):
    context_dirs: list[str] = ["config/context"]
    resource_patterns: list[str] = ["*.md"]


class SlurmConfig(BaseModel):
    partition: str = "debug"
    default_cpus: int = 1
    default_memory_gb: int = 4
    default_timeout: int = 3600


class ResourceLimits(BaseModel):
    local_timeout: int = 300
    slurm_timeout: int = 86400


class JobConfig(BaseModel):
    """Job execution and recovery settings."""
    proc_check: bool = True


class Config(BaseModel):
    namespace_defaults: NamespaceDefaults
    proxied_mcps: dict[str, ProxiedMCP]
    resource_defaults: ResourceDefaults
    slurm: SlurmConfig = SlurmConfig()
    resource_limits: ResourceLimits = ResourceLimits()
    job: JobConfig = JobConfig()
    instances_dir: str = "${HOME}/mcp_instances"

    def resolve_instances_dir(self) -> Path:
        return Path(os.path.expandvars(os.path.expanduser(self.instances_dir))).resolve()


def _default_config() -> Config:
    return Config(
        namespace_defaults=NamespaceDefaults(
            bind_ro=["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc"],
            workspace_mount="/workspace",
            flags=["--unshare-all", "--share-net", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"],
        ),
        proxied_mcps={},
        resource_defaults=ResourceDefaults(
            context_dirs=["config/context"],
            resource_patterns=["*.md"],
        ),
        slurm=SlurmConfig(),
        resource_limits=ResourceLimits(),
    )


def _load_toml(path: Path) -> dict:
    """Load a TOML file and return the raw dict. Returns {} on error."""
    try:
        raw = path.read_bytes()
        return tomli.loads(raw.decode())
    except (FileNotFoundError, tomli.TOMLDecodeError, OSError) as e:
        logger.warning("failed to load config %s: %s", path, e)
        return {}


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge ``override`` into ``base``.

    Dict values are merged recursively; all other values (including lists)
    are replaced by the override.  Returns a new dict, does not modify inputs.
    """
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_config(path: str | Path | None = None) -> Config:
    """Load configuration by merging multiple layers.

    Layering (highest priority last):
      1. Built-in defaults (``_default_config()``)
      2. ``~/.local/ns-hpc/config.toml`` (user-level overrides)
      3. Env-var or explicit ``path`` (highest priority)

    Dict values are merged recursively; lists are fully replaced by the
    higher-priority layer.
    """
    # 1. Start with built-in defaults
    config_dict = _default_config().model_dump()

    # 2. Apply user-level config
    user_config = Path("~/.local/ns-hpc/config.toml").expanduser()
    if user_config.exists():
        data = _load_toml(user_config)
        config_dict = _deep_merge(config_dict, data)

    # 3. Apply env-var or explicit path
    if path is None:
        path = os.environ.get("NS_HPC_CONFIG")
    if path is not None:
        p = Path(path)
        if p.exists():
            data = _load_toml(p)
            config_dict = _deep_merge(config_dict, data)
        else:
            logger.warning("config path %s not found, skipping", p)

    return Config.model_validate(config_dict)
