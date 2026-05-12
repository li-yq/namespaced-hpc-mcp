import os
from pathlib import Path

from pydantic import BaseModel
import tomli


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
            flags=["--unshare-all", "--share-net", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
                    "--die-with-parent"],
        ),
        proxied_mcps={},
        resource_defaults=ResourceDefaults(
            context_dirs=["config/context"],
            resource_patterns=["*.md"],
        ),
        slurm=SlurmConfig(),
        resource_limits=ResourceLimits(),
    )


def load_config(path: str | Path | None = None) -> Config:
    # 1. Use provided path or NS_HPC_CONFIG env var
    if path is None:
        path = os.environ.get("NS_HPC_CONFIG")

    if path is not None:
        p = Path(path)
        if p.exists():
            raw = p.read_bytes()
            data = tomli.loads(raw.decode())
            return Config.model_validate(data)

    # 2. Fallback to user-level config
    user_config = Path("~/.local/ns-hpc/config.toml").expanduser()
    if user_config.exists():
        raw = user_config.read_bytes()
        data = tomli.loads(raw.decode())
        return Config.model_validate(data)

    # 3. Built-in defaults
    return _default_config()
