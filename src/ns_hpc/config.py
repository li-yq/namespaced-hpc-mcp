import os
from pathlib import Path

from pydantic import BaseModel
import tomli


class NamespaceDefaults(BaseModel):
    bind_ro: list[str]
    workspace_mount: str
    flags: list[str]


class ProxiedMCP(BaseModel):
    command: str
    args: list[str] | None = None
    env: dict[str, str] | None = None


class ResourceDefaults(BaseModel):
    context_dirs: list[str]
    resource_patterns: list[str]


class Config(BaseModel):
    namespace_defaults: NamespaceDefaults
    proxied_mcps: dict[str, ProxiedMCP]
    resource_defaults: ResourceDefaults
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
            context_dirs=["context"],
            resource_patterns=["*.md"],
        ),
    )


def load_config(path: str | Path | None = None) -> Config:
    if path is None:
        path = os.environ.get("NS_HPC_CONFIG", "config.toml")
    path = Path(path)
    if not path.exists():
        return _default_config()
    raw = path.read_bytes()
    data = tomli.loads(raw.decode())
    return Config.model_validate(data)
