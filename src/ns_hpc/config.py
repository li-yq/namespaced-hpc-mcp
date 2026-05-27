import logging
import os
import typing
from pathlib import Path

from pydantic import BaseModel
import tomli


logger = logging.getLogger("ns-hpc")


class ProxiedMCP(BaseModel):
    command: str
    args: list[str] | None = None
    env: dict[str, str] | None = None
    include: list[str] = []
    exclude: list[str] = []


class DavExtraMount(BaseModel):
    """Extra WebDAV mount under /dav/{name}/."""
    path: str
    ro: bool = True


class DavConfig(BaseModel):
    """WebDAV file-access configuration.

    When enabled, instance workspaces and output dirs are served at:

        /dav/instances/{instance_id}/{workspace,output}/

    Extra mounts (config-controlled) are served at:

        /dav/{extra_name}/
    """
    enabled: bool = False
    extras: dict[str, DavExtraMount] = {}


class Namespace(BaseModel):
    instances_dir: str = "${HOME}/.local/share/ns-hpc/instances"
    bwrap_command: list[str]
    workspace_mount: str = "/workspace"
    output_mount: str = "/output"
    shared_output_mount: str = "/shared-output"
    status_fd: int = 3


class JobLocal(BaseModel):
    use_cgroups: bool = True
    cgroups_command: list[str]
    proc_check: bool = True


class JobSlurmResource(BaseModel):
    default: int
    max: int


class JobSlurm(BaseModel):
    sbatch_command: list[str]
    limit: dict[str, JobSlurmResource]


class JobsConfig(BaseModel):
    max_timeout: int = 3600
    local: JobLocal
    slurm: JobSlurm


class ResourceConfig(BaseModel):
    context_dirs: list[str] = ["context"]
    resource_patterns: list[str] = ["*.md"]


class Config(BaseModel):
    namespace: Namespace
    jobs: JobsConfig
    resource: ResourceConfig = ResourceConfig()
    dav: DavConfig = DavConfig()
    proxied_mcps: dict[str, ProxiedMCP]

    def resolve_instances_dir(self) -> Path:
        resolved = os.path.expandvars(os.path.expanduser(self.namespace.instances_dir))
        return Path(resolved).resolve()


def _default_config() -> Config:
    return Config(
        namespace=Namespace(
            bwrap_command=[
                "bwrap",
                "--unshare-all", "--share-net",
                "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
                "--ro-bind", "/usr", "/usr",
                "--ro-bind", "/lib", "/lib",
                "--ro-bind", "/lib64", "/lib64",
                "--ro-bind", "/bin", "/bin",
                "--ro-bind", "/sbin", "/sbin",
                "--ro-bind", "/etc", "/etc",
            ],
        ),
        jobs=JobsConfig(
            local=JobLocal(
                cgroups_command=[
                    "systemd-run", "--user", "--scope",
                    "-p", "CPUQuota=400%",
                    "-p", "MemoryMax=8G",
                    "--",
                ],
            ),
            slurm=JobSlurm(
                sbatch_command=[
                    "sbatch",
                    "--partition", "cpu",
                    "--cpus-per-task={cpus}",
                    "--mem={memory}M",
                ],
                limit={
                    "cpus": JobSlurmResource(default=1, max=8),
                    "memory": JobSlurmResource(default=4096, max=32768),
                },
            ),
        ),
        resource=ResourceConfig(
            context_dirs=["context"],
            resource_patterns=["*.md"],
        ),
        proxied_mcps={
            "filesystem": ProxiedMCP(
                command="npx",
                args=["-y", "@modelcontextprotocol/server-filesystem", "/"],
            ),
        },
    )


def _load_toml(path: Path) -> dict:
    """Load a TOML file and return the raw dict."""
    raw = path.read_bytes()
    return tomli.loads(raw.decode())


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


def _warn_unknown_keys(data: dict, model_cls: type[BaseModel], prefix: str = "") -> None:
    """Warn about keys in *data* that don't match the fields of *model_cls*.

    Recurses into nested dicts whose field type is itself a Pydantic model,
    and into dict-of-model fields (e.g. ``dict[str, ProxiedMCP]``).
    """
    for key in data:
        path = f"{prefix}.{key}" if prefix else key
        if key not in model_cls.model_fields:
            logger.warning("unrecognized config key '%s' will be ignored", path)
            continue
        val = data[key]
        if isinstance(val, dict):
            ft = model_cls.model_fields[key].annotation
            # Directly nested Pydantic model
            if hasattr(ft, "model_fields"):
                _warn_unknown_keys(val, ft, path)
            # dict[str, SomeModel] — check each entry's keys
            elif typing.get_origin(ft) is dict:
                value_type = typing.get_args(ft)[1]
                if hasattr(value_type, "model_fields"):
                    for sub_key, sub_val in val.items():
                        if isinstance(sub_val, dict):
                            _warn_unknown_keys(sub_val, value_type, f"{path}.{sub_key}")


def load_config(path: str | Path | None = None) -> Config:
    """Load configuration by merging multiple layers.

    Layering (highest priority last):
      1. Built-in defaults (``_default_config()``)
      2. ``${XDG_CONFIG_HOME:-~/.config}/ns-hpc/config.toml`` (user-level overrides)
      3. Env-var or explicit ``path`` (highest priority)

    Dict values are merged recursively; lists are fully replaced by the
    higher-priority layer.
    """
    # 1. Start with built-in defaults
    config_dict = _default_config().model_dump()

    # 2. Apply user-level config (XDG_CONFIG_HOME)
    user_config = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "ns-hpc" / "config.toml"
    if user_config.exists():
        try:
            data = _load_toml(user_config)
        except (tomli.TOMLDecodeError, OSError) as e:
            logger.warning("failed to load user config %s: %s", user_config, e)
            data = {}
        _warn_unknown_keys(data, Config)
        config_dict = _deep_merge(config_dict, data)

    # 3. Apply env-var or explicit path
    if path is None:
        path = os.environ.get("NS_HPC_CONFIG")
    if path is not None:
        p = Path(path)
        if p.exists():
            try:
                data = _load_toml(p)
            except (tomli.TOMLDecodeError, OSError) as e:
                logger.warning("failed to load config %s: %s", p, e)
                data = {}
            _warn_unknown_keys(data, Config)
            config_dict = _deep_merge(config_dict, data)
        else:
            logger.warning("config path %s not found, skipping", p)

    return Config.model_validate(config_dict)
