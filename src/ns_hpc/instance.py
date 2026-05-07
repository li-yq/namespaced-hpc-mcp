import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from ns_hpc.config import Config


class Instance:
    def __init__(self, instance_id: str, base_dir: Path) -> None:
        self.id = instance_id
        self.base_dir = base_dir
        self.workspace_dir = base_dir / "workspace"
        self.audit_log_path = base_dir / "audit.log"
        self.metadata_path = base_dir / "metadata.json"

    @property
    def exists(self) -> bool:
        return self.base_dir.exists()

    @staticmethod
    def create(instance_id: str, config: Config) -> "Instance":
        base_dir = config.resolve_instances_dir() / instance_id
        instance = Instance(instance_id, base_dir)

        instance.base_dir.mkdir(parents=True, exist_ok=True)
        instance.workspace_dir.mkdir(parents=True, exist_ok=True)

        metadata = {
            "id": instance_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "workspace": str(instance.workspace_dir),
            "hostname": __import__("socket").gethostname(),
        }
        instance.metadata_path.write_text(json.dumps(metadata, indent=2))

        return instance

    @staticmethod
    def load(instance_id: str, config: Config) -> "Instance | None":
        base_dir = config.resolve_instances_dir() / instance_id
        instance = Instance(instance_id, base_dir)
        if not instance.metadata_path.exists():
            return None
        return instance

    def write_audit(self, command: str, result: dict) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "command": command,
            "exit_code": result.get("exit_code"),
            "stdout_len": len(result.get("stdout", "")),
            "stderr_len": len(result.get("stderr", "")),
        }
        with open(self.audit_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    @staticmethod
    def list_instances(config: Config) -> list["Instance"]:
        instances_dir = config.resolve_instances_dir()
        if not instances_dir.exists():
            return []
        result: list[Instance] = []
        for child in instances_dir.iterdir():
            if child.is_dir() and (child / "metadata.json").exists():
                result.append(Instance(child.name, child))
        return result

    @staticmethod
    def destroy(instance_id: str, config: Config) -> bool:
        base_dir = config.resolve_instances_dir() / instance_id
        if not base_dir.exists():
            return False
        shutil.rmtree(base_dir)
        return True
