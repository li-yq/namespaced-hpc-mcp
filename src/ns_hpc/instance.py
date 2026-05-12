"""Instance lifecycle — directory management, metadata, audit logging."""
from __future__ import annotations

import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ns_hpc.config import Config


class Instance:
    """A sandbox instance tied to a persistent workspace directory.

    Instances are stateless in the bwrap sense (no persistent namespace) but
    maintain a workspace directory, audit log, output files, and metadata
    on the host.
    """

    def __init__(self, instance_id: str, base_dir: Path) -> None:
        self.id = instance_id
        self.base_dir = base_dir
        self.workspace_dir = base_dir / "workspace"
        self.audit_log_path = base_dir / "audit.log"
        self.metadata_path = base_dir / "metadata.json"
        self.output_dir = base_dir / "output"

    @property
    def exists(self) -> bool:
        return self.base_dir.exists()

    # ── Lifecycle ────────────────────────────────────────────────────────

    @staticmethod
    def create(instance_id: str, config: Config) -> Instance:
        """Create a new instance directory structure.

        Raises FileExistsError if the instance already exists.

        Args:
            instance_id: Unique instance identifier.
            config: Loaded ns-hpc configuration.

        Returns:
            New Instance object.
        """
        instances_dir = config.resolve_instances_dir()
        base_dir = instances_dir / instance_id

        if base_dir.exists():
            raise FileExistsError(f"Instance {instance_id} already exists at {base_dir}")

        base_dir.mkdir(parents=True, exist_ok=True)
        (base_dir / "workspace").mkdir(parents=True, exist_ok=True)
        (base_dir / "output").mkdir(parents=True, exist_ok=True)

        metadata = {
            "id": instance_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "workspace": str(base_dir / "workspace"),
            "hostname": __import__("socket").gethostname(),
        }
        (base_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

        return Instance(instance_id, base_dir)

    @staticmethod
    def load(instance_id: str, config: Config) -> Optional[Instance]:
        """Load an existing instance by ID. Returns None if not found."""
        base_dir = config.resolve_instances_dir() / instance_id
        instance = Instance(instance_id, base_dir)
        if not instance.metadata_path.exists():
            return None
        return instance

    @staticmethod
    def list_instances(config: Config) -> list[Instance]:
        """List all existing instances sorted by creation time."""
        instances_dir = config.resolve_instances_dir()
        if not instances_dir.exists():
            return []
        result: list[Instance] = []
        for child in sorted(instances_dir.iterdir()):
            if child.is_dir() and (child / "metadata.json").exists():
                result.append(Instance(child.name, child))
        return result

    @staticmethod
    def destroy(instance_id: str, config: Config) -> bool:
        """Remove an instance directory and all its contents."""
        base_dir = config.resolve_instances_dir() / instance_id
        if not base_dir.exists():
            return False
        shutil.rmtree(base_dir)
        return True

    # ── Audit logging ────────────────────────────────────────────────────

    def _ensure_output_dir(self) -> Path:
        """Create the output directory if it doesn't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        return self.output_dir

    def audit(self, command: str, exit_code: int,
              stdout: str = "", stderr: str = "") -> str:
        """Record a command execution: write output files and log entry.

        Creates a unique task ID, writes stdout/stderr to
        ``{output_dir}/{task_id}.{out,err}``, and appends a JSON line to
        the audit log containing all metadata.

        Args:
            command: The shell command that was executed.
            exit_code: The command's exit code.
            stdout: Standard output text.
            stderr: Standard error text.

        Returns:
            The generated task ID.
        """
        task_id = uuid.uuid4().hex[:12]
        self._ensure_output_dir()
        (self.output_dir / f"{task_id}.out").write_text(stdout or "")
        (self.output_dir / f"{task_id}.err").write_text(stderr or "")

        entry = {
            "task_id": task_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "command": command,
            "exit_code": exit_code,
            "stdout_path": str(self.output_dir / f"{task_id}.out"),
            "stderr_path": str(self.output_dir / f"{task_id}.err"),
            "stdout_len": len(stdout or ""),
            "stderr_len": len(stderr or ""),
        }
        with open(self.audit_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

        return task_id
