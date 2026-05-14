"""Instance lifecycle — directory management, metadata, audit logging."""
from __future__ import annotations

import json
import re
import shutil
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

    @property
    def output_path(self) -> Path:
        """Shared output directory at ``{instances_dir}/output/{id}/``."""
        return self.base_dir.parent / "output" / self.id

    @property
    def exists(self) -> bool:
        return self.base_dir.exists()

    # ── Lifecycle ────────────────────────────────────────────────────────

    @staticmethod
    def create(instance_id: str, config: Config, description: str = "") -> Instance:
        """Create a new instance directory structure.

        Raises FileExistsError if the instance already exists.

        Args:
            instance_id: Unique instance identifier.
            config: Loaded ns-hpc configuration.
            description: Optional human-readable description.

        Returns:
            New Instance object.
        """
        if not re.match(r"^[a-zA-Z0-9_.-]+$", instance_id):
            raise ValueError(
                f"Invalid instance_id {instance_id!r}: must match [a-zA-Z0-9_.-]+"
            )

        instances_dir = config.resolve_instances_dir()
        base_dir = instances_dir / instance_id

        if base_dir.exists():
            raise FileExistsError(f"Instance {instance_id} already exists at {base_dir}")

        base_dir.mkdir(parents=True, exist_ok=True)
        (base_dir / "workspace").mkdir(parents=True, exist_ok=True)

        # Shared output directory
        shared_root = instances_dir / "output"
        shared_root.mkdir(parents=True, exist_ok=True)
        (shared_root / instance_id).mkdir(parents=True, exist_ok=True)

        metadata = {
            "id": instance_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "workspace": str(base_dir / "workspace"),
            "hostname": __import__("socket").gethostname(),
        }
        if description:
            metadata["description"] = description
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
        # Also remove shared output directory
        shared_dir = config.resolve_instances_dir() / "output" / instance_id
        if shared_dir.exists():
            shutil.rmtree(shared_dir)
        return True

    # ── Audit logging ────────────────────────────────────────────────────

    def audit(self, event: str, **data: object) -> None:
        """Append a JSONL event to the audit log.

        The audit log is a line-delimited JSON file (``audit.log``) in the
        instance directory.  Each line is one event with at minimum an
        ``event`` type and a ``timestamp``, plus whatever extra fields the
        caller supplies.

        Example::

            inst.audit("job.completed", job_id="abc", exit_code=0,
                       command="echo hello")

        Args:
            event: Event type identifier (e.g. ``"job.submitted"``,
                   ``"instance.created"``, ``"job.completed"``).
            **data: Arbitrary key-value pairs to include in the log entry.
        """
        entry = {
            "event": event,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **data,
        }
        with open(self.audit_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    # ── Description ───────────────────────────────────────────────────────

    def get_description(self) -> str:
        """Read the instance description from metadata."""
        try:
            meta = json.loads(self.metadata_path.read_text())
            return meta.get("description", "")
        except (FileNotFoundError, json.JSONDecodeError):
            return ""

    def set_description(self, description: str) -> None:
        """Update the instance description in metadata."""
        try:
            meta = json.loads(self.metadata_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            meta = {}
        meta["description"] = description
        self.metadata_path.write_text(json.dumps(meta, indent=2))
