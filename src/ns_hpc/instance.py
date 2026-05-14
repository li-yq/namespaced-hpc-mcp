"""Instance lifecycle — directory management, metadata, audit logging."""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ns_hpc.config import Config

logger = logging.getLogger("ns-hpc")


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
    def archived_dir(self) -> Path:
        """Archived instances directory at ``{instances_dir}/.archived/``."""
        return self.base_dir.parent / ".archived"

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

        # Warn if an archived instance with this name exists
        archived_root = instances_dir / ".archived"
        if archived_root.exists():
            for child in archived_root.iterdir():
                if child.is_dir() and child.name.startswith(f"{instance_id}__"):
                    logger.warning(
                        "archived instance '%s' exists at %s",
                        instance_id, child,
                    )
                    break

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
        """List all existing instances sorted by directory name."""
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

    # ── Archiving ──────────────────────────────────────────────────────

    def is_archived(self) -> bool:
        """Check whether this instance has been archived.

        Returns True if the metadata explicitly says so, or if the
        instance directory no longer exists (moved to .archived/).
        """
        if not self.base_dir.exists():
            return True
        try:
            meta = json.loads(self.metadata_path.read_text())
            return meta.get("archived", False)
        except (FileNotFoundError, json.JSONDecodeError):
            return False

    def archive(self, config: Config) -> bool:
        """Archive this instance.

        Checks for running jobs first (raises RuntimeError if any exist),
        marks the metadata as archived, then moves the instance directory
        (and shared output directory) to timestamped paths under
        ``.archived/`` and ``output/`` respectively so the instance name
        can be reused.

        Returns False if already archived.
        """
        if not self.base_dir.exists():
            return False

        try:
            meta = json.loads(self.metadata_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            meta = {}

        if meta.get("archived"):
            return False

        # Check for running jobs
        from ns_hpc.job_manager import JobManager  # avoid circular import

        mgr = JobManager(self, config)
        running = [j for j in mgr.list_jobs() if j.get("status") == "running"]
        if running:
            raise RuntimeError(
                f"Cannot archive instance '{self.id}': "
                f"{len(running)} job(s) still running"
            )

        # Mark metadata as archived
        meta["archived"] = True
        meta["archived_at"] = datetime.now(timezone.utc).isoformat()
        self.metadata_path.write_text(json.dumps(meta, indent=2))

        # Move instance directory to .archived/<id>__<ts>/
        # Use microsecond precision to avoid collisions in rapid succession
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%f")
        suffix = f"{self.id}__{ts}"
        archive_inst_dir = self.archived_dir / suffix
        self.archived_dir.mkdir(parents=True, exist_ok=True)
        os.rename(str(self.base_dir), str(archive_inst_dir))

        # Move shared output directory if it exists
        shared_output = config.resolve_instances_dir() / "output" / self.id
        if shared_output.exists():
            archive_output_dir = config.resolve_instances_dir() / "output" / suffix
            archive_output_dir.parent.mkdir(parents=True, exist_ok=True)
            os.rename(str(shared_output), str(archive_output_dir))

        return True

    @staticmethod
    def list_archived_instances(config: Config) -> list[dict]:
        """List all archived instances by scanning ``.archived/``."""
        archived_root = config.resolve_instances_dir() / ".archived"
        if not archived_root.exists():
            return []

        results: list[dict] = []
        for child in sorted(archived_root.iterdir()):
            if not child.is_dir():
                continue
            meta_path = child / "metadata.json"
            if not meta_path.exists():
                continue
            try:
                meta = json.loads(meta_path.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            results.append({
                "instance_id": meta.get("id", child.name),
                "created_at": meta.get("created_at", ""),
                "archived_at": meta.get("archived_at", ""),
                "description": meta.get("description", ""),
                "archive_path": str(child),
            })
        return results
