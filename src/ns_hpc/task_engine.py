from __future__ import annotations

import json
import logging
import os
import shlex
import shutil
import signal
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from ns_hpc.config import Config
from ns_hpc.instance import Instance
from ns_hpc.namespace import build_bwrap_args

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


@dataclass
class TaskHandle:
    id: str
    status: TaskStatus
    mode: str  # 'local' or 'slurm'
    pid: int | None = None
    slurm_job_id: int | None = None
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    completed_at: str | None = None


class LocalTaskEngine:
    """Execute tasks as local subprocesses inside a bubblewrap sandbox."""

    def __init__(self, config: Config, instance: Instance) -> None:
        self.config = config
        self.instance = instance
        self._tasks: dict[str, dict] = {}

    def submit(self, command: str, timeout: int = 300) -> TaskHandle:
        bwrap_args = build_bwrap_args(
            command=["/bin/sh", "-c", command],
            workspace_host_path=str(self.instance.workspace_dir),
            config=self.config,
        )

        # Insert json-status-fd before "--" for reliable exit code reporting
        status_r_fd, status_w_fd = os.pipe()
        dash_pos = bwrap_args.index("--")
        bwrap_args[dash_pos:dash_pos] = ["--json-status-fd", str(status_w_fd)]

        proc = subprocess.Popen(
            bwrap_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(status_w_fd,),
        )
        os.close(status_w_fd)  # close write end in parent

        task_id = str(uuid.uuid4())
        handle = TaskHandle(
            id=task_id,
            status=TaskStatus.RUNNING,
            mode="local",
            pid=proc.pid,
        )
        self._tasks[task_id] = {
            "handle": handle,
            "proc": proc,
            "status_r_fd": status_r_fd,
        }
        return handle

    def get_status(self, task_id: str) -> TaskHandle | None:
        entry = self._tasks.get(task_id)
        if entry is None:
            return None

        handle = entry["handle"]
        if handle.status is not TaskStatus.RUNNING:
            return handle

        proc: subprocess.Popen = entry["proc"]
        retcode = proc.poll()

        if retcode is None:
            return handle  # still running

        # Process has exited — read json status fd
        try:
            raw_status = os.read(entry["status_r_fd"], 4096)
        except OSError:
            raw_status = b""
        finally:
            try:
                os.close(entry["status_r_fd"])
            except OSError:
                pass
            entry.pop("status_r_fd", None)

        # Drain stdout/stderr
        stdout_bytes, stderr_bytes = proc.communicate()

        exit_code = proc.returncode
        lines = [l for l in raw_status.splitlines() if l.strip()]
        if lines:
            try:
                status = __import__("json").loads(lines[-1])
                exit_code = status.get("exit-code", exit_code)
            except (__import__("json").JSONDecodeError, KeyError):
                pass

        handle.status = TaskStatus.COMPLETED if exit_code == 0 else TaskStatus.FAILED
        handle.exit_code = exit_code
        handle.stdout = stdout_bytes.decode() if stdout_bytes else ""
        handle.stderr = stderr_bytes.decode() if stderr_bytes else ""
        handle.completed_at = datetime.now(timezone.utc).isoformat()
        return handle

    def cancel(self, task_id: str) -> bool:
        entry = self._tasks.get(task_id)
        if entry is None:
            return False

        handle = entry["handle"]
        if handle.status is not TaskStatus.RUNNING:
            return False

        proc: subprocess.Popen = entry["proc"]

        # bwrap ignores SIGTERM when PID 1; try SIGTERM first, then SIGKILL
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

        # Read json status if pipe still open
        status_r_fd = entry.get("status_r_fd")
        if status_r_fd is not None:
            try:
                os.read(status_r_fd, 4096)
            except OSError:
                pass
            finally:
                try:
                    os.close(status_r_fd)
                except OSError:
                    pass
                entry.pop("status_r_fd", None)

        # Drain remaining output
        stdout_bytes, stderr_bytes = proc.communicate()

        handle.status = TaskStatus.CANCELLED
        handle.stdout = stdout_bytes.decode() if stdout_bytes else ""
        handle.stderr = stderr_bytes.decode() if stderr_bytes else ""
        handle.completed_at = datetime.now(timezone.utc).isoformat()
        return True

    def list_tasks(self) -> list[TaskHandle]:
        return [e["handle"] for e in self._tasks.values()]


class SlurmTaskEngine:
    """Execute tasks via Slurm job submission."""

    def __init__(self, config: Config, instance: Instance) -> None:
        self.config = config
        self.instance = instance
        self._tasks: dict[str, dict] = {}
        self._available: bool | None = None

    @property
    def available(self) -> bool:
        if self._available is None:
            self._available = shutil.which("sbatch") is not None
        return self._available

    def submit(
        self,
        command: str,
        timeout: int = 3600,
        cpus: int = 1,
        memory_gb: int = 4,
        partition: str = "debug",
    ) -> TaskHandle:
        if not self.available:
            raise RuntimeError("Slurm is not available on this system")

        bwrap_args = build_bwrap_args(
            command=["/bin/sh", "-c", command],
            workspace_host_path=str(self.instance.workspace_dir),
            config=self.config,
        )
        bwrap_cmd = " ".join(shlex.quote(a) for a in bwrap_args)

        script = f"""#!/bin/bash
#SBATCH --job-name=ns-hpc-{uuid.uuid4().hex[:8]}
#SBATCH --output={self.instance.workspace_dir / 'slurm_%j.out'}
#SBATCH --error={self.instance.workspace_dir / 'slurm_%j.err'}
#SBATCH --time={max(1, timeout // 60)}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={memory_gb}G
#SBATCH --partition={partition}

{bwrap_cmd}
"""
        script_path = (
            self.instance.workspace_dir / f"slurm_script_{uuid.uuid4().hex[:16]}.sh"
        )
        script_path.write_text(script)

        result = subprocess.run(
            ["sbatch", str(script_path)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(f"sbatch failed: {result.stderr.strip()}")

        job_id = int(result.stdout.strip().split()[-1])

        task_id = str(uuid.uuid4())
        handle = TaskHandle(
            id=task_id,
            status=TaskStatus.RUNNING,
            mode="slurm",
            slurm_job_id=job_id,
        )
        self._tasks[task_id] = {"handle": handle, "script_path": script_path}
        return handle

    def get_status(self, task_id: str) -> TaskHandle | None:
        entry = self._tasks.get(task_id)
        if entry is None:
            return None

        handle = entry["handle"]
        if handle.status is not TaskStatus.RUNNING:
            return handle

        # Poll via sacct (try --json first, fall back to --parsable2)
        try:
            result = subprocess.run(
                ["sacct", "-j", str(handle.slurm_job_id), "--json"],
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            handle.status = TaskStatus.UNKNOWN
            return handle

        if result.returncode != 0:
            handle.status = TaskStatus.UNKNOWN
            return handle

        try:
            data = json.loads(result.stdout)
            jobs = data.get("jobs", [])
        except (json.JSONDecodeError, TypeError):
            jobs = []

        matched = 0
        for job in jobs:
            state = (job.get("state") or "").strip()
            exit_code_str = (job.get("exit_code") or "").strip()

            if state in ("COMPLETED",):
                handle.status = TaskStatus.COMPLETED
                try:
                    handle.exit_code = int(exit_code_str.split(":")[0])
                except (ValueError, IndexError):
                    handle.exit_code = 0
                handle.completed_at = datetime.now(timezone.utc).isoformat()
                # Read output from slurm files
                out_file = self.instance.workspace_dir / f"slurm_{handle.slurm_job_id}.out"
                err_file = self.instance.workspace_dir / f"slurm_{handle.slurm_job_id}.err"
                if out_file.exists():
                    handle.stdout = out_file.read_text()
                if err_file.exists():
                    handle.stderr = err_file.read_text()
                matched += 1
            elif state in ("FAILED", "TIMEOUT", "NODE_FAIL"):
                handle.status = TaskStatus.FAILED
                handle.exit_code = -1
                handle.completed_at = datetime.now(timezone.utc).isoformat()
                matched += 1
            elif state in ("CANCELLED",):
                handle.status = TaskStatus.CANCELLED
                handle.completed_at = datetime.now(timezone.utc).isoformat()
                matched += 1

        if matched > 1:
            logger.warning(
                "sacct returned %d entries for slurm job %s; expected exactly 1",
                matched, handle.slurm_job_id,
            )

        if matched == 0:
            handle.status = TaskStatus.UNKNOWN

        return handle

    def cancel(self, task_id: str) -> bool:
        entry = self._tasks.get(task_id)
        if entry is None:
            return False

        handle = entry["handle"]
        if handle.status is not TaskStatus.RUNNING:
            return False

        try:
            result = subprocess.run(
                ["scancel", str(handle.slurm_job_id)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            success = result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            success = False

        if success:
            handle.status = TaskStatus.CANCELLED
            handle.completed_at = datetime.now(timezone.utc).isoformat()
        return success

    def list_tasks(self) -> list[TaskHandle]:
        return [e["handle"] for e in self._tasks.values()]
