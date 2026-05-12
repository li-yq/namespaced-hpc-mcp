"""Job manager — async job lifecycle with on-disk persistence.

Every job writes stdout/stderr directly to disk files via shell redirect.
Job state is persisted to a JSON file so it survives process restarts.
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

from ns_hpc.config import Config
from ns_hpc.instance import Instance


# Slurm 25.11+ changed sacct --json state/exit_code from flat strings to nested
# dicts.  These helpers normalise both formats.
def _parse_slurm_state(job: dict) -> str:
    raw = job.get("state", "")
    if isinstance(raw, dict):
        current = raw.get("current", [])
        return current[0] if current else ""
    return str(raw).strip()


def _parse_slurm_exit_code(job: dict) -> int | None:
    raw = job.get("exit_code", "")
    if isinstance(raw, dict):
        rc = raw.get("return_code", {})
        if rc.get("set"):
            return int(rc["number"])
        return None
    try:
        return int(str(raw).split(":")[0])
    except (ValueError, IndexError):
        return None


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


@dataclass
class JobResult:
    """Result returned by submit_job and poll_job."""

    job_id: str
    status: JobStatus
    exit_code: Optional[int] = None
    stdout_tail: str = ""
    stderr_tail: str = ""
    stdout_path: str = ""
    stderr_path: str = ""
    duration: float = 0.0

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "exit_code": self.exit_code,
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "duration": round(self.duration, 2),
        }


def _tail_file(path: Path, n: int = 50) -> str:
    """Read the last ``n`` lines of a file efficiently."""
    if n <= 0 or not path.exists() or path.stat().st_size == 0:
        return ""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                return ""

            # Read backwards in chunks until we have n lines or hit the start
            bufsize = min(size, 8192)
            f.seek(max(0, size - bufsize))
            data = f.read(bufsize)

            lines = data.decode(errors="replace").splitlines()
            if len(lines) >= n:
                return "\n".join(lines[-n:])

            # Need to read more
            remaining = size - bufsize
            while remaining > 0 and len(lines) < n:
                bufsize = min(remaining, 8192)
                remaining -= bufsize
                f.seek(max(0, remaining))
                data = f.read(bufsize) + data
                lines = data.decode(errors="replace").splitlines()

            return "\n".join(lines[-n:])
    except (OSError, UnicodeDecodeError):
        return ""


class JobManager:
    """Manages async jobs for an instance.

    Each job runs inside a bwrap sandbox (via ``ns-hpc bwrap`` CLI).
    stdout/stderr are redirected directly to disk files via shell redirect.
    Job state is persisted to a JSON file for survival across restarts.
    """

    def __init__(self, instance: Instance, config: Config):
        self.instance = instance
        self.config = config
        # In-memory subprocess handles for local (running) jobs
        self._procs: dict[str, subprocess.Popen] = {}
        # Disk-persisted job metadata
        self._jobs_path = instance.workspace_dir / ".ns_hpc_jobs.json"
        self._jobs = self._load_jobs()
        self._fixup_stale_jobs()

    # ── Persistence ──────────────────────────────────────────────────────

    def _load_jobs(self) -> dict[str, dict]:
        """Load job metadata from disk. Returns {} on any error."""
        try:
            if self._jobs_path.exists():
                return json.loads(self._jobs_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
        return {}

    def _save_jobs(self) -> None:
        """Atomically write job metadata to disk."""
        self._jobs_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._jobs_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._jobs, indent=2))
        os.replace(tmp, self._jobs_path)

    def _fixup_stale_jobs(self) -> None:
        """After restart, mark local 'running' jobs as UNKNOWN (PIDs are gone)."""
        changed = False
        for entry in self._jobs.values():
            if entry.get("mode") == "local" and entry.get("status") == "running":
                entry["status"] = "unknown"
                changed = True
        if changed:
            self._save_jobs()

    # ── Job ID & output dir ──────────────────────────────────────────────

    def _next_job_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def _ensure_output_dir(self) -> Path:
        p = self.instance.workspace_dir / ".ns_hpc_output"
        p.mkdir(parents=True, exist_ok=True)
        return p

    # ── Submit ───────────────────────────────────────────────────────────

    def submit(
        self,
        command: str,
        *,
        mode: str = "local",
        timeout: float = 60,
        tail: int = 50,
    ) -> JobResult:
        """Submit a job and wait up to ``timeout`` seconds.

        The command runs inside a bwrap sandbox via ``ns-hpc bwrap`` CLI.
        ``ns-hpc bwrap`` handles only the sandbox — no redirection.

        The bwrap invocation is wrapped by the outer layer:
          Local:  sh -c 'ns-hpc bwrap ... ><stdout> 2><stderr>'
          Slurm:  sbatch --wrap='ns-hpc bwrap ...' --output=<stdout> --error=<stderr>

        Always waits the full ``timeout`` (or until the job finishes).
        Returns tail lines of whatever output was produced.
        """
        job_id = self._next_job_id()
        output_dir = self._ensure_output_dir()
        stdout_path = output_dir / f"{job_id}.out"
        stderr_path = output_dir / f"{job_id}.err"

        if mode == "local":
            return self._submit_local(
                job_id, command, stdout_path, stderr_path, timeout, tail,
            )
        elif mode == "slurm":
            return self._submit_slurm(
                job_id, command, stdout_path, stderr_path, timeout, tail,
            )
        else:
            raise ValueError(f"Unknown mode: {mode}")

    def _submit_local(
        self,
        job_id: str,
        command: str,
        stdout_path: Path,
        stderr_path: Path,
        timeout: float,
        tail: int,
    ) -> JobResult:
        # sh -c '<python> -m ns_hpc bwrap <id> -- /bin/sh -c "<cmd>" ><out> 2><err>'
        shell_cmd = (
            f"{sys.executable} -m ns_hpc bwrap {self.instance.id} -- "
            f"/bin/sh -c {shlex.quote(command)}"
            f" >{shlex.quote(str(stdout_path))} 2>{shlex.quote(str(stderr_path))}"
        )

        stdout_path.touch()
        stderr_path.touch()

        proc = subprocess.Popen(["sh", "-c", shell_cmd])

        # Persist job metadata
        started_at = time.monotonic()
        self._jobs[job_id] = {
            "command": command,
            "mode": "local",
            "status": "running",
            "pid": proc.pid,
            "slurm_job_id": None,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._procs[job_id] = proc
        self._save_jobs()

        # Wait up to timeout — output is written to files by shell redirect
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            elapsed = time.monotonic() - started_at
            return JobResult(
                job_id=job_id,
                status=JobStatus.RUNNING,
                stdout_tail=_tail_file(stdout_path, tail),
                stderr_tail=_tail_file(stderr_path, tail),
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
                duration=elapsed,
            )

        # Process finished
        elapsed = time.monotonic() - started_at
        return self._finalize_local(job_id, proc, stdout_path, stderr_path, tail, elapsed)

    def _finalize_local(
        self,
        job_id: str,
        proc: subprocess.Popen,
        stdout_path: Path,
        stderr_path: Path,
        tail: int,
        elapsed: float,
    ) -> JobResult:
        exit_code = proc.returncode
        status = JobStatus.COMPLETED if exit_code == 0 else JobStatus.FAILED
        self._procs.pop(job_id, None)
        self._jobs.pop(job_id, None)
        self._save_jobs()
        return JobResult(
            job_id=job_id,
            status=status,
            exit_code=exit_code,
            stdout_tail=_tail_file(stdout_path, tail),
            stderr_tail=_tail_file(stderr_path, tail),
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            duration=elapsed,
        )

    def _submit_slurm(
        self,
        job_id: str,
        command: str,
        stdout_path: Path,
        stderr_path: Path,
        timeout: float,
        tail: int,
    ) -> JobResult:
        bwrap_cmd = (
            f"{sys.executable} -m ns_hpc bwrap {self.instance.id} -- "
            f"/bin/sh -c {shlex.quote(command)}"
        )

        result = subprocess.run(
            [
                "sbatch",
                "--wrap", bwrap_cmd,
                "--output", str(stdout_path),
                "--error", str(stderr_path),
                "--job-name", f"ns-hpc-{job_id[:8]}",
                "--time", str(max(1, int(timeout) // 60 + 1)),
                "--partition", self.config.slurm.partition,
            ],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(f"sbatch failed: {result.stderr.strip()}")

        slurm_job_id = int(result.stdout.strip().split()[-1])

        started_at = time.monotonic()
        self._jobs[job_id] = {
            "command": command,
            "mode": "slurm",
            "status": "running",
            "pid": None,
            "slurm_job_id": slurm_job_id,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_jobs()

        # scontrol wait_job blocks until the job completes or timeout expires
        elapsed = time.monotonic() - started_at
        remaining = max(1, int(timeout - elapsed))
        try:
            wj = subprocess.run(
                ["scontrol", "wait_job", str(slurm_job_id), str(remaining)],
                capture_output=True, text=True,
                timeout=remaining + 30,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            wj = None

        if wj is not None and wj.returncode == 0:
            # Job finished within the wait — query final state
            state, ec = self._slurm_job_state(slurm_job_id)
            if state in ("COMPLETED",):
                self._jobs.pop(job_id, None)
                self._save_jobs()
                return JobResult(
                    job_id=job_id, status=JobStatus.COMPLETED,
                    exit_code=ec if ec is not None else 0,
                    stdout_tail=_tail_file(stdout_path, tail),
                    stderr_tail=_tail_file(stderr_path, tail),
                    stdout_path=str(stdout_path), stderr_path=str(stderr_path),
                    duration=time.monotonic() - started_at,
                )
            elif state in ("FAILED", "TIMEOUT", "NODE_FAIL", "CANCELLED"):
                self._jobs.pop(job_id, None)
                self._save_jobs()
                return JobResult(
                    job_id=job_id, status=JobStatus.FAILED,
                    exit_code=ec if ec is not None else -1,
                    stdout_path=str(stdout_path), stderr_path=str(stderr_path),
                    duration=time.monotonic() - started_at,
                )

        return JobResult(
            job_id=job_id,
            status=JobStatus.RUNNING,
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            duration=time.monotonic() - started_at,
        )

    # ── Poll ─────────────────────────────────────────────────────────────

    def poll(
        self,
        job_id: str,
        *,
        timeout: float = 0,
        tail: int = 50,
    ) -> Optional[JobResult]:
        """Poll a running job.  Waits up to ``timeout`` seconds.

        Returns ``None`` if the job_id is unknown.
        """
        entry = self._jobs.get(job_id)
        if entry is None:
            return None

        if entry["mode"] == "local":
            return self._poll_local(job_id, entry, timeout, tail)
        else:
            return self._poll_slurm(job_id, entry, timeout, tail)

    def _poll_local(
        self,
        job_id: str,
        entry: dict,
        timeout: float,
        tail: int,
    ) -> JobResult:
        proc = self._procs.get(job_id)
        stdout_path: Path = Path(entry["stdout_path"])
        stderr_path: Path = Path(entry["stderr_path"])

        started_at = time.monotonic()

        if proc is None:
            # Process handle gone but entry still exists — stale/finished
            status = JobStatus(entry.get("status", "unknown"))
            return JobResult(
                job_id=job_id,
                status=status,
                exit_code=entry.get("exit_code"),
                stdout_tail=_tail_file(stdout_path, tail),
                stderr_tail=_tail_file(stderr_path, tail),
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
            )

        if timeout > 0 and proc.poll() is None:
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                elapsed = time.monotonic() - started_at
                return JobResult(
                    job_id=job_id,
                    status=JobStatus.RUNNING,
                    stdout_tail=_tail_file(stdout_path, tail),
                    stderr_tail=_tail_file(stderr_path, tail),
                    stdout_path=str(stdout_path),
                    stderr_path=str(stderr_path),
                    duration=elapsed,
                )

        if proc.poll() is None:
            elapsed = time.monotonic() - started_at
            return JobResult(
                job_id=job_id,
                status=JobStatus.RUNNING,
                stdout_tail=_tail_file(stdout_path, tail),
                stderr_tail=_tail_file(stderr_path, tail),
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
                duration=elapsed,
            )

        elapsed = time.monotonic() - started_at
        return self._finalize_local(
            job_id, proc, stdout_path, stderr_path, tail, elapsed,
        )

    def _slurm_job_state(self, slurm_job_id: int) -> tuple[str, int | None]:
        """Query sacct and return (state, exit_code) for a Slurm job."""
        try:
            result = subprocess.run(
                ["sacct", "-j", str(slurm_job_id), "--json", "-X"],
                capture_output=True, text=True, timeout=30,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return ("UNKNOWN", None)

        if result.returncode != 0:
            return ("UNKNOWN", None)

        try:
            data = json.loads(result.stdout)
            jobs = data.get("jobs", [])
        except (json.JSONDecodeError, TypeError):
            jobs = []

        for job in jobs:
            state = _parse_slurm_state(job)
            ec = _parse_slurm_exit_code(job)
            if state:
                return (state, ec)

        return ("UNKNOWN", None)

    def _poll_slurm(
        self,
        job_id: str,
        entry: dict,
        timeout: float,
        tail: int,
    ) -> JobResult:
        slurm_job_id = entry.get("slurm_job_id")
        stdout_path: Path = Path(entry["stdout_path"])
        stderr_path: Path = Path(entry["stderr_path"])

        if slurm_job_id is None:
            return JobResult(
                job_id=job_id, status=JobStatus.UNKNOWN,
                stdout_path=str(stdout_path), stderr_path=str(stderr_path),
            )

        # scontrol wait_job blocks until the job completes or timeout expires
        if timeout > 0:
            try:
                subprocess.run(
                    ["scontrol", "wait_job", str(slurm_job_id), str(int(timeout))],
                    capture_output=True, timeout=int(timeout) + 30,
                )
            except (FileNotFoundError, subprocess.TimeoutExpired):
                pass

        state, ec = self._slurm_job_state(slurm_job_id)

        if state in ("COMPLETED",):
            self._jobs.pop(job_id, None)
            self._save_jobs()
            return JobResult(
                job_id=job_id, status=JobStatus.COMPLETED,
                exit_code=ec if ec is not None else 0,
                stdout_tail=_tail_file(stdout_path, tail),
                stderr_tail=_tail_file(stderr_path, tail),
                stdout_path=str(stdout_path), stderr_path=str(stderr_path),
            )
        elif state in ("FAILED", "TIMEOUT", "NODE_FAIL", "CANCELLED"):
            self._jobs.pop(job_id, None)
            self._save_jobs()
            return JobResult(
                job_id=job_id, status=JobStatus.FAILED,
                exit_code=ec if ec is not None else -1,
                stdout_path=str(stdout_path), stderr_path=str(stderr_path),
            )

        entry["status"] = "unknown"
        self._save_jobs()
        return JobResult(
            job_id=job_id, status=JobStatus.UNKNOWN,
            stdout_path=str(stdout_path), stderr_path=str(stderr_path),
        )

    def cancel(self, job_id: str) -> bool:
        """Cancel a running job.  Returns True if cancelled."""
        entry = self._jobs.get(job_id)
        if entry is None:
            return False

        if entry["mode"] == "local":
            proc = self._procs.get(job_id)
            if proc and proc.poll() is None:
                # bwrap runs as PID 1 and ignores SIGTERM; SIGKILL is required.
                proc.kill()
                proc.wait()
            self._procs.pop(job_id, None)
            self._jobs.pop(job_id, None)
            self._save_jobs()
            return True
        else:
            slurm_job_id = entry.get("slurm_job_id")
            if slurm_job_id:
                subprocess.run(["scancel", str(slurm_job_id)], timeout=15)
            self._jobs.pop(job_id, None)
            self._save_jobs()
            return True

    def cancel_and_tail(self, result: JobResult, tail: int = 50) -> JobResult:
        """Cancel a running job and set status to TIMEOUT with output tail."""
        self.cancel(result.job_id)
        result.stdout_tail = _tail_file(Path(result.stdout_path), tail)
        result.stderr_tail = _tail_file(Path(result.stderr_path), tail)
        result.status = JobStatus.TIMEOUT
        return result

    def list_jobs(self) -> list[dict]:
        """List all tracked jobs from disk-persisted state."""
        result = []
        for job_id, entry in self._jobs.items():
            status = entry.get("status", "unknown")
            # For local jobs, check if process is still alive
            proc = self._procs.get(job_id)
            if proc and proc.poll() is not None:
                ec = proc.returncode
                status = "completed" if ec == 0 else "failed"
                self._jobs[job_id]["status"] = status
                self._save_jobs()

            result.append({
                "job_id": job_id,
                "status": status,
                "command": entry.get("command", ""),
                "mode": entry.get("mode", "local"),
                "created_at": entry.get("created_at", ""),
            })
        return result
