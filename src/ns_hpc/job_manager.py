"""Job manager — async job lifecycle with on-disk persistence.

Every job writes stdout/stderr directly to disk files via shell redirect.
Job state is persisted to a JSON file so it survives process restarts.
"""
from __future__ import annotations

import json
import os
import shlex
import signal
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
        """After restart, reconcile local jobs via status files.

        When ``status_fd`` is configured: check the on-disk status file.
        If the final ``exit-code`` line exists the job is complete.
        Otherwise it was killed by ``--die-with-parent`` on restart -> unknown.

        Without ``status_fd``: local 'running' entries are orphaned -> unknown.
        """
        changed = False
        status_fd = self.config.namespace_defaults.status_fd
        has_status_fd = status_fd is not None

        for entry in self._jobs.values():
            if entry.get("mode") != "local" or entry.get("status") != "running":
                continue

            if has_status_fd and "status_path" in entry:
                finished, exit_code, _ = self._read_status_file(Path(entry["status_path"]))
                if finished:
                    entry["status"] = "completed" if exit_code == 0 else "failed"
                    entry["exit_code"] = exit_code
                else:
                    entry["status"] = "unknown"
            else:
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

    # ── Status file helpers ─────────────────────────────────────────────────

    @staticmethod
    def _read_status_file(path: Path) -> tuple[bool, int | None, int | None]:
        """Read a ``bwrap --json-status-fd`` output file.

        Iterates all lines and extracts known fields so the parser is
        forward-compatible with additional JSON lines bwrap may add.

        Returns ``(finished, exit_code, child_pid)``.
        ``finished`` is True when an ``exit-code`` line has been written.
        """
        try:
            content = path.read_text()
        except (OSError, FileNotFoundError):
            return False, None, None

        child_pid: int | None = None
        exit_code: int | None = None

        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue  # skip unknown lines

            if "exit-code" in obj:
                exit_code = obj["exit-code"]
            if "child-pid" in obj:
                child_pid = obj["child-pid"]

        return exit_code is not None, exit_code, child_pid

    @staticmethod
    def _duration_since_created(entry: dict) -> float:
        """Seconds since the job's ``created_at`` timestamp."""
        created = datetime.fromisoformat(entry["created_at"])
        return (datetime.now(timezone.utc) - created).total_seconds()

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
        status_fd = self.config.namespace_defaults.status_fd

        # Optional disk-based status tracking via bwrap --json-status-fd.
        # The CLI bwrap command reads status_fd from config and adds
        # --json-status-fd automatically, so we only need to redirect the fd.
        status_path: Path | None = None
        fd_redirect = ""
        if status_fd is not None:
            status_path = (stdout_path.parent / f"{job_id}.status").resolve()
            fd_redirect = f" {status_fd}>{shlex.quote(str(status_path))}"

        # sh -c '<python> -m ns_hpc bwrap <id> -- ... ><out> 2><err> [<n>><status>]'
        shell_cmd = (
            f"{sys.executable} -m ns_hpc bwrap {self.instance.id}"
            f" -- "
            f"/bin/sh -c {shlex.quote(command)}"
            f" >{shlex.quote(str(stdout_path))} 2>{shlex.quote(str(stderr_path))}"
            f"{fd_redirect}"
        )

        proc = subprocess.Popen(["sh", "-c", shell_cmd])

        # Persist job metadata
        now = datetime.now(timezone.utc).isoformat()
        entry: dict = {
            "command": command,
            "mode": "local",
            "status": "running",
            "pid": proc.pid,
            "slurm_job_id": None,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "created_at": now,
        }
        if status_path is not None:
            entry["status_path"] = str(status_path)
        self._jobs[job_id] = entry
        self._procs[job_id] = proc
        self._save_jobs()

        # Wait up to timeout — output is written to files by shell redirect
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return JobResult(
                job_id=job_id,
                status=JobStatus.RUNNING,
                stdout_tail=_tail_file(stdout_path, tail),
                stderr_tail=_tail_file(stderr_path, tail),
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
                duration=self._duration_since_created(entry),
            )

        # Process finished — prefer status file exit code, fall back to proc
        if status_path is not None:
            _, exit_code, _ = self._read_status_file(status_path)
            if exit_code is None:
                exit_code = proc.returncode
        else:
            exit_code = proc.returncode

        self._procs.pop(job_id, None)
        self._jobs.pop(job_id, None)
        self._save_jobs()
        status = JobStatus.COMPLETED if exit_code == 0 else JobStatus.FAILED
        return JobResult(
            job_id=job_id,
            status=status,
            exit_code=exit_code,
            stdout_tail=_tail_file(stdout_path, tail),
            stderr_tail=_tail_file(stderr_path, tail),
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            duration=self._duration_since_created(entry),
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

        now = datetime.now(timezone.utc).isoformat()
        self._jobs[job_id] = {
            "command": command,
            "mode": "slurm",
            "status": "running",
            "pid": None,
            "slurm_job_id": slurm_job_id,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "created_at": now,
        }
        self._save_jobs()

        # Slurm may not schedule immediately; brief initial wait
        remaining = timeout - 5
        if remaining > 0:
            time.sleep(min(remaining, 5))

        return JobResult(
            job_id=job_id,
            status=JobStatus.RUNNING,
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            duration=self._duration_since_created(self._jobs[job_id]),
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
        has_status_file = "status_path" in entry

        # Wait up to timeout if we have a process handle
        if timeout > 0 and proc is not None and proc.poll() is None:
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                pass

        # If status file is available, use it as source of truth
        if has_status_file:
            status_path = Path(entry["status_path"])
            finished, exit_code, _ = self._read_status_file(status_path)

            if finished:
                self._procs.pop(job_id, None)
                self._jobs.pop(job_id, None)
                self._save_jobs()
                return JobResult(
                    job_id=job_id,
                    status=JobStatus.COMPLETED if exit_code == 0 else JobStatus.FAILED,
                    exit_code=exit_code,
                    stdout_tail=_tail_file(stdout_path, tail),
                    stderr_tail=_tail_file(stderr_path, tail),
                    stdout_path=str(stdout_path),
                    stderr_path=str(stderr_path),
                    duration=self._duration_since_created(entry),
                )

            return JobResult(
                job_id=job_id,
                status=JobStatus.RUNNING,
                stdout_tail=_tail_file(stdout_path, tail),
                stderr_tail=_tail_file(stderr_path, tail),
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
                duration=self._duration_since_created(entry),
            )

        # Legacy path (no status file) — rely on proc handle
        if proc is None:
            status = JobStatus(entry.get("status", "unknown"))
            return JobResult(
                job_id=job_id,
                status=status,
                exit_code=entry.get("exit_code"),
                stdout_tail=_tail_file(stdout_path, tail),
                stderr_tail=_tail_file(stderr_path, tail),
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
                duration=self._duration_since_created(entry),
            )

        if proc.poll() is None:
            return JobResult(
                job_id=job_id,
                status=JobStatus.RUNNING,
                stdout_tail=_tail_file(stdout_path, tail),
                stderr_tail=_tail_file(stderr_path, tail),
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
                duration=self._duration_since_created(entry),
            )

        # Process finished — capture exit code
        exit_code = proc.returncode
        self._procs.pop(job_id, None)
        self._jobs.pop(job_id, None)
        self._save_jobs()
        return JobResult(
            job_id=job_id,
            status=JobStatus.COMPLETED if exit_code == 0 else JobStatus.FAILED,
            exit_code=exit_code,
            stdout_tail=_tail_file(stdout_path, tail),
            stderr_tail=_tail_file(stderr_path, tail),
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            duration=self._duration_since_created(entry),
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
                duration=self._duration_since_created(entry),
            )
        elif state in ("FAILED", "TIMEOUT", "NODE_FAIL", "CANCELLED"):
            self._jobs.pop(job_id, None)
            self._save_jobs()
            return JobResult(
                job_id=job_id, status=JobStatus.FAILED,
                exit_code=ec if ec is not None else -1,
                stdout_path=str(stdout_path), stderr_path=str(stderr_path),
                duration=self._duration_since_created(entry),
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
            proc = self._procs.pop(job_id, None)
            if proc and proc.poll() is None:
                # bwrap runs as PID 1 and ignores SIGTERM; SIGKILL is required.
                proc.kill()
                proc.wait()
            elif proc is None and "status_path" in entry:
                # No proc handle — try killing by outer PID from stored entry
                pid = entry.get("pid")
                if pid is not None:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except OSError:
                        pass  # process already gone
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
        has_status_fd = self.config.namespace_defaults.status_fd is not None
        changed = False

        for job_id, entry in self._jobs.items():
            status = entry.get("status", "unknown")
            proc = self._procs.get(job_id)

            if status == "running" and has_status_fd and "status_path" in entry:
                finished, exit_code, _ = self._read_status_file(Path(entry["status_path"]))
                if finished:
                    status = "completed" if exit_code == 0 else "failed"
                    entry["status"] = status
                    entry["exit_code"] = exit_code
                    changed = True
            elif proc and proc.poll() is not None:
                ec = proc.returncode
                status = "completed" if ec == 0 else "failed"
                entry["status"] = status
                changed = True

            result.append({
                "job_id": job_id,
                "status": status,
                "command": entry.get("command", ""),
                "mode": entry.get("mode", "local"),
                "created_at": entry.get("created_at", ""),
            })

        if changed:
            self._save_jobs()
        return result
