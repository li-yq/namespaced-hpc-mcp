"""Job manager — async job lifecycle with on-disk output.

Every job writes stdout/stderr directly to disk files via shell redirect.
No pipes, no in-memory buffering. The job manager tracks PIDs, checks
status via polling, and reads tail lines from the output files on demand.
"""
from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

from ns_hpc.config import Config
from ns_hpc.instance import Instance
from ns_hpc.namespace import build_bwrap_args


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

    Each job runs inside a bwrap sandbox.  stdout/stderr are redirected
    directly to disk files via shell redirect — no Python-side pipes.
    """

    def __init__(self, instance: Instance, config: Config):
        self.instance = instance
        self.config = config
        self._jobs: dict[str, dict] = {}  # job_id -> {handle, proc, ...}

    def _next_job_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def _ensure_output_dir(self) -> Path:
        """Create output dir inside workspace so it's accessible from sandbox."""
        p = self.instance.workspace_dir / ".ns_hpc_output"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def submit(
        self,
        command: str,
        *,
        mode: str = "local",
        timeout: float = 60,
        tail: int = 50,
    ) -> JobResult:
        """Submit a job and wait up to ``timeout`` seconds.

        The command runs inside a bwrap sandbox.  stdout/stderr are written
        directly to ``{output_dir}/{job_id}.{out,err}`` via shell redirect.

        Always waits the full ``timeout`` (or until the job finishes).
        Returns tail lines of whatever output was produced.
        """
        job_id = self._next_job_id()
        output_dir = self._ensure_output_dir()
        stdout_path = output_dir / f"{job_id}.out"
        stderr_path = output_dir / f"{job_id}.err"

        # Build the wrapped command: redirect output using sandbox-visible paths
        mount = self.config.namespace_defaults.workspace_mount
        sandbox_stdout = f"{mount}/.ns_hpc_output/{job_id}.out"
        sandbox_stderr = f"{mount}/.ns_hpc_output/{job_id}.err"
        wrapped = f"({command}) >'{sandbox_stdout}' 2>'{sandbox_stderr}'"

        if mode == "local":
            return self._submit_local(
                job_id, command, wrapped, stdout_path, stderr_path, timeout, tail,
            )
        elif mode == "slurm":
            return self._submit_slurm(
                job_id, command, wrapped, stdout_path, stderr_path, timeout, tail,
            )
        else:
            raise ValueError(f"Unknown mode: {mode}")

    def _submit_local(
        self,
        job_id: str,
        raw_command: str,
        wrapped_command: str,
        stdout_path: Path,
        stderr_path: Path,
        timeout: float,
        tail: int,
    ) -> JobResult:
        argv = build_bwrap_args(
            command=["/bin/sh", "-c", wrapped_command],
            workspace_host_path=str(self.instance.workspace_dir),
            config=self.config,
        )

        proc = subprocess.Popen(
            argv,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Track the job
        started_at = time.monotonic()
        self._jobs[job_id] = {
            "proc": proc,
            "command": raw_command,
            "mode": "local",
            "stdout_path": stdout_path,
            "stderr_path": stderr_path,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Wait up to timeout
        try:
            proc.communicate(timeout=timeout)
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
        if job_id in self._jobs:
            del self._jobs[job_id]
        exit_code = proc.returncode
        status = JobStatus.COMPLETED if exit_code == 0 else JobStatus.FAILED
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
        raw_command: str,
        wrapped_command: str,
        stdout_path: Path,
        stderr_path: Path,
        timeout: float,
        tail: int,
    ) -> JobResult:
        import shlex

        bwrap_args = build_bwrap_args(
            command=["/bin/sh", "-c", wrapped_command],
            workspace_host_path=str(self.instance.workspace_dir),
            config=self.config,
        )
        bwrap_cmd = " ".join(shlex.quote(a) for a in bwrap_args)

        # Find a valid partition
        partition = self._detect_partition()

        script = f"""#!/bin/bash
#SBATCH --job-name=ns-hpc-{job_id[:8]}
#SBATCH --output={stdout_path}
#SBATCH --error={stderr_path}
#SBATCH --time={max(1, int(timeout) // 60 + 1)}
#SBATCH --partition={partition}

{bwrap_cmd}
"""
        script_path = self.instance.workspace_dir / f".ns_hpc_slurm_{job_id}.sh"
        script_path.write_text(script)

        result = subprocess.run(
            ["sbatch", str(script_path)],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(f"sbatch failed: {result.stderr.strip()}")

        slurm_job_id = int(result.stdout.strip().split()[-1])

        started_at = time.monotonic()
        self._jobs[job_id] = {
            "slurm_job_id": slurm_job_id,
            "command": raw_command,
            "mode": "slurm",
            "stdout_path": stdout_path,
            "stderr_path": stderr_path,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Slurm may not schedule immediately; wait up to timeout
        elapsed = time.monotonic() - started_at
        remaining = timeout - elapsed
        if remaining > 0:
            time.sleep(min(remaining, 5))  # brief initial wait

        # Return running — Slurm jobs are inherently async
        return JobResult(
            job_id=job_id,
            status=JobStatus.RUNNING,
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            duration=time.monotonic() - started_at,
        )

    def _detect_partition(self) -> str:
        """Find a usable Slurm partition."""
        try:
            r = subprocess.run(
                ["sinfo", "--noheader", "-o", "%P"],
                capture_output=True, text=True, timeout=15,
            )
            if r.returncode == 0:
                partitions = [
                    p.strip().rstrip("*")
                    for p in r.stdout.strip().splitlines()
                    if p.strip()
                ]
                if partitions:
                    return partitions[0]
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        return "debug"

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
        proc: subprocess.Popen = entry["proc"]
        stdout_path: Path = entry["stdout_path"]
        stderr_path: Path = entry["stderr_path"]

        started_at = time.monotonic()

        if timeout > 0 and proc.poll() is None:
            try:
                proc.communicate(timeout=timeout)
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

    def _poll_slurm(
        self,
        job_id: str,
        entry: dict,
        timeout: float,
        tail: int,
    ) -> JobResult:
        slurm_job_id = entry["slurm_job_id"]
        stdout_path: Path = entry["stdout_path"]
        stderr_path: Path = entry["stderr_path"]

        try:
            result = subprocess.run(
                ["sacct", "-j", str(slurm_job_id), "--json"],
                capture_output=True, text=True, timeout=30,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return JobResult(
                job_id=job_id,
                status=JobStatus.RUNNING,
                stdout_path=str(stdout_path),
                stderr_path=str(stderr_path),
            )

        if result.returncode != 0:
            return JobResult(
                job_id=job_id, status=JobStatus.RUNNING,
                stdout_path=str(stdout_path), stderr_path=str(stderr_path),
            )

        try:
            data = json.loads(result.stdout)
            jobs = data.get("jobs", [])
        except (json.JSONDecodeError, TypeError):
            jobs = []

        for job in jobs:
            state = (job.get("state") or "").strip()
            exit_code_str = (job.get("exit_code") or "").strip()

            if state in ("COMPLETED",):
                self._jobs.pop(job_id, None)
                ec = 0
                try:
                    ec = int(exit_code_str.split(":")[0])
                except (ValueError, IndexError):
                    pass
                return JobResult(
                    job_id=job_id, status=JobStatus.COMPLETED,
                    exit_code=ec,
                    stdout_tail=_tail_file(stdout_path, tail),
                    stderr_tail=_tail_file(stderr_path, tail),
                    stdout_path=str(stdout_path),
                    stderr_path=str(stderr_path),
                )
            elif state in ("FAILED", "TIMEOUT", "NODE_FAIL", "CANCELLED"):
                self._jobs.pop(job_id, None)
                return JobResult(
                    job_id=job_id, status=JobStatus.FAILED, exit_code=-1,
                    stdout_path=str(stdout_path), stderr_path=str(stderr_path),
                )

        return JobResult(
            job_id=job_id, status=JobStatus.RUNNING,
            stdout_path=str(stdout_path), stderr_path=str(stderr_path),
        )

    def cancel(self, job_id: str) -> bool:
        """Cancel a running job.  Returns True if cancelled."""
        entry = self._jobs.get(job_id)
        if entry is None:
            return False

        if entry["mode"] == "local":
            proc: subprocess.Popen = entry["proc"]
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.communicate()
            self._jobs.pop(job_id, None)
            return True
        else:
            slurm_job_id = entry.get("slurm_job_id")
            if slurm_job_id:
                subprocess.run(["scancel", str(slurm_job_id)], timeout=15)
            self._jobs.pop(job_id, None)
            return True

    def list_jobs(self) -> list[dict]:
        """List all tracked jobs."""
        result = []
        for job_id, entry in self._jobs.items():
            proc = entry.get("proc")
            status = JobStatus.RUNNING
            if proc and proc.poll() is not None:
                status = JobStatus.COMPLETED if proc.returncode == 0 else JobStatus.FAILED

            result.append({
                "job_id": job_id,
                "status": status.value,
                "command": entry.get("command", ""),
                "mode": entry.get("mode", "local"),
                "created_at": entry.get("created_at", ""),
            })
        return result
