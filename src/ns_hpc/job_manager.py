"""Job manager — async job lifecycle with on-disk persistence.

Every job writes stdout/stderr directly to disk files via shell redirect.
Job state is persisted in per-job JSON files (``.ns_hpc_jobs/{job_id}.state``)
so it survives process restarts.  Per-job files avoid the read-modify-write
race of a single shared file under concurrent job completion.
"""
from __future__ import annotations

import json
import logging
import os
import shlex
import signal
import sys
import time
import uuid
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger("ns-hpc")

from ns_hpc.config import Config, parse_memory
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
    UNKNOWN = "unknown"


@dataclass
class JobResult:
    """Result returned by submit_job and poll_job."""

    job_id: str
    status: JobStatus
    exit_code: Optional[int] = None
    message: str = ""
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
            "message": self.message,
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "duration": round(self.duration, 2),
        }


def _tail_file(path: Path, n: int = 50, max_bytes: int = 1048576) -> str:
    """Read the last ``n`` lines of a file, reading at most ``max_bytes``."""
    if n <= 0 or not path.exists() or path.stat().st_size == 0:
        return ""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                return ""

            read_size = min(size, max_bytes)
            f.seek(size - read_size)
            data = f.read(read_size)

            lines = data.decode(errors="replace").splitlines()
            return "\n".join(lines[-n:])
    except (OSError, UnicodeDecodeError):
        return ""


def _container_path(host_path: str, instance: Instance, config: Config) -> str:
    """Translate a host-side path to a container-side path inside the bwrap sandbox.

    The workspace directory is bind-mounted at ``workspace_mount`` inside the
    container.  Output files under ``workspace_dir`` are reachable by replacing
    the host prefix with the mount point.

    If the path is outside the workspace dir it is returned unchanged.
    """
    ws_host = str(instance.workspace_dir)
    ws_container = config.namespace_defaults.workspace_mount
    if host_path.startswith(ws_host):
        return host_path.replace(ws_host, ws_container, 1)
    return host_path


class JobManager:
    """Manages async jobs for an instance.

    Each job runs inside a bwrap sandbox (via ``ns-hpc bwrap`` CLI).
    stdout/stderr are redirected directly to disk files via shell redirect.
    Job state is persisted to per-job files for survival across restarts.
    """

    def __init__(self, instance: Instance, config: Config):
        self.instance = instance
        self.config = config
        # In-memory subprocess handles for local (running) jobs
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        # Disk-persisted job metadata — per-job files under .ns_hpc_jobs/
        self._jobs_dir = instance.base_dir / ".ns_hpc_jobs"
        self._jobs = self._load_jobs()
        self._fixup_stale_jobs()

    # ── Persistence ──────────────────────────────────────────────────────

    def _load_jobs(self) -> dict[str, dict]:
        """Load all per-job state files from ``_jobs_dir``.  Returns {} on error."""
        jobs: dict[str, dict] = {}
        try:
            if not self._jobs_dir.exists():
                return jobs
            for f in self._jobs_dir.iterdir():
                if f.suffix != ".state":
                    continue
                try:
                    data = json.loads(f.read_text())
                    if "created_at" in data:
                        jobs[f.stem] = data
                except (json.JSONDecodeError, OSError):
                    continue
        except OSError:
            pass
        return jobs

    def _state_path(self, job_id: str) -> Path:
        return self._jobs_dir / f"{job_id}.state"

    def _save_job(self, job_id: str) -> None:
        """Atomically write one job's state file."""
        entry = self._jobs.get(job_id)
        if entry is None:
            return
        self._jobs_dir.mkdir(parents=True, exist_ok=True)
        tmp = self._state_path(job_id).with_suffix(".tmp")
        tmp.write_text(json.dumps(entry, indent=2))
        os.replace(tmp, self._state_path(job_id))

    def _remove_job(self, job_id: str) -> None:
        """Remove one job's state file."""
        try:
            self._state_path(job_id).unlink(missing_ok=True)
        except OSError:
            pass

    def _fixup_stale_jobs(self) -> None:
        """After restart, reconcile local jobs via status files.

        When the status file shows ``exit-code`` the job is complete.
        When it shows ``child-pid`` but no ``exit-code``:

        * If ``config.job.proc_check`` is enabled (default), verify
          whether the bwrap process is alive via ``/proc`` (guarding
          against PID reuse).  If alive the job keeps running.
        * Otherwise mark the job ``FAILED`` — the bwrap process is
          gone and the kernel has torn down the namespace.
        """
        changed: list[str] = []
        cfg = self.config
        status_fd = cfg.namespace_defaults.status_fd

        for job_id, entry in self._jobs.items():
            if entry.get("mode") != "local" or entry.get("status") != "running":
                continue

            if "status_path" in entry:
                finished, exit_code, _ = self._read_status_file(Path(entry["status_path"]))
                if finished:
                    entry["status"] = "completed" if exit_code == 0 else "failed"
                    entry["exit_code"] = exit_code
                    changed.append(job_id)
                elif (
                    cfg.job.proc_check
                    and self._is_bwrap_alive(entry.get("pid", -1), status_fd, entry["status_path"])
                ):
                    pass  # still legitimately running
                else:
                    entry["status"] = "failed"
                    changed.append(job_id)
            else:
                entry["status"] = "unknown"
                changed.append(job_id)

        for job_id in changed:
            self._save_job(job_id)

    # ── Job ID & output dir ──────────────────────────────────────────────

    def _next_job_id(self) -> str:
        return uuid.uuid4().hex[:12]

    def _ensure_output_dir(self) -> Path:
        p = self.instance.workspace_dir / ".ns_hpc_output"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _status_dir(self) -> Path:
        """Directory for bwrap status files — outside workspace mount."""
        p = self.instance.base_dir / "status"
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

    @staticmethod
    def _is_bwrap_alive(pid: int, status_fd: int, status_path: str) -> bool:
        """Check if a bwrap process with the given fd is still alive.

        Verifies two things to guard against PID reuse:
          1. ``/proc/<pid>/comm`` is ``"bwrap"``
          2. ``/proc/<pid>/fd/<status_fd>`` → ``status_path``

        Returns ``True`` only when both checks pass.
        """
        try:
            proc_dir = Path(f"/proc/{pid}")
            if not proc_dir.exists():
                return False

            # Check process name — a reused PID's comm won't be "bwrap"
            comm = (proc_dir / "comm").read_text().strip()
            if comm != "bwrap":
                return False

            # Check that the status fd points to our expected file
            fd_path = proc_dir / "fd" / str(status_fd)
            if not fd_path.exists():
                return False

            return os.readlink(str(fd_path)) == status_path
        except (OSError, FileNotFoundError, IOError):
            return False

    # ── Submit ───────────────────────────────────────────────────────────

    async def submit(
        self,
        command: str,
        *,
        mode: str = "local",
        timeout: float = 60,
        tail: int = 50,
        slurm_resources: dict[str, int | str] | None = None,
    ) -> JobResult:
        """Submit a job, then poll up to ``timeout`` seconds.

        Creates the job via ``_submit_local`` or ``_submit_slurm``,
        then delegates to ``poll()`` so the wait logic is shared.

        Args:
            command: Shell command to run.
            mode: ``"local"`` (bwrap) or ``"slurm"`` (sbatch).
            timeout: Max seconds to wait for completion.
            tail: Number of tail lines to return.
            slurm_resources: Per-job resource overrides for Slurm
                (e.g. ``{"cpus": 4, "memory": "8G"}``).
        """
        if not command.strip():
            raise ValueError("command must not be empty")
        job_id = self._next_job_id()
        output_dir = self._ensure_output_dir()
        status_fd = self.config.namespace_defaults.status_fd
        stdout_path = output_dir / f"{job_id}.out"
        stderr_path = output_dir / f"{job_id}.err"
        status_path = self._status_dir() / f"{job_id}.status"

        if mode == "local":
            await self._submit_local(job_id, command, stdout_path, stderr_path, status_path)
        elif mode == "slurm":
            await self._submit_slurm(job_id, command, stdout_path, stderr_path, status_path, slurm_resources)
        else:
            raise ValueError(f"Unknown mode: {mode}")

        return await self.poll(job_id, timeout=timeout, tail=tail)

    async def _submit_local(
        self,
        job_id: str,
        command: str,
        stdout_path: Path,
        stderr_path: Path,
        status_path: Path,
    ) -> None:
        """Start a local bwrap job and persist its entry.  Does not wait."""
        status_fd = self.config.namespace_defaults.status_fd

        # exec so the outer bwrap reuses proc.pid (comm="bwrap"), making
        # _is_bwrap_alive work:
        #   sh -c exec python ... bwrap → os.execvp → bwrap (outer, proc.pid)
        #                                                   └─ bwrap (inner, child-pid)
        shell_cmd = (
            f"exec {sys.executable} -m ns_hpc bwrap {self.instance.id}"
            f" -- "
            f"/bin/sh -c {shlex.quote(command)}"
            f" >{shlex.quote(str(stdout_path))} 2>{shlex.quote(str(stderr_path))}"
            f" {status_fd}>{shlex.quote(str(status_path))}"
        )

        # Wrap with systemd-run for cgroup v2 resource limits (best-effort)
        if self.config.resources.use_systemd:
            cpus = self.config.resources.cpus
            memory = parse_memory(self.config.resources.memory)
            runner = [
                "systemd-run", "--user", "--scope",
                "-p", f"CPUQuota={cpus * 100}%",
                "-p", f"MemoryMax={memory}",
                "--", "sh", "-c",
            ]
            logger.debug("running: %s", shlex.join(runner + [shell_cmd]))
            proc = await asyncio.create_subprocess_exec(*runner, shell_cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        else:
            logger.debug("running: sh -c %s", shell_cmd[:200])
            proc = await asyncio.create_subprocess_exec("sh", "-c", shell_cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)

        self._jobs[job_id] = {
            "command": command,
            "mode": "local",
            "status": "running",
            "pid": proc.pid,
            "slurm_job_id": None,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "status_path": str(status_path),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._procs[job_id] = proc
        self._save_job(job_id)

    async def _submit_slurm(
        self,
        job_id: str,
        command: str,
        stdout_path: Path,
        stderr_path: Path,
        status_path: Path,
        slurm_resources: dict[str, int | str] | None = None,
    ) -> None:
        """Submit via sbatch and persist the entry.  Does not wait."""
        status_fd = self.config.namespace_defaults.status_fd
        bwrap_cmd = (
            f"exec {sys.executable} -m ns_hpc bwrap {self.instance.id} -- "
            f"/bin/sh -c {shlex.quote(command)}"
            f" {status_fd}>{shlex.quote(str(status_path))}"
        )

        # Build sbatch args with configured resource flags
        sbatch_args = [
            "sbatch",
            "--wrap", bwrap_cmd,
            "--output", str(stdout_path),
            "--error", str(stderr_path),
            "--job-name", f"ns-hpc-{job_id[:8]}",
            "--partition", self.config.slurm.partition,
        ]
        for name, spec in self.config.slurm.resources.items():
            value = (slurm_resources or {}).get(name, spec.default)
            if value is not None:
                # Validate against configured max
                spec_max = spec.max
                try:
                    v_int = int(value) if not isinstance(value, int) else value
                    m_int = int(spec_max) if not isinstance(spec_max, int) else spec_max
                    if v_int > m_int:
                        raise ValueError(
                            f"Slurm resource '{name}' value {value} exceeds maximum {spec_max}"
                        )
                except (ValueError, TypeError):
                    # Non-integer — try memory comparison
                    if isinstance(value, str) and isinstance(spec_max, str):
                        v_bytes = parse_memory(value)
                        m_bytes = parse_memory(spec_max)
                        if v_bytes > m_bytes:
                            raise ValueError(
                                f"Slurm resource '{name}' value {value} exceeds maximum {spec_max}"
                            )
                    else:
                        raise ValueError(
                            f"Slurm resource '{name}' value {value!r} (type={type(value).__name__}) "
                            f"cannot be compared against maximum {spec_max!r}"
                        )
                sbatch_args.append(spec.parameter.format(value))

        try:
            logger.debug("running: sbatch (wrapped command for job %s)", job_id)
            proc = await asyncio.create_subprocess_exec(
                *sbatch_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout_bytes, stderr_bytes = await asyncio.wait_for(
                    proc.communicate(), timeout=30,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise RuntimeError("sbatch timed out")
        except FileNotFoundError:
            raise RuntimeError("slurm scheduler is not available")
        if proc.returncode != 0:
            stderr_text = stderr_bytes.decode(errors="replace").strip() if stderr_bytes else ""
            raise RuntimeError(f"sbatch failed: {stderr_text}")

        stdout_text = stdout_bytes.decode(errors="replace").strip()
        await asyncio.sleep(2)

        self._jobs[job_id] = {
            "command": command,
            "mode": "slurm",
            "status": "running",
            "pid": None,
            "slurm_job_id": int(stdout_text.split()[-1]),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "status_path": str(status_path),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save_job(job_id)

    # ── Poll ─────────────────────────────────────────────────────────────

    async def poll(
        self,
        job_id: str,
        *,
        timeout: float = 0,
        tail: int = 50,
    ) -> Optional[JobResult]:
        """Poll a job.  Waits up to ``timeout`` seconds for completion.

        Returns the cached result for already-finished jobs.
        Returns ``None`` only when the ``job_id`` is unknown.
        """
        entry = self._jobs.get(job_id)
        if entry is None:
            return None

        status = entry.get("status")
        if status in ("completed", "failed", "cancelled", "unknown"):
            return self._result_from_entry(job_id, entry, tail)

        if entry["mode"] == "local":
            return await self._poll_local(job_id, entry, timeout, tail)
        return await self._poll_slurm(job_id, entry, timeout, tail)

    def _result_from_entry(self, job_id: str, entry: dict, tail: int) -> JobResult:
        """Build a JobResult from a persisted entry (already finished)."""
        status = JobStatus(entry.get("status", "unknown"))
        ec = entry.get("exit_code")
        duration = entry.get("duration")
        if duration is None:
            duration = self._duration_since_created(entry)
        message = ""
        if status == JobStatus.FAILED:
            message = f"job failed with exit code {ec}" if ec is not None else "job failed"
        elif status == JobStatus.CANCELLED:
            message = "job was cancelled"
        elif status == JobStatus.UNKNOWN:
            message = "job status is unknown"
        return JobResult(
            job_id=job_id,
            status=status,
            exit_code=ec,
            message=message,
            stdout_tail=_tail_file(Path(entry["stdout_path"]), tail),
            stderr_tail=_tail_file(Path(entry["stderr_path"]), tail),
            stdout_path=_container_path(entry.get("stdout_path", ""), self.instance, self.config),
            stderr_path=_container_path(entry.get("stderr_path", ""), self.instance, self.config),
            duration=duration,
        )

    async def _poll_local(
        self,
        job_id: str,
        entry: dict,
        timeout: float,
        tail: int,
    ) -> JobResult:
        proc = self._procs.get(job_id)
        stdout_path: Path = Path(entry["stdout_path"])
        stderr_path: Path = Path(entry["stderr_path"])
        status_path: Path = Path(entry["status_path"])

        # Wait up to timeout if we have a process handle
        if timeout > 0 and proc is not None and proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass

        # Recovery path: no process handle (server restart) — poll status file
        if timeout > 0 and proc is None:
            status_fd = self.config.namespace_defaults.status_fd
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                finished, exit_code, _ = self._read_status_file(status_path)
                if finished:
                    break
                # Process may have died without writing exit-code — check /proc
                pid = entry.get("pid", -1)
                if not self._is_bwrap_alive(pid, status_fd, entry.get("status_path", "")):
                    exit_code = -1
                    finished = True
                    break
                await asyncio.sleep(0.2)
            if not finished:
                finished, exit_code, _ = self._read_status_file(status_path)
        else:
            # Normal path: read the status file once
            finished, exit_code, _ = self._read_status_file(status_path)

        if not finished and proc and proc.returncode is not None:
            # Process gone but status file didn't flush — use proc returncode
            exit_code = proc.returncode
            finished = True

        if finished:
            self._procs.pop(job_id, None)
            entry["status"] = "completed" if exit_code == 0 else "failed"
            entry["exit_code"] = exit_code
            if status_path.exists():
                entry["finished_at"] = datetime.fromtimestamp(
                    status_path.stat().st_mtime, tz=timezone.utc
                ).isoformat()
            else:
                entry["finished_at"] = datetime.now(timezone.utc).isoformat()
            created = datetime.fromisoformat(entry["created_at"])
            entry["duration"] = round(
                (datetime.fromisoformat(entry["finished_at"]) - created).total_seconds(), 2
            )
            self._save_job(job_id)
            return self._result_from_entry(job_id, entry, tail)

        return JobResult(
            job_id=job_id,
            status=JobStatus.RUNNING,
            message="job is still running",
            stdout_tail=_tail_file(stdout_path, tail),
            stderr_tail=_tail_file(stderr_path, tail),
            stdout_path=_container_path(str(stdout_path), self.instance, self.config),
            stderr_path=_container_path(str(stderr_path), self.instance, self.config),
            duration=self._duration_since_created(entry),
        )

    async def _slurm_job_state(self, slurm_job_id: int) -> tuple[str, int | None]:
        """Query sacct and return (state, exit_code) for a Slurm job."""
        try:
            logger.debug("running: sacct -j %s --json -X", slurm_job_id)
            proc = await asyncio.create_subprocess_exec(
                "sacct", "-j", str(slurm_job_id), "--json", "-X",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout_bytes, stderr_bytes = await asyncio.wait_for(
                    proc.communicate(), timeout=30,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise RuntimeError("slurm query timed out")
        except FileNotFoundError:
            raise RuntimeError("slurm scheduler is not available")

        if proc.returncode != 0:
            stderr_text = stderr_bytes.decode(errors="replace").strip() if stderr_bytes else ""
            raise RuntimeError(f"slurm query failed: {stderr_text}")

        try:
            data = json.loads(stdout_bytes.decode(errors="replace"))
            jobs = data.get("jobs", [])
        except (json.JSONDecodeError, TypeError):
            jobs = []

        for job in jobs:
            state = _parse_slurm_state(job)
            ec = _parse_slurm_exit_code(job)
            if state:
                logger.debug("sacct %s state=%s exit_code=%s", slurm_job_id, state, ec)
                return (state, ec)

        raise RuntimeError(f"slurm job {slurm_job_id} not found in sacct output")

    async def _poll_slurm(
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
            raise ValueError("job has no slurm_job_id — missing slurm identifier")

        deadline = time.monotonic() + timeout if timeout > 0 else None

        while True:
            state, ec = await self._slurm_job_state(slurm_job_id)

            if state in ("COMPLETED",):
                # Prefer exit code from status file (more precise than sacct)
                if ec is None and "status_path" in entry:
                    _, st = self._read_status_file(Path(entry["status_path"]))
                    ec = st
                entry["status"] = "completed"
                entry["exit_code"] = ec if ec is not None else 0
                entry["finished_at"] = datetime.now(timezone.utc).isoformat()
                entry["duration"] = round(self._duration_since_created(entry), 2)
                self._save_job(job_id)
                return JobResult(
                    job_id=job_id, status=JobStatus.COMPLETED,
                    exit_code=ec if ec is not None else 0,
                    stdout_tail=_tail_file(stdout_path, tail),
                    stderr_tail=_tail_file(stderr_path, tail),
                    stdout_path=_container_path(str(stdout_path), self.instance, self.config),
                    stderr_path=_container_path(str(stderr_path), self.instance, self.config),
                    duration=entry["duration"],
                )

            if state in ("FAILED", "TIMEOUT", "NODE_FAIL"):
                # Prefer exit code from status file (more precise than sacct)
                if ec is None and "status_path" in entry:
                    _, st = self._read_status_file(Path(entry["status_path"]))
                    ec = st
                entry["status"] = "failed"
                entry["exit_code"] = ec if ec is not None else -1
                entry["finished_at"] = datetime.now(timezone.utc).isoformat()
                entry["duration"] = round(self._duration_since_created(entry), 2)
                self._save_job(job_id)
                return_code = ec if ec is not None else -1
                message = f"job failed with exit code {return_code}" if return_code != -1 else "job failed"
                return JobResult(
                    job_id=job_id, status=JobStatus.FAILED,
                    exit_code=return_code,
                    message=message,
                    stdout_tail=_tail_file(stdout_path, tail),
                    stderr_tail=_tail_file(stderr_path, tail),
                    stdout_path=_container_path(str(stdout_path), self.instance, self.config),
                    stderr_path=_container_path(str(stderr_path), self.instance, self.config),
                    duration=entry["duration"],
                )

            if state == "CANCELLED":
                # Externally cancelled (e.g. scancel, preemption)
                if ec is None and "status_path" in entry:
                    _, st = self._read_status_file(Path(entry["status_path"]))
                    ec = st
                entry["status"] = "cancelled"
                entry["exit_code"] = ec if ec is not None else -1
                entry["finished_at"] = datetime.now(timezone.utc).isoformat()
                entry["duration"] = round(self._duration_since_created(entry), 2)
                self._save_job(job_id)
                return JobResult(
                    job_id=job_id, status=JobStatus.CANCELLED,
                    exit_code=ec if ec is not None else -1,
                    message="job was cancelled",
                    stdout_tail=_tail_file(stdout_path, tail),
                    stderr_tail=_tail_file(stderr_path, tail),
                    stdout_path=_container_path(str(stdout_path), self.instance, self.config),
                    stderr_path=_container_path(str(stderr_path), self.instance, self.config),
                    duration=entry["duration"],
                )

            if deadline and time.monotonic() >= deadline:
                # Job still in scheduler — return RUNNING, not UNKNOWN
                return JobResult(
                    job_id=job_id, status=JobStatus.RUNNING,
                    message="job is still running",
                    stdout_path=_container_path(str(stdout_path), self.instance, self.config),
                    stderr_path=_container_path(str(stderr_path), self.instance, self.config),
                    duration=self._duration_since_created(entry),
                )

            if deadline is None:
                # timeout=0 means just peek — return immediately
                return JobResult(
                    job_id=job_id, status=JobStatus.RUNNING,
                    message="job is still running",
                    stdout_path=_container_path(str(stdout_path), self.instance, self.config),
                    stderr_path=_container_path(str(stderr_path), self.instance, self.config),
                    duration=self._duration_since_created(entry),
                )

            # sleep between sacct queries — slurmdbd warns against tight polling loops
            await asyncio.sleep(5)

    async def cancel(self, job_id: str) -> bool:
        """Cancel a running job.  Returns True if cancelled."""
        entry = self._jobs.get(job_id)
        if entry is None:
            return False

        # Already in a terminal state — nothing to cancel
        if entry.get("status") in ("completed", "failed", "cancelled", "unknown"):
            return True

        if entry["mode"] == "local":
            proc = self._procs.pop(job_id, None)
            if proc and proc.returncode is None:
                # Send SIGTERM first so the inner command can clean up.
                # Killing the outer bwrap triggers the kernel to tear
                # down the namespace, killing all processes inside.
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    proc.kill()
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        logger.warning(
                            "job %s: bwrap %d stuck even after SIGKILL (D state?), "
                            "abandoning to keep MCP responsive",
                            job_id, proc.pid,
                        )
            elif proc is None and "status_path" in entry:
                # No proc handle (recovery) — verify PID isn't reused,
                # then send SIGTERM first.
                pid = entry.get("pid")
                status_fd = self.config.namespace_defaults.status_fd
                if pid is not None and self._is_bwrap_alive(pid, status_fd, entry["status_path"]):
                    try:
                        os.kill(pid, signal.SIGTERM)
                        deadline = time.monotonic() + 5
                        while time.monotonic() < deadline:
                            if not Path(f"/proc/{pid}").exists():
                                break
                            await asyncio.sleep(0.2)
                        else:
                            os.kill(pid, signal.SIGKILL)
                    except OSError:
                        pass
        else:
            slurm_job_id = entry.get("slurm_job_id")
            if slurm_job_id:
                logger.debug("running: scancel %s", slurm_job_id)
                proc = await asyncio.create_subprocess_exec(
                    "scancel", str(slurm_job_id),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                try:
                    await asyncio.wait_for(proc.wait(), timeout=15)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()

        entry["status"] = "cancelled"
        self._save_job(job_id)
        return True

    def list_jobs(self) -> list[dict]:
        """List all tracked jobs from disk-persisted state."""
        result = []
        changed: list[str] = []

        for job_id, entry in self._jobs.items():
            status = entry.get("status", "unknown")

            # Auto-detect completed jobs via status file
            if status == "running" and "status_path" in entry:
                finished, exit_code, _ = self._read_status_file(Path(entry["status_path"]))
                if finished:
                    status = "completed" if exit_code == 0 else "failed"
                    entry["status"] = status
                    entry["exit_code"] = exit_code
                    changed.append(job_id)

            result.append({
                "job_id": job_id,
                "status": status,
                "command": entry.get("command", ""),
                "mode": entry.get("mode", "local"),
                "created_at": entry.get("created_at", ""),
            })

        for job_id in changed:
            self._save_job(job_id)
        return result
