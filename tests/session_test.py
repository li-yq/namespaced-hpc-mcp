#!/usr/bin/env python3
"""Integration test — simulated MCP sessions with full timing-edge-case coverage.

Produces a timestamped session log for manual review.

Usage:
    python tests/session_test.py                         # local only
    bash slurm/test_session.sh                           # in slurm cluster
"""
from __future__ import annotations

__test__ = False

import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

CONFIG_PATH = os.environ.get("NS_HPC_CONFIG", "config/config.toml")
os.environ["NS_HPC_CONFIG"] = CONFIG_PATH

import asyncio
from ns_hpc.server import mcp, server_lifespan
from ns_hpc.instance import Instance
from ns_hpc.config import Config, load_config
from ns_hpc.job_manager import JobManager, JobResult, JobStatus

# ── Config template ─────────────────────────────────────────────────────────

_CONFIG_TOML = """\
instances_dir = "{instances_dir}"

[namespace_defaults]
bind_ro = ["/usr", "/bin", "/lib", "/lib64"]
workspace_mount = "/workspace"
flags = ["--unshare-all", "--share-net", "--proc", "/proc", "--dev", "/dev",
         "--tmpfs", "/tmp"]

[proxied_mcps]

[resource_defaults]
context_dirs = ["config/context"]
resource_patterns = ["*.md"]

[slurm]
partition = "cpu"
default_cpus = 1
default_memory_gb = 4
default_timeout = 3600

[resource_limits]
local_timeout = 300
slurm_timeout = 86400
"""

# Produces ~10 lines over ~20s — great for tail and timing tests.
_SEQ_CMD = r'for i in `seq 10`; do echo "line-$i"; date; sleep 2; done'


def check_slurm() -> bool:
    return shutil.which("sbatch") is not None


def _test_config(tmp_dir: str) -> Config:
    instances_dir = tmp_dir
    if check_slurm():
        instances_dir = "/home/testuser/mcp_instances"
    config_path = Path(tmp_dir) / "config.toml"
    config_path.write_text(_CONFIG_TOML.format(instances_dir=instances_dir))
    context_dir = Path(tmp_dir) / "config" / "context"
    context_dir.mkdir(parents=True, exist_ok=True)
    (context_dir / "README.md").write_text("# Test context\n")
    os.environ["NS_HPC_CONFIG"] = str(config_path)
    return load_config(str(config_path))


# ── Session log ─────────────────────────────────────────────────────────────


class SessionLog:
    """Collects timestamped output and prints a summary at the end."""

    def __init__(self):
        self.results: list[tuple[str, bool, str]] = []
        self._t0 = time.time()

    def _ts(self) -> str:
        return f"[{time.time() - self._t0:8.1f}s]"

    def heading(self, title: str) -> None:
        print(f"\n{self._ts()} ═══ {title} ═══")

    def subheading(self, title: str) -> None:
        print(f"{self._ts()}   ─── {title} ───")

    def ok(self, label: str, detail: str = "") -> None:
        self.results.append((label, True, detail))
        print(f"{self._ts()}   ✓ {label}")
        if detail:
            for line in detail.split("\n"):
                print(f"          {line}")

    def fail(self, label: str, msg: str) -> None:
        self.results.append((label, False, msg))
        print(f"{self._ts()}   ✗ {label}: {msg}")

    def log_result(self, label: str, r: JobResult) -> None:
        d = r.to_dict()
        ok = d["status"] not in ("unknown",)
        (self.ok if ok else self.fail)(label, json.dumps(d, indent=2))

    def print_summary(self) -> int:
        fails = [(l, d) for l, ok, d in self.results if not ok]
        print(f"\n{'═' * 60}")
        total = len(self.results)
        passed = total - len(fails)
        print(f"Scenarios: {total}  Passed: {passed}  Failed: {len(fails)}")
        for label, detail in fails:
            print(f"  ✗ {label}\n      {detail}")
        return len(fails)


# ── Local timing scenarios ──────────────────────────────────────────────────


async def s_quick_job(log: SessionLog, mgr: JobManager) -> JobResult:
    """Job finishes before submit timeout → COMPLETED exit 0."""
    log.subheading("Quick job — finishes before timeout")
    r = mgr.submit("echo ok", timeout=10, tail=5)
    log.log_result("submit echo ok", r)
    assert r.status == JobStatus.COMPLETED, f"expected COMPLETED, got {r.status}"
    assert r.exit_code == 0
    assert "ok" in r.stdout_tail
    assert "/workspace/" in r.stdout_path
    return r


async def s_repoll_finished_cached(log: SessionLog, mgr: JobManager, quick: JobResult) -> None:
    """Re-poll an already-finished job → instant COMPLETED from cache."""
    log.subheading("Re-poll finished job — cached result")
    r = mgr.poll(quick.job_id, timeout=0, tail=3)
    assert r is not None
    log.log_result("poll finished (cached)", r)
    assert r.status == JobStatus.COMPLETED


async def s_nonzero_exit(log: SessionLog, mgr: JobManager) -> None:
    """exit 42 → FAILED exit_code=42."""
    log.subheading("Non-zero exit code")
    r = mgr.submit("exit 42", timeout=10)
    log.log_result("submit exit 42", r)
    assert r.status == JobStatus.FAILED
    assert r.exit_code == 42


async def s_timeout_kill(log: SessionLog, mgr: JobManager) -> None:
    """Long job with short timeout + no-detach → TIMEOUT with partial output."""
    log.subheading("Timeout + kill — short timeout, job killed, partial tail")
    r = mgr.submit(_SEQ_CMD, timeout=3, tail=3)
    # Simulate the MCP layer's no-detach handling (cancel_and_tail)
    if r.status == JobStatus.RUNNING:
        r = mgr.cancel_and_tail(r, tail=3)
    log.log_result("submit seq timeout=3 (killed)", r)
    assert r.status == JobStatus.TIMEOUT, f"expected TIMEOUT, got {r.status}"
    assert r.exit_code is None


async def s_timeout_detach(log: SessionLog, mgr: JobManager) -> JobResult:
    """Long job with short timeout + detach → RUNNING with partial output."""
    log.subheading("Timeout + detach — submit seq, detach while running")
    r = mgr.submit(_SEQ_CMD, timeout=3, tail=3)
    log.log_result("submit seq timeout=3 (detached)", r)
    assert r.status == JobStatus.RUNNING, f"expected RUNNING, got {r.status}"
    assert len(r.stdout_tail) > 0, "expected partial stdout tail"
    return r


async def s_poll_running(log: SessionLog, mgr: JobManager, running: JobResult) -> None:
    """Poll a still-running job (timeout=0) → RUNNING."""
    log.subheading("Poll running job — peek, no wait")
    r = mgr.poll(running.job_id, timeout=0, tail=3)
    assert r is not None
    log.log_result("poll running (timeout=0)", r)
    assert r.status == JobStatus.RUNNING, f"expected RUNNING, got {r.status}"


async def s_poll_then_kill(log: SessionLog, mgr: JobManager, running: JobResult) -> None:
    """Poll a running job, then kill it after timeout."""
    log.subheading("Poll running then kill — wait 2s, cancel")
    r = mgr.poll(running.job_id, timeout=2, tail=3)
    assert r is not None
    log.log_result("poll running (timeout=2)", r)
    # If it finished in the 2s window, great; else cancel
    if r.status == JobStatus.RUNNING:
        ok = mgr.cancel(running.job_id)
        log.ok("cancelled after poll", f"ok={ok}")
        r2 = mgr.poll(running.job_id, timeout=0, tail=3)
        assert r2 is not None
        log.log_result("poll after cancel", r2)
        assert r2.status == JobStatus.CANCELLED


async def s_poll_finished_after_submit(log: SessionLog, mgr: JobManager) -> JobResult:
    """Submit seq, let it finish naturally, poll → COMPLETED full tail."""
    log.subheading("Submit detach, wait for natural completion, poll")
    r = mgr.submit(_SEQ_CMD, timeout=5, tail=3)
    log.log_result("submit seq timeout=5 (detach)", r)
    if r.status == JobStatus.COMPLETED:
        log.ok("(already finished in submit window)")
        return r
    # The seq loop needs ~20s total; we've waited 5s already.
    # Poll with a generous timeout to let it finish.
    r2 = mgr.poll(r.job_id, timeout=30, tail=5)
    assert r2 is not None
    log.log_result("poll after completion", r2)
    assert r2.status == JobStatus.COMPLETED, f"expected COMPLETED, got {r2.status}"
    assert r2.exit_code == 0
    assert len(r2.stdout_tail) > 0
    return r2


async def s_cancel_running(log: SessionLog, mgr: JobManager) -> None:
    """Cancel a running job → CANCELLED with output."""
    log.subheading("Cancel running job")
    r = mgr.submit(_SEQ_CMD, timeout=3, tail=3)
    assert r.status == JobStatus.RUNNING, f"expected RUNNING, got {r.status}"
    log.log_result("submit seq (detach)", r)
    ok = mgr.cancel(r.job_id)
    log.ok("cancel", f"ok={ok}")
    assert ok
    r2 = mgr.poll(r.job_id, timeout=0, tail=3)
    assert r2 is not None
    log.log_result("poll after cancel", r2)
    assert r2.status == JobStatus.CANCELLED, f"expected CANCELLED, got {r2.status}"


async def s_cancel_finished(log: SessionLog, mgr: JobManager, finished: JobResult) -> None:
    """Cancel an already-finished job → no-op, still COMPLETED."""
    log.subheading("Cancel already-finished job — no-op")
    ok = mgr.cancel(finished.job_id)
    log.ok("cancel finished", f"ok={ok}")
    assert ok
    r = mgr.poll(finished.job_id, timeout=0, tail=3)
    assert r is not None
    log.log_result("poll after cancel finished", r)
    assert r.status == JobStatus.COMPLETED


async def s_list_jobs(log: SessionLog, mgr: JobManager) -> None:
    """List jobs — mode field present, no slurm internals."""
    log.subheading("List jobs — verify no slurm internals exposed")
    jobs = mgr.list_jobs()
    assert len(jobs) >= 1
    for j in jobs:
        assert "slurm_job_id" not in j, "slurm_job_id must not leak to client"
        assert j.get("mode") in ("local", "slurm")
    log.ok("list_jobs", json.dumps(jobs, indent=2))


# ── Slurm timing scenarios ─────────────────────────────────────────────────


async def s_slurm_quick(log: SessionLog, mgr: JobManager) -> JobResult:
    log.subheading("Slurm quick job")
    r = mgr.submit("echo hello_slurm", mode="slurm", timeout=30, tail=5)
    log.log_result("submit slurm echo", r)
    assert r.status == JobStatus.COMPLETED
    assert r.exit_code == 0
    assert "hello_slurm" in r.stdout_tail
    return r


async def s_slurm_nonzero(log: SessionLog, mgr: JobManager) -> None:
    log.subheading("Slurm non-zero exit")
    r = mgr.submit("exit 42", mode="slurm", timeout=30)
    log.log_result("submit slurm exit 42", r)
    assert r.status == JobStatus.FAILED
    assert r.exit_code == 42


async def s_slurm_detach(log: SessionLog, mgr: JobManager) -> JobResult:
    log.subheading("Slurm timeout + detach")
    r = mgr.submit(_SEQ_CMD, mode="slurm", timeout=5, tail=3)
    log.log_result("submit slurm seq (detach)", r)
    assert r.status == JobStatus.RUNNING, f"expected RUNNING, got {r.status}"
    return r


async def s_slurm_poll(log: SessionLog, mgr: JobManager, running: JobResult) -> None:
    log.subheading("Slurm poll running")
    r = mgr.poll(running.job_id, timeout=0, tail=3)
    assert r is not None
    log.log_result("poll slurm", r)
    assert r.status in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.COMPLETED)


async def s_slurm_repoll_cached(log: SessionLog, mgr: JobManager, quick: JobResult) -> None:
    log.subheading("Slurm re-poll finished (cached)")
    r = mgr.poll(quick.job_id, timeout=0, tail=3)
    assert r is not None
    log.log_result("poll slurm cached", r)
    assert r.status == JobStatus.COMPLETED


async def s_slurm_cancel(log: SessionLog, mgr: JobManager, running: JobResult) -> None:
    log.subheading("Slurm cancel running")
    ok = mgr.cancel(running.job_id)
    log.ok("cancel slurm", f"ok={ok}")
    assert ok
    r = mgr.poll(running.job_id, timeout=0, tail=3)
    assert r is not None
    log.log_result("poll after slurm cancel", r)
    # Job may have finished between the submit poll and cancel; that's fine
    assert r.status in (JobStatus.CANCELLED, JobStatus.COMPLETED), f"got {r.status}"


async def s_slurm_status_file(log: SessionLog, mgr: JobManager,
                               inst: Instance, quick: JobResult) -> None:
    log.subheading("Slurm status file on compute node")
    sf = inst.workspace_dir / ".ns_hpc_output" / f"{quick.job_id}.status"
    if sf.exists():
        raw = sf.read_text()
        has_pid = '"child-pid"' in raw
        has_exit = '"exit-code"' in raw
        log.ok("status file", f"exists  child-pid={has_pid}  exit-code={has_exit}")
        assert has_pid and has_exit
    else:
        log.fail("status file", f"not found at {sf}")


# ── Recovery from disk (simulate server restart) ──────────────────────────


async def s_recovery_completed(log: SessionLog, inst: Instance, cfg: Config) -> None:
    """Submit quick job via mgr1, create mgr2, verify COMPLETED loaded from disk."""
    log.subheading("Recovery — completed job survives server restart")
    mgr1 = JobManager(inst, cfg)
    r = mgr1.submit("echo recovery_check", timeout=10)
    log.log_result("mgr1 submit echo", r)
    assert r.status == JobStatus.COMPLETED
    job_id = r.job_id

    # Simulate server restart — new JobManager reads from disk
    mgr2 = JobManager(inst, cfg)
    r2 = mgr2.poll(job_id, timeout=0, tail=5)
    assert r2 is not None
    log.log_result("mgr2 poll (after restart)", r2)
    assert r2.status == JobStatus.COMPLETED, f"expected COMPLETED, got {r2.status}"
    assert r2.exit_code == 0
    assert "recovery_check" in r2.stdout_tail


async def s_recovery_running_then_completed(log: SessionLog, inst: Instance, cfg: Config) -> None:
    """Submit seq loop via mgr1, let it finish, create mgr2, verify status file detects completion."""
    log.subheading("Recovery — job finishes while server is down, status file recovery")
    mgr1 = JobManager(inst, cfg)
    r = mgr1.submit(_SEQ_CMD, timeout=3, tail=3)
    log.log_result("mgr1 submit seq (detach)", r)
    assert r.status == JobStatus.RUNNING
    job_id = r.job_id

    # Wait for the seq loop to finish naturally (~20s total, waited 3s already)
    r_final = mgr1.poll(job_id, timeout=30, tail=5)
    assert r_final is not None and r_final.status == JobStatus.COMPLETED
    log.log_result("job finished before restart", r_final)

    # Simulate server restart — new mgr should detect COMPLETED via status file
    mgr2 = JobManager(inst, cfg)
    r2 = mgr2.poll(job_id, timeout=0, tail=5)
    assert r2 is not None
    log.log_result("mgr2 poll (after restart)", r2)
    assert r2.status == JobStatus.COMPLETED, f"expected COMPLETED, got {r2.status}"
    assert r2.exit_code == 0
    assert len(r2.stdout_tail) > 0


async def s_recovery_running_still_alive(log: SessionLog, inst: Instance, cfg: Config) -> None:
    """Job still running when server restarts — /proc liveness check keeps it RUNNING."""
    log.subheading("Recovery — job still running after restart")
    mgr1 = JobManager(inst, cfg)
    r = mgr1.submit(_SEQ_CMD, timeout=2, tail=3)
    log.log_result("mgr1 submit seq (detach)", r)
    assert r.status == JobStatus.RUNNING

    # With exec in shell_cmd, proc.pid is the outer bwrap (comm="bwrap"),
    # so _is_bwrap_alive finds it alive and keeps the job RUNNING.
    mgr2 = JobManager(inst, cfg)
    r2 = mgr2.poll(r.job_id, timeout=0, tail=3)
    assert r2 is not None
    log.log_result("mgr2 poll (still running after restart)", r2)
    assert r2.status == JobStatus.RUNNING, f"expected RUNNING, got {r2.status}"
    r3 = mgr2.poll(r.job_id, timeout=30, tail=5)
    assert r3 is not None
    log.log_result("mgr2 poll (after completion)", r3)
    assert r3.status == JobStatus.COMPLETED
    assert r3.exit_code == 0

    # Cleanup — cancel the original mgr1 job so it doesn't linger
    if r.status == JobStatus.RUNNING:
        mgr1.cancel(r.job_id)


async def s_recovery_cancelled(log: SessionLog, inst: Instance, cfg: Config) -> None:
    """Submit + cancel via mgr1, create mgr2, verify CANCELLED loaded from disk."""
    log.subheading("Recovery — cancelled job survives server restart")
    mgr1 = JobManager(inst, cfg)
    r = mgr1.submit("sleep 30", timeout=2, tail=3)
    assert r.status == JobStatus.RUNNING
    mgr1.cancel(r.job_id)
    log.ok("mgr1 submit + cancel", f"job={r.job_id}")

    # Simulate server restart
    mgr2 = JobManager(inst, cfg)
    r2 = mgr2.poll(r.job_id, timeout=0, tail=3)
    assert r2 is not None
    log.log_result("mgr2 poll cancelled (after restart)", r2)
    assert r2.status == JobStatus.CANCELLED, f"expected CANCELLED, got {r2.status}"


async def s_recovery_list_across_restart(log: SessionLog, inst: Instance, cfg: Config) -> None:
    """Submit two jobs via mgr1, restart, mgr2.list_jobs() sees both."""
    log.subheading("Recovery — list_jobs after restart")
    mgr1 = JobManager(inst, cfg)
    r1 = mgr1.submit("echo job_a", timeout=10)
    r2 = mgr1.submit("echo job_b", timeout=10)
    log.ok("mgr1 submitted two jobs", f"{r1.job_id}, {r2.job_id}")

    mgr2 = JobManager(inst, cfg)
    jobs = mgr2.list_jobs()
    ids = [j["job_id"] for j in jobs]
    assert r1.job_id in ids
    assert r2.job_id in ids
    log.ok("mgr2 list_jobs after restart", json.dumps(jobs, indent=2))


async def s_slurm_recovery_completed(log: SessionLog, inst: Instance, cfg: Config) -> None:
    """Submit slurm job via mgr1, restart, mgr2 loads COMPLETED from disk."""
    log.subheading("Slurm recovery — completed job survives restart")
    mgr1 = JobManager(inst, cfg)
    r = mgr1.submit("echo slurm_recovery", mode="slurm", timeout=30)
    log.log_result("mgr1 submit slurm", r)
    assert r.status == JobStatus.COMPLETED

    mgr2 = JobManager(inst, cfg)
    r2 = mgr2.poll(r.job_id, timeout=0, tail=5)
    assert r2 is not None
    log.log_result("mgr2 poll slurm (after restart)", r2)
    assert r2.status == JobStatus.COMPLETED


async def s_slurm_recovery_running(log: SessionLog, inst: Instance, cfg: Config) -> None:
    """Submit slurm detach via mgr1, restart, mgr2 sees RUNNING and waits for completion."""
    log.subheading("Slurm recovery — running job survives restart, poll waits for completion")
    mgr1 = JobManager(inst, cfg)
    r = mgr1.submit(_SEQ_CMD, mode="slurm", timeout=5, tail=3)
    log.log_result("mgr1 submit slurm seq (detach)", r)
    assert r.status == JobStatus.RUNNING

    # Simulate server restart — mgr2 picks up the running job
    mgr2 = JobManager(inst, cfg)
    # _fixup_stale_jobs runs in __init__, but it only reconciles "local" mode
    # For slurm, the job entry stays "running" and mgr2 can poll sacct
    r2 = mgr2.poll(r.job_id, timeout=30, tail=5)
    assert r2 is not None
    log.log_result("mgr2 poll slurm (after restart)", r2)
    assert r2.status == JobStatus.COMPLETED, f"expected COMPLETED, got {r2.status}"


# ── Orchestrator ────────────────────────────────────────────────────────────


async def run_local(log: SessionLog, inst: Instance, cfg: Config) -> None:
    """All local timing scenarios on one instance."""
    mgr = JobManager(inst, cfg)
    quick = await s_quick_job(log, mgr)
    await s_repoll_finished_cached(log, mgr, quick)
    await s_nonzero_exit(log, mgr)
    await s_timeout_kill(log, mgr)

    # Submit a detach job, poke it, then kill
    running = await s_timeout_detach(log, mgr)
    await s_poll_running(log, mgr, running)
    await s_poll_then_kill(log, mgr, running)

    # Job that finishes between tool calls
    finished = await s_poll_finished_after_submit(log, mgr)

    # Cancel scenarios
    await s_cancel_running(log, mgr)
    await s_cancel_finished(log, mgr, finished)
    await s_list_jobs(log, mgr)

    # Recovery from disk (simulate server restart)
    await s_recovery_completed(log, inst, cfg)
    await s_recovery_running_still_alive(log, inst, cfg)
    await s_recovery_running_then_completed(log, inst, cfg)
    await s_recovery_cancelled(log, inst, cfg)
    await s_recovery_list_across_restart(log, inst, cfg)

    # Final cleanup — make sure no runaway processes
    for jid in list(mgr._procs):
        p = mgr._procs[jid]
        if p and p.poll() is None:
            p.kill()
            p.wait()


async def run_slurm(log: SessionLog, inst: Instance, cfg: Config) -> None:
    """All slurm timing scenarios on one instance."""
    if not check_slurm():
        log.ok("slurm: sbatch not found, skipping")
        return
    mgr = JobManager(inst, cfg)
    quick = await s_slurm_quick(log, mgr)
    await s_slurm_nonzero(log, mgr)
    await s_slurm_status_file(log, mgr, inst, quick)
    await s_slurm_repoll_cached(log, mgr, quick)

    running = await s_slurm_detach(log, mgr)
    await s_slurm_poll(log, mgr, running)
    await s_slurm_cancel(log, mgr, running)

    # Slurm recovery from disk
    await s_slurm_recovery_completed(log, inst, cfg)
    await s_slurm_recovery_running(log, inst, cfg)


async def run_resources(log: SessionLog) -> None:
    """MCP context resource discovery and reading."""
    log.heading("MCP Context Resources")
    async with server_lifespan(mcp):
        resources = await mcp.list_resources()
        assert len(resources) > 0, "no resources registered"
        log.ok(f"resources/list: {len(resources)} resources",
               "\n".join(str(r.uri) for r in resources))
        for r in resources:
            content = (await mcp.read_resource(r.uri)).contents[0].content
            assert len(content) > 10


async def main():
    tmp_dir = tempfile.mkdtemp(prefix="ns-hpc-int-")
    cfg = _test_config(tmp_dir)
    log = SessionLog()

    print(f"ns-hpc integration tests — full session log")
    print(f"Config path:     {os.environ['NS_HPC_CONFIG']}")
    print(f"Instances dir:   {cfg.resolve_instances_dir()}")
    print(f"Slurm available: {check_slurm()}")
    print(f"Workspace mount: {cfg.namespace_defaults.workspace_mount}")
    print(f"Seq command:     {_SEQ_CMD}")
    print()

    await run_resources(log)
    log.heading("Local timing scenarios")
    suffix = int(time.time())
    inst = Instance.create(f"local-{suffix}", cfg)
    await run_local(log, inst, cfg)
    Instance.destroy(f"local-{suffix}", cfg)
    log.ok("instance destroyed")

    log.heading("Slurm timing scenarios")
    slurm_inst = Instance.create(f"slurm-{suffix}", cfg)
    await run_slurm(log, slurm_inst, cfg)
    Instance.destroy(f"slurm-{suffix}", cfg)
    log.ok("instance destroyed")

    fails = log.print_summary()
    print()
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
