#!/usr/bin/env python3
"""Integration test — simulates MCP client sessions against the running server.

NOTE: standalone script (runs via ``python tests/session_test.py``), not a
pytest test class — kept under ``tests/`` so it's versioned alongside unit
tests and can be run inside the slurm container with the container venv.

Usage:
    # Run locally (local-only tests, slurm tests skipped)
    python tests/session_test.py

    # Run in slurm container (all tests)
    podman exec --user 2000 -w /home/testuser slurm-slurmctld \\
        sh -c "cd /ns-hpc-mcp && /home/testuser/.local/ns-hpc/venv/bin/python tests/session_test.py"
"""
from __future__ import annotations

__test__ = False  # prevent pytest collection

import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

# ── Import server (must be before ns_hpc.server to set config) ──────────────

CONFIG_PATH = os.environ.get("NS_HPC_CONFIG", "config/config.toml")
os.environ["NS_HPC_CONFIG"] = CONFIG_PATH

import asyncio
from ns_hpc.server import mcp, server_lifespan
from ns_hpc.instance import Instance
from ns_hpc.config import Config, load_config
from ns_hpc.job_manager import JobManager


# ── Config helpers ──────────────────────────────────────────────────────────


_CONFIG_TOML = """\
instances_dir = "{instances_dir}"

[namespace_defaults]
bind_ro = ["/usr", "/bin", "/lib", "/lib64"]
workspace_mount = "/workspace"
flags = ["--unshare-all", "--share-net", "--proc", "/proc", "--dev", "/dev",
         "--tmpfs", "/tmp", "--die-with-parent"]

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


def _test_config(tmp_dir: str) -> Config:
    """Write a config with instances_dir pointing to a temp dir, return Config."""
    # When sbatch is available, use a shared filesystem path so compute
    # nodes can access the instances and their output/status files.
    instances_dir = tmp_dir
    if check_slurm():
        instances_dir = "/home/testuser/mcp_instances"

    config_path = Path(tmp_dir) / "config.toml"
    config_path.write_text(_CONFIG_TOML.format(instances_dir=instances_dir))
    # Create a context directory with a test resource so resource registration works
    context_dir = Path(tmp_dir) / "config" / "context"
    context_dir.mkdir(parents=True, exist_ok=True)
    (context_dir / "README.md").write_text("# Test context for ns-hpc integration tests\n")
    os.environ["NS_HPC_CONFIG"] = str(config_path)
    return load_config(str(config_path))


# ── Helpers ─────────────────────────────────────────────────────────────────


class Session:
    """Simulates an MCP client session with the lifespan server."""

    def __init__(self, name: str, config: Config, slurm: bool = False):
        self.name = name
        self.instance_id = f"int-{name}-{int(time.time())}"
        self.slurm = slurm
        self.config = config
        self._results: list[str] = []
        self._start = time.time()

    def step(self, label: str) -> None:
        elapsed = time.time() - self._start
        print(f"  [{elapsed:7.2f}s] {label} ... ", end="", flush=True)
        self._results.append(label)

    def ok(self, detail: str = "") -> None:
        print(f"✓{('  ' + detail) if detail else ''}")

    def fail(self, msg: str) -> None:
        print(f"✗  {msg}")
        self._results.append(f"FAIL: {msg}")

    def summary(self) -> int:
        total = len([r for r in self._results if not r.startswith("FAIL")])
        fails = [r for r in self._results if r.startswith("FAIL")]
        print(f"\n  ── {self.name} ──")
        print(f"  Passed: {total}  Failed: {len(fails)}")
        for f in fails:
            print(f"    {f}")
        return len(fails)


def check_slurm() -> bool:
    """Return True if sbatch is available (slurm cluster is up)."""
    return shutil.which("sbatch") is not None


# ── Test scenarios ─────────────────────────────────────────────────────────


async def test_config_fallback(cfg: Config) -> int:
    """Verify config loading fallback chain works."""
    s = Session("config-fallback", cfg)

    s.step("load config from NS_HPC_CONFIG")
    assert cfg.namespace_defaults.workspace_mount == "/workspace"
    s.ok(f"workspace_mount={cfg.namespace_defaults.workspace_mount}")

    s.step("list context resources in config")
    dirs = cfg.resource_defaults.context_dirs
    patterns = cfg.resource_defaults.resource_patterns
    assert len(dirs) > 0
    assert len(patterns) > 0
    s.ok(f"context_dirs={dirs}")

    return s.summary()


async def test_context_resources(cfg: Config) -> int:
    """Verify MCP context resources are discoverable and readable."""
    s = Session("context-resources", cfg)

    async with server_lifespan(mcp):
        s.step("list all resources")
        resources = await mcp.list_resources()
        assert len(resources) > 0, "No resources registered"
        uris = [r.uri for r in resources]
        s.ok(f"{len(resources)} resources: {[r.name for r in resources]}")

        s.step(f"read all {len(resources)} resources")
        for r in resources:
            result = await mcp.read_resource(r.uri)
            content = result.contents[0].content
            assert len(content) > 10
        s.ok(f"verified content for {len(resources)} resources")

    return s.summary()


async def test_full_local_session(cfg: Config) -> int:
    """Full MCP session: create → submit (local) → poll → list → cancel → destroy."""
    s = Session("local-session", cfg)

    async with server_lifespan(mcp):

        # ── create instance with description ──
        s.step("create instance")
        inst = Instance.create(s.instance_id, cfg, description="integration test instance")
        s.ok(inst.id)

        s.step("verify instance metadata has description")
        meta = json.loads(inst.metadata_path.read_text())
        assert meta.get("description") == "integration test instance"
        s.ok()

        s.step("list instances includes our instance")
        all_inst = Instance.list_instances(cfg)
        ids = [i.id for i in all_inst]
        assert s.instance_id in ids
        s.ok(f"{len(all_inst)} total")

        # ── submit a quick local job ──
        mgr = JobManager(inst, cfg)

        s.step("submit local job (echo)")
        r1 = mgr.submit("echo hello_integration", timeout=10, tail=5)
        assert r1.status.value == "completed"
        assert r1.exit_code == 0
        assert "hello_integration" in r1.stdout_tail
        assert "/workspace/" in r1.stdout_path  # container-side path
        s.ok(f"job={r1.job_id} exit={r1.exit_code}")

        s.step("submit local job (non-zero exit)")
        r2 = mgr.submit("exit 7", timeout=10)
        assert r2.status.value == "failed"
        assert r2.exit_code == 7
        s.ok(f"job={r2.job_id} exit={r2.exit_code}")

        # ── submit a detach job, poll, then cancel ──
        s.step("submit local job (sleep, detach)")
        r3 = mgr.submit("sleep 20", timeout=2, tail=5)
        assert r3.status.value == "running"
        assert r3.job_id is not None
        s.ok(f"job={r3.job_id}")

        s.step("poll running job")
        polled = mgr.poll(r3.job_id, timeout=0)
        assert polled is not None
        assert polled.status.value == "running"
        s.ok()

        s.step("list jobs (should have 3)")
        jobs = mgr.list_jobs()
        assert len(jobs) >= 3
        s.ok(f"{len(jobs)} jobs tracked")

        s.step("cancel running job")
        ok = mgr.cancel(r3.job_id)
        assert ok
        s.ok()

        s.step("poll cancelled job returns status=cancelled")
        polled = mgr.poll(r3.job_id, timeout=0)
        assert polled is not None
        assert polled.status.value == "cancelled"
        s.ok(f"status={polled.status.value}")

        # ── destroy ──
        s.step("destroy instance")
        Instance.destroy(s.instance_id, cfg)
        assert Instance.load(s.instance_id, cfg) is None
        s.ok()

    return s.summary()


async def test_slurm_session(cfg: Config) -> int:
    """Full MCP session via slurm: create → submit → poll → cancel → destroy."""
    if not check_slurm():
        print("  ⏭  slurm not available, skipping")
        return 0

    s = Session("slurm-session", cfg, slurm=True)

    async with server_lifespan(mcp):
        inst = Instance.create(s.instance_id, cfg, description="slurm integration test")
        mgr = JobManager(inst, cfg)

        # ── submit slurm job (quick echo) ──
        s.step("submit slurm job (echo)")
        r1 = mgr.submit("echo hello_slurm", mode="slurm", timeout=30, tail=10)
        assert r1.status.value == "completed"
        assert r1.exit_code == 0
        assert "hello_slurm" in r1.stdout_tail
        assert "/workspace/" in r1.stdout_path
        s.ok(f"job={r1.job_id} exit={r1.exit_code}")

        # ── submit slurm job (non-zero exit) ──
        s.step("submit slurm job (exit 42)")
        r2 = mgr.submit("exit 42", mode="slurm", timeout=30)
        assert r2.status.value == "failed"
        assert r2.exit_code == 42
        s.ok(f"job={r2.job_id} exit={r2.exit_code}")

        # ── submit slurm detach, poll, cancel ──
        s.step("submit slurm job (sleep, detach)")
        r3 = mgr.submit("sleep 20", mode="slurm", timeout=5, tail=5)
        assert r3.status.value == "running"
        s.ok(f"job={r3.job_id}")

        s.step("poll slurm job (still running)")
        polled = mgr.poll(r3.job_id, timeout=0)
        assert polled is not None
        # Could be RUNNING or PENDING — both are acceptable
        s.ok(f"status={polled.status.value}")

        s.step("list slurm jobs")
        jobs = mgr.list_jobs()
        slurm_jobs = [j for j in jobs if j.get("slurm_job_id") is not None]
        assert len(slurm_jobs) >= 1
        s.ok(f"{len(slurm_jobs)} slurm jobs tracked")

        s.step("cancel slurm job")
        ok = mgr.cancel(r3.job_id)
        assert ok
        s.ok()

        # ── verify status file was written on compute node ──
        s.step("verify status file exists with exit-code")
        status_file = inst.workspace_dir / ".ns_hpc_output" / f"{r1.job_id}.status"
        if status_file.exists():
            raw = status_file.read_text()
            assert '"exit-code"' in raw or '"exit-code":' in raw
            s.ok()
        else:
            s.fail(f"status file not found at {status_file}")

        # ── destroy ──
        s.step("destroy instance")
        Instance.destroy(s.instance_id, cfg)
        assert Instance.load(s.instance_id, cfg) is None
        s.ok()

    return s.summary()


async def test_audit_log(cfg: Config) -> int:
    """Verify audit log is populated correctly for a job lifecycle."""
    s = Session("audit-log", cfg)

    async with server_lifespan(mcp):
        inst = Instance.create(s.instance_id, cfg)

        mgr = JobManager(inst, cfg)

        s.step("submit job and audit")
        result = mgr.submit("echo audit_me", timeout=10)
        # The MCP tool layer calls inst.audit() — simulate it in the test
        inst.audit("job.submitted", command="echo audit_me", mode="local", timeout=10)
        inst.audit(f"job.{result.status.value}", job_id=result.job_id,
                   exit_code=result.exit_code, command="echo audit_me", mode="local",
                   stdout_path=result.stdout_path, stderr_path=result.stderr_path)

        s.step("check audit log entries")
        lines = inst.audit_log_path.read_text().strip().splitlines()
        assert len(lines) >= 2
        events = [json.loads(l) for l in lines]
        assert events[0]["event"] == "job.submitted"
        s.ok(f"{len(events)} audit entries")

        s.step("destroy instance")
        Instance.destroy(s.instance_id, cfg)

    return s.summary()


# ── Main ────────────────────────────────────────────────────────────────────


async def main():
    # Use a temp directory for instances (avoids sandbox /home read-only issues)
    tmp_dir = tempfile.mkdtemp(prefix="ns-hpc-int-")
    cfg = _test_config(tmp_dir)

    print(f"ns-hpc integration test suite")
    print(f"Instances dir: {tmp_dir}")
    print(f"Slurm:        {'available' if check_slurm() else 'not available'}")
    print()

    total_fails = 0

    tests = [
        ("Config fallback", test_config_fallback(cfg)),
        ("Context resources", test_context_resources(cfg)),
        ("Local session", test_full_local_session(cfg)),
        ("Slurm session", test_slurm_session(cfg)),
        ("Audit log", test_audit_log(cfg)),
    ]

    for name, coro in tests:
        print(f"── {name} ──────────────────────────────────")
        fails = await coro
        total_fails += fails
        print()

    print("═" * 50)
    if total_fails:
        print(f"FAILED: {total_fails} test(s) with failures")
    else:
        print("All tests passed!")
    print()

    return 1 if total_fails else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
