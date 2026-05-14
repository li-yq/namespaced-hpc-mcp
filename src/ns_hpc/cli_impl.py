import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ns_hpc.config import load_config
from ns_hpc.instance import Instance


def _check(label: str, ok: bool) -> None:
    mark = "✓" if ok else "✗"
    print(f"  {mark} {label}")


def run_doctor() -> None:
    """Run system diagnostics and print results."""
    print("ns-hpc doctor")
    print()

    all_ok = True

    # 1. Check bwrap exists
    bwrap_path = shutil.which("bwrap")
    bwrap_ok = bwrap_path is not None
    _check("bwrap found", bwrap_ok)
    if not bwrap_ok:
        all_ok = False

    # 2. Check unshare -r works
    try:
        r = subprocess.run(
            ["unshare", "-r", "--", "true"],
            capture_output=True,
            timeout=10,
        )
        unshare_ok = r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        unshare_ok = False
    _check("unshare -r works", unshare_ok)
    if not unshare_ok:
        all_ok = False

    # 3. Check max_user_namespaces > 0
    val = None
    try:
        val = int(Path("/proc/sys/user/max_user_namespaces").read_text().strip())
        ns_ok = val > 0
    except (FileNotFoundError, ValueError):
        ns_ok = False
    _check(f"max_user_namespaces ({val if val is not None else '?'}) > 0", ns_ok)
    if not ns_ok:
        all_ok = False

    # 4. Check /etc/subuid has current user
    user = os.environ.get("USER", "")
    try:
        content = Path("/etc/subuid").read_text()
        subuid_ok = bool(user) and user in content
    except FileNotFoundError:
        subuid_ok = False
    _check(f"/etc/subuid contains user '{user}'", subuid_ok)
    if not subuid_ok:
        all_ok = False

    # 5. Check configured bind paths exist
    try:
        cfg = load_config()
        bind_paths = cfg.namespace_defaults.bind_ro
        all_bind_ok = True
        for p in bind_paths:
            exists = Path(p).exists()
            if not exists:
                all_bind_ok = False
            _check(f"  bind path '{p}' exists", exists)
        if not all_bind_ok:
            all_ok = False

        instances_dir = cfg.resolve_instances_dir()
        dir_ok = instances_dir.exists() or instances_dir.parent.exists()
        _check(f"instances dir '{instances_dir}' accessible", dir_ok)
        if not dir_ok:
            all_ok = False
    except Exception:
        pass

    # 6. Check Slurm binaries
    for bin_name in ["sbatch", "squeue", "sacct", "scancel"]:
        found = shutil.which(bin_name) is not None
        _check(f"{bin_name} found", found)
        if not found:
            all_ok = False

    # 7. Check /tmp is writable
    tmp_ok = os.access("/tmp", os.W_OK)
    _check("/tmp writable", tmp_ok)
    if not tmp_ok:
        all_ok = False

    # 8. Smoke test bwrap --json-status-fd
    r_fd, w_fd = os.pipe()
    try:
        proc = subprocess.Popen(
            [
                "bwrap",
                "--ro-bind", "/usr", "/usr",
                "--ro-bind", "/bin", "/bin",
                "--ro-bind", "/lib", "/lib",
                "--ro-bind", "/lib64", "/lib64",
                "--proc", "/proc",
                "--dev", "/dev",
                "--tmpfs", "/tmp",
                "--unshare-all",
                "--share-net",
                "--json-status-fd", str(w_fd),
                "--",
                "/bin/sh", "-c", "exit 0",
            ],
            pass_fds=(w_fd,),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        os.close(w_fd)
        proc.communicate(timeout=10)
        raw = os.read(r_fd, 4096)
        lines = [l for l in raw.splitlines() if l.strip()]
        status = json.loads(lines[-1])
        smoke_ok = status.get("exit-code") == 0
    except Exception:
        smoke_ok = False
    finally:
        for fd in (r_fd, w_fd):
            try:
                os.close(fd)
            except OSError:
                pass
    _check("bwrap smoke test passes", smoke_ok)
    if not smoke_ok:
        all_ok = False

    print()
    if all_ok:
        print("All checks passed.")
    else:
        print("Some checks failed. See above for details.")
        sys.exit(1)


def clean_instances(days: int, force: bool) -> None:
    """Remove instances older than N days."""
    cfg = load_config()
    instances = Instance.list_instances(cfg)

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    stale: list[tuple[Instance, datetime | None]] = []

    for inst in instances:
        try:
            meta = json.loads(inst.metadata_path.read_text())
            created = datetime.fromisoformat(meta["created_at"])
            if created < cutoff:
                stale.append((inst, created))
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            stale.append((inst, None))

    if not stale:
        print(f"No instances older than {days} days found.")
        return

    print(f"Found {len(stale)} stale instance(s) older than {days} days:")
    for inst, created in stale:
        age = f"(created: {created})" if created else "(metadata missing)"
        print(f"  {inst.id} {age}")

    if not force:
        confirm = input("Archive these instances? [y/N] ")
        if confirm.lower() not in ("y", "yes"):
            print("Aborted.")
            return

    for inst, _ in stale:
        try:
            inst.archive(cfg)
        except RuntimeError as e:
            print(f"  Skipped {inst.id}: {e}")
            continue
        print(f"  Archived {inst.id}")
