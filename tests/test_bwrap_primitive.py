import json
import os
import shutil
import subprocess
import tempfile

BWRAP = shutil.which("bwrap") or "bwrap"

SANDBOX_ARGS = [
    "--ro-bind", "/usr", "/usr",
    "--ro-bind", "/lib", "/lib",
    "--ro-bind", "/lib64", "/lib64",
    "--ro-bind", "/bin", "/bin",
    "--proc", "/proc",
    "--dev", "/dev",
    "--tmpfs", "/tmp",
    "--unshare-all",
    "--share-net",
]


def test_bwrap_json_status_fd():
    """Verify --json-status-fd reports the correct exit code."""
    r_fd, w_fd = os.pipe()
    try:
        args = [
            BWRAP,
            *SANDBOX_ARGS,
            "--json-status-fd",
            str(w_fd),
            "--",
            "sh",
            "-c",
            "exit 42",
        ]
        proc = subprocess.Popen(args, pass_fds=(w_fd,))
        os.close(w_fd)
        proc.communicate()
        raw = os.read(r_fd, 4096)
    finally:
        os.close(r_fd)
        try:
            os.close(w_fd)
        except OSError:
            pass

    # bwrap writes two JSON lines: namespace info then exit status.
    # Parse the last non-empty line.
    lines = [l for l in raw.splitlines() if l.strip()]
    status = json.loads(lines[-1])
    assert status["exit-code"] == 42


def test_bwrap_workspace_isolation():
    """Verify /tmp is isolated — host files are invisible inside the sandbox."""
    marker_fd, marker_path = tempfile.mkstemp(dir="/tmp", prefix="bwrap-marker-")
    os.close(marker_fd)

    try:
        with tempfile.TemporaryDirectory() as workspace:
            args = [
                BWRAP,
                *SANDBOX_ARGS,
                "--ro-bind",
                workspace,
                "/workspace",
                "--",
                "sh",
                "-c",
                f"test -f '{marker_path}' && echo UNSAFE || echo SAFE",
            ]
            result = subprocess.run(
                args, capture_output=True, text=True, timeout=30
            )
            output = result.stdout.strip()
            assert output == "SAFE", f"Expected SAFE, got: {output!r}"
    finally:
        try:
            os.unlink(marker_path)
        except FileNotFoundError:
            pass
