"""Tests for the MCP server module."""

import json
from types import SimpleNamespace

import pytest

from ns_hpc.config import Config
from ns_hpc.instance import Instance
from ns_hpc.config import HostCommand
from ns_hpc.server import CreateInstanceInput, HostExecInput, ListInstancesInput, ListJobsInput, host_exec, mcp
from ns_hpc.server import _tool_result, create_instance, list_instances, list_jobs
from fastmcp.exceptions import ToolError


def _config(tmp_dir: str, result_type: str = "text") -> Config:
    return Config(
        namespace={
            "instances_dir": tmp_dir,
            "bwrap_command": ["bwrap"],
        },
        jobs={
            "local": {
                "use_cgroups": False,
                "cgroups_command": [],
            },
            "slurm": {
                "sbatch_command": ["sbatch"],
                "limit": {},
            },
        },
        proxied_mcps={},
        mcp={"result_type": result_type},
    )


def _ctx(config: Config) -> SimpleNamespace:
    return SimpleNamespace(lifespan_context=SimpleNamespace(config=config, job_managers={}))


def _content_text(result) -> str:
    return "\n".join(c.text for c in result.content)


@pytest.mark.asyncio
async def test_server_imports():
    """Verify the server module loads and has tools registered."""
    assert mcp is not None
    tools = await mcp.list_tools()
    tool_names = {t.name for t in tools}
    assert "submit_job" in tool_names, f"Expected submit_job in {tool_names}"
    assert "poll_job" in tool_names, f"Expected poll_job in {tool_names}"
    assert "list_jobs" in tool_names, f"Expected list_jobs in {tool_names}"
    assert "cancel_job" in tool_names, f"Expected cancel_job in {tool_names}"
    assert "run_command" not in tool_names, "run_command should be replaced by submit_job"


def test_tool_result_text_mode(tmp_path):
    cfg = _config(str(tmp_path), "text")
    result = _tool_result(cfg, "summary", {"value": 1})
    assert _content_text(result) == "summary"
    assert result.structured_content is None


def test_tool_result_structured_mode(tmp_path):
    cfg = _config(str(tmp_path), "structured")
    result = _tool_result(cfg, "summary", {"value": 1})
    assert result.content == []
    assert result.structured_content == {"value": 1}


def test_tool_result_both_mode(tmp_path):
    cfg = _config(str(tmp_path), "both")
    result = _tool_result(cfg, "summary", {"value": 1})
    assert _content_text(result) == "summary"
    assert result.structured_content == {"value": 1}


@pytest.mark.asyncio
async def test_create_instance_returns_configured_structured_result(tmp_path):
    cfg = _config(str(tmp_path), "both")
    result = await create_instance(
        CreateInstanceInput(instance_id="server-test", description="demo"),
        _ctx(cfg),
    )

    assert _content_text(result) == "Instance 'server-test' created."
    assert result.structured_content == {
        "instance_id": "server-test",
        "created": True,
        "description": "demo",
    }


@pytest.mark.asyncio
async def test_list_instances_empty_returns_structured_empty_collection(tmp_path):
    cfg = _config(str(tmp_path), "both")
    result = await list_instances(ListInstancesInput(), _ctx(cfg))

    assert _content_text(result) == "No instances found."
    assert result.structured_content == {"total": 0, "instances": []}


@pytest.mark.asyncio
async def test_list_instances_text_mode_suppresses_structured_content(tmp_path):
    cfg = _config(str(tmp_path), "text")
    Instance.create("text-only", cfg, "demo")

    result = await list_instances(ListInstancesInput(), _ctx(cfg))

    assert "text-only" in _content_text(result)
    assert result.structured_content is None


@pytest.mark.asyncio
async def test_list_jobs_empty_returns_structured_empty_collection(tmp_path):
    cfg = _config(str(tmp_path), "both")
    Instance.create("jobs-empty", cfg)

    result = await list_jobs(ListJobsInput(instance_id="jobs-empty"), _ctx(cfg))

    assert _content_text(result) == "No jobs found for this instance."
    assert result.structured_content == {"total": 0, "jobs": []}


@pytest.mark.asyncio
async def test_list_jobs_text_includes_job_entries(tmp_path):
    cfg = _config(str(tmp_path), "text")
    inst = Instance.create("jobs-text", cfg)
    jobs_dir = inst.base_dir / ".ns_hpc_jobs"
    jobs_dir.mkdir()
    (jobs_dir / "abc123.state").write_text(json.dumps({
        "job_id": "abc123",
        "status": "completed",
        "created_at": "2026-06-12T00:00:00+00:00",
        "command": "echo hello",
        "mode": "local",
    }))

    result = await list_jobs(ListJobsInput(instance_id="jobs-text"), _ctx(cfg))

    assert _content_text(result) == (
        "Jobs 1-1 of 1 (limit=15, offset=0)\n"
        "abc123: completed [local] created: 2026-06-12T00:00:00+00:00 — echo hello"
    )
    assert result.structured_content is None


@pytest.mark.asyncio
async def test_host_exec_list_empty(tmp_path):
    """host_exec with no arg returns empty when no commands configured."""
    cfg = _config(str(tmp_path), "both")
    result = await host_exec(HostExecInput(command=None), _ctx(cfg))
    assert "No host commands configured" in _content_text(result)
    assert result.structured_content == {"commands": {}}


@pytest.mark.asyncio
async def test_host_exec_list_with_commands(tmp_path):
    """host_exec with no arg lists configured commands."""
    cfg = _config(str(tmp_path), "both")
    cfg.host_commands = {
        "df": HostCommand(command="df -h", description="Disk usage", timeout=10),
        "quota": HostCommand(command="lfs quota /data"),
    }
    result = await host_exec(HostExecInput(command=None), _ctx(cfg))
    text = _content_text(result)
    assert "df" in text
    assert "Disk usage" in text
    assert "quota" in text
    assert result.structured_content["commands"]["df"]["description"] == "Disk usage"
    assert result.structured_content["commands"]["df"]["command"] == "df -h"
    assert result.structured_content["commands"]["quota"]["description"] == ""


@pytest.mark.asyncio
async def test_host_exec_runs_command(tmp_path):
    """host_exec with a key runs the command and returns stdout/exit_code."""
    cfg = _config(str(tmp_path), "both")
    cfg.host_commands = {
        "greet": HostCommand(command="echo hello", timeout=10),
    }
    result = await host_exec(HostExecInput(command="greet"), _ctx(cfg))
    text = _content_text(result)
    assert "host:greet" in text
    assert "exit=0" in text
    assert "hello" in text
    sc = result.structured_content
    assert sc["exit_code"] == 0
    assert sc["stdout"] == "hello"
    assert sc["stderr"] == ""
    assert sc["elapsed"] >= 0


@pytest.mark.asyncio
async def test_host_exec_unknown_key(tmp_path):
    """host_exec with unknown key raises ToolError listing available commands."""
    cfg = _config(str(tmp_path), "text")
    cfg.host_commands = {
        "df": HostCommand(command="df -h"),
    }
    with pytest.raises(ToolError, match="Unknown host command"):
        await host_exec(HostExecInput(command="nonexistent"), _ctx(cfg))


@pytest.mark.asyncio
async def test_host_exec_stderr_captured(tmp_path):
    """stderr from host commands is captured in the result."""
    cfg = _config(str(tmp_path), "both")
    cfg.host_commands = {
        "err": HostCommand(command="echo ok; echo bad >&2", timeout=10),
    }
    result = await host_exec(HostExecInput(command="err"), _ctx(cfg))
    sc = result.structured_content
    assert sc["stdout"] == "ok"
    assert sc["stderr"] == "bad"
    assert sc["exit_code"] == 0

