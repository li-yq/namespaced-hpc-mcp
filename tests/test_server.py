"""Tests for the MCP server module."""

from types import SimpleNamespace

import pytest

from ns_hpc.config import Config
from ns_hpc.instance import Instance
from ns_hpc.server import CreateInstanceInput, ListInstancesInput, ListJobsInput, mcp
from ns_hpc.server import _tool_result, create_instance, list_instances, list_jobs


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
