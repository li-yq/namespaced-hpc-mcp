# Repository Guidelines

## Project Structure

```
src/ns_hpc/          # Python package
  server.py          # FastMCP server — tools, lifespan, proxied MCPs
  cli.py             # Typer CLI entry point
  cli_impl.py        # CLI helpers (doctor, clean)
  config.py          # TOML config — layered merge (defaults → user → env)
  instance.py        # Instance lifecycle — create, archive, audit
  job_manager.py     # Async job submit/poll/cancel (local & Slurm)
  namespace.py       # bwrap argument builder
  proxy.py           # Proxied MCPs inside sandbox
tests/               # pytest suite, one file per source module
config/              # Reference config.toml + context resources
slurm/               # Podman Slurm cluster for integration testing
```

New features go in a new module or extend an existing one. MCP tools stay in `server.py`; CLI commands in `cli.py`.

## Build, Test, and Development Commands

```bash
uv sync                              # Install dependencies
uv run pytest                        # Full test suite (verbose: add -v)
uv run python -m ns_hpc doctor       # System diagnostics
uv run python -m ns_hpc run          # Start MCP server (stdio)
```

**Tests by tier** (see `SLURM_TEST_ENV.md`):

| Tier | Command |
|------|---------|
| Pure unit (no bwrap) | `uv run pytest tests/test_{config,instance,namespace,proxy,server}.py -v` |
| Unit + bwrap | `uv run pytest tests/test_job_manager.py tests/test_bwrap_primitive.py -v` |
| Full Slurm integration | `cd slurm && bash setup.sh && bash test_session.sh` |

## Coding Style

- **Python 3.12+** with `from __future__ import annotations`.
- Lines ~100 chars. No enforced formatter; follow existing codebase patterns.
- **Type hints** required on all public signatures. Use `| None`, not `Optional`.
- **Naming**: `snake_case` for functions/vars/modules; `PascalCase` for classes and Pydantic models.
- **Logging**: `logging.getLogger("ns-hpc")` — never `print()` in library code. Debug-log external commands.

## Testing Guidelines

- **Framework**: `pytest` + `pytest-asyncio` (`asyncio_default_fixture_loop_scope = "function"`).
- **Files**: `tests/test_<module>.py`. **Functions**: `async def test_<behavior>()` or `def test_<behavior>()`.
- Use `monkeypatch` + `tmp_path` for isolated configs (see `_CONFIG_TOML` pattern in `test_job_manager.py`).
- Write tests for new features; run the pure-unit suite before pushing.

## Commit & PR Guidelines

- **Conventional Commits**: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`.
- One logical change per commit. **PRs**: Summarize what changed and why.

## Configuration

Config merges defaults → `~/.config/ns-hpc/config.toml` → `NS_HPC_CONFIG` env var. To add options: (1) add the field to the Pydantic model in `config.py` with a default, (2) add a commented example to `config/config.toml`. `_warn_unknown_keys` handles unrecognized keys automatically.

## Key Architecture

- **Fresh sandbox per command** — no persistent Linux namespaces.
- **Async jobs** — `submit_job` may return before completion; poll with `poll_job`.
- **Output on disk** at `{workspace}/.ns_hpc_output/{job_id}.{out,err}`. API returns tail lines only.
- **Audit log** is host-side only — never exposed to the sandbox.
- **Proxy MCPs** discover tool schemas at startup, lazy-start per instance.
