# ns-hpc Job Management Design

## Principles

1. Every command execution is a **job** with a unique job_id
2. Jobs are async — `submit_job` may return before the job finishes
3. Output is written directly to disk files, never buffered entirely in memory
4. API responses only return tail lines (like `tail -n N`)
5. Full output is available via the file path
6. Job management and audit logging are **separate concerns**
   - Job manager tracks lifecycle (output files, PIDs, status)
   - Audit log is an independent record; audit is called separately when desired

## MCP Tools

```
submit_job(instance_id, command, mode, timeout, detach, tail)
  Always waits up to `timeout` seconds (or until job finishes if sooner).
  Returns tail of whatever output was produced.
  
  ┌──────────────┬───────────────────────────────────┐
  │ Outcome      │ Behavior                          │
  ├──────────────┼───────────────────────────────────┤
  │ Finishes     │ {status: "completed", exit_code,  │
  │ before       │  stdout_tail, stderr_tail, paths} │
  │ timeout      │                                   │
  ├──────────────┼───────────────────────────────────┤
  │ Timeout +    │ Kill process, return what we have │
  │ detach=false │ {status: "timeout", partial tail} │
  ├──────────────┼───────────────────────────────────┤
  │ Timeout +    │ Keep running in background,       │
  │ detach=true  │ return {status: "running",        │
  │              │  job_id, partial tail}            │
  └──────────────┴───────────────────────────────────┘

poll_job(instance_id, job_id, timeout, detach, tail)
  Same peek semantics: always wait up to `timeout`.
  For detach=true: return "running" if still going.
  For detach=false: kill if still going at timeout.

list_jobs(instance_id)
  → [{job_id, status, command, created_at, mode, ...}]

cancel_job(instance_id, job_id)
  → {job_id, status: "cancelled"}
```

## Separation: Job Manager vs Audit

```
Job Manager (new module: job_manager.py):
  - submit(instance, command, mode, config) -> JobHandle
  - poll(job_id, timeout) -> JobStatus
  - cancel(job_id) -> bool
  - list_jobs(instance_id) -> list[JobHandle]
  - Tracks: PID, output files, status, slurm_job_id
  - Output files live at: {instance.output_dir}/{job_id}.{out,err}

Instance.audit():
  - Independent concern, called when desired (e.g. after job completion)
  - Records: command, exit_code, output file paths, timestamp
  - Not coupled to job lifecycle
```

## CLI

```
ns-hpc instance run <id> [--detach] [--slurm] [--timeout N] [--tail N] -- <command>
ns-hpc instance status <id> <job-id> [--timeout N] [--detach]
ns-hpc instance jobs <id>
ns-hpc instance cancel <id> <job-id>
```

## Output File Layout

```
{instance_dir}/
├── workspace/
├── metadata.json
├── audit.log              # separate audit trail
└── output/
    ├── {job_id}.out        # stdout for this job
    └── {job_id}.err        # stderr for this job
```

Output files are written incrementally — the process writes to them
directly (via file redirects or tee), not buffered in Python memory.
The tail_lines read is done by seeking to the end of the file.
