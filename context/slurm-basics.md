# Slurm Basics for ns-hpc

## Partitions

- `debug` — 10 min limit, interactive testing
- `compute` — 48h limit, production jobs
- `gpu` — 24h limit, GPU nodes

## Common Commands

- `sinfo` — partition/node status
- `squeue -u $USER` — your running jobs
- `scancel <jobid>` — cancel a job
- `sacct -j <jobid> --format=JobID,State,ExitCode` — job history

## sbatch Script Template

```bash
#!/bin/bash
#SBATCH --job-name=my-job
#SBATCH --output=job-%j.out
#SBATCH --error=job-%j.err
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --partition=compute

module load python/3.11
cd /workspace
python my_script.py
```
