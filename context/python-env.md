# Python Environment on HPC

Available via modules:
- `module load python/3.11`
- `module load cuda/12.4` (for GPU nodes)

Use `uv` for package management (pre-installed in workspace).
For heavy dependencies, use `uv sync` with the project's pyproject.toml.

## Common Setup

```bash
module load python/3.11
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Or with uv:
```bash
module load python/3.11
uv sync
```

## GPU Jobs

```bash
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
module load cuda/12.4
uv sync
python train.py --device cuda
```
