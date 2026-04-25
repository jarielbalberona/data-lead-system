# Lead Extraction & Identity Resolution Assignment

Minimal Python pipeline for Part 1 of the hiring assignment.

## Current Status

- project skeleton is in place
- schema and identity/source documentation are in `docs/`
- pipeline entrypoint is `src/run_pipeline.py`

## Planned Outputs

- `data/raw/` for extracted source records
- `data/processed/` for normalized and dedupe-ready records
- `data/final/leads.csv` for final accepted lead rows

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 src/run_pipeline.py
```

The pipeline is currently scaffolded and will be expanded issue by issue.
