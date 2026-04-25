# Lead Extraction & Identity Resolution Assignment

Minimal Python pipeline for Part 1 of the hiring assignment.

## What This Project Does

- extracts real public business leads for:
  - property managers
  - interior designers
- normalizes contact and location fields
- applies identity-resolution logic instead of naive row deletion
- suppresses repeated outreach to the same reachable contact path
- exports a clean assignment-ready CSV to `data/final/leads.csv`

## Project Structure

- `data/raw/`
  - latest raw extractor outputs
  - timestamped raw snapshots under `data/raw/archive/`
- `data/final/leads.csv`
  - final cleaned export
- `docs/schema.md`
- `docs/identity-model.md`
- `docs/source-selection.md`
- `docs/process-explanation.md`
- `docs/quality-summary.md`
- `src/run_pipeline.py`
  - pipeline entrypoint

## Sources and Niches

Current public directory sources:

- property managers: `hoamanagementcompanies.net`
- interior designers: `interiordesignlink.com`

The pipeline is intentionally narrow. It prefers a small number of stable public pages over broad brittle scraping.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the Pipeline

```bash
python3 src/run_pipeline.py
```

## Output Locations

- raw latest files:
  - `data/raw/property_managers_raw.json`
  - `data/raw/interior_designers_raw.json`
- raw archive snapshots:
  - `data/raw/archive/...`
- final export:
  - `data/final/leads.csv`
- generated review summary:
  - `docs/quality-summary.md`

## How Dedupe and Outreach Suppression Work

This project treats dedupe as identity resolution.

A row can represent:

- a contact or inbox
- a business
- a location
- an outreach target

The pipeline therefore keeps separate grouping fields:

- `contact_group_id`
  - shared normalized email or phone
- `business_group_id`
  - shared website domain or tightly supported repeated business identity
- `location_group_id`
  - conservative same-location grouping with supporting evidence
- `outreach_suppression_key`
  - priority order:
    1. normalized email
    2. normalized phone
    3. website domain
    4. normalized business-name signature + city

The point is simple: preserve useful business/location rows without repeatedly contacting the same reachable target.

## Known Limitations

- Source coverage is only as good as the chosen public directory pages.
- Public directories can contain stale listings or weak contact details.
- Only a small number of rows expose public email addresses.
- Address normalization is practical, not postal-grade.
- Generic inboxes such as `info@` are still usable for suppression, but they are not perfect person-level identity proof.
- This is a batch assignment pipeline, not an ongoing crawler or CRM.

## Production Improvements

If this were being turned into a real production system, the next sensible steps would be:

- source monitoring and retry/reporting
- stronger address/entity normalization
- richer source diversity with source-specific parsers
- explicit review queues for ambiguous duplicates
- persistent run metadata and audit tables
- tested enrichment layers for websites, categories, and contact discovery

What it does not need for this assignment:

- FastAPI
- Celery
- Postgres
- Redis
- dashboards
- auth

That would be architecture theater for a five-day exercise.
