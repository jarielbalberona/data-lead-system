# Lead Enrichment & Outreach-Ready Export

Python-first lead pipeline with a thin Flask showcase UI layered on top of the existing extraction and enrichment runtime.

## What This Project Does

The canonical flow is:

```txt
user input
-> directory/source discovery
-> candidate listing URL collection
-> listing-page classification
-> accepted listing pages
-> extracted candidate lead rows
-> website enrichment and validation
-> identity-aware grouping
-> representative-row selection
-> dual exports
-> saved run artifacts
```

This is still assignment-grade software. The UI is a demo shell, not a platform rewrite.

## Why Directory Discovery Still Comes First

The runtime does not replace directory discovery with website-only extraction.

- directory/category/search pages are better for finding candidate businesses
- official business websites are better for enrichment and contact validation
- outreach-ready export should choose the best row per outreach target after enrichment, not the first row that survived

## Current Scope

Supported niches:

- property managers
- interior designers

Supported **place** options in the showcase (state-level targets):

- **New York** — HOA: state + optional NYC-metro subpages from the directory. Interior (theidslist.com): Mid-Atlantic regional listing (not NY-exclusive).
- **California** — HOA: California state directory. Interior: West Coast regional listing (not California-exclusive).
- **Pennsylvania** — HOA: Pennsylvania state directory. Interior: same Mid-Atlantic regional listing as New York (not Pennsylvania-exclusive).

**Honesty:** Sources are mixed: some are full state pages (HOA), others are **regional** directories on the designer site. The goal is usable leads and clear labeling, not implied city- or state-line precision the sources do not provide.

## Sources

Current discovery/extraction sources in the runtime:

- property managers:
  - discovery and extraction path through `hoamanagementcompanies.net`
- interior designers:
  - discovery and extraction path through `theidslist.com`

Allowed source policy is documented in:

- `docs/discovery-strategy.md`

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the Showcase UI

```bash
python3 app.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000). The home page lets you choose a niche and place, submit a real run, and view resulting artifacts in-browser.

Optional: write run outputs to a custom base directory (same layout as on Render: `{DATA_ROOT}/final/...`):

```bash
DATA_ROOT=/tmp/lead-data python3 app.py
```

## Deploy on Render (Blueprint)

This repo includes a [`render.yaml`](render.yaml) Blueprint for a single **Python web service** with a **persistent disk**. Render only attaches disks to **paid** instance types, so use at least a starter (or higher) plan when creating the service.

1. In the [Render Dashboard](https://dashboard.render.com), create a **New Blueprint** and connect this repository.
2. Render reads `render.yaml` from the repo root and provisions the `lead-system-demo` web service.
3. **Build:** `pip install -r requirements.txt`  
4. **Start:** `gunicorn app:app --bind 0.0.0.0:$PORT`  
5. **Health check:** `GET /healthz` (plain 200)  
6. **Disk:** mounted at `/var/data`; **`DATA_ROOT=/var/data`** so run artifacts survive redeploys and restarts.

**Where outputs live on Render:** under `/var/data/final/{niche_slug}/{place_slug}/{run_id}/` (for example `/var/data/final/property-managers/new-york/2026-04-27t173500z/`).

**Local vs production:** locally, if `DATA_ROOT` is unset, the app uses the repo’s `data/` directory (`data/final/...`) as today. On Render, `DATA_ROOT` points at the attached disk. Shared pipeline paths under `data/raw` and `data/processed` in code also follow `DATA_ROOT` when set, so intermediate files on Render stay on the disk, not the ephemeral filesystem.

**Limitations (unchanged):** the browser blocks on `/run` until the pipeline finishes; there is no job queue, worker, or request timeout offloading. Very long runs may hit HTTP proxy timeouts—addressing that would require async jobs or a worker (out of scope for this demo).

## Run the Pipeline Directly

```bash
python3 src/run_pipeline.py --niche "property managers" --place "New York"
```

Optional arguments:

- `--run-id`
- `--output-dir`

## Run Output Layout

Every run is isolated under:

```txt
{DATA_ROOT or repo data}/final/{niche_slug}/{place_slug}/{run_id}/
```

Locally (default), `{DATA_ROOT}` is the `data/` folder next to the repo, so paths look like `data/final/...`. With `DATA_ROOT` set, the same structure is rooted there (for example `DATA_ROOT=/var/data` on Render → `/var/data/final/...`).

Example:

```txt
data/final/property-managers/new-york/2026-04-27t173500z/
```

Each run folder contains at least:

- `leads_master.csv`
- `leads_outreach_ready.csv`
- `run_metadata.json`
- `quality_summary.md`
- `raw/`
- `processed/`

This replaces the old shared final-output model. The showcase UI depends on per-run isolation.

## Route Structure

Primary routes:

- `/` — home
- `/healthz` — load balancer health (200, plain `ok` body)
- `/runs` — list runs
- `/results/{niche}/{place}/{run_id}` — run overview
- `/results/{niche}/{place}/{run_id}/master` — master CSV preview
- `/results/{niche}/{place}/{run_id}/outreach-ready` — outreach preview
- `/download/{niche}/{place}/{run_id}/{filename}` — download artifact

## Master vs Outreach-Ready

Master dataset:

- preserves all useful rows that survived validation and conservative dedupe
- keeps listing-derived and website-derived evidence
- keeps lineage and traceability fields

Outreach-ready dataset:

- emits one best representative row per outreach/contact group
- uses transparent ranking instead of “first surviving row”
- keeps only outreach-usable rows
- is the simplified final human-facing lead list, not the internal system export
- exposes only final human-usable fields:
  - `business_name`
  - `phone`
  - `email`
  - `website`
  - `address`
  - `city`
  - `state`
  - `source_url`

In other words:

- `leads_master.csv` = internal/full pipeline truth with traceability and evidence
- `leads_outreach_ready.csv` = simplified final list for human review and marketing/outreach use

## Enrichment Model

The pipeline preserves both listing-derived and website-derived evidence.

Examples:

- `listing_email`
- `listing_phone`
- `website_email`
- `website_phone`
- `preferred_email`
- `preferred_phone`
- `website_validation_status`

Website enrichment probes a constrained set of same-site pages:

- `/`
- `/contact`
- `/contact-us`
- `/about`
- `/team`

It extracts:

- visible emails
- `mailto:` emails
- visible phones
- `tel:` links

It also records:

- source URL
- extraction method
- confidence
- validation outcome

## Identity Resolution

This project treats dedupe as identity resolution, not row deletion.

A row may represent:

- a contact/inbox
- a business
- a location
- an outreach target

The pipeline therefore keeps:

- `contact_group_id`
- `business_group_id`
- `location_group_id`
- `outreach_suppression_key`

Post-enrichment guardrails:

- generic inboxes such as `info@` stay outreach-eligible
- generic inbox alone is still weak identity evidence
- generic email does not create a global merge across unrelated domains

## Representative-Row Selection

The outreach-ready export does not keep the first surviving row.

It ranks rows within each outreach group using:

1. website-derived email
2. website-derived phone
3. both preferred email and phone
4. email confidence
5. phone confidence
6. source priority
7. location completeness
8. quality score
9. niche relevance
10. stable `lead_id` tie-break

Selection evidence is carried with:

- `representative_group_key`
- `representative_rank_reason`

## Outreach Readiness

The outreach-ready export includes:

- `ready_for_email`
- `ready_for_phone`
- `ready_for_outreach`
- `outreach_block_reason`

This keeps blocked rows explainable instead of silently dropping them.

## Known Limitations

- The UI runs the pipeline synchronously. Long runs will hold the browser request open until the run completes.
- The UI is a showcase shell. There is no auth, queue, worker pool, or multi-user isolation.
- Source coverage is still narrow and depends on stable public pages.
- Some public directory records contain stale websites or stale phone numbers.
- Website validation is intentionally conservative. Off-domain redirects are treated as mismatches; same-domain brand mismatch inference is not implemented.
- Website enrichment only probes a small set of likely contact pages.
- Some websites are dead, parked, or DNS-broken; those failures are recorded instead of hidden.
- Discovery and extraction are still source-specific. This is not a universal extractor.

## What This Project Deliberately Does Not Add

- FastAPI
- Celery
- Postgres
- Redis
- Airflow
- dashboards
- auth

That would be architecture theater for this assignment.
