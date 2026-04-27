# Lead Enrichment & Outreach-Ready Export

Python-first lead pipeline for the enrichment phase of the assignment.

## What This Project Does

The runtime now follows this path:

```txt
directory/source discovery
-> candidate listing URL collection
-> listing-page classification
-> accepted listing pages
-> extracted candidate lead rows
-> website enrichment and validation
-> identity-aware grouping
-> representative-row selection
-> dual exports
```

This is still an assignment-grade terminal pipeline. It is not a service, scheduler, or CRM.

## Why Directory Discovery Still Comes First

The pipeline does not replace directory discovery with website-only extraction.

- directory/category/search pages are better for finding candidate businesses
- official business websites are better for enrichment and contact validation
- outreach-ready export should choose the best row per outreach target after enrichment, not the first row that survived

## Current Scope

Target niches:

- property managers
- interior designers

Target geography:

- New York City
- Brooklyn
- Queens
- Manhattan
- Bronx
- Staten Island
- Yonkers
- White Plains
- New Rochelle
- Mount Vernon
- Long Island
- Hempstead
- Oyster Bay
- Huntington
- Brookhaven
- Islip

`New York` is treated as a broader geography target, not as Manhattan-only.

## Sources

Current discovery/extraction sources in the runtime:

- property managers:
  - discovery and extraction path through `hoamanagementcompanies.net`
- interior designers:
  - discovery and extraction path through `theidslist.com`

Allowed source policy is documented in:

- `docs/discovery-strategy.md`

## Output Files

Primary outputs:

- `data/final/leads_master.csv`
- `data/final/leads_outreach_ready.csv`
- `docs/quality-summary.md`

Supporting artifacts:

- `data/raw/discovery_candidates_raw.json`
- `data/processed/listing_pages_classified.json`
- `data/processed/source_registry.json`
- `data/raw/property_managers_raw.json`
- `data/raw/interior_designers_raw.json`
- `data/raw/website_page_attempts_raw.json`
- `data/processed/website_contacts.json`

Timestamped snapshots are kept under the matching `archive/` folders.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python3 src/run_pipeline.py
```

The pipeline writes the latest master and outreach-ready CSVs into `data/final/`.

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

Post-enrichment guardrail:

- generic inboxes such as `info@` are useful for outreach
- generic inbox alone is weak identity evidence
- generic email no longer creates a global contact merge across unrelated website domains

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

- Source coverage is still narrow and depends on stable public pages.
- Some public directory records contain stale websites or stale phone numbers.
- Website validation is intentionally conservative. Off-domain redirects are treated as mismatches; same-domain brand mismatch inference is not implemented.
- Website enrichment only probes a small set of likely contact pages.
- Some websites are dead, parked, or DNS-broken; those failures are recorded instead of hidden.
- Discovery and extraction are still source-specific. This is not a universal extractor.
- The pipeline is batch-oriented and file-based. It is not a long-running crawler or review queue.

## What This Project Deliberately Does Not Add

- FastAPI
- Celery
- Postgres
- Redis
- Airflow
- dashboards
- auth

That would be architecture theater for this assignment.
