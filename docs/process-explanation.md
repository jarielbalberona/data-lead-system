# Process Explanation

## Goal

Build a minimal Python pipeline that extracts public business leads for two niches, cleans the data, applies identity-resolution logic, and exports a reviewable CSV for outreach planning.

## Extraction

The pipeline uses a small number of public business-directory pages instead of broad scraping:

- `hoamanagementcompanies.net` for property managers
- `interiordesignlink.com` for interior designers

Each extracted row preserves:

- niche
- business name
- phone
- email if exposed publicly
- website if exposed publicly
- address/location
- source URL
- extraction timestamp

Raw outputs are written to `data/raw/` and timestamped snapshots are archived under `data/raw/archive/`.

## Normalization

Before dedupe, the pipeline normalizes:

- phone numbers into consistent matching keys
- emails to lowercase
- websites into canonical URLs plus `website_domain`
- business names, addresses, cities, and states into cleaner structured fields

The goal is practical consistency for matching, not perfect postal or legal-name normalization.

## Validation

The pipeline marks rows with:

- `validation_status`
- `rejection_reason`

Current rejection logic covers:

- missing business name
- rows outside the target niches
- obvious placeholder/directory junk
- rows with no usable phone, email, or website domain

## Identity Resolution

This project does not treat dedupe as row deletion.

It separately models:

- `contact_group_id` for shared normalized email or phone
- `business_group_id` for shared website domain or tightly supported repeated business names
- `location_group_id` for conservative same-location grouping when supported by business/contact evidence
- `outreach_suppression_key` to prevent repeated outreach to the same reachable target

Each final row also includes:

- `dedupe_status`
- `dedupe_reason`

That makes the grouping behavior reviewable instead of opaque.

## Final Export

The final CSV export:

- keeps only valid rows
- excludes `confirmed_duplicate` rows
- preserves ambiguous rows with review flags
- assigns `lead_id`
- assigns a transparent `quality_score`

The final deliverable is `data/final/leads.csv`.
