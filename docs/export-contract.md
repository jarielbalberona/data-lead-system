# Dual Export Contract

## Purpose

This phase produces two different outputs because one file cannot serve both truth preservation and outreach execution cleanly.

Outputs:

- `data/final/leads_master.csv`
- `data/final/leads_outreach_ready.csv`

## Export 1: Master Dataset

Path:

- `data/final/leads_master.csv`

Intent:

- preserve all useful rows
- preserve grouped rows
- preserve review-needed rows
- preserve listing-derived and website-derived evidence
- remain the auditable truth source for the assignment

Included rows:

- valid rows
- rows with `same_contact_multiple_locations`
- rows with `same_business_multiple_locations`
- rows with `possible_duplicate_needs_review`

Excluded rows:

- clearly rejected rows such as `rejected_irrelevant`
- clearly unusable rows such as `rejected_missing_required_fields`

Required fields:

- `lead_id`
- `niche`
- `business_name`
- `address`
- `city`
- `state`
- `source_url`
- `source_listing_url`
- `source_directory`
- `source_type`
- `source_priority`
- `discovery_query`
- `discovery_geography`
- `listing_phone`
- `listing_email`
- `website`
- `website_phone`
- `website_email`
- `preferred_phone`
- `preferred_email`
- `phone_source_url`
- `email_source_url`
- `phone_extraction_method`
- `email_extraction_method`
- `phone_confidence`
- `email_confidence`
- `website_validation_status`
- `website_validation_reason`
- `contact_group_id`
- `business_group_id`
- `location_group_id`
- `dedupe_status`
- `dedupe_reason`
- `outreach_suppression_key`
- `quality_score`
- `validation_status`
- `rejection_reason`
- `extraction_timestamp`

## Export 2: Outreach-Ready Dataset

Path:

- `data/final/leads_outreach_ready.csv`

Intent:

- provide one best representative row per outreach target
- expose preferred contact fields
- expose readiness flags
- preserve enough lineage to defend why the row was selected

Included rows:

- one selected row per outreach/contact group
- only rows that survive representative selection logic
- rows may still be blocked from outreach if no usable contact exists, but the block reason must be explicit

Required fields:

- `lead_id`
- `niche`
- `business_name`
- `website`
- `address`
- `city`
- `state`
- `preferred_phone`
- `preferred_email`
- `phone_confidence`
- `email_confidence`
- `preferred_email_is_generic`
- `website_validation_status`
- `source_url`
- `source_listing_url`
- `source_type`
- `source_priority`
- `contact_group_id`
- `business_group_id`
- `location_group_id`
- `outreach_suppression_key`
- `representative_group_key`
- `representative_rank_reason`
- `ready_for_email`
- `ready_for_phone`
- `ready_for_outreach`
- `outreach_block_reason`
- `quality_score`

## Relationship Between Exports

The outreach-ready export is derived from the master dataset.
It is not a separate truth system.

Rules:

- every outreach-ready row must map back to one master row by `lead_id`
- the master dataset keeps evidence that the outreach-ready export intentionally compresses
- no evidence should exist only in outreach-ready output

## Selection Transparency

The outreach-ready export must explain selection at a practical level.

At minimum:

- what group the row represents
- why it won over other rows
- whether the row is actually ready for email, phone, or neither

If selection cannot be explained, the ranking logic is too opaque.

## File Semantics

### `leads_master.csv`

Use this file for:

- auditing source quality
- reviewing grouped rows
- investigating mismatches
- checking whether enrichment improved contact richness

### `leads_outreach_ready.csv`

Use this file for:

- practical outreach preparation
- one-row-per-target exports
- lightweight operational handoff

Do not use it as a substitute for the master truth file.

## Final Principle

Trying to force one CSV to do both jobs would be lazy design.
The master file preserves truth.
The outreach-ready file preserves actionability.
