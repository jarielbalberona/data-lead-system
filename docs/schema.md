# Final Lead CSV Schema

This document defines the final output schema for `data/final/leads.csv`.

## Required Fields

| Field | Purpose | Example |
| --- | --- | --- |
| `lead_id` | Stable row identifier for a final exported lead row. | `lead_8f5f639e` |
| `niche` | Assignment niche classification. | `property_manager` |
| `business_name` | Public-facing company or business name. | `Acme Property Management` |
| `phone` | Best public phone string preserved for review. | `(312) 555-0102` |
| `email` | Best public email preserved for review. | `leasing@acmepm.com` |
| `website` | Public website URL kept for reviewer traceability. | `https://www.acmepm.com` |
| `address` | Street-level location string when available. | `123 Main St` |
| `city` | Normalized city. | `Chicago` |
| `state` | Normalized state or region abbreviation/name. | `IL` |
| `source_url` | Public page URL where the record came from. | `https://www.example-directory.com/acme-property-management` |
| `contact_group_id` | Identity group for shared contact endpoints like email or phone. | `contact_2c4bb4da` |
| `business_group_id` | Identity group for likely same business across multiple rows or locations. | `business_32d1904d` |
| `location_group_id` | Conservative location grouping key for same normalized address/city/state. | `location_55e75dc9` |
| `dedupe_status` | Final identity-resolution outcome for the row. | `same_business_multiple_locations` |
| `dedupe_reason` | Human-readable explanation for grouping or rejection. | `Same website domain and matching business name across multiple locations.` |
| `outreach_suppression_key` | Preferred suppression key for repeated outreach prevention. | `email:leasing@acmepm.com` |
| `quality_score` | Transparent assignment score based on contact and location completeness. | `90` |

## Optional Supporting Fields

| Field | Purpose | Example |
| --- | --- | --- |
| `extraction_timestamp` | UTC timestamp when the row was extracted. | `2026-04-26T09:15:42Z` |
| `website_domain` | Parsed comparison domain used in business matching. | `acmepm.com` |
| `normalized_phone` | Dedupe-ready normalized phone token. | `13125550102` |
| `normalized_email` | Lowercased normalized email used for grouping. | `leasing@acmepm.com` |
| `validation_status` | Accepted or rejected processing state. | `accepted` |
| `rejection_reason` | Reason a row was excluded from final export. | `Missing business name and no usable contact or website.` |

## Field Semantics

- `lead_id` identifies the exported row, not the person or business.
- `contact_group_id` represents likely shared reachable contact identity.
- `business_group_id` represents likely shared company identity.
- `location_group_id` represents a conservative physical place grouping.
- `outreach_suppression_key` is the practical guardrail that prevents repeated outreach.

## Null/Blank Handling

- Preserve blank strings as empty values in CSV for missing optional fields.
- Do not invent email, phone, or address data.
- A row may remain exportable without email if the business has another usable outreach surface such as phone or website.

## Export Rules

- `data/final/leads.csv` contains only accepted final rows.
- Rejected rows remain inspectable in processed outputs and are not silently discarded.
- Required output fields are always present as columns even when individual cell values are blank.
