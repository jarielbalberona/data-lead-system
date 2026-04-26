# Website Enrichment Field Model

## Purpose

This document defines how listing-derived fields, website-derived fields, preferred canonical fields, provenance fields, and validation fields should coexist.

The rule is simple:

- do not overwrite listing evidence blindly
- preserve website-derived evidence separately
- compute preferred canonical fields explicitly

## Core Contact Fields

These fields preserve the raw public contact values that survive normalization.

| Field | Meaning |
| --- | --- |
| `listing_phone` | Phone captured from the directory or listing page. |
| `listing_email` | Email captured from the directory or listing page. |
| `website_phone` | Best phone captured from the official business website. |
| `website_email` | Best email captured from the official business website. |
| `preferred_phone` | Canonical phone selected after comparing listing and website evidence. |
| `preferred_email` | Canonical email selected after comparing listing and website evidence. |

## Website Extraction Fields

These fields describe how the website enrichment pass found the preferred contact values.

| Field | Meaning |
| --- | --- |
| `phone_source_url` | Page URL that produced the selected phone value. |
| `email_source_url` | Page URL that produced the selected email value. |
| `phone_extraction_method` | Method used to find the selected phone, such as `tel_link` or `visible_text`. |
| `email_extraction_method` | Method used to find the selected email, such as `mailto` or `visible_text`. |
| `phone_confidence` | Confidence label for the selected phone. |
| `email_confidence` | Confidence label for the selected email. |

## Generic Contact Flags

These fields are operationally useful for ranking and outreach policy.

| Field | Meaning |
| --- | --- |
| `preferred_email_is_generic` | `true` when the selected email is a generic inbox such as `info@` or `contact@`. |
| `website_email_is_generic` | `true` when the website-derived email is generic. |
| `listing_email_is_generic` | `true` when the listing-derived email is generic. |

Generic emails are still outreach-eligible. They just rank below named or more direct emails.

## Website Validation Fields

These fields describe whether a lead website was usable.

| Field | Meaning |
| --- | --- |
| `website_validation_status` | Final website status such as `valid`, `dead`, `mismatch`, `no_contact_found`, or `missing_website`. |
| `website_validation_reason` | Human-readable explanation for the website validation outcome. |
| `website_final_url` | Final URL after redirects. |
| `website_pages_attempted` | Count of homepage/contact/about/team pages attempted. |

## Discovery and Source Fields

These fields connect a lead row back to discovery and source priority.

| Field | Meaning |
| --- | --- |
| `source_url` | Public page URL where the row itself was extracted. |
| `source_listing_url` | Listing page URL that produced the row. |
| `source_directory` | Source domain or directory identifier. |
| `source_type` | Source class such as `association_directory` or `local_aggregator`. |
| `source_priority` | Numeric priority where lower is better. |
| `discovery_query` | Query or seed used to discover the listing page. |
| `discovery_geography` | Geography modifier used during discovery. |

## Confidence Model

Keep it simple and deterministic.

### Email Confidence

- `high`
  - `mailto` email on a same-domain contact page
  - clearly visible same-domain email on a same-domain contact page
- `medium`
  - clearly visible same-domain email on homepage, footer, about page, or team page
- `low`
  - ambiguous visible email
  - listing-only email with no website confirmation

### Phone Confidence

- `high`
  - `tel` link on a same-domain contact page
  - clearly visible phone on a same-domain contact page
- `medium`
  - clearly visible phone on homepage, footer, about page, or team page
- `low`
  - listing-only phone with no website confirmation

## Preferred Field Selection Rules

Preferred fields are not the same as raw fields.

Preferred selection rules:

1. prefer website-derived evidence over listing-derived evidence when the website evidence has stronger provenance or confidence
2. do not replace a stronger listing value with a weaker website value
3. preserve both raw values even when a preferred value is selected
4. if two candidates are tied, prefer the one with better source priority and then stable deterministic ordering

Examples:

- website mailto email beats listing email
- listing phone can remain preferred if the website has no usable phone
- a generic website email can beat a blank listing email, but should rank below a named website email later

## Normalized Comparison Fields

The pipeline should continue using normalized matching fields for identity logic:

- `normalized_phone`
- `normalized_email`
- `website_domain`

When preferred values exist, normalized comparison should use:

- `preferred_phone` for `normalized_phone`
- `preferred_email` for `normalized_email`

But listing and website raw values must remain available for auditability.

## Final Principle

This schema exists to preserve truth.

The master dataset should show what the listing said, what the website said, what the pipeline preferred, and why.
If the pipeline cannot explain a preferred contact value, that value is not trustworthy enough to use.
