# Representative Row Ranking

## Purpose

The outreach-ready export must choose one best representative row per outreach or contact group.

This is not the same as dedupe deletion.
The master dataset preserves all useful rows.
The outreach-ready dataset chooses the best single row for action.

## Ranking Unit

Ranking is performed within one outreach target group.

Default grouping basis:

- first preference: `contact_group_id`
- fallback when no strong contact group exists: `outreach_suppression_key`

The grouping choice must remain deterministic and traceable.

## Hard Guards

Do not select a row for outreach-ready export when:

- `validation_status` is rejected
- `website_validation_status` is a confirmed mismatch and no usable listing-only contact survives
- the row is too ambiguous to support a safe outreach target

These rows remain in the master dataset.

## Ordered Ranking Priorities

Within an eligible group, sort rows by these priorities in order:

1. has website-derived email
2. has website-derived phone
3. has both usable preferred email and preferred phone
4. higher email confidence
5. higher phone confidence
6. stronger source priority
7. richer location completeness
8. higher overall quality score
9. clearer niche relevance
10. stable deterministic tie-breaker by `lead_id`

This order matters more than any single combined vanity score.

## Practical Scoring Model

Implementation can convert the ordered priorities into a transparent tuple or weighted score.

Recommended sort tuple:

1. `website_email_present`
2. `website_phone_present`
3. `preferred_email_present and preferred_phone_present`
4. `email_confidence_rank`
5. `phone_confidence_rank`
6. `source_priority_rank`
7. `location_completeness_rank`
8. `quality_score`
9. `niche_relevance_rank`
10. `lead_id`

Where:

- `high > medium > low > blank`
- lower source priority number is better
- `lead_id` sorts ascending for deterministic final ties

## Confidence Rank Mapping

Use a simple mapping:

- `high = 3`
- `medium = 2`
- `low = 1`
- blank or unknown = 0

## Source Priority Mapping

Use the documented source-priority model:

1. official business website
2. business association or member directory
3. niche-specific directory
4. local business directory or aggregator
5. generic listing source or weak search-result page

When ranking, lower numeric priority wins.

## Location Completeness

Rows with stronger location context should rank above weak rows when contact evidence is tied.

Suggested completeness order:

1. address + city + state
2. city + state
3. broader New York term only
4. blank geography

This helps avoid selecting thin rows when a richer row exists for the same outreach target.

## Generic Email Policy

Generic inboxes are outreach-eligible.
They should not be discarded.

But when other evidence is equal:

- named or clearly direct email beats generic email
- same-domain generic website email still beats no email
- generic listing email should not outrank a higher-confidence website-derived named email

## Review-Needed Behavior

Rows marked `possible_duplicate_needs_review` can still appear in the master dataset.
They should only win outreach-ready selection when they still have the best available outreach evidence and are not blocked for ambiguity.

This keeps the outreach export practical without pretending review uncertainty disappeared.

## Selection Evidence

The outreach-ready export should retain evidence explaining why the chosen row won.

At minimum preserve:

- selected group identifier
- representative selection rank or reason
- preferred email and phone with confidence
- source priority
- website validation status

## Final Principle

Do not keep the first surviving row just because it came earlier in extraction order.
That is lazy and it will produce a worse outreach file.

The winning row must be the best-supported outreach target in the group, and the pipeline must be able to explain why.
