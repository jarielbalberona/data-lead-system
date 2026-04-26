# Discovery Strategy

## Purpose

This phase keeps directory and listing pages as the discovery entry point.
Business websites are used later for enrichment and validation, not for primary discovery.

Pipeline shape:

1. generate niche + geography search seeds
2. collect candidate listing URLs
3. classify likely listing pages
4. dedupe accepted listing pages
5. extract candidate businesses from accepted listing pages
6. enrich candidate businesses from official websites

## Geography Scope

Discovery is constrained to New York only.

Primary geography modifiers:

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
- NYC
- NY

Rules:

- Do not interpret `New York` as Manhattan only.
- Treat borough and metro terms as valid discovery modifiers and relevance hints.
- A final lead row may resolve only to broader `New York`, `NY`, or `NYC` context when the source is weak, but discovery should deliberately target the geography list above.

## Target Niches

- property managers
- interior designers

Niche seed generation should be reusable and deterministic. One-off hardcoded search phrases are not enough.

## Allowed Source Types

Allowed:

- public business directory category/location pages
- public local business aggregators
- public niche directories
- public association or member directories
- public search result pages only when accessible without login, CAPTCHA, or obvious bot fencing

Disallowed:

- login-gated or paywalled pages
- CAPTCHA-protected flows
- Google Maps scraping
- private personal profile sites
- social media as a primary source
- aggressive search engine scraping as the main architecture
- anything that bypasses authentication, robots intent, or anti-bot controls

## Source Priority

Representative selection and evidence reconciliation should preserve this default priority:

1. official business website
2. business association or member directory
3. niche-specific directory
4. local business directory or aggregator
5. generic listing source or weak search-result page

This priority is not just metadata. Later ranking should prefer evidence from stronger sources.

## Discovery Collection Model

Discovery should be automated, but constrained.

Allowed bootstrap:

- a small curated seed list of known directory domains or listing patterns

Not allowed:

- manually pasting every listing page by hand as the operating model

Expected discovery outputs:

- discovery query
- niche
- geography modifier
- discovered URL
- source domain
- source type
- discovered timestamp
- raw response metadata needed for auditability

## Listing Classification Rules

The classifier should separate at least these buckets:

- accepted_listing_page
- rejected_business_website
- rejected_article_or_blog
- rejected_irrelevant
- rejected_duplicate

Initial classification should be heuristic and deterministic:

- URL patterns
- domain allow/deny hints
- title text
- heading text
- density of business-card or listing-like structures

Do not overbuild this. A transparent ruleset is enough for the assignment.

## Crawl Policy

- default rate: 1 request per second per domain
- timeout: 10 to 15 seconds
- retry at most once on transient failure
- use an honest project-specific User-Agent
- do not parallelize aggressively
- record failure reason instead of silently skipping

For website enrichment specifically:

- probe at most 5 likely pages per website

## Traceability

Every extracted lead must retain lineage back to:

- discovery query
- discovered listing URL
- listing source domain
- source type
- classification outcome
- extraction timestamp

The source registry is a first-class artifact, not a debug afterthought.

## Why This Model

Directory and category pages are better for discovery breadth.
Official websites are better for contact validation and enrichment.

Replacing directory discovery with website-only discovery would be a design mistake because it throws away the efficient surface that finds businesses in the first place.
