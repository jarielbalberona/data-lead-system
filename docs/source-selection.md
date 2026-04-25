# Source And Niche Selection Rules

This assignment uses public business directory or public business listing pages only.

## Allowed Source Types

- public business directory result pages
- public business profile pages linked from directory results
- public company contact pages linked from those public profiles

## Disallowed Source Types

- authenticated pages
- paywalled pages
- captcha-gated pages
- sources that require bypassing anti-bot protections
- private personal profiles
- personal social accounts used as private contact sources

## Ethical Limits

- collect only publicly listed business contact information
- keep source URLs for every extracted row
- do not bypass rate limits or bot controls
- do not scrape hidden emails or personal data not intentionally exposed for business contact

## Target Niches

### Property Managers

Relevant entries:
- property management companies
- apartment management companies
- real estate management firms
- commercial property management companies

Irrelevant entries:
- individual real estate agents with no management service
- cleaning vendors
- handyman services
- unrelated landlords without business contact pages

### Interior Designers

Relevant entries:
- interior design firms
- interior decorating businesses
- commercial or residential interior design studios

Irrelevant entries:
- furniture stores without design service
- architects unless interior design is clearly a public service line
- DIY blogs or content sites
- marketplaces or directories listing suppliers instead of service providers

## Minimum Useful Record Criteria

A row is worth keeping for processing when it has:

- a business name
- a source URL
- at least one usable outreach surface:
  - phone
  - email
  - website

Rows with no usable contact or website may still be extracted into raw data, but should be marked for rejection during validation.

## Source Strategy

Preferred approach:
- start with public directory result pages that already list business name, website, phone, and location
- follow through to public business websites only when needed for missing details such as email

Avoid:
- broad speculative crawling
- sources with brittle Javascript-only rendering unless there is no better public alternative

## Assignment-Time Tradeoff

This is a 5-day assignment, not a data vendor platform build.

Practical source selection means:
- choose public sources that yield enough real leads quickly
- prefer stable HTML pages over fragile interactive flows
- switch sources when a directory is blocked, unreliable, or low-signal

## Review Standard

Every extracted row must be explainable to a reviewer:

- where it came from
- why it was considered relevant
- why it was kept, grouped, or rejected
