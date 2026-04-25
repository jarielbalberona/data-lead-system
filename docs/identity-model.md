# Lead Identity Model

This project treats dedupe as identity resolution. That means a discovered row is not assumed to be a one-to-one representation of a single contact.

## Identity Layers

### 1. Lead Row

A lead row is one discovered public record from one public page.

Example:
- `Acme Property Management`
- `123 Main St, Chicago, IL`
- `leasing@acmepm.com`
- `https://www.acmepm.com`

This row is an observation, not the truth source for business identity.

### 2. Contact Group

A contact group represents a likely shared reachable endpoint. Strong evidence:

- same normalized email
- same normalized phone

Purpose:
- prevent repeated outreach to the same inbox or number
- preserve multiple useful rows when that contact covers multiple locations

Example:
- Same phone, different addresses: likely same contact or company managing multiple properties.
- Keep both rows.
- Assign the same `contact_group_id`.

### 3. Business Group

A business group represents likely same company identity across rows. Strong evidence:

- same website domain
- same website plus matching or highly similar business name
- same brand with different locations

Purpose:
- distinguish same company with multiple locations from separate independent businesses
- support reviewable business-level grouping without deleting location-specific rows

Example:
- Same website, different addresses: likely one business with multiple locations.
- Keep the rows.
- Assign the same `business_group_id`.

### 4. Location Group

A location group represents a conservative physical place grouping based on normalized address, city, and state.

Weak evidence:
- same address alone

Purpose:
- identify repeated location rows without over-merging companies or contacts
- support reviewer understanding when multiple businesses share a building

Example:
- Same address only does not prove same business.
- Use `location_group_id` as context, not automatic duplicate proof.

### 5. Outreach Suppression Key

The outreach suppression key is the practical anti-spam identity used to avoid repeated contact.

Priority:
1. normalized email
2. normalized phone
3. website domain
4. normalized business name plus city

Purpose:
- stop repeated contact attempts to the same reachable target
- remain useful even when contact and business groups are imperfect

## Generic Email Handling

Generic inboxes such as `info@`, `hello@`, and `contact@` are useful but weak.

Rules:
- they can still generate contact grouping
- they should not be treated as strong personal identity
- dedupe reasons should call out that the grouping is based on a generic inbox

## Identity Examples

### Same Phone, Different Address

Interpretation:
- likely same contact/company managing multiple locations

Action:
- keep both rows
- same `contact_group_id`
- likely same `business_group_id` if website/name aligns
- different or shared `location_group_id` depending on address
- `dedupe_status = same_contact_multiple_locations`

### Same Email, Different Phone

Interpretation:
- likely same person or shared inbox with different listed numbers

Action:
- same `contact_group_id`
- keep both rows if location/business context is still useful

### Same Website, Different Addresses

Interpretation:
- likely same company with multiple locations

Action:
- same `business_group_id`
- distinct location rows preserved
- `dedupe_status = same_business_multiple_locations`

### Same Address Only

Interpretation:
- weak evidence

Action:
- do not confirm duplicate based on address alone
- at most mark as `possible_duplicate_needs_review` if other weak signals stack up

## Final Principle

The system keeps useful business and location records while grouping shared contact and company identity where evidence is strong enough. That is the difference between usable lead ops logic and naive duplicate deletion.
