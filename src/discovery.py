from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from config import PipelineConfig


@dataclass(frozen=True)
class GeographyTarget:
    slug: str
    display_name: str
    state: str
    discovery_terms: tuple[str, ...]
    relevance_terms: tuple[str, ...]


@dataclass(frozen=True)
class NicheDiscoveryProfile:
    niche: str
    search_terms: tuple[str, ...]
    source_hints: tuple[str, ...]


@dataclass(frozen=True)
class DiscoverySeed:
    niche: str
    geography_slug: str
    geography_name: str
    geography_state: str
    search_term: str
    source_hint: str
    query: str
    relevance_terms: tuple[str, ...]


@dataclass(frozen=True)
class DiscoveryRegistryEntry:
    source_registry_id: str
    niche: str
    source_name: str
    source_type: str
    source_priority: int
    bootstrap_url: str


@dataclass(frozen=True)
class CandidateListingUrl:
    source_registry_id: str
    discovery_candidate_id: str
    niche: str
    source_name: str
    source_type: str
    source_priority: int
    discovery_query: str
    geography_slug: str
    geography_name: str
    candidate_url: str
    discovery_source_url: str
    discovered_at: str
    status: str
    http_status: int | None
    failure_reason: str


@dataclass(frozen=True)
class ClassifiedListingPage:
    source_registry_id: str
    listing_page_id: str
    niche: str
    source_name: str
    source_type: str
    source_priority: int
    canonical_url: str
    listing_page_status: str
    classification_reason: str
    http_status: int | None
    supporting_geography_slugs: tuple[str, ...]
    supporting_geography_names: tuple[str, ...]
    supporting_queries: tuple[str, ...]
    supporting_candidate_ids: tuple[str, ...]
    raw_candidate_count: int
    classified_at: str


NEW_YORK_GEOGRAPHIES: tuple[GeographyTarget, ...] = (
    GeographyTarget(
        slug="new-york",
        display_name="New York",
        state="NY",
        discovery_terms=("New York", "New York City", "NYC", "New York NY"),
        relevance_terms=(
            "New York",
            "New York City",
            "NYC",
            "Brooklyn",
            "Queens",
            "Manhattan",
            "Bronx",
            "Staten Island",
            "NY",
        ),
    ),
    GeographyTarget(
        slug="long-island",
        display_name="Long Island",
        state="NY",
        discovery_terms=("Long Island", "Long Island NY", "Nassau County", "Suffolk County"),
        relevance_terms=(
            "Long Island",
            "Nassau",
            "Suffolk",
            "Hempstead",
            "Oyster Bay",
            "Huntington",
            "Brookhaven",
            "Islip",
            "New York",
            "NY",
        ),
    ),
    GeographyTarget(
        slug="westchester",
        display_name="Westchester",
        state="NY",
        discovery_terms=("Westchester", "Westchester County", "Westchester NY"),
        relevance_terms=(
            "Westchester",
            "Yonkers",
            "White Plains",
            "New Rochelle",
            "Mount Vernon",
            "New York",
            "NY",
        ),
    ),
)

# HOA state page links use city/area display names, not the bucket name. Map each bucket to
# link labels we look for; all matches are tagged with the bucket (not borough- or city-level).
HOA_CITY_LINK_LABELS: dict[str, tuple[str, ...]] = {
    "new-york": (
        "New York",
        "Brooklyn",
        "Queens",
        "Manhattan",
        "Bronx",
        "Staten Island",
    ),
    "long-island": (
        "Long Island",
        "Nassau",
        "Suffolk",
        "Hempstead",
        "Oyster Bay",
        "Huntington",
        "Brookhaven",
        "Islip",
    ),
    "westchester": (
        "Westchester",
        "Yonkers",
        "White Plains",
        "New Rochelle",
        "Mount Vernon",
    ),
}

NY_BROAD_BUCKET_SLUGS: frozenset[str] = frozenset(g.slug for g in NEW_YORK_GEOGRAPHIES)

_GEOGRAPHY_BY_SLUG: dict[str, GeographyTarget] = {g.slug: g for g in NEW_YORK_GEOGRAPHIES}


def _geography_target_for_slug(slug: str) -> GeographyTarget:
    if slug not in _GEOGRAPHY_BY_SLUG:
        raise ValueError(f"Unknown geography slug: {slug}")
    return _GEOGRAPHY_BY_SLUG[slug]


NICHE_DISCOVERY_PROFILES: tuple[NicheDiscoveryProfile, ...] = (
    NicheDiscoveryProfile(
        niche="property_manager",
        search_terms=(
            "property managers",
            "property management companies",
            "property management",
            "association property management",
        ),
        source_hints=(
            "directory",
            "listing",
            "association directory",
            "management company directory",
            "category page",
        ),
    ),
    NicheDiscoveryProfile(
        niche="interior_designer",
        search_terms=(
            "interior designers",
            "interior design firms",
            "interior decorators",
            "interior design directory",
        ),
        source_hints=(
            "directory",
            "listing",
            "member directory",
            "designer directory",
            "category page",
        ),
    ),
)


def _stable_token(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]


DISCOVERY_SOURCE_REGISTRY: tuple[DiscoveryRegistryEntry, ...] = (
    DiscoveryRegistryEntry(
        source_registry_id="source_registry_" + _stable_token(
            "property_manager|hoamanagementcompanies.net|local_business_directory|4|https://hoamanagementcompanies.net/"
        ),
        niche="property_manager",
        source_name="hoamanagementcompanies.net",
        source_type="local_business_directory",
        source_priority=4,
        bootstrap_url="https://hoamanagementcompanies.net/",
    ),
    DiscoveryRegistryEntry(
        source_registry_id="source_registry_" + _stable_token(
            "interior_designer|theidslist.com|association_member_directory|2|https://www.theidslist.com/"
        ),
        niche="interior_designer",
        source_name="theidslist.com",
        source_type="association_member_directory",
        source_priority=2,
        bootstrap_url="https://www.theidslist.com/",
    ),
)

DISCOVERY_SEED_LOOKUP: dict[tuple[str, str, str], str] = {}


def build_discovery_seeds() -> list[DiscoverySeed]:
    seeds: list[DiscoverySeed] = []

    for geography in NEW_YORK_GEOGRAPHIES:
        for profile in NICHE_DISCOVERY_PROFILES:
            for search_term in profile.search_terms:
                for source_hint in profile.source_hints:
                    for geography_term in geography.discovery_terms:
                        seeds.append(
                            DiscoverySeed(
                                niche=profile.niche,
                                geography_slug=geography.slug,
                                geography_name=geography.display_name,
                                geography_state=geography.state,
                                search_term=search_term,
                                source_hint=source_hint,
                                query=_normalize_query(search_term, geography_term, source_hint),
                                relevance_terms=geography.relevance_terms,
                            )
                        )

    unique_payloads: dict[tuple[str, str, str], DiscoverySeed] = {}
    for seed in seeds:
        key = (seed.niche, seed.geography_slug, seed.query)
        unique_payloads[key] = seed

    return list(unique_payloads.values())


def write_discovery_seeds(
    output_path: Path | None = None,
    *,
    config: PipelineConfig | None = None,
) -> Path:
    runtime_config = config or PipelineConfig()
    path = output_path or runtime_config.discovery_seeds_output_path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(seed) for seed in build_discovery_seeds()]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def collect_candidate_listing_urls(config: PipelineConfig | None = None) -> list[CandidateListingUrl]:
    runtime_config = config or PipelineConfig()
    runtime_config.ensure_directories()
    session = _build_session(runtime_config)

    candidates: list[CandidateListingUrl] = []
    candidates.extend(_collect_property_management_candidates(session, runtime_config))
    candidates.extend(_collect_ids_candidates(session, runtime_config))
    return candidates


def write_candidate_listing_urls(
    output_path: Path | None = None,
    *,
    config: PipelineConfig | None = None,
    candidates: list[CandidateListingUrl] | None = None,
) -> Path:
    runtime_config = config or PipelineConfig()
    path = output_path or runtime_config.discovery_raw_output_path
    candidate_rows = candidates if candidates is not None else collect_candidate_listing_urls(runtime_config)
    path.parent.mkdir(parents=True, exist_ok=True)
    archive_dir = path.parent / "archive" / path.stem
    archive_dir.mkdir(parents=True, exist_ok=True)

    payload = json.dumps([asdict(candidate) for candidate in candidate_rows], indent=2)
    path.write_text(payload, encoding="utf-8")

    timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    archive_dir.joinpath(f"{_snapshot_token(timestamp)}.json").write_text(payload, encoding="utf-8")
    return path


def classify_candidate_listing_urls(
    config: PipelineConfig | None = None,
    candidates: list[CandidateListingUrl] | None = None,
) -> list[ClassifiedListingPage]:
    runtime_config = config or PipelineConfig()
    session = _build_session(runtime_config)
    candidate_rows = candidates if candidates is not None else collect_candidate_listing_urls(runtime_config)

    grouped_candidates: dict[str, list[CandidateListingUrl]] = defaultdict(list)
    for candidate in candidate_rows:
        grouped_candidates[_canonicalize_url(candidate.candidate_url)].append(candidate)

    classifications: list[ClassifiedListingPage] = []
    for canonical_url, group in grouped_candidates.items():
        representative = group[0]
        response = _fetch_url(session, canonical_url, runtime_config)
        listing_status, reason = _classify_listing_page(
            niche=representative.niche,
            url=canonical_url,
            html=response["text"] if response["ok"] else "",
            http_status=response["status_code"],
        )
        classifications.append(
            ClassifiedListingPage(
                source_registry_id=representative.source_registry_id,
                listing_page_id=_listing_page_id(representative.source_registry_id, canonical_url),
                niche=representative.niche,
                source_name=representative.source_name,
                source_type=representative.source_type,
                source_priority=representative.source_priority,
                canonical_url=canonical_url,
                listing_page_status=listing_status,
                classification_reason=reason,
                http_status=response["status_code"],
                supporting_geography_slugs=tuple(sorted({candidate.geography_slug for candidate in group})),
                supporting_geography_names=tuple(sorted({candidate.geography_name for candidate in group})),
                supporting_queries=tuple(sorted({candidate.discovery_query for candidate in group})),
                supporting_candidate_ids=tuple(sorted({candidate.discovery_candidate_id for candidate in group})),
                raw_candidate_count=len(group),
                classified_at=_timestamp_now(),
            )
        )

    classifications.sort(key=lambda row: (row.niche, row.canonical_url))
    return classifications


def write_classified_listing_pages(
    output_path: Path | None = None,
    *,
    config: PipelineConfig | None = None,
    classified_rows: list[ClassifiedListingPage] | None = None,
) -> Path:
    runtime_config = config or PipelineConfig()
    path = output_path or runtime_config.classified_listing_pages_output_path
    path.parent.mkdir(parents=True, exist_ok=True)
    archive_dir = path.parent / "archive" / path.stem
    archive_dir.mkdir(parents=True, exist_ok=True)

    rows = classified_rows or classify_candidate_listing_urls(runtime_config)
    payload = json.dumps([asdict(row) for row in rows], indent=2)
    path.write_text(payload, encoding="utf-8")

    timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    archive_dir.joinpath(f"{_snapshot_token(timestamp)}.json").write_text(payload, encoding="utf-8")
    return path


def write_source_registry(
    output_path: Path | None = None,
    *,
    config: PipelineConfig | None = None,
) -> Path:
    runtime_config = config or PipelineConfig()
    path = output_path or runtime_config.source_registry_output_path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps([asdict(entry) for entry in DISCOVERY_SOURCE_REGISTRY], indent=2)
    path.write_text(payload, encoding="utf-8")
    return path


def _normalize_query(search_term: str, geography_term: str, source_hint: str) -> str:
    return " ".join((search_term, source_hint, geography_term))


def _build_session(config: PipelineConfig) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": config.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def _collect_property_management_candidates(
    session: requests.Session,
    config: PipelineConfig,
) -> list[CandidateListingUrl]:
    registry_entry = _registry_entry_for("property_manager")
    homepage_result = _fetch_url(session, registry_entry.bootstrap_url, config)
    if not homepage_result["ok"]:
        return []

    homepage_soup = BeautifulSoup(str(homepage_result["text"]), "html.parser")
    state_page_url = _extract_hoa_state_page_url(homepage_soup, "new york", registry_entry.bootstrap_url)
    if not state_page_url:
        return []

    state_page_result = _fetch_url(session, state_page_url, config)
    candidates: list[CandidateListingUrl] = []

    candidates.append(
        CandidateListingUrl(
            source_registry_id=registry_entry.source_registry_id,
            discovery_candidate_id=_candidate_id(
                registry_entry.source_registry_id,
                state_page_url,
                "new-york",
                "New York",
            ),
            niche="property_manager",
            source_name=registry_entry.source_name,
            source_type=registry_entry.source_type,
            source_priority=registry_entry.source_priority,
            discovery_query=_preferred_query("property_manager", "new-york", "directory"),
            geography_slug="new-york",
            geography_name="New York",
            candidate_url=state_page_url,
            discovery_source_url=registry_entry.bootstrap_url,
            discovered_at=_timestamp_now(),
            status="discovered" if state_page_result["ok"] else "fetch_failed",
            http_status=state_page_result["status_code"],
            failure_reason=state_page_result["failure_reason"],
        )
    )

    if state_page_result["ok"]:
        state_soup = BeautifulSoup(str(state_page_result["text"]), "html.parser")
        seen_city_urls: set[str] = set()
        for bucket_slug, link_labels in HOA_CITY_LINK_LABELS.items():
            geography = _geography_target_for_slug(bucket_slug)
            for link_label in link_labels:
                city_page_url = _extract_hoa_city_page_url(state_soup, link_label, state_page_url)
                if not city_page_url:
                    continue
                url_key = _canonicalize_url(city_page_url)
                if url_key in seen_city_urls:
                    continue
                seen_city_urls.add(url_key)
                response = _fetch_url(session, city_page_url, config)
                candidates.append(
                    CandidateListingUrl(
                        source_registry_id=registry_entry.source_registry_id,
                        discovery_candidate_id=_candidate_id(
                            registry_entry.source_registry_id, city_page_url, geography.slug, geography.display_name
                        ),
                        niche="property_manager",
                        source_name=registry_entry.source_name,
                        source_type=registry_entry.source_type,
                        source_priority=registry_entry.source_priority,
                        discovery_query=_preferred_query("property_manager", geography.slug, "directory"),
                        geography_slug=geography.slug,
                        geography_name=geography.display_name,
                        candidate_url=city_page_url,
                        discovery_source_url=state_page_url,
                        discovered_at=_timestamp_now(),
                        status="discovered" if response["ok"] else "fetch_failed",
                        http_status=response["status_code"],
                        failure_reason=response["failure_reason"],
                    )
                )

        # New York state directory is statewide; if no city link was found for a bucket, the same
        # state page is the honest broader fallback (not a local precision claim).
        for slug, display in (("long-island", "Long Island"), ("westchester", "Westchester")):
            if any(c.geography_slug == slug for c in candidates):
                continue
            candidates.append(
                CandidateListingUrl(
                    source_registry_id=registry_entry.source_registry_id,
                    discovery_candidate_id=_candidate_id(
                        registry_entry.source_registry_id, state_page_url, slug, display
                    ),
                    niche="property_manager",
                    source_name=registry_entry.source_name,
                    source_type=registry_entry.source_type,
                    source_priority=registry_entry.source_priority,
                    discovery_query=_preferred_query("property_manager", slug, "directory"),
                    geography_slug=slug,
                    geography_name=display,
                    candidate_url=state_page_url,
                    discovery_source_url=state_page_url,
                    discovered_at=_timestamp_now(),
                    status="discovered" if state_page_result["ok"] else "fetch_failed",
                    http_status=state_page_result["status_code"],
                    failure_reason=state_page_result["failure_reason"],
                )
            )

    return candidates


def _collect_ids_candidates(
    session: requests.Session,
    config: PipelineConfig,
) -> list[CandidateListingUrl]:
    registry_entry = _registry_entry_for("interior_designer")
    homepage_result = _fetch_url(session, registry_entry.bootstrap_url, config)
    homepage_html = homepage_result["text"] if homepage_result["ok"] else ""
    ny_region_url = _extract_ids_region_url(homepage_html, state_code="NY")
    candidates: list[CandidateListingUrl] = []

    if not homepage_result["ok"]:
        for geography in NEW_YORK_GEOGRAPHIES:
            candidates.append(
                CandidateListingUrl(
                    source_registry_id=registry_entry.source_registry_id,
                    discovery_candidate_id=_candidate_id(
                        registry_entry.source_registry_id,
                        registry_entry.bootstrap_url,
                        geography.slug,
                        geography.display_name,
                    ),
                    niche="interior_designer",
                    source_name=registry_entry.source_name,
                    source_type=registry_entry.source_type,
                    source_priority=registry_entry.source_priority,
                    discovery_query=_preferred_query("interior_designer", geography.slug, "member directory"),
                    geography_slug=geography.slug,
                    geography_name=geography.display_name,
                    candidate_url=registry_entry.bootstrap_url,
                    discovery_source_url=registry_entry.bootstrap_url,
                    discovered_at=_timestamp_now(),
                    status="fetch_failed",
                    http_status=homepage_result["status_code"],
                    failure_reason=homepage_result["failure_reason"],
                )
            )
        return candidates

    # The IDS "NY" link is a region-scale listing (not city- or county-specific). One URL backs all
    # New York bucket choices; we emit one row per bucket so the pipeline can filter by slug
    # without claiming borough- or local precision.
    if ny_region_url:
        region_result = _fetch_url(session, ny_region_url, config)
        region_status = "discovered" if region_result["ok"] else "fetch_failed"
        region_http = region_result["status_code"]
        region_fail = region_result["failure_reason"]
    else:
        region_status = "bootstrap_only"
        region_http = homepage_result["status_code"]
        region_fail = "NY region link not discovered from IDS homepage."

    for geography in NEW_YORK_GEOGRAPHIES:
        candidates.append(
            CandidateListingUrl(
                source_registry_id=registry_entry.source_registry_id,
                discovery_candidate_id=_candidate_id(
                    registry_entry.source_registry_id,
                    ny_region_url or registry_entry.bootstrap_url,
                    geography.slug,
                    geography.display_name,
                ),
                niche="interior_designer",
                source_name=registry_entry.source_name,
                source_type=registry_entry.source_type,
                source_priority=registry_entry.source_priority,
                discovery_query=_preferred_query("interior_designer", geography.slug, "member directory"),
                geography_slug=geography.slug,
                geography_name=geography.display_name,
                candidate_url=ny_region_url or registry_entry.bootstrap_url,
                discovery_source_url=registry_entry.bootstrap_url,
                discovered_at=_timestamp_now(),
                status=region_status if ny_region_url else "bootstrap_only",
                http_status=region_http,
                failure_reason=region_fail if ny_region_url else "NY region link not discovered from IDS homepage.",
            )
        )

    return candidates


IDS_NY_REGION_FALLBACK_URL = "https://www.theidslist.com/mid-atlantic"


def _extract_ids_region_url(homepage_html: str, *, state_code: str) -> str:
    match = re.search(
        rf"ivalue\['{re.escape(state_code)}'\]\s*=\s*'(?P<url>https://www\.theidslist\.com/[^']+)'",
        homepage_html,
    )
    if match:
        return match.group("url")
    # Homepage no longer always embeds the legacy `ivalue` map. The Mid-Atlantic listing is still
    # the real regional directory the site serves for NY-adjacent buckets (regional, not city-precise).
    if state_code == "NY":
        return IDS_NY_REGION_FALLBACK_URL
    return ""


def _extract_hoa_state_page_url(soup: BeautifulSoup, state_name: str, base_url: str) -> str:
    for anchor in soup.select("a[href]"):
        title = (anchor.get("title") or "").strip().lower()
        text = _normalize_link_text(anchor.get_text(" ", strip=True))
        href = (anchor.get("href") or "").strip()
        if state_name in title or text == state_name:
            return _absolute_url(base_url, href)
    return ""


def _extract_hoa_city_page_url(soup: BeautifulSoup, geography_name: str, base_url: str) -> str:
    target = _normalize_link_text(geography_name)
    for anchor in soup.select("a[href]"):
        if _normalize_link_text(anchor.get_text(" ", strip=True)) == target:
            return _absolute_url(base_url, (anchor.get("href") or "").strip())
    return ""


def _classify_listing_page(
    *,
    niche: str,
    url: str,
    html: str,
    http_status: int | None,
) -> tuple[str, str]:
    if http_status is None or http_status >= 400:
        return "rejected_fetch_failed", f"Unable to verify listing page content: HTTP {http_status or 'error'}."

    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.get_text(" ", strip=True) if soup.title else "").lower()
    text = soup.get_text(" ", strip=True).lower()
    path = urlparse(url).path.rstrip("/")

    if niche == "property_manager":
        if "hoa management companies in new york" in title or "property management companies for hoa in new york by city" in text:
            return "accepted_listing_page", "New York state directory page: regional coverage; city subpages (when found) are tagged to broader geography buckets, not as precise as five boroughs."
        if "hoa management company" in text and "/new-york" in path:
            return "accepted_listing_page", "HOA city or sub-area listing under New York: source-backed, bucket-tagged, not a guarantee of a single city."
        if "hoa management company" in text and any(
            token in path
            for token in (
                "/brooklyn",
                "/queens",
                "/manhattan",
                "/bronx",
                "/staten-island",
                "/nassau",
                "/suffolk",
                "/long-island",
                "/westchester",
                "/yonkers",
                "/white-plains",
            )
        ):
            return "accepted_listing_page", "HOA sub-area listing page: bucket-level geographic targeting, not a promise of an exact city boundary from the source."
        return "rejected_irrelevant", "Property management candidate did not match the expected HOA state/city listing pattern."

    if niche == "interior_designer":
        profile_links = _extract_ids_profile_links(soup)
        if path.endswith("/mid-atlantic") and len(profile_links) >= 5:
            return (
                "accepted_listing_page",
                "IDS region listing (Mid-Atlantic / NY) — regional, not a city- or county-specific directory.",
            )
        if "the ids list" in title and "designer" in text:
            return "review_listing_page", "Page looks related to IDS designer discovery but did not meet the profile-link threshold."
        return "rejected_irrelevant", "Interior designer candidate did not match the expected IDS region listing pattern."

    return "rejected_irrelevant", "Unsupported discovery niche."


def _extract_ids_profile_links(soup: BeautifulSoup) -> set[str]:
    blocked_paths = {
        "/",
        "/designers",
        "/designer",
        "/search",
        "/homeowner",
        "/homeowner-2",
        "/homeowner-blog",
        "/featuredinteriors",
        "/contact-ids",
        "/request-help",
        "/find-your-way",
        "/find-your-style",
    }
    profile_links: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href.startswith("/"):
            continue
        if href in blocked_paths or href.startswith("/#"):
            continue
        if "?" in href or "#" in href:
            continue
        if href.count("/") != 1:
            continue
        profile_links.add(href)
    return profile_links


def _fetch_url(
    session: requests.Session,
    url: str,
    config: PipelineConfig,
) -> dict[str, object]:
    last_status: int | None = None
    last_failure = ""

    for attempt in range(config.retry_attempts + 1):
        if attempt > 0:
            time.sleep(config.crawl_delay_seconds)

        try:
            response = session.get(url, timeout=config.request_timeout_seconds)
            last_status = response.status_code
            if 200 <= response.status_code < 400:
                _sleep_for_rate_limit(url, config)
                return {
                    "ok": True,
                    "status_code": response.status_code,
                    "failure_reason": "",
                    "text": response.text,
                }
            last_failure = f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            last_failure = str(exc)

    _sleep_for_rate_limit(url, config)
    return {
        "ok": False,
        "status_code": last_status,
        "failure_reason": last_failure,
        "text": "",
    }


def _sleep_for_rate_limit(url: str, config: PipelineConfig) -> None:
    # Assignment-grade crawl politeness: a fixed delay is enough here.
    if urlparse(url).netloc:
        time.sleep(config.crawl_delay_seconds)


def _preferred_query(niche: str, geography_slug: str, source_hint: str) -> str:
    if not DISCOVERY_SEED_LOOKUP:
        for seed in build_discovery_seeds():
            DISCOVERY_SEED_LOOKUP.setdefault((seed.niche, seed.geography_slug, seed.source_hint), seed.query)

    key = (niche, geography_slug, source_hint)
    if key not in DISCOVERY_SEED_LOOKUP:
        raise ValueError(
            f"Missing discovery seed for niche={niche}, geography={geography_slug}, source_hint={source_hint}"
        )
    return DISCOVERY_SEED_LOOKUP[key]


def _registry_entry_for(niche: str) -> DiscoveryRegistryEntry:
    for entry in DISCOVERY_SOURCE_REGISTRY:
        if entry.niche == niche:
            return entry
    raise ValueError(f"Missing source registry entry for niche: {niche}")


def _snapshot_token(timestamp: str) -> str:
    return timestamp.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    normalized_path = parsed.path.rstrip("/") or "/"
    return f"{parsed.scheme}://{parsed.netloc}{normalized_path}"


def _absolute_url(base_url: str, href: str) -> str:
    return requests.compat.urljoin(base_url, href)


def _normalize_link_text(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _candidate_id(source_registry_id: str, candidate_url: str, geography_slug: str, geography_name: str) -> str:
    return "discovery_candidate_" + _stable_token(
        f"{source_registry_id}|{candidate_url}|{geography_slug}|{geography_name}"
    )


def _listing_page_id(source_registry_id: str, canonical_url: str) -> str:
    return "listing_page_" + _stable_token(f"{source_registry_id}|{canonical_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discovery utilities for the lead enrichment pipeline.")
    parser.add_argument(
        "command",
        choices=("seeds", "candidates", "classify", "registry"),
        nargs="?",
        default="seeds",
        help="Which discovery artifact to generate.",
    )
    args = parser.parse_args()

    if args.command == "seeds":
        output_path = write_discovery_seeds()
        print(f"Wrote discovery seeds to {output_path}")
    elif args.command == "candidates":
        output_path = write_candidate_listing_urls()
        print(f"Wrote candidate listing URLs to {output_path}")
    elif args.command == "classify":
        output_path = write_classified_listing_pages()
        print(f"Wrote classified listing pages to {output_path}")
    else:
        output_path = write_source_registry()
        print(f"Wrote source registry to {output_path}")
