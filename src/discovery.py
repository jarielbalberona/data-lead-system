from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
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
    niche: str
    source_name: str
    source_type: str
    source_priority: int
    bootstrap_url: str


@dataclass(frozen=True)
class CandidateListingUrl:
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


NEW_YORK_GEOGRAPHIES: tuple[GeographyTarget, ...] = (
    GeographyTarget(
        slug="new-york-city",
        display_name="New York City",
        state="NY",
        discovery_terms=("New York City", "NYC", "New York NY"),
        relevance_terms=("New York City", "NYC", "New York", "NY"),
    ),
    GeographyTarget(
        slug="brooklyn",
        display_name="Brooklyn",
        state="NY",
        discovery_terms=("Brooklyn", "Brooklyn NY"),
        relevance_terms=("Brooklyn", "NYC", "New York", "NY"),
    ),
    GeographyTarget(
        slug="queens",
        display_name="Queens",
        state="NY",
        discovery_terms=("Queens", "Queens NY"),
        relevance_terms=("Queens", "NYC", "New York", "NY"),
    ),
    GeographyTarget(
        slug="manhattan",
        display_name="Manhattan",
        state="NY",
        discovery_terms=("Manhattan", "Manhattan NY"),
        relevance_terms=("Manhattan", "NYC", "New York", "NY"),
    ),
    GeographyTarget(
        slug="bronx",
        display_name="Bronx",
        state="NY",
        discovery_terms=("Bronx", "Bronx NY"),
        relevance_terms=("Bronx", "NYC", "New York", "NY"),
    ),
    GeographyTarget(
        slug="staten-island",
        display_name="Staten Island",
        state="NY",
        discovery_terms=("Staten Island", "Staten Island NY"),
        relevance_terms=("Staten Island", "NYC", "New York", "NY"),
    ),
    GeographyTarget(
        slug="yonkers",
        display_name="Yonkers",
        state="NY",
        discovery_terms=("Yonkers", "Yonkers NY"),
        relevance_terms=("Yonkers", "New York", "NY"),
    ),
    GeographyTarget(
        slug="white-plains",
        display_name="White Plains",
        state="NY",
        discovery_terms=("White Plains", "White Plains NY"),
        relevance_terms=("White Plains", "Westchester", "New York", "NY"),
    ),
    GeographyTarget(
        slug="new-rochelle",
        display_name="New Rochelle",
        state="NY",
        discovery_terms=("New Rochelle", "New Rochelle NY"),
        relevance_terms=("New Rochelle", "Westchester", "New York", "NY"),
    ),
    GeographyTarget(
        slug="mount-vernon",
        display_name="Mount Vernon",
        state="NY",
        discovery_terms=("Mount Vernon", "Mount Vernon NY"),
        relevance_terms=("Mount Vernon", "Westchester", "New York", "NY"),
    ),
    GeographyTarget(
        slug="long-island",
        display_name="Long Island",
        state="NY",
        discovery_terms=("Long Island", "Long Island NY"),
        relevance_terms=("Long Island", "Nassau", "Suffolk", "New York", "NY"),
    ),
    GeographyTarget(
        slug="hempstead",
        display_name="Hempstead",
        state="NY",
        discovery_terms=("Hempstead", "Hempstead NY"),
        relevance_terms=("Hempstead", "Nassau", "Long Island", "New York", "NY"),
    ),
    GeographyTarget(
        slug="oyster-bay",
        display_name="Oyster Bay",
        state="NY",
        discovery_terms=("Oyster Bay", "Oyster Bay NY"),
        relevance_terms=("Oyster Bay", "Nassau", "Long Island", "New York", "NY"),
    ),
    GeographyTarget(
        slug="huntington",
        display_name="Huntington",
        state="NY",
        discovery_terms=("Huntington", "Huntington NY"),
        relevance_terms=("Huntington", "Suffolk", "Long Island", "New York", "NY"),
    ),
    GeographyTarget(
        slug="brookhaven",
        display_name="Brookhaven",
        state="NY",
        discovery_terms=("Brookhaven", "Brookhaven NY"),
        relevance_terms=("Brookhaven", "Suffolk", "Long Island", "New York", "NY"),
    ),
    GeographyTarget(
        slug="islip",
        display_name="Islip",
        state="NY",
        discovery_terms=("Islip", "Islip NY"),
        relevance_terms=("Islip", "Suffolk", "Long Island", "New York", "NY"),
    ),
)

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

DISCOVERY_SOURCE_REGISTRY: tuple[DiscoveryRegistryEntry, ...] = (
    DiscoveryRegistryEntry(
        niche="property_manager",
        source_name="propertymanagement.com",
        source_type="local_business_directory",
        source_priority=4,
        bootstrap_url="https://propertymanagement.com/location/ny",
    ),
    DiscoveryRegistryEntry(
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
) -> Path:
    runtime_config = config or PipelineConfig()
    path = output_path or runtime_config.discovery_raw_output_path
    candidates = collect_candidate_listing_urls(runtime_config)
    path.parent.mkdir(parents=True, exist_ok=True)
    archive_dir = path.parent / "archive" / path.stem
    archive_dir.mkdir(parents=True, exist_ok=True)

    payload = json.dumps([asdict(candidate) for candidate in candidates], indent=2)
    path.write_text(payload, encoding="utf-8")

    timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    archive_dir.joinpath(f"{_snapshot_token(timestamp)}.json").write_text(payload, encoding="utf-8")
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
    candidates: list[CandidateListingUrl] = []

    for geography in NEW_YORK_GEOGRAPHIES:
        candidate_url = (
            f"https://propertymanagement.com/location/ny/{_property_management_location_slug(geography)}"
        )
        response = _fetch_url(session, candidate_url, config)
        candidates.append(
            CandidateListingUrl(
                niche="property_manager",
                source_name=registry_entry.source_name,
                source_type=registry_entry.source_type,
                source_priority=registry_entry.source_priority,
                discovery_query=_preferred_query("property_manager", geography.slug, "directory"),
                geography_slug=geography.slug,
                geography_name=geography.display_name,
                candidate_url=candidate_url,
                discovery_source_url=registry_entry.bootstrap_url,
                discovered_at=_timestamp_now(),
                status="discovered" if response["ok"] else "fetch_failed",
                http_status=response["status_code"],
                failure_reason=response["failure_reason"],
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
        candidates.append(
            CandidateListingUrl(
                niche="interior_designer",
                source_name=registry_entry.source_name,
                source_type=registry_entry.source_type,
                source_priority=registry_entry.source_priority,
                discovery_query=_preferred_query("interior_designer", "new-york-city", "member directory"),
                geography_slug="new-york-city",
                geography_name="New York City",
                candidate_url=registry_entry.bootstrap_url,
                discovery_source_url=registry_entry.bootstrap_url,
                discovered_at=_timestamp_now(),
                status="fetch_failed",
                http_status=homepage_result["status_code"],
                failure_reason=homepage_result["failure_reason"],
            )
        )
        return candidates

    if ny_region_url:
        region_result = _fetch_url(session, ny_region_url, config)
        candidates.append(
            CandidateListingUrl(
                niche="interior_designer",
                source_name=registry_entry.source_name,
                source_type=registry_entry.source_type,
                source_priority=registry_entry.source_priority,
                discovery_query=_preferred_query("interior_designer", "new-york-city", "member directory"),
                geography_slug="new-york-city",
                geography_name="New York City",
                candidate_url=ny_region_url,
                discovery_source_url=registry_entry.bootstrap_url,
                discovered_at=_timestamp_now(),
                status="discovered" if region_result["ok"] else "fetch_failed",
                http_status=region_result["status_code"],
                failure_reason=region_result["failure_reason"],
            )
        )

    for geography in NEW_YORK_GEOGRAPHIES:
        if geography.slug == "new-york-city":
            continue
        candidates.append(
            CandidateListingUrl(
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
                status="discovered" if ny_region_url else "bootstrap_only",
                http_status=homepage_result["status_code"],
                failure_reason="" if ny_region_url else "NY region link not discovered from IDS homepage.",
            )
        )

    return candidates


def _extract_ids_region_url(homepage_html: str, *, state_code: str) -> str:
    match = re.search(
        rf"ivalue\['{re.escape(state_code)}'\]\s*=\s*'(?P<url>https://www\.theidslist\.com/[^']+)'",
        homepage_html,
    )
    if not match:
        return ""
    return match.group("url")


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


def _property_management_location_slug(geography: GeographyTarget) -> str:
    if geography.slug == "new-york-city":
        return "new-york"
    return geography.slug


def _registry_entry_for(niche: str) -> DiscoveryRegistryEntry:
    for entry in DISCOVERY_SOURCE_REGISTRY:
        if entry.niche == niche:
            return entry
    raise ValueError(f"Missing source registry entry for niche: {niche}")


def _snapshot_token(timestamp: str) -> str:
    return timestamp.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discovery utilities for the lead enrichment pipeline.")
    parser.add_argument(
        "command",
        choices=("seeds", "candidates"),
        nargs="?",
        default="seeds",
        help="Which discovery artifact to generate.",
    )
    args = parser.parse_args()

    if args.command == "seeds":
        output_path = write_discovery_seeds()
        print(f"Wrote discovery seeds to {output_path}")
    else:
        output_path = write_candidate_listing_urls()
        print(f"Wrote candidate listing URLs to {output_path}")
