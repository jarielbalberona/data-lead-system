from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

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


def _normalize_query(search_term: str, geography_term: str, source_hint: str) -> str:
    return " ".join((search_term, source_hint, geography_term))


if __name__ == "__main__":
    output_path = write_discovery_seeds()
    print(f"Wrote discovery seeds to {output_path}")
