from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from config import FINAL_DIR, PROJECT_ROOT, PipelineConfig
from discovery import NEW_YORK_GEOGRAPHIES


SUPPORTED_NICHES: dict[str, dict[str, object]] = {
    "property_manager": {
        "slug": "property-managers",
        "display_name": "Property Managers",
        "aliases": {
            "property manager",
            "property managers",
            "property-management",
            "property-management-companies",
            "property management",
            "property management companies",
        },
    },
    "interior_designer": {
        "slug": "interior-designers",
        "display_name": "Interior Designers",
        "aliases": {
            "interior designer",
            "interior designers",
            "interior decorator",
            "interior decorators",
            "interior design firms",
            "interior design",
        },
    },
}

PLACE_ALIASES = {
    "new-york": "new-york-city",
    "nyc": "new-york-city",
}


@dataclass(frozen=True)
class RunContext:
    run_id: str
    niche_input: str
    place_input: str
    niche_key: str
    niche_slug: str
    niche_display_name: str
    place_slug: str
    place_display_name: str
    output_dir: Path
    project_root: Path = PROJECT_ROOT

    @property
    def raw_dir(self) -> Path:
        return self.output_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.output_dir / "processed"

    @property
    def quality_summary_path(self) -> Path:
        return self.output_dir / "quality_summary.md"

    @property
    def run_metadata_path(self) -> Path:
        return self.output_dir / "run_metadata.json"

    def build_config(self) -> PipelineConfig:
        return PipelineConfig(
            project_root=self.project_root,
            raw_dir=self.raw_dir,
            processed_dir=self.processed_dir,
            final_dir=self.output_dir,
            docs_dir=self.output_dir,
            discovery_seeds_output_path=self.processed_dir / "discovery_seeds.json",
            classified_listing_pages_output_path=self.processed_dir / "listing_pages_classified.json",
            source_registry_output_path=self.processed_dir / "source_registry.json",
            discovery_raw_output_path=self.raw_dir / "discovery_candidates_raw.json",
            website_page_attempts_output_path=self.raw_dir / "website_page_attempts_raw.json",
            website_contacts_output_path=self.processed_dir / "website_contacts.json",
        )

    def output_paths(self) -> dict[str, str]:
        return {
            "run_dir": _relative_path(self.output_dir, self.project_root),
            "master_csv": _relative_path(self.output_dir / "leads_master.csv", self.project_root),
            "outreach_ready_csv": _relative_path(self.output_dir / "leads_outreach_ready.csv", self.project_root),
            "run_metadata": _relative_path(self.run_metadata_path, self.project_root),
            "quality_summary": _relative_path(self.quality_summary_path, self.project_root),
            "raw_dir": _relative_path(self.raw_dir, self.project_root),
            "processed_dir": _relative_path(self.processed_dir, self.project_root),
        }

    def metadata_payload(
        self,
        *,
        started_at: str,
        status: str,
        finished_at: str | None = None,
        key_counts: dict[str, int] | None = None,
        error_summary: str = "",
    ) -> dict[str, Any]:
        payload = {
            "run_id": self.run_id,
            "niche_input": self.niche_input,
            "place_input": self.place_input,
            "niche_slug": self.niche_slug,
            "place_slug": self.place_slug,
            "niche_key": self.niche_key,
            "niche_display_name": self.niche_display_name,
            "place_display_name": self.place_display_name,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "output_paths": self.output_paths(),
            "key_counts": key_counts or {},
            "error_summary": error_summary,
        }
        return payload

    def write_metadata(
        self,
        *,
        started_at: str,
        status: str,
        finished_at: str | None = None,
        key_counts: dict[str, int] | None = None,
        error_summary: str = "",
    ) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        payload = self.metadata_payload(
            started_at=started_at,
            status=status,
            finished_at=finished_at,
            key_counts=key_counts,
            error_summary=error_summary,
        )
        self.run_metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload


def slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return normalized or "run"


def generate_run_id(now: datetime | None = None) -> str:
    moment = now or datetime.now(UTC)
    return moment.strftime("%Y-%m-%dt%H%M%Sz").lower()


def resolve_run_context(
    niche_input: str,
    place_input: str,
    *,
    run_id: str | None = None,
    output_dir: str | Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> RunContext:
    if not niche_input or not niche_input.strip():
        raise ValueError("A niche input is required.")
    if not place_input or not place_input.strip():
        raise ValueError("A place input is required.")

    niche_key, niche_slug, niche_display_name = _resolve_niche(niche_input)
    place_slug, place_display_name = _resolve_place(place_input)
    resolved_run_id = run_id or generate_run_id()
    resolved_output_dir = Path(output_dir) if output_dir else FINAL_DIR / niche_slug / place_slug / resolved_run_id

    return RunContext(
        run_id=resolved_run_id,
        niche_input=niche_input.strip(),
        place_input=place_input.strip(),
        niche_key=niche_key,
        niche_slug=niche_slug,
        niche_display_name=niche_display_name,
        place_slug=place_slug,
        place_display_name=place_display_name,
        output_dir=resolved_output_dir,
        project_root=project_root,
    )


def load_run_metadata(metadata_path: Path) -> dict[str, Any]:
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def resolve_run_dir(
    niche_slug: str,
    place_slug: str,
    run_id: str,
    *,
    final_root: Path = FINAL_DIR,
) -> Path:
    return final_root / slugify(niche_slug) / slugify(place_slug) / run_id


def list_run_metadata(
    *,
    niche_slug: str | None = None,
    place_slug: str | None = None,
    limit: int | None = None,
    final_root: Path = FINAL_DIR,
) -> list[dict[str, Any]]:
    path_parts = [final_root]
    if niche_slug:
        path_parts.append(Path(slugify(niche_slug)))
    if place_slug:
        path_parts.append(Path(slugify(place_slug)))

    root = Path(*path_parts)
    if not root.exists():
        return []

    metadata_rows: list[dict[str, Any]] = []
    for metadata_path in root.glob("**/run_metadata.json"):
        try:
            metadata = load_run_metadata(metadata_path)
        except (OSError, json.JSONDecodeError):
            continue
        metadata["_metadata_path"] = _relative_path(metadata_path, PROJECT_ROOT)
        metadata_rows.append(metadata)

    metadata_rows.sort(
        key=lambda row: str(row.get("started_at") or row.get("run_id") or ""),
        reverse=True,
    )

    if limit is not None:
        return metadata_rows[:limit]
    return metadata_rows


def _resolve_niche(niche_input: str) -> tuple[str, str, str]:
    normalized = slugify(niche_input)
    for niche_key, metadata in SUPPORTED_NICHES.items():
        aliases = {slugify(alias) for alias in metadata["aliases"]} | {slugify(niche_key), str(metadata["slug"])}
        if normalized in aliases:
            return niche_key, str(metadata["slug"]), str(metadata["display_name"])
    supported = ", ".join(sorted(str(metadata["display_name"]) for metadata in SUPPORTED_NICHES.values()))
    raise ValueError(f"Unsupported niche '{niche_input}'. Supported niches: {supported}.")


def _resolve_place(place_input: str) -> tuple[str, str]:
    normalized = slugify(place_input)
    normalized = PLACE_ALIASES.get(normalized, normalized)

    for geography in NEW_YORK_GEOGRAPHIES:
        aliases = {slugify(term) for term in geography.discovery_terms} | {slugify(geography.display_name), geography.slug}
        if normalized in aliases:
            return geography.slug, geography.display_name

    supported = ", ".join(geography.display_name for geography in NEW_YORK_GEOGRAPHIES)
    raise ValueError(
        f"Unsupported place '{place_input}'. This showcase only supports New York targets: {supported}."
    )


def _relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)
