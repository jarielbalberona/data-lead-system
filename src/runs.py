from __future__ import annotations

import json
import os
import re
import signal
import shutil
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
        pipeline_pid: int | None = None,
        stop_reason: str = "",
        result_summary: str = "",
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
            "pipeline_pid": pipeline_pid,
            "stop_reason": stop_reason,
            "result_summary": result_summary,
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
        pipeline_pid: int | None = None,
        stop_reason: str = "",
        result_summary: str | None = None,
    ) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        existing: dict[str, Any] = {}
        if self.run_metadata_path.exists():
            try:
                existing = load_run_metadata(self.run_metadata_path)
            except (OSError, json.JSONDecodeError):
                existing = {}
        if result_summary is not None:
            resolved_result_summary = result_summary
        else:
            resolved_result_summary = str(existing.get("result_summary", ""))
        payload = self.metadata_payload(
            started_at=started_at,
            status=status,
            finished_at=finished_at,
            key_counts=key_counts,
            error_summary=error_summary,
            pipeline_pid=existing.get("pipeline_pid") if pipeline_pid is None else pipeline_pid,
            stop_reason=stop_reason or str(existing.get("stop_reason", "")),
            result_summary=resolved_result_summary,
        )
        self.run_metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload

    @classmethod
    def from_metadata(cls, metadata: dict[str, Any], *, project_root: Path = PROJECT_ROOT) -> "RunContext":
        return cls(
            run_id=str(metadata["run_id"]),
            niche_input=str(metadata.get("niche_input", metadata.get("niche_display_name", ""))),
            place_input=str(metadata.get("place_input", metadata.get("place_display_name", ""))),
            niche_key=str(metadata["niche_key"]),
            niche_slug=str(metadata["niche_slug"]),
            niche_display_name=str(metadata["niche_display_name"]),
            place_slug=str(metadata["place_slug"]),
            place_display_name=str(metadata["place_display_name"]),
            output_dir=resolve_run_dir(
                str(metadata["niche_slug"]),
                str(metadata["place_slug"]),
                str(metadata["run_id"]),
            ),
            project_root=project_root,
        )


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


def build_completed_run_result_summary(key_counts: dict[str, int]) -> str:
    """Human-readable explanation of pipeline outcome for a completed run (including zero-lead cases)."""
    raw = int(key_counts.get("raw_discovery_url_count", 0))
    accepted = int(key_counts.get("accepted_listing_page_count", 0))
    extracted = int(key_counts.get("extracted_lead_count", 0))
    master = int(key_counts.get("master_row_count", 0))
    outreach = int(key_counts.get("outreach_ready_row_count", 0))

    lines: list[str] = [
        "Run finished successfully. Stage counts:",
        f"· Discovery URLs (raw): {raw}",
        f"· Listing pages accepted for extraction: {accepted}",
        f"· Rows extracted (before dedupe): {extracted}",
        f"· Master export rows: {master}",
        f"· Outreach-ready rows: {outreach}",
    ]

    if raw == 0:
        lines.append("")
        lines.append(
            "No candidate listing URLs were found for this niche and geography. "
            "The pipeline did not fail—there was nothing to fetch for this combination."
        )
    elif accepted == 0:
        lines.append("")
        lines.append(
            f"URLs were discovered ({raw}) but none were classified as accepted listing pages. "
            "Classifier output may help: see processed/listing_pages_classified.json."
        )
    elif extracted == 0:
        lines.append("")
        lines.append(
            f"{accepted} listing page(s) were accepted, but HTML extraction produced no lead rows. "
            "Pages may not match the extractor, or listed no parseable businesses."
        )
    elif master == 0:
        lines.append("")
        lines.append(
            "Extraction produced rows, but the master dataset is empty after normalization and validation. "
            "See quality_summary.md and raw/processed artifacts."
        )
    elif outreach == 0 and master > 0:
        lines.append("")
        lines.append(
            f"Master has {master} row(s), but outreach-ready is empty: "
            "rows may be invalid, marked duplicate, or fail contact/website checks for outreach."
        )
    else:
        lines.append("")
        lines.append(
            f"Leads are in the CSV exports ({master} master row(s), {outreach} outreach-ready row(s))."
        )

    return "\n".join(lines)


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


def stop_run_from_metadata(metadata: dict[str, Any], *, reason: str = "Stopped by operator") -> dict[str, Any]:
    context = RunContext.from_metadata(metadata)
    pid = _parse_pid(metadata.get("pipeline_pid"))
    finished_at = _utc_now()

    if str(metadata.get("status", "")).strip() != "running":
        return context.write_metadata(
            started_at=str(metadata.get("started_at") or finished_at),
            finished_at=str(metadata.get("finished_at") or finished_at),
            status=str(metadata.get("status") or "stopped"),
            key_counts=_coerce_key_counts(metadata.get("key_counts")),
            error_summary=str(metadata.get("error_summary", "")),
            pipeline_pid=pid,
            stop_reason=reason if str(metadata.get("status", "")).strip() == "stopped" else "",
        )

    if pid is not None:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            reason = "Process was already gone when stop was requested"
        except PermissionError as error:
            reason = f"PermissionError: {error}"

    return context.write_metadata(
        started_at=str(metadata.get("started_at") or finished_at),
        finished_at=finished_at,
        status="stopped",
        key_counts=_coerce_key_counts(metadata.get("key_counts")),
        error_summary=str(metadata.get("error_summary", "")),
        pipeline_pid=pid,
        stop_reason=reason,
    )


def delete_run_dir(run_dir: Path, metadata: dict[str, Any] | None = None) -> None:
    if metadata is not None and str(metadata.get("status", "")).strip() == "running":
        stop_run_from_metadata(metadata, reason="Stopped before deletion")
    if run_dir.exists():
        shutil.rmtree(run_dir)


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


def _coerce_key_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    counts: dict[str, int] = {}
    for key, item in value.items():
        try:
            counts[str(key)] = int(item)
        except (TypeError, ValueError):
            continue
    return counts


def _parse_pid(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
