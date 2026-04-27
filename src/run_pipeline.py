from __future__ import annotations

import argparse

from config import PipelineConfig
from dedupe import apply_identity_resolution
from discovery import (
    classify_candidate_listing_urls,
    collect_candidate_listing_urls,
    write_discovery_seeds,
    write_candidate_listing_urls,
    write_classified_listing_pages,
    write_source_registry,
)
from enrich import enrich_records
from export import (
    export_master_csv,
    export_outreach_ready_csv,
    prepare_master_export,
    prepare_outreach_ready_export,
    write_quality_summary,
)
from extract import extract_interior_designers, extract_property_managers
from normalize import normalize_records
from runs import RunContext, resolve_run_context


def run(
    *,
    niche_input: str,
    place_input: str,
    run_id: str | None = None,
    output_dir: str | None = None,
) -> dict[str, object]:
    context = resolve_run_context(
        niche_input=niche_input,
        place_input=place_input,
        run_id=run_id,
        output_dir=output_dir,
    )
    return run_with_context(context)


def run_with_context(context: RunContext) -> dict[str, object]:
    config = context.build_config()
    config.ensure_directories()
    started_at = _utc_now()
    context.write_metadata(started_at=started_at, status="running")

    try:
        write_discovery_seeds(config=config)
        write_source_registry(config=config)
        candidate_listing_urls = _filtered_candidates(context, collect_candidate_listing_urls(config))
        write_candidate_listing_urls(config=config, candidates=candidate_listing_urls)
        classified_listing_pages = classify_candidate_listing_urls(config, candidate_listing_urls)
        write_classified_listing_pages(config=config, classified_rows=classified_listing_pages)
        accepted_listing_pages = [
            page
            for page in classified_listing_pages
            if page.listing_page_status == "accepted_listing_page"
        ]

        records = _extract_records_for_niche(context, config, accepted_listing_pages)
        enriched_records = enrich_records(records, config)
        normalized = normalize_records(enriched_records)
        deduped = apply_identity_resolution(normalized)

        master_export = prepare_master_export(deduped)
        outreach_ready_export = prepare_outreach_ready_export(deduped)
        export_master_csv(deduped, config.master_output_path)
        export_outreach_ready_csv(deduped, config.outreach_ready_output_path)
        write_quality_summary(
            raw_record_count=len(records),
            processed_dataframe=deduped,
            master_dataframe=master_export,
            outreach_ready_dataframe=outreach_ready_export,
            output_path=config.quality_summary_output_path,
            discovery_raw_output_path=config.discovery_raw_output_path,
            classified_listing_pages_output_path=config.classified_listing_pages_output_path,
        )

        finished_at = _utc_now()
        key_counts = {
            "raw_discovery_url_count": len(candidate_listing_urls),
            "accepted_listing_page_count": len(accepted_listing_pages),
            "extracted_lead_count": len(records),
            "master_row_count": len(master_export),
            "outreach_ready_row_count": len(outreach_ready_export),
        }
        metadata = context.write_metadata(
            started_at=started_at,
            finished_at=finished_at,
            status="completed",
            key_counts=key_counts,
        )
    except Exception as error:
        finished_at = _utc_now()
        context.write_metadata(
            started_at=started_at,
            finished_at=finished_at,
            status="failed",
            error_summary=f"{type(error).__name__}: {error}",
        )
        raise

    print(f"Exported {len(master_export)} rows to {config.master_output_path}")
    print(f"Exported {len(outreach_ready_export)} rows to {config.outreach_ready_output_path}")
    return metadata


def _filtered_candidates(context: RunContext, candidates: list[object]) -> list[object]:
    return [
        candidate
        for candidate in candidates
        if getattr(candidate, "niche", "") == context.niche_key
        and getattr(candidate, "geography_slug", "") == context.place_slug
    ]


def _extract_records_for_niche(
    context: RunContext,
    config: PipelineConfig,
    accepted_listing_pages: list[object],
) -> list[dict[str, object]]:
    if context.niche_key == "property_manager":
        return extract_property_managers(config, accepted_listing_pages)
    if context.niche_key == "interior_designer":
        return extract_interior_designers(config, accepted_listing_pages)
    raise ValueError(f"Unsupported niche key '{context.niche_key}'.")


def _utc_now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the lead extraction pipeline for one niche/place showcase run.")
    parser.add_argument("--niche", required=True, help="Lead niche to run, for example 'property managers'.")
    parser.add_argument("--place", required=True, help="New York place to target, for example 'Brooklyn'.")
    parser.add_argument("--run-id", help="Optional explicit run id. Defaults to a UTC timestamp token.")
    parser.add_argument(
        "--output-dir",
        help="Optional explicit output directory. Defaults to data/final/{niche_slug}/{place_slug}/{run_id}/.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(niche_input=args.niche, place_input=args.place, run_id=args.run_id, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
