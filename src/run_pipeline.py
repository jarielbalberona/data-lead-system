from __future__ import annotations

from config import PipelineConfig
from dedupe import apply_identity_resolution
from discovery import (
    classify_candidate_listing_urls,
    collect_candidate_listing_urls,
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


def run() -> None:
    config = PipelineConfig()
    config.ensure_directories()

    write_source_registry(config=config)
    candidate_listing_urls = collect_candidate_listing_urls(config)
    write_candidate_listing_urls(config=config, candidates=candidate_listing_urls)
    classified_listing_pages = classify_candidate_listing_urls(config, candidate_listing_urls)
    write_classified_listing_pages(config=config, classified_rows=classified_listing_pages)
    accepted_listing_pages = [
        page
        for page in classified_listing_pages
        if page.listing_page_status == "accepted_listing_page"
    ]

    records = extract_property_managers(config, accepted_listing_pages) + extract_interior_designers(
        config,
        accepted_listing_pages,
    )
    enriched_records = enrich_records(records, config)
    normalized = normalize_records(enriched_records)
    deduped = apply_identity_resolution(normalized)

    if deduped.empty:
        print("Pipeline scaffold executed. No records extracted yet.")
        return

    master_export = prepare_master_export(deduped)
    outreach_ready_export = prepare_outreach_ready_export(deduped)
    export_master_csv(deduped, config.final_dir / "leads_master.csv")
    export_outreach_ready_csv(deduped, config.final_dir / "leads_outreach_ready.csv")
    write_quality_summary(
        raw_record_count=len(records),
        processed_dataframe=deduped,
        master_dataframe=master_export,
        outreach_ready_dataframe=outreach_ready_export,
        output_path=config.docs_dir / "quality-summary.md",
        discovery_raw_output_path=config.discovery_raw_output_path,
        classified_listing_pages_output_path=config.classified_listing_pages_output_path,
    )
    print(f"Exported {len(master_export)} rows to {config.final_dir / 'leads_master.csv'}")
    print(f"Exported {len(outreach_ready_export)} rows to {config.final_dir / 'leads_outreach_ready.csv'}")


if __name__ == "__main__":
    run()
