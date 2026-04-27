from __future__ import annotations

from config import PipelineConfig
from dedupe import apply_identity_resolution
from enrich import enrich_records
from export import export_final_csv, export_master_csv, prepare_final_export, write_quality_summary
from extract import extract_interior_designers, extract_property_managers
from normalize import normalize_records


def run() -> None:
    config = PipelineConfig()
    config.ensure_directories()

    records = extract_property_managers(config) + extract_interior_designers(config)
    enriched_records = enrich_records(records, config)
    normalized = normalize_records(enriched_records)
    deduped = apply_identity_resolution(normalized)

    if deduped.empty:
        print("Pipeline scaffold executed. No records extracted yet.")
        return

    final_export = prepare_final_export(deduped)
    export_master_csv(deduped, config.final_dir / "leads_master.csv")
    export_final_csv(deduped, config.final_dir / "leads.csv")
    write_quality_summary(
        raw_record_count=len(records),
        processed_dataframe=deduped,
        final_dataframe=final_export,
        output_path=config.docs_dir / "quality-summary.md",
    )
    print(f"Exported {len(final_export)} rows to {config.final_dir / 'leads.csv'}")


if __name__ == "__main__":
    run()
