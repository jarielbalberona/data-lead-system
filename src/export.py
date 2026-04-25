from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd


FINAL_EXPORT_COLUMNS = [
    "lead_id",
    "niche",
    "business_name",
    "phone",
    "email",
    "website",
    "address",
    "city",
    "state",
    "source_url",
    "contact_group_id",
    "business_group_id",
    "location_group_id",
    "dedupe_status",
    "dedupe_reason",
    "outreach_suppression_key",
    "quality_score",
    "extraction_timestamp",
    "website_domain",
    "normalized_phone",
    "normalized_email",
    "validation_status",
    "rejection_reason",
]


def _build_lead_id(row: pd.Series) -> str:
    digest_input = "|".join(
        [
            str(row.get("niche", "")).strip(),
            str(row.get("business_name", "")).strip(),
            str(row.get("source_url", "")).strip(),
            str(row.get("normalized_address", row.get("address", ""))).strip(),
        ]
    )
    return f"lead_{hashlib.sha1(digest_input.encode('utf-8')).hexdigest()[:12]}"


def _quality_score(row: pd.Series) -> int:
    score = 0
    if str(row.get("normalized_phone", "")).strip():
        score += 30
    if str(row.get("website_domain", "")).strip():
        score += 25
    if str(row.get("normalized_email", "")).strip():
        score += 25
    if (
        str(row.get("address", "")).strip()
        and str(row.get("city", "")).strip()
        and str(row.get("state", "")).strip()
    ):
        score += 10
    if bool(row.get("matches_target_niche", False)):
        score += 10
    return score


def prepare_final_export(dataframe: pd.DataFrame) -> pd.DataFrame:
    prepared = dataframe.copy()
    if prepared.empty:
        return prepared

    prepared = prepared[
        (prepared["validation_status"] == "valid")
        & (prepared["dedupe_status"] != "confirmed_duplicate")
    ].copy()

    prepared["lead_id"] = prepared.apply(_build_lead_id, axis=1)
    prepared["quality_score"] = prepared.apply(_quality_score, axis=1)

    prepared = prepared.sort_values(
        by=["niche", "quality_score", "business_name", "city"],
        ascending=[True, False, True, True],
    ).reset_index(drop=True)

    if len(prepared) > 100:
        prepared = prepared.head(100).copy()

    for column_name in FINAL_EXPORT_COLUMNS:
        if column_name not in prepared.columns:
            prepared[column_name] = ""

    return prepared[FINAL_EXPORT_COLUMNS]


def export_final_csv(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prepare_final_export(dataframe).to_csv(output_path, index=False)
