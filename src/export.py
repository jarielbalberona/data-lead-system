from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

CONFIDENCE_RANK = {"high": 3, "medium": 2, "low": 1, "": 0}

MASTER_EXPORT_COLUMNS = [
    "lead_id",
    "niche",
    "business_name",
    "address",
    "city",
    "state",
    "source_url",
    "source_listing_url",
    "source_directory",
    "source_type",
    "source_priority",
    "discovery_query",
    "discovery_geography",
    "listing_phone",
    "listing_email",
    "website",
    "website_phone",
    "website_email",
    "preferred_phone",
    "preferred_email",
    "phone_source_url",
    "email_source_url",
    "phone_extraction_method",
    "email_extraction_method",
    "phone_confidence",
    "email_confidence",
    "website_validation_status",
    "website_validation_reason",
    "contact_group_id",
    "business_group_id",
    "location_group_id",
    "dedupe_status",
    "dedupe_reason",
    "outreach_suppression_key",
    "quality_score",
    "validation_status",
    "rejection_reason",
    "extraction_timestamp",
]

OUTREACH_EXPORT_COLUMNS = [
    "business_name",
    "phone",
    "email",
    "website",
    "address",
    "city",
    "state",
    "source_url",
]

TARGET_GEOGRAPHY_TERMS = [
    "New York City",
    "Brooklyn",
    "Queens",
    "Manhattan",
    "Bronx",
    "Staten Island",
    "Yonkers",
    "White Plains",
    "New Rochelle",
    "Mount Vernon",
    "Long Island",
    "Hempstead",
    "Oyster Bay",
    "Huntington",
    "Brookhaven",
    "Islip",
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


def _ensure_column(dataframe: pd.DataFrame, column_name: str, default: object = "") -> None:
    if column_name not in dataframe.columns:
        dataframe[column_name] = default


def _location_completeness_rank(row: pd.Series) -> int:
    address = str(row.get("address", "")).strip()
    city = str(row.get("city", "")).strip()
    state = str(row.get("state", "")).strip()
    supporting_geographies = str(row.get("supporting_geographies", "")).strip()

    if address and city and state:
        return 3
    if city and state:
        return 2
    if supporting_geographies:
        return 1
    return 0


def _representative_group_key(row: pd.Series) -> str:
    contact_group_id = str(row.get("contact_group_id", "")).strip()
    if contact_group_id:
        return f"contact:{contact_group_id}"

    outreach_suppression_key = str(row.get("outreach_suppression_key", "")).strip()
    if outreach_suppression_key:
        return f"suppression:{outreach_suppression_key}"

    return f"lead:{str(row.get('lead_id', '')).strip()}"


def _representative_rank_reason(row: pd.Series) -> str:
    reasons: list[str] = []

    if str(row.get("website_email", "")).strip():
        reasons.append("website email")
    if str(row.get("website_phone", "")).strip():
        reasons.append("website phone")
    if str(row.get("preferred_email", "")).strip() and str(row.get("preferred_phone", "")).strip():
        reasons.append("email and phone")

    email_confidence = str(row.get("email_confidence", "")).strip()
    phone_confidence = str(row.get("phone_confidence", "")).strip()
    if email_confidence:
        reasons.append(f"email confidence {email_confidence}")
    if phone_confidence:
        reasons.append(f"phone confidence {phone_confidence}")

    source_priority = str(row.get("source_priority", "")).strip()
    if source_priority:
        reasons.append(f"source priority {source_priority}")

    location_rank = _location_completeness_rank(row)
    if location_rank == 3:
        reasons.append("full location context")
    elif location_rank == 2:
        reasons.append("city and state context")

    if not reasons:
        reasons.append("deterministic tie-break")

    return "Selected for strongest outreach evidence: " + ", ".join(reasons) + "."


def select_representative_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    selected = dataframe.copy()
    if selected.empty:
        return selected

    for column_name in [
        "validation_status",
        "contact_group_id",
        "outreach_suppression_key",
        "website_email",
        "website_phone",
        "preferred_email",
        "preferred_phone",
        "email_confidence",
        "phone_confidence",
        "source_priority",
        "website_validation_status",
        "dedupe_status",
        "lead_id",
        "quality_score",
        "matches_target_niche",
        "supporting_geographies",
    ]:
        _ensure_column(selected, column_name)

    selected["lead_id"] = selected.apply(_build_lead_id, axis=1)
    selected["quality_score"] = selected.apply(_quality_score, axis=1)
    selected["representative_group_key"] = selected.apply(_representative_group_key, axis=1)
    selected["location_completeness_rank"] = selected.apply(_location_completeness_rank, axis=1)
    selected["website_email_present"] = selected["website_email"].astype(str).str.strip().astype(bool)
    selected["website_phone_present"] = selected["website_phone"].astype(str).str.strip().astype(bool)
    selected["preferred_email_present"] = selected["preferred_email"].astype(str).str.strip().astype(bool)
    selected["preferred_phone_present"] = selected["preferred_phone"].astype(str).str.strip().astype(bool)
    selected["has_both_preferred_contacts"] = (
        selected["preferred_email_present"] & selected["preferred_phone_present"]
    )
    selected["email_confidence_rank"] = (
        selected["email_confidence"].astype(str).str.strip().map(CONFIDENCE_RANK).fillna(0).astype(int)
    )
    selected["phone_confidence_rank"] = (
        selected["phone_confidence"].astype(str).str.strip().map(CONFIDENCE_RANK).fillna(0).astype(int)
    )
    selected["source_priority_rank"] = pd.to_numeric(selected["source_priority"], errors="coerce").fillna(99).astype(int)
    selected["niche_relevance_rank"] = selected["matches_target_niche"].astype(bool).astype(int)

    eligible = selected[
        (selected["validation_status"] == "valid")
        & (selected["dedupe_status"] != "confirmed_duplicate")
        & ~(
            (selected["website_validation_status"] == "mismatch")
            & ~selected["preferred_email_present"]
            & ~selected["preferred_phone_present"]
        )
    ].copy()

    if eligible.empty:
        return eligible

    eligible = eligible.sort_values(
        by=[
            "representative_group_key",
            "website_email_present",
            "website_phone_present",
            "has_both_preferred_contacts",
            "email_confidence_rank",
            "phone_confidence_rank",
            "source_priority_rank",
            "location_completeness_rank",
            "quality_score",
            "niche_relevance_rank",
            "lead_id",
        ],
        ascending=[True, False, False, False, False, False, True, False, False, False, True],
        kind="stable",
    )

    selected_rows = eligible.groupby("representative_group_key", sort=False, as_index=False).head(1).copy()
    selected_rows["representative_rank_reason"] = selected_rows.apply(_representative_rank_reason, axis=1)
    return selected_rows.drop(
        columns=[
            "location_completeness_rank",
            "website_email_present",
            "website_phone_present",
            "preferred_email_present",
            "preferred_phone_present",
            "has_both_preferred_contacts",
            "email_confidence_rank",
            "phone_confidence_rank",
            "source_priority_rank",
            "niche_relevance_rank",
        ],
        errors="ignore",
    )


def apply_outreach_readiness(dataframe: pd.DataFrame) -> pd.DataFrame:
    prepared = dataframe.copy()
    if prepared.empty:
        return prepared

    for column_name in [
        "preferred_email",
        "preferred_phone",
        "preferred_email_source",
        "preferred_phone_source",
        "website_validation_status",
        "validation_status",
        "rejection_reason",
    ]:
        _ensure_column(prepared, column_name)

    ready_for_email: list[bool] = []
    ready_for_phone: list[bool] = []
    ready_for_outreach: list[bool] = []
    block_reasons: list[str] = []

    for _, row in prepared.iterrows():
        validation_status = str(row.get("validation_status", "")).strip()
        website_validation_status = str(row.get("website_validation_status", "")).strip()
        preferred_email = str(row.get("preferred_email", "")).strip()
        preferred_phone = str(row.get("preferred_phone", "")).strip()
        preferred_email_source = str(row.get("preferred_email_source", "")).strip()
        preferred_phone_source = str(row.get("preferred_phone_source", "")).strip()

        email_ready = bool(preferred_email) and not (
            website_validation_status == "mismatch" and preferred_email_source == "website"
        )
        phone_ready = bool(preferred_phone) and not (
            website_validation_status == "mismatch" and preferred_phone_source == "website"
        )
        outreach_ready = email_ready or phone_ready

        if validation_status != "valid":
            block_reason = str(row.get("rejection_reason", "")).strip() or "Row failed base validation."
        elif website_validation_status == "mismatch" and not outreach_ready:
            block_reason = "Website mismatched and no safe listing-derived contact survived."
        elif not outreach_ready:
            block_reason = "No usable preferred email or phone is available for outreach."
        else:
            block_reason = ""

        ready_for_email.append(email_ready)
        ready_for_phone.append(phone_ready)
        ready_for_outreach.append(outreach_ready)
        block_reasons.append(block_reason)

    prepared["ready_for_email"] = ready_for_email
    prepared["ready_for_phone"] = ready_for_phone
    prepared["ready_for_outreach"] = ready_for_outreach
    prepared["outreach_block_reason"] = block_reasons
    return prepared


def prepare_master_export(dataframe: pd.DataFrame) -> pd.DataFrame:
    prepared = dataframe.copy()
    if prepared.empty:
        return pd.DataFrame(columns=MASTER_EXPORT_COLUMNS)

    prepared = prepared[
        (prepared["validation_status"] == "valid")
        & (prepared["dedupe_status"] != "confirmed_duplicate")
    ].copy()

    prepared["lead_id"] = prepared.apply(_build_lead_id, axis=1)
    prepared["quality_score"] = prepared.apply(_quality_score, axis=1)

    _ensure_column(prepared, "source_type")
    _ensure_column(prepared, "discovery_query")
    _ensure_column(prepared, "discovery_geography")

    prepared["discovery_query"] = prepared["discovery_query"].where(
        prepared["discovery_query"].astype(str).str.strip().astype(bool),
        prepared.get("discovery_queries", ""),
    )
    prepared["discovery_geography"] = prepared["discovery_geography"].where(
        prepared["discovery_geography"].astype(str).str.strip().astype(bool),
        prepared.get("supporting_geographies", ""),
    )

    prepared = prepared.sort_values(
        by=["niche", "business_name", "contact_group_id", "city", "state", "lead_id"],
        ascending=[True, True, True, True, True, True],
        kind="stable",
    ).reset_index(drop=True)

    for column_name in MASTER_EXPORT_COLUMNS:
        if column_name not in prepared.columns:
            prepared[column_name] = ""

    return prepared[MASTER_EXPORT_COLUMNS]


def prepare_final_export(dataframe: pd.DataFrame) -> pd.DataFrame:
    return prepare_outreach_ready_export(dataframe)


def prepare_outreach_ready_export(dataframe: pd.DataFrame) -> pd.DataFrame:
    prepared = dataframe.copy()
    if prepared.empty:
        return pd.DataFrame(columns=OUTREACH_EXPORT_COLUMNS)

    prepared = select_representative_rows(prepared)
    prepared = apply_outreach_readiness(prepared)
    prepared = prepared[prepared["ready_for_outreach"] == True].copy()

    prepared["phone"] = prepared["preferred_phone"]
    prepared["email"] = prepared["preferred_email"]

    prepared = prepared.sort_values(
        by=["business_name", "city", "state"],
        ascending=[True, True, True],
        kind="stable",
    ).reset_index(drop=True)

    for column_name in OUTREACH_EXPORT_COLUMNS:
        if column_name not in prepared.columns:
            prepared[column_name] = ""

    return prepared[OUTREACH_EXPORT_COLUMNS]


def write_quality_summary(
    raw_record_count: int,
    processed_dataframe: pd.DataFrame,
    master_dataframe: pd.DataFrame,
    outreach_ready_dataframe: pd.DataFrame,
    output_path: Path,
    *,
    discovery_raw_output_path: Path | None = None,
    classified_listing_pages_output_path: Path | None = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    processed_validation = _series_from_column(processed_dataframe, "validation_status")
    processed_contact_groups = _series_from_column(processed_dataframe, "contact_group_id")
    processed_business_groups = _series_from_column(processed_dataframe, "business_group_id")
    processed_location_groups = _series_from_column(processed_dataframe, "location_group_id")
    processed_website_validation = _series_from_column(
        processed_dataframe,
        "website_validation_status",
        default="missing_website",
    )
    processed_website = _series_from_column(processed_dataframe, "website")
    processed_listing_email = _series_from_column(processed_dataframe, "listing_email")
    processed_preferred_email = _series_from_column(processed_dataframe, "preferred_email")
    processed_listing_phone = _series_from_column(processed_dataframe, "listing_phone")
    processed_preferred_phone = _series_from_column(processed_dataframe, "preferred_phone")
    processed_dedupe_status = _series_from_column(processed_dataframe, "dedupe_status")

    rejected_count = int((processed_validation != "valid").sum())
    master_niche_counts = master_dataframe["niche"].value_counts().to_dict()
    multi_contact_groups = int((processed_contact_groups.value_counts() > 1).sum())
    multi_business_groups = int((processed_business_groups.value_counts() > 1).sum())
    multi_location_groups = int((processed_location_groups.value_counts() > 1).sum())
    outreach_email_count = _truthy_count(_series_from_column(outreach_ready_dataframe, "email"))
    outreach_phone_count = _truthy_count(_series_from_column(outreach_ready_dataframe, "phone"))
    outreach_website_count = _truthy_count(_series_from_column(outreach_ready_dataframe, "website"))
    confirmed_duplicates = int((processed_dedupe_status == "confirmed_duplicate").sum())
    review_rows = int((_series_from_column(master_dataframe, "dedupe_status") == "possible_duplicate_needs_review").sum())
    discovery_raw_count = _load_json_row_count(discovery_raw_output_path)
    classified_listing_pages = _load_json_rows(classified_listing_pages_output_path)
    accepted_listing_pages = sum(
        1 for row in classified_listing_pages if str(row.get("listing_page_status", "")).strip() == "accepted_listing_page"
    )
    rejected_listing_pages = max(len(classified_listing_pages) - accepted_listing_pages, 0)
    rows_with_website = _truthy_count(processed_website)
    website_enriched_rows = int(processed_website_validation.astype(str).str.strip().ne("missing_website").sum())
    listing_email_count = _truthy_count(processed_listing_email)
    preferred_email_count = _truthy_count(processed_preferred_email)
    listing_phone_count = _truthy_count(processed_listing_phone)
    preferred_phone_count = _truthy_count(processed_preferred_phone)
    dead_website_count = int((processed_website_validation == "dead").sum())
    mismatched_website_count = int((processed_website_validation == "mismatch").sum())
    ready_series = _bool_series(outreach_ready_dataframe, "ready_for_outreach")
    blocked_outreach_rows = 0 if "ready_for_outreach" not in outreach_ready_dataframe.columns else int((~ready_series).sum())
    ready_outreach_rows = len(outreach_ready_dataframe) if "ready_for_outreach" not in outreach_ready_dataframe.columns else int(ready_series.sum())
    geography_counts = _geography_counts(master_dataframe)

    lines = [
        "# Quality Summary",
        "",
        "## Discovery Metrics",
        "",
        f"- raw discovery URL count: `{discovery_raw_count}`",
        f"- listing pages accepted: `{accepted_listing_pages}`",
        f"- listing pages rejected: `{rejected_listing_pages}`",
        f"- extracted lead count before enrichment: `{raw_record_count}`",
        "",
        "## Core Counts",
        "",
        f"- processed record count: `{len(processed_dataframe)}`",
        f"- master row count: `{len(master_dataframe)}`",
        f"- outreach-ready row count: `{len(outreach_ready_dataframe)}`",
        f"- rejected count: `{rejected_count}`",
        "",
        "## Enrichment Metrics",
        "",
        f"- rows with website: `{rows_with_website}`",
        f"- website enrichment coverage: `{website_enriched_rows}/{rows_with_website}`",
        f"- email coverage before enrichment: `{listing_email_count}`",
        f"- email coverage after enrichment: `{preferred_email_count}`",
        f"- phone coverage before enrichment: `{listing_phone_count}`",
        f"- phone coverage after enrichment: `{preferred_phone_count}`",
        f"- dead website count: `{dead_website_count}`",
        f"- mismatched website count: `{mismatched_website_count}`",
        "",
        "## Count by Niche",
        "",
    ]

    for niche, count in master_niche_counts.items():
        lines.append(f"- {niche}: `{count}`")

    lines.extend(
        [
            "",
            "## Grouping Metrics",
            "",
            f"- multi-row contact groups: `{multi_contact_groups}`",
            f"- multi-row business groups: `{multi_business_groups}`",
            f"- multi-row location groups: `{multi_location_groups}`",
            f"- confirmed duplicates excluded from final export: `{confirmed_duplicates}`",
            f"- possible duplicate rows retained for review: `{review_rows}`",
            "",
            "## Representative Selection",
            "",
            f"- rows reduced by representative selection: `{max(len(master_dataframe) - len(outreach_ready_dataframe), 0)}`",
            f"- outreach-ready rows marked ready: `{ready_outreach_rows}`",
            f"- outreach-ready rows blocked: `{blocked_outreach_rows}`",
            f"- representative groups emitted: `{len(outreach_ready_dataframe)}`",
            "",
            "## Outreach-Ready Contact Coverage",
            "",
            f"- records with email: `{outreach_email_count}`",
            f"- records with phone: `{outreach_phone_count}`",
            f"- records with website: `{outreach_website_count}`",
        ]
    )

    if geography_counts:
        lines.extend(["", "## Count by Geography", ""])
        for geography_name, count in geography_counts.items():
            lines.append(f"- {geography_name}: `{count}`")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_json_rows(path: Path | None) -> list[dict[str, object]]:
    if path is None or not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _load_json_row_count(path: Path | None) -> int:
    return len(_load_json_rows(path))


def _series_from_column(dataframe: pd.DataFrame, column_name: str, default: object = "") -> pd.Series:
    if column_name not in dataframe.columns:
        return pd.Series([default] * len(dataframe), index=dataframe.index, dtype="object")
    return dataframe[column_name]


def _truthy_count(series: pd.Series) -> int:
    return int(series.astype(str).str.strip().astype(bool).sum())


def _bool_series(dataframe: pd.DataFrame, column_name: str) -> pd.Series:
    if column_name not in dataframe.columns:
        return pd.Series([False] * len(dataframe), index=dataframe.index, dtype="bool")
    return dataframe[column_name].fillna(False).astype(bool)


def _geography_counts(dataframe: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    if dataframe.empty:
        return counts

    for _, row in dataframe.iterrows():
        haystacks = " || ".join(
            [
                str(row.get("city", "")).strip(),
                str(row.get("state", "")).strip(),
                str(row.get("discovery_geography", "")).strip(),
                str(row.get("supporting_geographies", "")).strip(),
            ]
        ).lower()
        for geography_name in TARGET_GEOGRAPHY_TERMS:
            if geography_name.lower() in haystacks:
                counts[geography_name] = counts.get(geography_name, 0) + 1

    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def export_master_csv(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prepare_master_export(dataframe).to_csv(output_path, index=False)


def export_outreach_ready_csv(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prepare_outreach_ready_export(dataframe).to_csv(output_path, index=False)


def export_final_csv(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prepare_outreach_ready_export(dataframe).to_csv(output_path, index=False)
