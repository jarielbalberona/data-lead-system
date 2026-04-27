from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

CONFIDENCE_RANK = {"high": 3, "medium": 2, "low": 1, "": 0}

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
    "website_validation_status",
    "website_validation_reason",
    "website_final_url",
    "website_pages_attempted",
    "representative_group_key",
    "representative_rank_reason",
    "ready_for_email",
    "ready_for_phone",
    "ready_for_outreach",
    "outreach_block_reason",
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


def prepare_final_export(dataframe: pd.DataFrame) -> pd.DataFrame:
    prepared = dataframe.copy()
    if prepared.empty:
        return prepared

    prepared = select_representative_rows(prepared)
    prepared = apply_outreach_readiness(prepared)

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


def write_quality_summary(
    raw_record_count: int,
    processed_dataframe: pd.DataFrame,
    final_dataframe: pd.DataFrame,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rejected_count = int((processed_dataframe["validation_status"] != "valid").sum())
    final_niche_counts = final_dataframe["niche"].value_counts().to_dict()
    multi_contact_groups = int((processed_dataframe["contact_group_id"].value_counts() > 1).sum())
    multi_business_groups = int((processed_dataframe["business_group_id"].value_counts() > 1).sum())
    multi_location_groups = int((processed_dataframe["location_group_id"].value_counts() > 1).sum())
    final_email_count = int(final_dataframe["normalized_email"].astype(bool).sum())
    final_phone_count = int(final_dataframe["normalized_phone"].astype(bool).sum())
    final_website_count = int(final_dataframe["website_domain"].astype(bool).sum())
    confirmed_duplicates = int((processed_dataframe["dedupe_status"] == "confirmed_duplicate").sum())
    review_rows = int((final_dataframe["dedupe_status"] == "possible_duplicate_needs_review").sum())

    lines = [
        "# Quality Summary",
        "",
        "## Core Counts",
        "",
        f"- raw record count: `{raw_record_count}`",
        f"- processed record count: `{len(processed_dataframe)}`",
        f"- final record count: `{len(final_dataframe)}`",
        f"- rejected count: `{rejected_count}`",
        "",
        "## Final Count by Niche",
        "",
    ]

    for niche, count in final_niche_counts.items():
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
            "## Final Contact Coverage",
            "",
            f"- records with email: `{final_email_count}`",
            f"- records with phone: `{final_phone_count}`",
            f"- records with website: `{final_website_count}`",
        ]
    )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def export_final_csv(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prepare_final_export(dataframe).to_csv(output_path, index=False)
