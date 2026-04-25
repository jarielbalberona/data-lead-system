from __future__ import annotations

import re
from urllib.parse import urlparse

import pandas as pd
import phonenumbers


TARGET_NICHES = {"property_manager", "interior_designer"}


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def _normalize_email(email: object) -> str:
    normalized = _normalize_text(email).lower()
    if normalized.startswith("mailto:"):
        normalized = normalized.removeprefix("mailto:")
    return normalized


def _normalize_phone(phone: object) -> str:
    normalized = _normalize_text(phone)
    if not normalized:
        return ""

    try:
        parsed = phonenumbers.parse(normalized, "US")
        if phonenumbers.is_possible_number(parsed) and phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass

    digits_only = re.sub(r"\D+", "", normalized)
    if len(digits_only) == 10:
        return f"+1{digits_only}"
    if len(digits_only) == 11 and digits_only.startswith("1"):
        return f"+{digits_only}"
    return digits_only


def _normalize_website(website: object) -> tuple[str, str]:
    normalized = _normalize_text(website)
    if not normalized:
        return "", ""

    if normalized.startswith("www."):
        normalized = f"https://{normalized}"
    if "://" not in normalized:
        normalized = f"https://{normalized}"

    parsed = urlparse(normalized)
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")

    if netloc.startswith("www."):
        netloc = netloc.removeprefix("www.")

    cleaned_website = f"{parsed.scheme.lower()}://{netloc}{path}" if netloc else ""
    if parsed.query:
        cleaned_website = f"{cleaned_website}?{parsed.query}"

    return cleaned_website, netloc


def _ensure_column(dataframe: pd.DataFrame, column_name: str) -> None:
    if column_name not in dataframe.columns:
        dataframe[column_name] = ""


def normalize_records(records: list[dict[str, object]]) -> pd.DataFrame:
    dataframe = pd.DataFrame(records).copy()
    if dataframe.empty:
        return dataframe

    for column_name in [
        "niche",
        "business_name",
        "phone",
        "email",
        "website",
        "address",
        "city",
        "state",
        "source_url",
        "extraction_timestamp",
    ]:
        _ensure_column(dataframe, column_name)

    dataframe["niche"] = dataframe["niche"].map(_normalize_text)
    dataframe["phone"] = dataframe["phone"].map(_normalize_text)
    dataframe["email"] = dataframe["email"].map(_normalize_email)

    website_values = dataframe["website"].map(_normalize_website)
    dataframe["website"] = website_values.map(lambda item: item[0])
    dataframe["website_domain"] = website_values.map(lambda item: item[1])

    dataframe["normalized_phone"] = dataframe["phone"].map(_normalize_phone)
    dataframe["normalized_email"] = dataframe["email"]
    dataframe["matches_target_niche"] = dataframe["niche"].isin(TARGET_NICHES)

    return dataframe
