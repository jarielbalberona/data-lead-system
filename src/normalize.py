from __future__ import annotations

import re
from urllib.parse import urlparse

import pandas as pd
import phonenumbers


TARGET_NICHES = {"property_manager", "interior_designer"}
BUSINESS_SUFFIXES = {"llc", "l.l.c.", "inc", "inc.", "lp", "l.p.", "hoa", "poa", "co", "corp", "ltd"}
ADDRESS_UPPER_TOKENS = {"nw", "ne", "sw", "se", "n", "s", "e", "w", "po", "box", "ste", "unit"}
IRRELEVANT_NAME_PATTERNS = (
    "your listing here",
    "get listed today",
    "management company directory",
    "interior design link",
)


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def _normalize_email(email: object) -> str:
    normalized = _normalize_text(email).lower()
    if normalized.startswith("mailto:"):
        normalized = normalized.removeprefix("mailto:")
    return normalized


def _smart_title(value: str, uppercase_tokens: set[str] | None = None) -> str:
    if not value:
        return ""

    tokens = value.split(" ")
    normalized_tokens: list[str] = []
    forced_upper = uppercase_tokens or set()

    for token in tokens:
        stripped = token.strip()
        lowered = stripped.lower()

        if lowered in forced_upper:
            normalized_tokens.append(lowered.upper())
            continue

        if "/" in stripped:
            normalized_tokens.append("/".join(part.capitalize() for part in stripped.split("/")))
            continue

        normalized_tokens.append(stripped.capitalize())

    return " ".join(normalized_tokens)


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


def _normalize_business_name(business_name: object) -> str:
    normalized = _normalize_text(business_name)
    if not normalized:
        return ""

    if normalized.isupper() or normalized.islower():
        tokens = normalized.split(" ")
        formatted_tokens: list[str] = []
        for token in tokens:
            lowered = token.lower()
            if lowered in BUSINESS_SUFFIXES:
                formatted_tokens.append(lowered.upper().replace(".", ""))
            else:
                formatted_tokens.append(token.capitalize())
        return " ".join(formatted_tokens)

    return normalized


def _split_address_and_city(tokens: list[str], state: str) -> tuple[str, str, str]:
    if not tokens:
        return "", "", state

    if len(tokens) >= 3 and tokens[0].lower() == "po" and tokens[1].lower() == "box":
        address_tokens = tokens[:3]
        city_tokens = tokens[3:]
        return _normalize_text(" ".join(address_tokens)), _normalize_text(" ".join(city_tokens)), state

    street_suffixes = {
        "aly",
        "ave",
        "avenue",
        "blvd",
        "boulevard",
        "cir",
        "circle",
        "court",
        "ct",
        "dr",
        "drive",
        "hwy",
        "lane",
        "ln",
        "loop",
        "parkway",
        "pkwy",
        "pl",
        "place",
        "rd",
        "road",
        "sq",
        "st",
        "street",
        "ter",
        "terrace",
        "trl",
        "way",
    }
    unit_markers = {"suite", "ste", "#", "unit"}
    city_prefixes = {
        "beach",
        "carlos",
        "cajon",
        "cordova",
        "el",
        "francisco",
        "hills",
        "island",
        "jose",
        "lake",
        "los",
        "monica",
        "new",
        "park",
        "rancho",
        "san",
        "santa",
        "tinley",
        "west",
        "woodlands",
    }

    address_end_index = -1
    for index, token in enumerate(tokens):
        normalized_token = token.rstrip(".,").lower()
        if normalized_token in street_suffixes:
            address_end_index = index
            break

    if address_end_index >= 0:
        next_index = address_end_index + 1
        if next_index < len(tokens) and tokens[next_index].rstrip(".,").lower() in unit_markers:
            address_end_index = next_index
            if address_end_index + 1 < len(tokens):
                address_end_index += 1

        address_tokens = tokens[: address_end_index + 1]
        city_tokens = tokens[address_end_index + 1 :]
    else:
        city_token_count = 2 if len(tokens) >= 3 and tokens[-2].lower() in city_prefixes else 1
        address_tokens = tokens[:-city_token_count]
        city_tokens = tokens[-city_token_count:]

    return _normalize_text(" ".join(address_tokens)), _normalize_text(" ".join(city_tokens)), state


def _parse_location_from_address(address: str) -> tuple[str, str, str]:
    normalized = _normalize_text(address)
    state_match = re.search(r",\s*(?P<state>[A-Z]{2})\s+\d{5}(?:-\d{4})?$", normalized)
    if not state_match:
        return normalized, "", ""

    state = _normalize_text(state_match.group("state"))
    address_city_text = _normalize_text(normalized[: state_match.start()])
    tokens = address_city_text.replace(",", "").split()
    return _split_address_and_city(tokens, state)


def _normalize_location_fields(address: object, city: object, state: object) -> tuple[str, str, str]:
    normalized_address = _normalize_text(address)
    if normalized_address.startswith("/>"):
        normalized_address = normalized_address.removeprefix("/>").strip()
    normalized_city = _normalize_text(city)
    normalized_state = _normalize_text(state).upper()

    if normalized_address and (not normalized_city or not normalized_state):
        parsed_address, parsed_city, parsed_state = _parse_location_from_address(normalized_address)
        if parsed_city and not normalized_city:
            normalized_city = parsed_city
        if parsed_state and not normalized_state:
            normalized_state = parsed_state
        normalized_address = parsed_address

    if normalized_address and (normalized_address.isupper() or normalized_address.islower()):
        normalized_address = _smart_title(normalized_address, uppercase_tokens=ADDRESS_UPPER_TOKENS)

    if normalized_city and (normalized_city.isupper() or normalized_city.islower()):
        normalized_city = _smart_title(normalized_city)

    if normalized_state:
        normalized_state = normalized_state.upper()

    return normalized_address, normalized_city, normalized_state


def _validation_result(row: pd.Series) -> tuple[str, str]:
    business_name = _normalize_text(row.get("business_name", ""))
    niche = _normalize_text(row.get("niche", ""))
    normalized_phone = _normalize_text(row.get("normalized_phone", ""))
    normalized_email = _normalize_text(row.get("normalized_email", ""))
    website_domain = _normalize_text(row.get("website_domain", ""))

    if not business_name:
        return "rejected_missing_required_fields", "Missing business name."

    if niche not in TARGET_NICHES:
        return "rejected_irrelevant", f"Niche `{niche}` is outside the assignment scope."

    lowered_name = business_name.lower()
    for pattern in IRRELEVANT_NAME_PATTERNS:
        if pattern in lowered_name:
            return "rejected_irrelevant", f"Business name matched irrelevant pattern `{pattern}`."

    if not normalized_phone and not normalized_email and not website_domain:
        return (
            "rejected_missing_required_fields",
            "No usable phone, email, or website domain was available for outreach or grouping.",
        )

    return "valid", ""


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
    dataframe["business_name"] = dataframe["business_name"].map(_normalize_business_name)

    website_values = dataframe["website"].map(_normalize_website)
    dataframe["website"] = website_values.map(lambda item: item[0])
    dataframe["website_domain"] = website_values.map(lambda item: item[1])

    location_values = dataframe.apply(
        lambda row: _normalize_location_fields(row["address"], row["city"], row["state"]),
        axis=1,
    )
    dataframe["address"] = location_values.map(lambda item: item[0])
    dataframe["city"] = location_values.map(lambda item: item[1])
    dataframe["state"] = location_values.map(lambda item: item[2])

    dataframe["normalized_phone"] = dataframe["phone"].map(_normalize_phone)
    dataframe["normalized_email"] = dataframe["email"]
    dataframe["normalized_business_name"] = dataframe["business_name"].map(_normalize_text)
    dataframe["normalized_address"] = dataframe["address"].map(_normalize_text)
    dataframe["normalized_city"] = dataframe["city"].map(_normalize_text)
    dataframe["normalized_state"] = dataframe["state"].map(_normalize_text)
    dataframe["matches_target_niche"] = dataframe["niche"].isin(TARGET_NICHES)

    validation_values = dataframe.apply(_validation_result, axis=1)
    dataframe["validation_status"] = validation_values.map(lambda item: item[0])
    dataframe["rejection_reason"] = validation_values.map(lambda item: item[1])

    return dataframe
