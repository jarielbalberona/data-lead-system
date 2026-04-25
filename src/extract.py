from __future__ import annotations

import gzip
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config import (
    INTERIOR_DESIGNER_SOURCE_URLS,
    PROPERTY_MANAGER_SOURCE_URLS,
    PipelineConfig,
)

@dataclass(frozen=True)
class RawLeadRecord:
    niche: str
    business_name: str
    phone: str
    email: str
    website: str
    address: str
    city: str
    state: str
    source_url: str
    extraction_timestamp: str
    source_directory: str
    source_listing_url: str


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.replace("\xa0", " ").split())


def _build_session(config: PipelineConfig) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": config.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def _fetch_soup(session: requests.Session, url: str, config: PipelineConfig) -> BeautifulSoup:
    response = session.get(url, timeout=config.request_timeout_seconds)
    response.raise_for_status()
    content = response.content
    if content.startswith(b"\x1f\x8b"):
        try:
            content = gzip.decompress(content)
        except Exception:
            pass

    encoding = response.encoding or "utf-8"
    html = content.decode(encoding, errors="replace")
    return BeautifulSoup(html, "html.parser")


def _snapshot_token(timestamp: str) -> str:
    return timestamp.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")


def _write_raw_records(
    records: list[RawLeadRecord],
    output_path: Path,
    extraction_timestamp: str,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    archive_dir = output_path.parent / "archive" / output_path.stem
    archive_dir.mkdir(parents=True, exist_ok=True)

    payload = json.dumps([asdict(record) for record in records], indent=2)
    output_path.write_text(payload, encoding="utf-8")
    (archive_dir / f"{_snapshot_token(extraction_timestamp)}.json").write_text(
        payload,
        encoding="utf-8",
    )


def _extract_hoa_company_links(
    session: requests.Session,
    source_url: str,
    config: PipelineConfig,
    max_links: int = 12,
) -> list[str]:
    soup = _fetch_soup(session, source_url, config)
    company_links: list[str] = []

    heading = soup.find("h2")
    listing_container = heading.find_next("ul") if heading else None
    if listing_container is None:
        return company_links

    for anchor in listing_container.select("li a[href]"):
        href = anchor.get("href", "").strip()
        if not href.startswith("/") or href.startswith("/zipcode/"):
            continue
        company_links.append(urljoin(source_url, href))
        if len(company_links) >= max_links:
            break

    return company_links


def _extract_labeled_details(paragraph: BeautifulSoup) -> dict[str, str]:
    tokens = [_normalize_text(token) for token in paragraph.stripped_strings]
    details: dict[str, str] = {}
    current_label = ""

    for token in tokens:
        if token.endswith(":"):
            current_label = token.removesuffix(":").lower()
            details[current_label] = ""
            continue
        if current_label:
            details[current_label] = token
            current_label = ""

    return details


def _extract_business_name(soup: BeautifulSoup) -> str:
    for heading in soup.find_all("h2"):
        text = _normalize_text(heading.get_text(" ", strip=True))
        if "HOA Property Management Company:" in text:
            return _normalize_text(text.split(":", 1)[1])

    title = soup.find("title")
    title_text = _normalize_text(title.get_text(" ", strip=True) if title else "")
    if title_text and title_text != "HOA Management Company Directory":
        return title_text

    return "HOA Management Company Directory"


def _extract_hoa_profile(
    session: requests.Session,
    profile_url: str,
    listing_url: str,
    config: PipelineConfig,
) -> RawLeadRecord:
    soup = _fetch_soup(session, profile_url, config)

    details_paragraph = None
    for paragraph in soup.find_all("p"):
        if "Address:" in paragraph.get_text(" ", strip=True):
            details_paragraph = paragraph
            break

    business_name = _extract_business_name(soup)
    phone = ""
    website = ""
    address_line = ""

    if details_paragraph is not None:
        details = _extract_labeled_details(details_paragraph)
        address_line = details.get("address", "")
        phone = details.get("phone", "")
        website = details.get("website", "")

    return RawLeadRecord(
        niche="property_manager",
        business_name=business_name,
        phone=phone,
        email="",
        website=website,
        address=_normalize_text(address_line),
        city="",
        state="",
        source_url=profile_url,
        extraction_timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        source_directory="hoamanagementcompanies.net",
        source_listing_url=listing_url,
    )


def _split_location_line(location_text: str) -> tuple[str, str, str]:
    normalized = _normalize_text(location_text)
    location_match = re.search(
        r"^(?P<address>.+?)\s+(?P<city>[A-Za-z .'-]+),\s*(?P<state>[A-Z]{2})\s+\d{5}(?:-\d{4})?$",
        normalized,
    )
    if not location_match:
        return normalized, "", ""

    return (
        _normalize_text(location_match.group("address")),
        _normalize_text(location_match.group("city")),
        _normalize_text(location_match.group("state")),
    )


def _parse_standard_listing_location(remaining_text: str) -> tuple[str, str, str]:
    state_match = re.search(r",\s*(?P<state>[A-Z]{2})\s+\d{5}(?:-\d{4})?$", remaining_text)
    if not state_match:
        return _normalize_text(remaining_text), "", ""

    state = _normalize_text(state_match.group("state"))
    address_city_text = _normalize_text(remaining_text[: state_match.start()])
    tokens = address_city_text.replace(",", "").split()
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
        "rancho",
        "san",
        "santa",
        "west",
        "woodlands",
    }

    address_end_index = -1
    for index, token in enumerate(tokens):
        normalized_token = token.rstrip(".,").lower()
        if normalized_token in street_suffixes:
            address_end_index = index

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

    address = _normalize_text(" ".join(address_tokens))
    city = _normalize_text(" ".join(city_tokens))
    return address, city, state


def _extract_interiordesignlink_standard_record(
    listing_text: str,
    page_url: str,
    extraction_timestamp: str,
) -> RawLeadRecord | None:
    normalized = _normalize_text(listing_text)
    phone_match = re.search(r"(\d{3}-\d{3}-\d{4})$", normalized)
    if not phone_match:
        return None

    phone = phone_match.group(1)
    details_text = normalized[: phone_match.start()].strip()
    address_start_match = re.search(r"\b(?:PO\s+Box|\d+[A-Za-z0-9#/-]*)\b", details_text)
    if address_start_match is None:
        return None

    split_index = address_start_match.start()
    business_name = _normalize_text(details_text[:split_index])
    address, city, state = _parse_standard_listing_location(details_text[split_index:])
    if not business_name:
        return None

    return RawLeadRecord(
        niche="interior_designer",
        business_name=business_name,
        phone=phone,
        email="",
        website="",
        address=address,
        city=city,
        state=state,
        source_url=page_url,
        extraction_timestamp=extraction_timestamp,
        source_directory="interiordesignlink.com",
        source_listing_url=page_url,
    )


def _extract_interiordesignlink_elite_record(
    listing: BeautifulSoup,
    page_url: str,
    extraction_timestamp: str,
) -> RawLeadRecord | None:
    business_anchor = listing.select_one("div.listing a.linkBlue14[href]")
    if business_anchor is None:
        return None

    business_name = _normalize_text(business_anchor.get_text(" ", strip=True))
    if not business_name:
        return None

    source_url = urljoin(page_url, business_anchor.get("href", "").strip()) or page_url

    email_anchor = listing.select_one("a[href^='mailto:']")
    website_anchor = listing.select_one("a[href^='http']")

    listing_block = listing.select_one("div.listing")
    lines = []
    if listing_block is not None:
        raw_lines = listing_block.decode_contents().split("<br")
        for raw_line in raw_lines[1:]:
            text = BeautifulSoup(raw_line, "html.parser").get_text(" ", strip=True)
            normalized_line = _normalize_text(text)
            if normalized_line:
                lines.append(normalized_line)

    address = ""
    city = ""
    state = ""
    phone = ""
    if lines:
        address, city, state = _split_location_line(lines[0])
    if len(lines) > 1:
        phone = _normalize_text(lines[1].split("\xa0", 1)[0].strip())

    return RawLeadRecord(
        niche="interior_designer",
        business_name=business_name,
        phone=phone,
        email=_normalize_text(email_anchor.get_text(" ", strip=True) if email_anchor else ""),
        website=_normalize_text(website_anchor.get("href", "").strip() if website_anchor else ""),
        address=address,
        city=city,
        state=state,
        source_url=source_url,
        extraction_timestamp=extraction_timestamp,
        source_directory="interiordesignlink.com",
        source_listing_url=page_url,
    )


def _extract_interiordesignlink_page(
    session: requests.Session,
    page_url: str,
    config: PipelineConfig,
) -> list[RawLeadRecord]:
    soup = _fetch_soup(session, page_url, config)
    page_records: list[RawLeadRecord] = []
    extraction_timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    for elite_listing in soup.select("div.listing_container.border-bottom.py-2"):
        record = _extract_interiordesignlink_elite_record(
            listing=elite_listing,
            page_url=page_url,
            extraction_timestamp=extraction_timestamp,
        )
        if record is not None:
            page_records.append(record)

    for standard_listing in soup.select("div.listing_standard"):
        record = _extract_interiordesignlink_standard_record(
            listing_text=standard_listing.get_text(" ", strip=True),
            page_url=page_url,
            extraction_timestamp=extraction_timestamp,
        )
        if record is not None:
            page_records.append(record)

    return page_records


def extract_property_managers(config: PipelineConfig | None = None) -> list[dict[str, Any]]:
    active_config = config or PipelineConfig()
    active_config.ensure_directories()
    session = _build_session(active_config)
    extraction_timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    extracted_records: list[RawLeadRecord] = []

    for listing_url in PROPERTY_MANAGER_SOURCE_URLS:
        for company_url in _extract_hoa_company_links(session, listing_url, active_config):
            record = _extract_hoa_profile(
                session=session,
                profile_url=company_url,
                listing_url=listing_url,
                config=active_config,
            )
            if record.phone or record.website or record.address:
                extracted_records.append(record)

    _write_raw_records(
        extracted_records,
        active_config.raw_dir / "property_managers_raw.json",
        extraction_timestamp,
    )
    return [asdict(record) for record in extracted_records]


def extract_interior_designers(config: PipelineConfig | None = None) -> list[dict[str, Any]]:
    active_config = config or PipelineConfig()
    active_config.ensure_directories()
    session = _build_session(active_config)
    extraction_timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    extracted_records: list[RawLeadRecord] = []

    for listing_url in INTERIOR_DESIGNER_SOURCE_URLS:
        extracted_records.extend(_extract_interiordesignlink_page(session, listing_url, active_config))

    _write_raw_records(
        extracted_records,
        active_config.raw_dir / "interior_designers_raw.json",
        extraction_timestamp,
    )
    return [asdict(record) for record in extracted_records]
