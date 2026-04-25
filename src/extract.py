from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config import PROPERTY_MANAGER_SOURCE_URLS, PipelineConfig

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
    return BeautifulSoup(response.text, "html.parser")


def _write_raw_records(records: list[RawLeadRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps([asdict(record) for record in records], indent=2),
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


def extract_property_managers(config: PipelineConfig | None = None) -> list[dict[str, Any]]:
    active_config = config or PipelineConfig()
    active_config.ensure_directories()
    session = _build_session(active_config)

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
    )
    return [asdict(record) for record in extracted_records]


def extract_interior_designers(config: PipelineConfig | None = None) -> list[dict[str, Any]]:
    return []
