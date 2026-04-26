from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import phonenumbers
import requests
from bs4 import BeautifulSoup

from config import PipelineConfig


LIKELY_CONTACT_PATHS: tuple[str, ...] = ("/", "/contact", "/contact-us", "/about", "/team")
EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_PATTERN = re.compile(r"(?:\+?1[\s.-]*)?(?:\(?\d{3}\)?[\s.-]*)\d{3}[\s.-]*\d{4}")
CONFIDENCE_RANK = {"high": 3, "medium": 2, "low": 1}
METHOD_RANK = {"mailto_link": 3, "tel_link": 3, "visible_text": 2}
PATH_RANK = {"/contact": 4, "/contact-us": 4, "/team": 2, "/about": 2, "/": 1}


@dataclass(frozen=True)
class WebsitePageAttempt:
    website: str
    website_domain: str
    path_hint: str
    requested_url: str
    final_url: str
    http_status: int | None
    fetch_status: str
    failure_reason: str
    attempted_at: str


@dataclass(frozen=True)
class WebsiteContactFinding:
    website: str
    website_domain: str
    page_url: str
    path_hint: str
    contact_type: str
    contact_value: str
    normalized_value: str
    extraction_method: str
    confidence: str
    is_generic_email: bool
    extracted_at: str


def discover_candidate_pages(
    records: list[dict[str, object]],
    config: PipelineConfig | None = None,
) -> list[WebsitePageAttempt]:
    runtime_config = config or PipelineConfig()
    runtime_config.ensure_directories()
    session = _build_session(runtime_config)

    attempts: list[WebsitePageAttempt] = []
    seen_urls: set[str] = set()

    for record in records:
        website = str(record.get("website", "")).strip()
        if not website:
            continue

        normalized_website, website_domain = _normalize_website(website)
        if not normalized_website or not website_domain:
            continue

        for path_hint in LIKELY_CONTACT_PATHS:
            requested_url = _build_probe_url(normalized_website, path_hint)
            if requested_url in seen_urls:
                continue
            seen_urls.add(requested_url)
            attempts.append(_fetch_attempt(session, requested_url, path_hint, website_domain, runtime_config))

    return attempts


def extract_website_contacts(
    records: list[dict[str, object]],
    config: PipelineConfig | None = None,
) -> list[WebsiteContactFinding]:
    runtime_config = config or PipelineConfig()
    runtime_config.ensure_directories()
    session = _build_session(runtime_config)
    findings: list[WebsiteContactFinding] = []
    seen_urls: set[str] = set()

    for record in records:
        website = str(record.get("website", "")).strip()
        if not website:
            continue

        normalized_website, website_domain = _normalize_website(website)
        if not normalized_website or not website_domain:
            continue

        for path_hint in LIKELY_CONTACT_PATHS:
            requested_url = _build_probe_url(normalized_website, path_hint)
            if requested_url in seen_urls:
                continue
            seen_urls.add(requested_url)

            response = _fetch_page(session, requested_url, runtime_config)
            if not response["ok"]:
                continue

            html = str(response["text"])
            final_url = str(response["final_url"])
            findings.extend(
                _extract_contact_findings(
                    website=normalized_website,
                    website_domain=website_domain,
                    page_url=final_url or requested_url,
                    path_hint=path_hint,
                    html=html,
                )
            )

    return _dedupe_contact_findings(findings)


def apply_contact_enrichment(
    records: list[dict[str, object]],
    findings: list[WebsiteContactFinding],
) -> list[dict[str, object]]:
    findings_by_domain: dict[str, list[WebsiteContactFinding]] = {}
    for finding in findings:
        findings_by_domain.setdefault(finding.website_domain, []).append(finding)

    enriched_records: list[dict[str, object]] = []
    for record in records:
        enriched = dict(record)
        _, website_domain = _normalize_website(str(record.get("website", "")).strip())
        domain_findings = findings_by_domain.get(website_domain, [])

        listing_email = str(record.get("email", "")).strip().lower()
        listing_phone = _normalize_phone(str(record.get("phone", "")).strip())
        best_email = _best_finding(domain_findings, "email")
        best_phone = _best_finding(domain_findings, "phone")

        enriched["listing_email"] = listing_email
        enriched["listing_phone"] = listing_phone
        enriched["website_email"] = best_email.normalized_value if best_email else ""
        enriched["website_phone"] = best_phone.normalized_value if best_phone else ""
        enriched["email_source_url"] = best_email.page_url if best_email else ""
        enriched["phone_source_url"] = best_phone.page_url if best_phone else ""
        enriched["email_extraction_method"] = best_email.extraction_method if best_email else ""
        enriched["phone_extraction_method"] = best_phone.extraction_method if best_phone else ""
        enriched["email_confidence"] = best_email.confidence if best_email else ""
        enriched["phone_confidence"] = best_phone.confidence if best_phone else ""
        enriched["website_email_is_generic"] = bool(best_email.is_generic_email) if best_email else False
        enriched["preferred_email"] = best_email.normalized_value if best_email else listing_email
        enriched["preferred_phone"] = best_phone.normalized_value if best_phone else listing_phone
        enriched["preferred_email_source"] = "website" if best_email else ("listing" if listing_email else "")
        enriched["preferred_phone_source"] = "website" if best_phone else ("listing" if listing_phone else "")
        enriched_records.append(enriched)

    return enriched_records


def write_candidate_pages(
    records: list[dict[str, object]],
    output_path: Path | None = None,
    *,
    config: PipelineConfig | None = None,
) -> Path:
    runtime_config = config or PipelineConfig()
    path = output_path or runtime_config.website_page_attempts_output_path
    path.parent.mkdir(parents=True, exist_ok=True)
    archive_dir = path.parent / "archive" / path.stem
    archive_dir.mkdir(parents=True, exist_ok=True)

    attempts = discover_candidate_pages(records, runtime_config)
    payload = json.dumps([asdict(attempt) for attempt in attempts], indent=2)
    path.write_text(payload, encoding="utf-8")

    timestamp = _timestamp_now()
    archive_dir.joinpath(f"{_snapshot_token(timestamp)}.json").write_text(payload, encoding="utf-8")
    return path


def write_website_contacts(
    records: list[dict[str, object]],
    output_path: Path | None = None,
    *,
    config: PipelineConfig | None = None,
) -> Path:
    runtime_config = config or PipelineConfig()
    path = output_path or runtime_config.website_contacts_output_path
    path.parent.mkdir(parents=True, exist_ok=True)
    archive_dir = path.parent / "archive" / path.stem
    archive_dir.mkdir(parents=True, exist_ok=True)

    findings = extract_website_contacts(records, runtime_config)
    payload = json.dumps([asdict(finding) for finding in findings], indent=2)
    path.write_text(payload, encoding="utf-8")

    timestamp = _timestamp_now()
    archive_dir.joinpath(f"{_snapshot_token(timestamp)}.json").write_text(payload, encoding="utf-8")
    return path


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


def _fetch_attempt(
    session: requests.Session,
    requested_url: str,
    path_hint: str,
    website_domain: str,
    config: PipelineConfig,
) -> WebsitePageAttempt:
    response = _fetch_page(session, requested_url, config)
    return WebsitePageAttempt(
        website=requested_url if path_hint == "/" else requested_url.rsplit(path_hint, 1)[0],
        website_domain=website_domain,
        path_hint=path_hint,
        requested_url=requested_url,
        final_url=str(response["final_url"]),
        http_status=response["status_code"],
        fetch_status="fetched" if response["ok"] else "fetch_failed",
        failure_reason=str(response["failure_reason"]),
        attempted_at=_timestamp_now(),
    )


def _normalize_website(website: str) -> tuple[str, str]:
    normalized = website.strip()
    if normalized.startswith("www."):
        normalized = f"https://{normalized}"
    if "://" not in normalized:
        normalized = f"https://{normalized}"

    parsed = urlparse(normalized)
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc.removeprefix("www.")
    if not netloc:
        return "", ""

    return f"{parsed.scheme.lower()}://{netloc}", netloc


def _build_probe_url(website: str, path_hint: str) -> str:
    if path_hint == "/":
        return website
    return urljoin(f"{website}/", path_hint.lstrip("/"))


def _sleep_for_rate_limit(url: str, config: PipelineConfig) -> None:
    if urlparse(url).netloc:
        time.sleep(config.crawl_delay_seconds)


def _fetch_page(
    session: requests.Session,
    requested_url: str,
    config: PipelineConfig,
) -> dict[str, object]:
    last_status: int | None = None
    last_failure = ""
    final_url = ""

    for attempt in range(config.retry_attempts + 1):
        if attempt > 0:
            time.sleep(config.crawl_delay_seconds)

        try:
            response = session.get(
                requested_url,
                timeout=config.request_timeout_seconds,
                allow_redirects=True,
            )
            last_status = response.status_code
            final_url = response.url
            if 200 <= response.status_code < 400:
                _sleep_for_rate_limit(requested_url, config)
                return {
                    "ok": True,
                    "status_code": response.status_code,
                    "failure_reason": "",
                    "final_url": response.url,
                    "text": response.text,
                }
            last_failure = f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            last_failure = str(exc)

    _sleep_for_rate_limit(requested_url, config)
    return {
        "ok": False,
        "status_code": last_status,
        "failure_reason": last_failure,
        "final_url": final_url,
        "text": "",
    }


def _extract_contact_findings(
    *,
    website: str,
    website_domain: str,
    page_url: str,
    path_hint: str,
    html: str,
) -> list[WebsiteContactFinding]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)
    findings: list[WebsiteContactFinding] = []

    for anchor in soup.select("a[href^='mailto:']"):
        raw_value = unquote((anchor.get("href") or "").removeprefix("mailto:").split("?", 1)[0]).strip()
        normalized = raw_value.lower()
        if not normalized:
            continue
        findings.append(
            WebsiteContactFinding(
                website=website,
                website_domain=website_domain,
                page_url=page_url,
                path_hint=path_hint,
                contact_type="email",
                contact_value=raw_value,
                normalized_value=normalized,
                extraction_method="mailto_link",
                confidence=_email_confidence(normalized, website_domain, path_hint, "mailto_link"),
                is_generic_email=_is_generic_email(normalized),
                extracted_at=_timestamp_now(),
            )
        )

    for email in sorted(set(EMAIL_PATTERN.findall(page_text))):
        normalized = email.lower()
        findings.append(
            WebsiteContactFinding(
                website=website,
                website_domain=website_domain,
                page_url=page_url,
                path_hint=path_hint,
                contact_type="email",
                contact_value=email,
                normalized_value=normalized,
                extraction_method="visible_text",
                confidence=_email_confidence(normalized, website_domain, path_hint, "visible_text"),
                is_generic_email=_is_generic_email(normalized),
                extracted_at=_timestamp_now(),
            )
        )

    for anchor in soup.select("a[href^='tel:']"):
        raw_value = unquote((anchor.get("href") or "").removeprefix("tel:").strip())
        normalized = _normalize_phone(raw_value)
        if not normalized:
            continue
        findings.append(
            WebsiteContactFinding(
                website=website,
                website_domain=website_domain,
                page_url=page_url,
                path_hint=path_hint,
                contact_type="phone",
                contact_value=raw_value,
                normalized_value=normalized,
                extraction_method="tel_link",
                confidence=_phone_confidence(path_hint, "tel_link"),
                is_generic_email=False,
                extracted_at=_timestamp_now(),
            )
        )

    for raw_phone in sorted(set(PHONE_PATTERN.findall(page_text))):
        normalized = _normalize_phone(raw_phone)
        if not normalized:
            continue
        findings.append(
            WebsiteContactFinding(
                website=website,
                website_domain=website_domain,
                page_url=page_url,
                path_hint=path_hint,
                contact_type="phone",
                contact_value=raw_phone,
                normalized_value=normalized,
                extraction_method="visible_text",
                confidence=_phone_confidence(path_hint, "visible_text"),
                is_generic_email=False,
                extracted_at=_timestamp_now(),
            )
        )

    return findings


def _email_confidence(email: str, website_domain: str, path_hint: str, extraction_method: str) -> str:
    email_domain = email.split("@", 1)[1] if "@" in email else ""
    if extraction_method == "mailto_link" and email_domain == website_domain and path_hint in {"/contact", "/contact-us"}:
        return "high"
    if email_domain == website_domain and path_hint in {"/contact", "/contact-us"}:
        return "high"
    if email_domain == website_domain and path_hint in {"/", "/about", "/team"}:
        return "medium"
    return "low"


def _phone_confidence(path_hint: str, extraction_method: str) -> str:
    if extraction_method == "tel_link" and path_hint in {"/contact", "/contact-us"}:
        return "high"
    if path_hint in {"/contact", "/contact-us"}:
        return "high"
    if path_hint in {"/", "/about", "/team"}:
        return "medium"
    return "low"


def _normalize_phone(phone: str) -> str:
    try:
        parsed = phonenumbers.parse(phone, "US")
        if phonenumbers.is_possible_number(parsed) and phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass

    digits_only = re.sub(r"\D+", "", phone)
    if len(digits_only) == 10:
        return f"+1{digits_only}"
    if len(digits_only) == 11 and digits_only.startswith("1"):
        return f"+{digits_only}"
    return ""


def _is_generic_email(email: str) -> bool:
    local_part = email.split("@", 1)[0] if "@" in email else ""
    return local_part in {"info", "hello", "contact", "office", "admin"}


def _dedupe_contact_findings(findings: list[WebsiteContactFinding]) -> list[WebsiteContactFinding]:
    deduped: dict[tuple[str, str, str, str], WebsiteContactFinding] = {}
    for finding in findings:
        key = (finding.website_domain, finding.page_url, finding.contact_type, finding.normalized_value)
        existing = deduped.get(key)
        if existing is None or CONFIDENCE_RANK[finding.confidence] > CONFIDENCE_RANK[existing.confidence]:
            deduped[key] = finding
    return list(deduped.values())


def _best_finding(findings: list[WebsiteContactFinding], contact_type: str) -> WebsiteContactFinding | None:
    candidates = [finding for finding in findings if finding.contact_type == contact_type]
    if not candidates:
        return None

    return sorted(
        candidates,
        key=lambda finding: (
            CONFIDENCE_RANK.get(finding.confidence, 0),
            0 if finding.is_generic_email else 1,
            METHOD_RANK.get(finding.extraction_method, 0),
            PATH_RANK.get(finding.path_hint, 0),
            finding.page_url,
        ),
        reverse=True,
    )[0]


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _snapshot_token(timestamp: str) -> str:
    return timestamp.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")
