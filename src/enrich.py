from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

from config import PipelineConfig


LIKELY_CONTACT_PATHS: tuple[str, ...] = ("/", "/contact", "/contact-us", "/about", "/team")


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
    last_status: int | None = None
    last_failure = ""

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
            if 200 <= response.status_code < 400:
                _sleep_for_rate_limit(requested_url, config)
                return WebsitePageAttempt(
                    website=requested_url if path_hint == "/" else requested_url.rsplit(path_hint, 1)[0],
                    website_domain=website_domain,
                    path_hint=path_hint,
                    requested_url=requested_url,
                    final_url=response.url,
                    http_status=response.status_code,
                    fetch_status="fetched",
                    failure_reason="",
                    attempted_at=_timestamp_now(),
                )
            last_failure = f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            last_failure = str(exc)

    _sleep_for_rate_limit(requested_url, config)
    return WebsitePageAttempt(
        website=requested_url if path_hint == "/" else requested_url.rsplit(path_hint, 1)[0],
        website_domain=website_domain,
        path_hint=path_hint,
        requested_url=requested_url,
        final_url="",
        http_status=last_status,
        fetch_status="fetch_failed",
        failure_reason=last_failure,
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


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _snapshot_token(timestamp: str) -> str:
    return timestamp.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")
