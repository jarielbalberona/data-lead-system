from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
FINAL_DIR = DATA_DIR / "final"
DOCS_DIR = PROJECT_ROOT / "docs"
DISCOVERY_SEEDS_OUTPUT_PATH = PROCESSED_DIR / "discovery_seeds.json"
CLASSIFIED_LISTING_PAGES_OUTPUT_PATH = PROCESSED_DIR / "listing_pages_classified.json"
SOURCE_REGISTRY_OUTPUT_PATH = PROCESSED_DIR / "source_registry.json"
DISCOVERY_RAW_OUTPUT_PATH = RAW_DIR / "discovery_candidates_raw.json"
WEBSITE_PAGE_ATTEMPTS_OUTPUT_PATH = RAW_DIR / "website_page_attempts_raw.json"
WEBSITE_CONTACTS_OUTPUT_PATH = PROCESSED_DIR / "website_contacts.json"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 15
DEFAULT_RETRY_ATTEMPTS = 1
DEFAULT_CRAWL_DELAY_SECONDS = 1.0
DEFAULT_USER_AGENT = "LeadExtractionAssignmentBot/2.0 (+terminal-based educational pipeline; contact: local-run)"

PROPERTY_MANAGER_SOURCE_URLS = [
    "https://hoamanagementcompanies.net/illinois/hoa-management-companies-in-chicago-il",
    "https://hoamanagementcompanies.net/texas/hoa-management-companies-in-dallas-tx",
    "https://hoamanagementcompanies.net/california/hoa-management-companies-in-los-angeles-ca",
]

INTERIOR_DESIGNER_SOURCE_URLS = [
    "https://www.interiordesignlink.com/california-interior-designers-decorators.html",
    "https://www.interiordesignlink.com/illinois-interior-designers-decorators.html",
    "https://www.interiordesignlink.com/texas-interior-designers-decorators.html",
    "https://www.interiordesignlink.com/chicago-interior-designers-decorators-illinois-60290.html",
]


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path = PROJECT_ROOT
    raw_dir: Path = RAW_DIR
    processed_dir: Path = PROCESSED_DIR
    final_dir: Path = FINAL_DIR
    docs_dir: Path = DOCS_DIR
    discovery_seeds_output_path: Path = DISCOVERY_SEEDS_OUTPUT_PATH
    classified_listing_pages_output_path: Path = CLASSIFIED_LISTING_PAGES_OUTPUT_PATH
    source_registry_output_path: Path = SOURCE_REGISTRY_OUTPUT_PATH
    discovery_raw_output_path: Path = DISCOVERY_RAW_OUTPUT_PATH
    website_page_attempts_output_path: Path = WEBSITE_PAGE_ATTEMPTS_OUTPUT_PATH
    website_contacts_output_path: Path = WEBSITE_CONTACTS_OUTPUT_PATH
    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    crawl_delay_seconds: float = DEFAULT_CRAWL_DELAY_SECONDS
    user_agent: str = DEFAULT_USER_AGENT

    def ensure_directories(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.final_dir.mkdir(parents=True, exist_ok=True)
