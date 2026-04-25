from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
FINAL_DIR = DATA_DIR / "final"
DOCS_DIR = PROJECT_ROOT / "docs"


@dataclass(frozen=True)
class PipelineConfig:
    project_root: Path = PROJECT_ROOT
    raw_dir: Path = RAW_DIR
    processed_dir: Path = PROCESSED_DIR
    final_dir: Path = FINAL_DIR
    docs_dir: Path = DOCS_DIR

    def ensure_directories(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.final_dir.mkdir(parents=True, exist_ok=True)
