from __future__ import annotations

import pandas as pd


def normalize_records(records: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(records)
