from __future__ import annotations

import pandas as pd


class UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, index: int) -> int:
        if self.parent[index] != index:
            self.parent[index] = self.find(self.parent[index])
        return self.parent[index]

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def _ensure_column(dataframe: pd.DataFrame, column_name: str) -> None:
    if column_name not in dataframe.columns:
        dataframe[column_name] = ""


def _assign_contact_group_ids(dataframe: pd.DataFrame) -> pd.DataFrame:
    normalized = dataframe.copy()
    union_find = UnionFind(len(normalized))

    for column_name in ["normalized_email", "normalized_phone"]:
        groups: dict[str, list[int]] = {}
        for index, value in normalized[column_name].fillna("").items():
            normalized_value = str(value).strip()
            if not normalized_value:
                continue
            groups.setdefault(normalized_value, []).append(index)

        for matching_indexes in groups.values():
            anchor = matching_indexes[0]
            for duplicate_index in matching_indexes[1:]:
                union_find.union(anchor, duplicate_index)

    root_to_group_id: dict[int, str] = {}
    contact_group_ids: list[str] = []

    for position, index in enumerate(normalized.index, start=1):
        root = union_find.find(position - 1)
        if root not in root_to_group_id:
            root_to_group_id[root] = f"contact_{len(root_to_group_id) + 1:03d}"
        contact_group_ids.append(root_to_group_id[root])

    normalized["contact_group_id"] = contact_group_ids
    return normalized


def apply_identity_resolution(dataframe: pd.DataFrame) -> pd.DataFrame:
    normalized = dataframe.copy()
    if normalized.empty:
        return normalized

    for column_name in ["contact_group_id", "business_group_id", "location_group_id"]:
        _ensure_column(normalized, column_name)

    normalized = _assign_contact_group_ids(normalized)
    return normalized
