from __future__ import annotations

import re

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


def _business_signature(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _assign_business_group_ids(dataframe: pd.DataFrame) -> pd.DataFrame:
    normalized = dataframe.copy()
    union_find = UnionFind(len(normalized))

    domain_groups: dict[str, list[int]] = {}
    for index, value in normalized["website_domain"].fillna("").items():
        normalized_value = str(value).strip().lower()
        if not normalized_value:
            continue
        domain_groups.setdefault(normalized_value, []).append(index)

    for matching_indexes in domain_groups.values():
        anchor = matching_indexes[0]
        for duplicate_index in matching_indexes[1:]:
            union_find.union(anchor, duplicate_index)

    name_groups: dict[str, list[int]] = {}
    for index, value in normalized["normalized_business_name"].fillna("").items():
        signature = _business_signature(str(value).strip())
        if not signature:
            continue
        name_groups.setdefault(signature, []).append(index)

    for matching_indexes in name_groups.values():
        if len(matching_indexes) < 2:
            continue

        anchor = matching_indexes[0]
        anchor_row = normalized.loc[anchor]
        for duplicate_index in matching_indexes[1:]:
            duplicate_row = normalized.loc[duplicate_index]
            shared_contact = (
                str(anchor_row.get("contact_group_id", "")).strip()
                and str(anchor_row.get("contact_group_id", "")).strip()
                == str(duplicate_row.get("contact_group_id", "")).strip()
            )
            shared_location = (
                str(anchor_row.get("normalized_city", "")).strip()
                and str(anchor_row.get("normalized_city", "")).strip()
                == str(duplicate_row.get("normalized_city", "")).strip()
                and str(anchor_row.get("normalized_state", "")).strip()
                == str(duplicate_row.get("normalized_state", "")).strip()
            )
            if shared_contact or shared_location:
                union_find.union(anchor, duplicate_index)

    root_to_group_id: dict[int, str] = {}
    business_group_ids: list[str] = []

    for position, index in enumerate(normalized.index, start=1):
        root = union_find.find(position - 1)
        if root not in root_to_group_id:
            root_to_group_id[root] = f"business_{len(root_to_group_id) + 1:03d}"
        business_group_ids.append(root_to_group_id[root])

    normalized["business_group_id"] = business_group_ids
    return normalized


def apply_identity_resolution(dataframe: pd.DataFrame) -> pd.DataFrame:
    normalized = dataframe.copy()
    if normalized.empty:
        return normalized

    for column_name in ["contact_group_id", "business_group_id", "location_group_id"]:
        _ensure_column(normalized, column_name)

    normalized = _assign_contact_group_ids(normalized)
    normalized = _assign_business_group_ids(normalized)
    return normalized
