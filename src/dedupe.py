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


def _assign_location_group_ids(dataframe: pd.DataFrame) -> pd.DataFrame:
    normalized = dataframe.copy()
    location_groups: dict[str, list[int]] = {}

    for index, row in normalized.iterrows():
        address = str(row.get("normalized_address", "")).strip().lower()
        city = str(row.get("normalized_city", "")).strip().lower()
        state = str(row.get("normalized_state", "")).strip().lower()
        if not address or not city or not state:
            continue
        key = f"{address}|{city}|{state}"
        location_groups.setdefault(key, []).append(index)

    location_group_ids: list[str] = []
    row_to_location_group_id: dict[int, str] = {}
    next_group_number = 1

    for index, row in normalized.iterrows():
        if index in row_to_location_group_id:
            location_group_ids.append(row_to_location_group_id[index])
            continue

        address = str(row.get("normalized_address", "")).strip().lower()
        city = str(row.get("normalized_city", "")).strip().lower()
        state = str(row.get("normalized_state", "")).strip().lower()

        if address and city and state:
            key = f"{address}|{city}|{state}"
            candidates = location_groups.get(key, [])
            same_location_indexes = [
                candidate_index
                for candidate_index in candidates
                if (
                    str(normalized.loc[candidate_index, "business_group_id"]).strip()
                    == str(row.get("business_group_id", "")).strip()
                    or str(normalized.loc[candidate_index, "contact_group_id"]).strip()
                    == str(row.get("contact_group_id", "")).strip()
                )
            ]
        else:
            same_location_indexes = []

        location_group_id = f"location_{next_group_number:03d}"
        next_group_number += 1

        if same_location_indexes:
            for candidate_index in same_location_indexes:
                row_to_location_group_id[candidate_index] = location_group_id
        row_to_location_group_id[index] = location_group_id
        location_group_ids.append(location_group_id)

    normalized["location_group_id"] = location_group_ids
    return normalized


def apply_identity_resolution(dataframe: pd.DataFrame) -> pd.DataFrame:
    normalized = dataframe.copy()
    if normalized.empty:
        return normalized

    for column_name in ["contact_group_id", "business_group_id", "location_group_id"]:
        _ensure_column(normalized, column_name)

    normalized = _assign_contact_group_ids(normalized)
    normalized = _assign_business_group_ids(normalized)
    normalized = _assign_location_group_ids(normalized)
    return normalized
