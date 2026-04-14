#!/usr/bin/env python3
"""
LHI Related Table Sync Utility

Synchronize a target field in an ArcGIS Online / Portal hosted feature layer
from a source field in a related or companion hosted table by matching on a
shared key.

Key features
- Dry-run mode
- Audit CSV output
- Batch update support for AGOL
- Config-driven behavior
- Source record selection rules: latest, earliest, first_non_null

Tested pattern
- Main feature layer with null target values
- Source table with authoritative values
- Shared key such as TREE_ID / Asset_ID / Inspection_ID
"""

from __future__ import annotations

import csv
import datetime as dt
import getpass
import json
import os
import sys
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from arcgis.features import FeatureLayer
from arcgis.gis import GIS


def ensure_folder(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_oid_field(layer_obj: FeatureLayer) -> str:
    props = layer_obj.properties
    if hasattr(props, "objectIdField") and props.objectIdField:
        return props.objectIdField
    for fld in props.fields:
        if fld["type"] == "esriFieldTypeOID":
            return fld["name"]
    raise ValueError(f"Could not determine ObjectID field for {layer_obj.url}")


def normalize_key(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return str(int(value)).strip()
    except Exception:
        return str(value).strip()


def epoch_ms_to_datetime(value: Any) -> Optional[dt.datetime]:
    if value in (None, "", 0):
        return None
    if isinstance(value, dt.datetime):
        return value
    try:
        return dt.datetime.fromtimestamp(value / 1000.0, tz=dt.timezone.utc)
    except Exception:
        return None


def datetime_to_epoch_ms(value: Optional[dt.datetime]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return int(value.timestamp() * 1000)


def format_datetime(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        value = epoch_ms_to_datetime(value)
    if value is None:
        return ""
    return value.astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_features_in_chunks(
    layer_obj: FeatureLayer,
    where: str = "1=1",
    out_fields: str = "*",
    return_geometry: bool = False,
    chunk_size: int = 1000,
) -> List[Any]:
    oid_field = get_oid_field(layer_obj)
    oid_info = layer_obj.query(where=where, return_ids_only=True)
    object_ids = oid_info.get("objectIds", []) if isinstance(oid_info, dict) else []

    if not object_ids:
        return []

    object_ids = sorted(object_ids)
    total = len(object_ids)
    print(f"Found {total:,} records in {layer_obj.url}")

    all_features: List[Any] = []
    for idx in range(0, total, chunk_size):
        batch_ids = object_ids[idx : idx + chunk_size]
        where_ids = f"{oid_field} IN ({','.join(map(str, batch_ids))})"
        fs = layer_obj.query(
            where=where_ids,
            out_fields=out_fields,
            return_geometry=return_geometry,
        )
        all_features.extend(fs.features)
        print(f"  fetched {min(idx + chunk_size, total):,}/{total:,}")
    return all_features


def choose_value(
    existing: Optional[dt.datetime],
    candidate: dt.datetime,
    selection_rule: str,
) -> dt.datetime:
    if existing is None:
        return candidate
    if selection_rule == "latest":
        return candidate if candidate > existing else existing
    if selection_rule == "earliest":
        return candidate if candidate < existing else existing
    if selection_rule == "first_non_null":
        return existing
    raise ValueError(
        "selection_rule must be one of: latest, earliest, first_non_null"
    )


def build_source_lookup(
    source_rows: List[Any],
    source_key_field: str,
    source_value_field: str,
    selection_rule: str,
) -> Tuple[Dict[str, dt.datetime], Dict[str, int]]:
    value_by_key: Dict[str, dt.datetime] = {}
    counts_by_key: Dict[str, int] = defaultdict(int)

    for row in source_rows:
        attrs = row.attributes
        key = normalize_key(attrs.get(source_key_field))
        value_dt = epoch_ms_to_datetime(attrs.get(source_value_field))

        if key is None or value_dt is None:
            continue

        counts_by_key[key] += 1
        value_by_key[key] = choose_value(value_by_key.get(key), value_dt, selection_rule)

    return value_by_key, counts_by_key


def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def connect_gis(portal_url: str, username: Optional[str]) -> GIS:
    user = username or input("Portal username: ").strip()
    password = getpass.getpass("Portal password: ")
    print("Connecting to portal...")
    gis = GIS(portal_url, user, password)
    print(f"Connected as: {gis.users.me.username}")
    return gis


def main(config_path: str) -> int:
    config = load_config(config_path)

    portal_url = config["portal_url"]
    username = config.get("username")
    main_layer_url = config["main_layer_url"]
    source_table_url = config["source_table_url"]
    main_key_field = config["main_key_field"]
    source_key_field = config["source_key_field"]
    source_value_field = config["source_value_field"]
    target_value_field = config["target_value_field"]
    selection_rule = config.get("selection_rule", "latest")
    main_where = config.get("main_where", f"{target_value_field} IS NULL AND {main_key_field} IS NOT NULL")
    source_where = config.get("source_where", f"{source_value_field} IS NOT NULL AND {source_key_field} IS NOT NULL")
    output_folder = config["output_folder"]
    audit_csv_name = config.get("audit_csv_name", "lhi_related_table_sync_audit.csv")
    final_audit_csv_name = config.get("final_audit_csv_name", audit_csv_name.replace(".csv", "_final.csv"))
    dry_run = bool(config.get("dry_run", True))
    batch_size = int(config.get("batch_size", 200))
    query_chunk_size = int(config.get("query_chunk_size", 1000))
    action_label = config.get("action_label", "FIELD_SYNC")

    ensure_folder(output_folder)
    audit_path = os.path.join(output_folder, audit_csv_name)
    final_audit_path = os.path.join(output_folder, final_audit_csv_name)

    gis = connect_gis(portal_url, username)
    main_layer = FeatureLayer(main_layer_url, gis=gis)
    source_table = FeatureLayer(source_table_url, gis=gis)

    main_oid_field = get_oid_field(main_layer)
    source_oid_field = get_oid_field(source_table)

    print(f"Main layer OID field: {main_oid_field}")
    print(f"Source table OID field: {source_oid_field}")

    print("\nReading source table...")
    source_rows = fetch_features_in_chunks(
        source_table,
        where=source_where,
        out_fields=f"{source_oid_field},{source_key_field},{source_value_field}",
        return_geometry=False,
        chunk_size=query_chunk_size,
    )
    value_by_key, counts_by_key = build_source_lookup(
        source_rows,
        source_key_field=source_key_field,
        source_value_field=source_value_field,
        selection_rule=selection_rule,
    )
    multi_record_count = sum(1 for _, count in counts_by_key.items() if count > 1)
    print(f"Unique source keys with usable values: {len(value_by_key):,}")
    print(f"Source keys with multiple source records: {multi_record_count:,}")

    if source_rows:
        print(f"Source rows fetched: {len(source_rows):,}")
        print(f"Sample source row keys: {list(source_rows[0].attributes.keys())}")
        print(f"Sample source row values: {source_rows[0].attributes}")

    print("\nReading target layer candidates...")
    main_rows = fetch_features_in_chunks(
        main_layer,
        where=main_where,
        out_fields=f"{main_oid_field},{main_key_field},{target_value_field}",
        return_geometry=False,
        chunk_size=query_chunk_size,
    )
    print(f"Candidate main features: {len(main_rows):,}")

    print("\nMatching records and preparing updates...")
    updates: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []

    for feat in main_rows:
        attrs = feat.attributes
        oid = attrs.get(main_oid_field)
        key = normalize_key(attrs.get(main_key_field))
        current_target = attrs.get(target_value_field)
        new_dt = value_by_key.get(key)

        if key is None or new_dt is None:
            continue

        updates.append(
            {
                "attributes": {
                    main_oid_field: oid,
                    target_value_field: datetime_to_epoch_ms(new_dt),
                }
            }
        )
        audit_rows.append(
            {
                "OBJECTID": oid,
                main_key_field: key,
                f"Old_{target_value_field}": format_datetime(current_target),
                f"New_{target_value_field}": format_datetime(new_dt),
                "Source_Record_Count": counts_by_key.get(key, 0),
                "Action": "WOULD_UPDATE" if dry_run else "UPDATED_PENDING",
                "Action_Label": action_label,
            }
        )

    print(f"Matched records to update: {len(updates):,}")

    fieldnames = [
        "OBJECTID",
        main_key_field,
        f"Old_{target_value_field}",
        f"New_{target_value_field}",
        "Source_Record_Count",
        "Action",
        "Action_Label",
    ]
    write_csv(audit_path, audit_rows, fieldnames)
    print(f"\nAudit CSV written to:\n{audit_path}")

    if dry_run:
        print("\nDRY_RUN = True")
        print("No edits were applied.")
        return 0

    print("\nApplying updates to portal...")
    total_updated = 0
    total_failed = 0
    success_oids = set()
    failed_oids = set()

    for batch_num, batch in enumerate(chunked(updates, batch_size), start=1):
        result = main_layer.edit_features(updates=batch)
        update_results = result.get("updateResults", []) if isinstance(result, dict) else []

        batch_success = 0
        batch_failed = 0
        for feat_dict, res in zip(batch, update_results):
            oid = feat_dict["attributes"][main_oid_field]
            if res.get("success"):
                batch_success += 1
                success_oids.add(oid)
            else:
                batch_failed += 1
                failed_oids.add(oid)

        total_updated += batch_success
        total_failed += batch_failed
        print(f"  batch {batch_num}: success={batch_success}, failed={batch_failed}")

    print("\nUpdate complete.")
    print(f"Total successful updates: {total_updated:,}")
    print(f"Total failed updates:     {total_failed:,}")

    for row in audit_rows:
        oid = row["OBJECTID"]
        if oid in success_oids:
            row["Action"] = "UPDATED"
        elif oid in failed_oids:
            row["Action"] = "FAILED"
        else:
            row["Action"] = "UNKNOWN"

    write_csv(final_audit_path, audit_rows, fieldnames)
    print(f"Final audit CSV written to:\n{final_audit_path}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python lhi_related_table_sync.py <config.json>")
        sys.exit(1)
    sys.exit(main(sys.argv[1]))
