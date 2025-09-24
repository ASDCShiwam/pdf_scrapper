"""Maintain a manifest of downloaded and indexed PDF documents."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional

_MANIFEST_FILENAME = "manifest.json"

_STATUS_PRIORITY: Mapping[str, int] = {
    "indexed": 40,
    "duplicate": 30,
    "no_text": 20,
    "not_indexed": 10,
    "unknown": 0,
}


def _manifest_path(base_dir: Path) -> Path:
    return Path(base_dir) / _MANIFEST_FILENAME


@dataclass
class Manifest:
    documents: List[Dict[str, object]]
    stats: Dict[str, object]
    updated_at: Optional[str]


def _default_manifest() -> Manifest:
    return Manifest(documents=[], stats=_compute_stats([]), updated_at=None)


def load_manifest(base_dir: Path) -> Manifest:
    """Load the manifest from ``base_dir`` if it exists."""

    path = _manifest_path(base_dir)
    if not path.exists():
        return _default_manifest()

    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return _default_manifest()

    documents = list(data.get("documents", []))
    for doc in documents:
        doc.setdefault("status", "unknown")
        doc.setdefault("indexed", doc.get("status") == "indexed")
    stats = _compute_stats(documents)
    updated_at = data.get("updated_at")
    return Manifest(documents=documents, stats=stats, updated_at=updated_at)


def update_manifest(base_dir: Path, records: Iterable[Mapping[str, object]]) -> Manifest:
    """Update the manifest with the provided document ``records``."""

    base_dir = Path(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    path = _manifest_path(base_dir)

    existing = load_manifest(base_dir)
    by_hash: MutableMapping[str, Dict[str, object]] = {
        str(doc.get("sha256") or doc.get("id")): dict(doc)
        for doc in existing.documents
        if doc.get("sha256") or doc.get("id")
    }

    changed = False
    for record in records:
        sha = str(record.get("sha256") or record.get("id") or "")
        if not sha:
            continue

        current = by_hash.get(sha, {})
        merged = _merge_record(current, record)
        if current != merged:
            by_hash[sha] = merged
            changed = True

    if not changed and path.exists():
        return existing

    documents = sorted(
        by_hash.values(),
        key=lambda item: str(item.get("downloaded_at", "")),
        reverse=True,
    )
    stats = _compute_stats(documents)
    manifest = Manifest(
        documents=documents,
        stats=stats,
        updated_at=datetime.utcnow().isoformat() + "Z",
    )

    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "documents": manifest.documents,
                "stats": manifest.stats,
                "updated_at": manifest.updated_at,
            },
            handle,
            indent=2,
        )

    return manifest


def _merge_record(
    existing: Mapping[str, object], new_data: Mapping[str, object]
) -> Dict[str, object]:
    merged = dict(existing)
    for key in {
        "name",
        "path",
        "url",
        "source_page",
        "downloaded_at",
        "size",
        "sha256",
        "id",
    }:
        value = new_data.get(key)
        if value is not None:
            merged[key] = value

    new_status = str(new_data.get("status") or "unknown")
    current_status = str(merged.get("status") or "unknown")
    if _STATUS_PRIORITY.get(new_status, 0) >= _STATUS_PRIORITY.get(current_status, 0):
        merged["status"] = new_status

    if "indexed" in new_data:
        new_flag = bool(new_data.get("indexed"))
        existing_flag = bool(merged.get("indexed"))
        merged["indexed"] = new_flag or existing_flag
    else:
        merged.setdefault("indexed", merged.get("status") == "indexed")

    return merged


def _compute_stats(documents: Iterable[Mapping[str, object]]) -> Dict[str, object]:
    stats: Dict[str, object] = {
        "total": 0,
        "indexed": 0,
        "duplicates": 0,
        "skipped": 0,
        "total_size": 0,
        "status_breakdown": {},
    }

    for doc in documents:
        stats["total"] += 1
        size = doc.get("size")
        if isinstance(size, (int, float)):
            stats["total_size"] += int(size)

        status = str(doc.get("status") or "unknown")
        breakdown: Dict[str, int] = stats["status_breakdown"]  # type: ignore[assignment]
        breakdown[status] = breakdown.get(status, 0) + 1

        if status == "indexed":
            stats["indexed"] += 1
        elif status == "duplicate":
            stats["duplicates"] += 1
        else:
            stats["skipped"] += 1

    return stats
