"""Manifest loading and parsing for dbt-guard."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .exceptions import ManifestNotFoundError, ManifestParseError
from .models import ColumnInfo, ModelColumns

logger = logging.getLogger(__name__)

# Resource types we diff; everything else (test, analysis, snapshot) is ignored.
_DIFFABLE_TYPES = {"model", "seed"}

# Resource type prefixes present in child_map keys / values that we care about.
_DIFFABLE_PREFIXES = ("model.", "seed.")


def load_manifest(
    manifest_dir: Path,
) -> Tuple[Dict[str, ModelColumns], Dict[str, List[str]]]:
    """Load manifest.json from *manifest_dir*.

    Returns
    -------
    models:
        Mapping of ``unique_id`` -> :class:`ModelColumns` for every model/seed
        node found in the manifest.
    child_map:
        Mapping of ``unique_id`` -> list of child ``unique_id`` values, limited
        to model/seed nodes only (tests, analyses, etc. are excluded).
    """
    manifest_path = manifest_dir / "manifest.json"
    if not manifest_path.exists():
        raise ManifestNotFoundError(f"manifest.json not found in {manifest_dir}")

    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ManifestParseError(f"Invalid JSON in {manifest_path}: {exc}") from exc

    nodes: dict = data.get("nodes", {})
    raw_child_map: dict = data.get("child_map", {})

    models: Dict[str, ModelColumns] = {}

    # Process nodes (models, seeds, tests, …)
    for uid, node in nodes.items():
        if node.get("resource_type") not in _DIFFABLE_TYPES:
            continue
        _parse_node(uid, node, manifest_dir, models)

    # Sources are never directly diffed but could appear in child_map keys.
    # We do not add them to models — only models/seeds are diffed.

    # Build child_map containing only model/seed relationships
    child_map: Dict[str, List[str]] = {}
    for parent_id, children in raw_child_map.items():
        if not parent_id.startswith(_DIFFABLE_PREFIXES):
            continue
        model_children = [c for c in children if c.startswith(_DIFFABLE_PREFIXES)]
        if model_children:
            child_map[parent_id] = model_children

    logger.debug(
        "Loaded manifest from %s: %d models, %d child relationships",
        manifest_dir,
        len(models),
        len(child_map),
    )
    return models, child_map


def _parse_node(
    uid: str,
    node: dict,
    manifest_dir: Path,
    models: Dict[str, ModelColumns],
) -> None:
    """Parse a single manifest node and add it to *models*."""
    cols: Dict[str, ColumnInfo] = {}
    for col_name, col_data in node.get("columns", {}).items():
        key = col_name.lower()
        cols[key] = ColumnInfo(
            name=key,
            data_type=col_data.get("data_type") or None,
        )

    compiled_sql = _get_compiled_sql(node, manifest_dir)

    models[uid] = ModelColumns(
        model_id=uid,
        model_name=node["name"],
        columns=cols,
        has_compiled_sql=compiled_sql is not None,
        _compiled_sql=compiled_sql,
    )


def _get_compiled_sql(node: dict, manifest_dir: Path) -> Optional[str]:
    """Return the compiled SQL text for *node*, or ``None`` if unavailable.

    dbt ``parse`` outputs do not populate ``compiled_code`` or
    ``compiled_path`` on nodes.  The compiled file lives on disk at:

        ``{manifest_dir}/compiled/{package_name}/{original_file_path}``

    We try that path directly; if the file is absent we return ``None`` and
    the caller will fall back to documented-column metadata.
    """
    pkg = node.get("package_name") or ""
    orig = node.get("original_file_path") or ""

    if not pkg or not orig:
        return None

    candidate = manifest_dir / "compiled" / pkg / orig
    if candidate.exists():
        try:
            return candidate.read_text(encoding="utf-8")
        except OSError as exc:
            logger.debug("Could not read compiled SQL at %s: %s", candidate, exc)
            return None

    # Some manifests store the path with a leading models/ or seeds/ prefix
    # already included in original_file_path; others do not.  The formula
    # above is correct per dbt internals — no fallback needed.
    return None
