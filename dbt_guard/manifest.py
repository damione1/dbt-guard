"""Manifest loading and parsing for dbt-guard."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from .exceptions import ManifestNotFoundError, ManifestParseError
from .models import ColumnInfo, ExposureInfo, ModelColumns

logger = logging.getLogger(__name__)

# Resource types we diff; everything else (test, analysis) is ignored by default.
_DIFFABLE_TYPES = {"model", "seed"}

# Resource type prefixes present in child_map keys / values that we care about.
_DIFFABLE_PREFIXES = ("model.", "seed.")


@dataclass
class ManifestData:
    """Parsed manifest contents."""

    models: Dict[str, ModelColumns] = field(default_factory=dict)
    child_map: Dict[str, List[str]] = field(default_factory=dict)
    sources: Dict[str, ModelColumns] = field(default_factory=dict)
    exposures: Dict[str, ExposureInfo] = field(default_factory=dict)
    undocumented_sources: List[str] = field(default_factory=list)


def load_manifest(
    manifest_dir: Path,
    include_sources: bool = False,
    include_snapshots: bool = False,
    include_exposures: bool = False,
    warn_undocumented_sources: bool = False,
) -> ManifestData:
    """Load manifest.json from *manifest_dir*.

    Returns a :class:`ManifestData` with models, child_map, and optionally
    sources, exposures, and undocumented source warnings.
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

    # Build the set of diffable types and child_map prefixes
    diffable_types = set(_DIFFABLE_TYPES)
    diffable_prefixes = list(_DIFFABLE_PREFIXES)

    if include_snapshots:
        diffable_types.add("snapshot")
        diffable_prefixes.append("snapshot.")

    result = ManifestData()

    # Process nodes (models, seeds, snapshots, …)
    for uid, node in nodes.items():
        if node.get("resource_type") not in diffable_types:
            continue
        _parse_node(uid, node, manifest_dir, result.models)

    # Process sources
    if include_sources:
        diffable_prefixes.append("source.")
        raw_sources: dict = data.get("sources", {})
        for uid, source_node in raw_sources.items():
            _parse_source(uid, source_node, result.sources)
            if warn_undocumented_sources and not source_node.get("columns"):
                result.undocumented_sources.append(uid)

    # Process exposures
    if include_exposures:
        raw_exposures: dict = data.get("exposures", {})
        for uid, exp_node in raw_exposures.items():
            _parse_exposure(uid, exp_node, result.exposures)

    # Build child_map containing only relevant relationships
    prefix_tuple = tuple(diffable_prefixes)
    for parent_id, children in raw_child_map.items():
        if not parent_id.startswith(prefix_tuple):
            continue
        relevant_children = [c for c in children if c.startswith(prefix_tuple)]
        if relevant_children:
            result.child_map[parent_id] = relevant_children

    logger.debug(
        "Loaded manifest from %s: %d models, %d child relationships, "
        "%d sources, %d exposures",
        manifest_dir,
        len(result.models),
        len(result.child_map),
        len(result.sources),
        len(result.exposures),
    )
    return result


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
        resource_type=node.get("resource_type", "model"),
        columns=cols,
        has_compiled_sql=compiled_sql is not None,
        compiled_sql=compiled_sql,
    )


def _parse_source(
    uid: str,
    source_node: dict,
    sources: Dict[str, ModelColumns],
) -> None:
    """Parse a dbt source into a ModelColumns (no compiled SQL)."""
    cols: Dict[str, ColumnInfo] = {}
    for col_name, col_data in source_node.get("columns", {}).items():
        key = col_name.lower()
        cols[key] = ColumnInfo(
            name=key,
            data_type=col_data.get("data_type") or None,
        )

    sources[uid] = ModelColumns(
        model_id=uid,
        model_name=source_node.get("name", uid.split(".")[-1]),
        resource_type="source",
        columns=cols,
        has_compiled_sql=False,
    )


def _parse_exposure(
    uid: str,
    exp_node: dict,
    exposures: Dict[str, ExposureInfo],
) -> None:
    """Parse a dbt exposure into ExposureInfo."""
    owner = exp_node.get("owner", {}) or {}
    depends_on = exp_node.get("depends_on", {}) or {}

    exposures[uid] = ExposureInfo(
        exposure_id=uid,
        name=exp_node.get("name", uid.split(".")[-1]),
        type=exp_node.get("type", "unknown"),
        owner_name=owner.get("name"),
        owner_email=owner.get("email"),
        url=exp_node.get("url"),
        depends_on_nodes=depends_on.get("nodes", []),
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
