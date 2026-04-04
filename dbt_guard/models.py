"""Data models for dbt-guard."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional


@dataclass
class ColumnInfo:
    """Metadata for a single column in a dbt model."""

    name: str
    data_type: Optional[str] = None  # None when undocumented (very common in practice)


@dataclass
class ModelColumns:
    """Column inventory for a single dbt model or seed."""

    model_id: str       # unique_id: "model.alesco.DT_PAYMENTS"
    model_name: str     # short name: "DT_PAYMENTS"
    resource_type: str = "model"  # "model", "seed", "source", "snapshot"
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)  # keyed by lowercase name
    has_compiled_sql: bool = False
    # Compiled SQL text, stored separately to keep repr clean.
    # Populated by manifest.py when the compiled file exists on disk.
    _compiled_sql: Optional[str] = field(default=None, repr=False, compare=False)


@dataclass
class ColumnChange:
    """A detected change between two column inventories."""

    change_type: Literal["removed", "renamed", "type_changed", "added"]
    model_id: str
    model_name: str
    column_name: str       # column in the *base* model (or new name for "added")
    old_value: Optional[str] = None  # renamed: old name; type_changed: old type
    new_value: Optional[str] = None  # renamed: new name; type_changed: new type
    is_breaking: bool = True


@dataclass
class ImpactedModel:
    """A downstream model transitively affected by a breaking change."""

    model_id: str
    model_name: str
    distance: int  # BFS hops from the directly-changed model


@dataclass
class ExposureInfo:
    """Metadata for a dbt exposure (dashboard, notebook, etc.)."""

    exposure_id: str
    name: str
    type: str  # "dashboard", "notebook", etc.
    owner_name: Optional[str] = None
    owner_email: Optional[str] = None
    url: Optional[str] = None
    depends_on_nodes: List[str] = field(default_factory=list)


@dataclass
class ImpactedExposure:
    """An exposure affected by breaking changes in its upstream models."""

    exposure_id: str
    name: str
    type: str
    owner_name: Optional[str] = None
    owner_email: Optional[str] = None
    url: Optional[str] = None
    impacted_models: List[str] = field(default_factory=list)  # model names
    impacted_columns: Dict[str, List[str]] = field(default_factory=dict)  # model_name -> [col_names]


@dataclass
class ColumnLineageLink:
    """A single node in a column-level lineage chain."""

    model_id: str
    model_name: str
    column_name: str


@dataclass
class ImpactedColumn:
    """A column in a downstream model affected by an upstream change."""

    column_name: str
    reason: str  # e.g. "references removed column X from model Y"
    chain: List[ColumnLineageLink] = field(default_factory=list)


@dataclass
class ColumnLineageImpact:
    """Column-level lineage impact for a single downstream model."""

    model_id: str
    model_name: str
    impacted_columns: List[ImpactedColumn] = field(default_factory=list)
    cleared: bool = False  # True = column lineage proved no impact


@dataclass
class DiffReport:
    """Full output of a dbt-guard diff run."""

    base_path: str
    current_path: str
    breaking_changes: List[ColumnChange] = field(default_factory=list)
    non_breaking_changes: List[ColumnChange] = field(default_factory=list)
    impacted_models: List[ImpactedModel] = field(default_factory=list)
    # v0.2 fields — all default to empty for backward compatibility
    source_changes: List[ColumnChange] = field(default_factory=list)
    column_lineage_impacts: List[ColumnLineageImpact] = field(default_factory=list)
    cleared_models: List[str] = field(default_factory=list)
    impacted_exposures: List[ImpactedExposure] = field(default_factory=list)
    undocumented_sources: List[str] = field(default_factory=list)

    @property
    def has_breaking_changes(self) -> bool:
        return len(self.breaking_changes) > 0 or any(
            c.is_breaking for c in self.source_changes
        )

    @property
    def total_changes(self) -> int:
        return len(self.breaking_changes) + len(self.non_breaking_changes)
