"""Custom exceptions for dbt-guard."""


class DbtGuardError(Exception):
    """Base exception for all dbt-guard errors."""


class ManifestNotFoundError(DbtGuardError):
    """Raised when manifest.json cannot be found at the expected path."""


class ManifestParseError(DbtGuardError):
    """Raised when manifest.json exists but cannot be parsed as valid JSON."""


class LineageExtractionError(DbtGuardError):
    """Raised when compiled SQL cannot be parsed for column extraction."""


class ColumnLineageError(DbtGuardError):
    """Raised when column-level lineage resolution fails in strict mode."""
