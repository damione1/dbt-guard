# dbt-guard

[![PyPI](https://img.shields.io/pypi/v/dbt-guard)](https://pypi.org/project/dbt-guard/)
[![CI](https://github.com/damione1/dbt-guard/actions/workflows/ci.yml/badge.svg)](https://github.com/damione1/dbt-guard/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

Column-level lineage breaking change detection for dbt Core CI pipelines.

dbt-guard detects when a model's output columns change in a way that would break downstream consumers — before the code reaches production. It works by comparing two `manifest.json` files (the base branch vs. the PR branch) using static analysis only: no database connection required.

This tool addresses the gap described in [dbt-core issue #6869](https://github.com/dbt-labs/dbt-core/issues/6869): dbt has no built-in mechanism for blocking PRs that silently remove or rename columns that downstream models depend on.

## Quick start

```bash
pip install dbt-guard
```

Then in your CI pipeline, after running `dbt compile` on both the base branch and the PR branch:

```bash
dbt-guard diff \
  --base path/to/base/target/ \
  --current path/to/current/target/ \
  --dialect snowflake \
  --format github \
  --include-sources \
  --include-exposures \
  --column-lineage
```

Exit code 0 means no breaking changes. Exit code 1 means breaking changes were detected.

## GitHub Actions integration

```yaml
- name: Generate base manifest
  run: |
    git stash
    dbt compile --profiles-dir . --target ci
    cp -r target/ /tmp/base_target/
    git stash pop

- name: Generate current manifest
  run: dbt compile --profiles-dir . --target ci

- name: Column lineage check
  run: |
    dbt-guard diff \
      --base /tmp/base_target \
      --current target/ \
      --dialect snowflake \
      --format github \
      --include-sources \
      --include-exposures \
      --column-lineage
```

> **Note:** `dbt compile` produces compiled SQL files that enable column-level lineage resolution. If you use `dbt parse` instead, dbt-guard still works but falls back to documented columns only (no column-level tracing).

## Bitbucket Pipelines integration

```yaml
pipelines:
  pull-requests:
    '**':
      - step:
          name: Column lineage check
          image: python:3.12-slim
          script:
            - pip install dbt-guard dbt-core dbt-snowflake
            - git fetch origin $BITBUCKET_PR_DESTINATION_BRANCH
            - git stash
            - dbt compile --profiles-dir . --target ci
            - cp -r target/ /tmp/base_target/
            - git stash pop
            - dbt compile --profiles-dir . --target ci
            - dbt-guard diff --base /tmp/base_target --current target/ --dialect snowflake --column-lineage
```

## CLI reference

```
dbt-guard diff [OPTIONS]

Options:
  --base PATH           Directory containing base manifest.json  [required]
  --current PATH        Directory containing current manifest.json  [required]
  --dialect TEXT        SQL dialect: default, snowflake, bigquery, databricks,
                        redshift, trino  [default: default]
  --format TEXT         Output format: text, json, github  [default: text]
  --fail-on TEXT        When to exit non-zero: breaking, any, never
                        [default: breaking]
  --no-impact           Skip downstream impact analysis
  --max-depth INT       Max DAG hops for impact traversal  [default: 10]
  --output PATH         Write report to file instead of stdout
  --select MODEL        Limit diff to specific model names (repeatable)
  --quiet               Print one-line summary only
  --include-sources     Include dbt sources in the diff analysis
  --include-exposures   Include dbt exposures in impact analysis
  --include-snapshots   Include dbt snapshots in the diff analysis
  --column-lineage      Enable column-level lineage to reduce false positives
  --strict-lineage      Fail if compiled SQL is missing (requires --column-lineage)
  --warn-undocumented-sources  Warn about sources with no documented columns
  --version             Show version and exit
  --help                Show this message and exit
```

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | No breaking changes (or --fail-on never) |
| 1 | Breaking changes detected (or any changes with --fail-on any) |
| 2 | Tool error (manifest not found, invalid JSON, etc.) |

## How it works

1. **Parse both manifests.** dbt-guard reads `manifest.json` from the base and current target directories. No dbt execution, no database connection.

2. **Extract column inventories.** For each model (and optionally sources and snapshots), it reads the documented columns from `manifest.json`. If compiled SQL is present on disk (in `target/compiled/`), it additionally parses the SQL with [SQLGlot](https://github.com/tobymao/sqlglot) to detect undocumented columns.

3. **Diff columns.** For each model present in both manifests, it compares column sets:
   - Column removed → breaking
   - Column renamed (1 removed + 1 added, matching type) → breaking
   - Column type changed (only when documented on both sides) → breaking
   - Column added → non-breaking

4. **Impact analysis.** For each breaking change, it traverses the `child_map` in the manifest via BFS to find downstream models affected transitively.

5. **Column-level lineage (opt-in).** When `--column-lineage` is enabled, dbt-guard parses each downstream model's compiled SQL and uses `sqlglot.lineage` to trace column-to-column dependencies. Models that don't reference any changed column are cleared from the impact list. This propagates through the DAG: if model B references a changed column from model A, model B's affected output columns are tracked into model C, and so on.

6. **Exposure impact (opt-in).** When `--include-exposures` is enabled, dbt-guard checks which exposures depend on changed or impacted models and reports owner, type, and URL for each affected exposure.

7. **Report.** Output in text, JSON, or GitHub Actions annotation format.

### Column-level lineage: eliminating false positives

Without `--column-lineage`, dbt-guard uses model-level BFS: if model A has a breaking change, every downstream model is flagged. This produces false positives when a downstream model doesn't actually use the changed column.

With `--column-lineage`, dbt-guard traces which output columns reference the changed upstream column. Models with no dependency are **cleared** and removed from the impact list.

```
Example: stg_users.phone removed

Without --column-lineage:
  stg_users → int_order_summary  ← IMPACTED (false positive — uses name, not phone)
  stg_users → int_user_metrics   ← IMPACTED (true positive — uses phone)

With --column-lineage:
  stg_users → int_order_summary  ← CLEARED
  stg_users → int_user_metrics   ← IMPACTED
```

## Output formats

### Text (default)

Human-readable report with sections for breaking changes, non-breaking changes, downstream impact, source changes, column lineage detail, cleared models, exposure impact, and warnings.

### JSON

Machine-readable output for CI artifacts:

```json
{
  "summary": {
    "breaking": 1,
    "non_breaking": 1,
    "impacted_models": 1,
    "sources_changed": 1,
    "models_cleared": 1,
    "exposures_impacted": 1
  },
  "breaking_changes": [...],
  "non_breaking_changes": [...],
  "impacted_models": [...],
  "source_changes": [...],
  "column_lineage_impact": [...],
  "cleared_models": [...],
  "exposure_impact": [...],
  "undocumented_sources": [...]
}
```

### GitHub Actions

Annotation format:

- `::error::` for breaking changes (model and source)
- `::warning::` for exposure impacts
- `::notice::` for models cleared by column-level lineage

## What counts as breaking vs. non-breaking

| Change | Breaking? | Why |
|--------|-----------|-----|
| Column removed | Yes | Downstream SELECT or JOIN on that column will fail |
| Column renamed | Yes | All references to the old name break |
| Column type changed | Yes | Implicit casts may fail or produce wrong results |
| Column added | No | Additive; downstream consumers are unaffected |
| New model added | No | Nothing depends on it yet |
| Model removed from current | No | Not diffed; dbt will surface this as a ref() error |
| Source column removed | Yes | Models referencing that source column will fail |
| Source column type changed | Yes | Type mismatches in downstream models |

## Limitations

**SELECT * expansion.** When a model ends with `SELECT * FROM final_cte`, dbt-guard tries to resolve the star by tracing back through the CTE chain. If the star references a physical table (not a CTE), expansion fails and dbt-guard falls back to documented columns from schema.yml.

**No catalog required.** dbt-guard does not need `catalog.json` (the output of `dbt docs generate`). Column types are taken from schema.yml documentation when available. Type-change detection only fires when both the base and current sides have a documented `data_type`. Models where columns are entirely undocumented are still diffed by column name (removal/addition), just not by type.

**Parse-only manifests.** `dbt parse` does not compile SQL (compiled files are absent). In this mode, dbt-guard works exclusively from documented columns. Run `dbt compile` instead of `dbt parse` to enable SQL-based column extraction and column-level lineage resolution.

**Rename heuristic.** The rename detection (1 removed + 1 added with matching type) is a best-effort heuristic. If a model removes one column and adds a different one in the same PR, dbt-guard will report it as a rename. Use `--format json` to inspect the raw events.

**Column ordering.** dbt-guard does not detect column reordering. Changing the position of a column in a SELECT is non-breaking for named references but breaking for positional references (e.g. `SELECT * FROM upstream` in the middle of a CTE). This is a known gap.

**Column lineage accuracy.** The `--column-lineage` feature relies on SQLGlot's ability to parse and trace column references through SQL. Complex SQL patterns (UDFs, dynamic SQL, certain dialect-specific syntax) may not resolve correctly. When tracing fails for a column, dbt-guard conservatively marks it as impacted rather than clearing it.

## Contributing

Contributions are welcome. The project uses standard Python tooling:

```bash
# Clone and install in editable mode with dev dependencies
git clone https://github.com/dbt-guard/dbt-guard
cd dbt-guard
pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check dbt_guard/

# Type check
mypy dbt_guard/
```

The test suite uses synthetic manifest fixtures in `tests/fixtures/manifests/`. To add a new test scenario, add a manifest pair there and write the corresponding test.

Key design decisions:
- Minimal dependencies: only `sqlglot` and `click`. No pandas, no dbt-core.
- Graceful degradation: if SQL parsing or column lineage tracing fails, fall back to documented columns or model-level impact rather than raising.
- Static analysis only: no database connection, no `dbt run` needed.

## License

Apache 2.0. See [LICENSE](LICENSE).
