# dbt-optimizer

AI-powered SQL optimization advisor for dbt projects. Combines deterministic rule-based checks with Claude-powered deep analysis to surface actionable recommendations across any dbt project.

## Features

- **13 built-in rules** covering performance anti-patterns, data correctness risks, and dbt best practices
- **AI analysis via Claude** for nuanced, context-aware suggestions that go beyond pattern matching
- **dbt MCP integration** — connect to the dbt MCP server to analyze compiled SQL (Jinja resolved) and lineage-aware rules
- **Any project size** — handles large monorepos with hundreds of models by batching AI analysis and prioritizing models with existing issues
- **CI/CD-ready** — `--fail-on-severity` exit codes, JSON output, and `--select` filtering
- **Zero dbt dependency** — reads your project files directly, no dbt installation required

## Quickstart

```bash
# Install
pip install -e .

# Analyze current dbt project (AI enabled by default)
export ANTHROPIC_API_KEY=your_key_here
dbt-optimizer analyze

# Analyze a specific project path
dbt-optimizer analyze ~/projects/my_dbt_project

# Rule-based only (no API key needed)
dbt-optimizer analyze --no-ai

# Only show high and medium severity
dbt-optimizer analyze --min-severity medium

# Analyze models matching a pattern
dbt-optimizer analyze --select orders

# Write JSON report
dbt-optimizer analyze -o report.json

# Fail with exit code 1 if any high-severity issues found (for CI)
dbt-optimizer analyze --fail-on-severity high --no-ai
```

## Installation

**Requirements:** Python 3.9+

```bash
git clone https://github.com/your-org/dbt-optimizer
cd dbt-optimizer
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
pytest
```

## Rules Reference

| Rule ID | Title | Severity | Description |
|---------|-------|----------|-------------|
| OPT001 | SELECT \* usage | MEDIUM | Explicit column lists improve performance and prevent schema drift |
| OPT002 | Implicit cross join | HIGH | Comma-separated tables in FROM create cartesian products |
| OPT003 | SELECT DISTINCT overuse | MEDIUM | Often masks upstream fan-out bugs rather than fixing root cause |
| OPT004 | Subquery in FROM | LOW | Inline subqueries hurt readability; prefer CTEs |
| OPT005 | Function on filter column | MEDIUM | Non-sargable predicates prevent partition pruning |
| OPT006 | Hardcoded date literals | LOW | Magic dates create brittle models requiring code edits to update |
| OPT007 | High model complexity | MEDIUM | Models over 200+ lines or 8+ JOINs are hard to test and maintain |
| OPT008 | Table mat. on time-series model | MEDIUM | Full refreshes are expensive; consider incremental materialization |
| OPT009 | Missing dbt tests | MEDIUM | Models without tests allow silent data quality regressions |
| OPT010 | Missing model description | INFO | Undocumented models slow down onboarding and discoverability |
| OPT011 | Multiple aggregation levels | LOW | Multiple GROUP BY clauses in one model indicates mixed concerns |
| OPT012 | UNION without ALL | LOW | UNION (no ALL) runs an expensive sort + dedup pass |
| OPT013 | Direct table reference | HIGH | Bypasses dbt lineage, ref() resolution, and environment promotion |
| AI | AI deep analysis | varies | Claude analyzes SQL logic, join correctness, and warehouse-specific patterns |

Skip specific rules with `--skip-rules OPT001,OPT010`.

## dbt MCP Integration

The dbt MCP server gives dbt-optimizer access to **compiled SQL** (Jinja fully resolved) and **lineage data** that isn't available from reading project files alone. This unlocks:

- Analysis of the actual SQL sent to your warehouse, not the raw Jinja template
- Two additional lineage-aware rules: **LIN001** (orphaned models) and **LIN002** (high-fan-out views)
- More accurate AI analysis, since Claude sees resolved `{{ ref() }}` and `{{ source() }}` calls

### 1. Install the MCP extra

```bash
pip install -e ".[mcp]"
```

This installs the `mcp` Python package required for the MCP client. You also need the dbt MCP server itself:

```bash
pip install dbt-mcp
```

> The dbt MCP server requires a working dbt project (dbt Core or dbt Cloud) and valid `profiles.yml` / environment configuration. See the [dbt MCP docs](https://docs.getdbt.com/docs/core/connect-data-platform/about-core-connections) for setup.

### 2. Run with `--mcp`

```bash
# Enable MCP (compiled SQL + lineage)
dbt-optimizer analyze --mcp

# Skip lineage fetching for faster runs on large projects
dbt-optimizer analyze --mcp --mcp-no-lineage

# Use a non-default MCP server command
dbt-optimizer analyze --mcp --mcp-command /path/to/dbt-mcp

# Pass extra arguments to the MCP server
dbt-optimizer analyze --mcp --mcp-args "--profiles-dir /path/to/profiles"

# Combine with AI and other options
dbt-optimizer analyze --mcp --ai --min-severity medium -o report.json
```

### 3. What changes with MCP enabled

| Without MCP | With MCP |
|-------------|----------|
| Analyzes raw `.sql` files (Jinja templates) | Analyzes compiled SQL (Jinja fully resolved) |
| No lineage information | Upstream + downstream model lists populated |
| LIN001 / LIN002 silently skipped | LIN001 / LIN002 run and may fire |
| AI sees template variables | AI sees real table/column references |

A status line confirms what was fetched:

```
MCP: compiled SQL for 42/42 models, lineage fetched
```

### Lineage rules (require `--mcp`)

| Rule ID | Title | Severity |
|---------|-------|----------|
| LIN001 | Orphaned model (no downstream consumers) | LOW |
| LIN002 | High-fan-out view — consider materializing as table | MEDIUM |

These rules are listed under "Requires MCP (lineage)" in `dbt-optimizer list-rules`.

### Troubleshooting MCP

**`McpNotAvailableError`** — install the mcp extra: `pip install 'dbt-optimizer[mcp]'`

**`TimeoutError: Timed out waiting for dbt MCP server to start`** — verify `dbt-mcp` is on your `PATH` and your dbt project compiles cleanly (`dbt parse`).

**MCP enrichment failed / falling back to file-based analysis** — dbt-optimizer degrades gracefully; check the error message. Common causes: missing `profiles.yml`, incorrect `--mcp-command`, or dbt project errors.

## AI Analysis

When `ANTHROPIC_API_KEY` is set, dbt-optimizer sends each model to Claude for deeper analysis covering:

- Complex join correctness and fanout risks
- Warehouse-specific optimization opportunities (Snowflake, BigQuery, Databricks, Redshift)
- Aggregation grain mismatches
- Business logic that might produce incorrect results
- Patterns too nuanced for regex-based rules

**Cost control:** AI analysis is capped at `--ai-max-models 20` by default. Models are prioritized by existing rule violations and line count, so the most complex/problematic models are analyzed first. Set `--ai-max-models 0` for unlimited.

## CLI Reference

```
dbt-optimizer analyze [OPTIONS] [PROJECT_PATH]

Arguments:
  PROJECT_PATH    Root of the dbt project [default: .]

Options:
  --ai / --no-ai            Enable Claude AI analysis [default: ai]
  --ai-max-models N         Max models for AI analysis [default: 20]
  --min-severity LEVEL      Minimum severity to display: high|medium|low|info [default: info]
  --output, -o FILE         Write JSON report to file
  --format [terminal|json]  Output format [default: terminal]
  --group-by-model          Group output by model instead of severity
  --select PATTERN          Only analyze models whose name contains PATTERN
  --skip-rules RULE_IDS     Comma-separated rule IDs to skip
  --fail-on-severity LEVEL  Exit 1 if issues at this level or higher exist
  --mcp / --no-mcp          Connect to the dbt MCP server for compiled SQL and lineage [default: no-mcp]
  --mcp-command CMD         Command to launch the MCP server [default: dbt-mcp]
  --mcp-args ARGS           Space-separated extra arguments passed to the MCP server
  --mcp-no-lineage          Skip lineage fetching when using MCP (faster for large projects)
  --version                 Show version and exit

dbt-optimizer list-rules   List all available rules
```

## JSON Output

Use `--format json` or `-o report.json` for machine-readable output:

```json
{
  "project_name": "my_project",
  "project_path": "/path/to/project",
  "models_analyzed": 42,
  "ai_analyzed_models": 20,
  "suggestion_count": 17,
  "suggestions_by_severity": {
    "high": 2,
    "medium": 8,
    "low": 5,
    "info": 2
  },
  "suggestions": [
    {
      "rule_id": "OPT002",
      "title": "Implicit cross join / cartesian product risk",
      "description": "...",
      "severity": "high",
      "model_name": "fct_orders",
      "model_path": "models/marts/fct_orders.sql",
      "context": "...",
      "recommendation": "...",
      "source": "rule"
    }
  ]
}
```

## CI/CD Integration

### GitHub Actions

```yaml
- name: Run dbt-optimizer
  env:
    ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
  run: |
    pip install dbt-optimizer
    dbt-optimizer analyze . \
      --min-severity medium \
      --fail-on-severity high \
      --output optimizer-report.json

- name: Upload report
  uses: actions/upload-artifact@v4
  with:
    name: dbt-optimizer-report
    path: optimizer-report.json
```

### Pre-commit Hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: dbt-optimizer
        name: dbt-optimizer
        entry: dbt-optimizer analyze --no-ai --min-severity high --fail-on-severity high
        language: python
        pass_filenames: false
        files: \.sql$
```

## Architecture

```
dbt_optimizer/
├── cli.py          # Click CLI, orchestrates the full pipeline
├── project.py      # dbt project discovery: reads dbt_project.yml, finds models,
│                   # resolves materialization and schema.yml metadata
├── models.py       # Data classes: DbtModel, Suggestion, AnalysisResult, Severity
├── rules/
│   ├── base.py          # Abstract BaseRule
│   ├── sql_rules.py     # 13 static rule implementations
│   └── lineage_rules.py # 2 lineage-aware rules (LIN001, LIN002; require MCP)
├── mcp_client.py   # Synchronous wrapper around the dbt MCP server (stdio)
├── ai_analyzer.py  # Claude integration: prompt construction, response parsing
└── reporter.py     # Rich terminal output + JSON reporter
```

**Analysis pipeline:**

1. `DbtProjectParser.load()` — reads `dbt_project.yml`, discovers all `.sql` model files, resolves materialization from inline `config()`, `schema.yml`, and project-level defaults
2. MCP enrichment *(optional, `--mcp`)* — `DbtMcpClient` connects to the dbt MCP server via stdio, fetches compiled SQL for each model (`dbt_compile`) and lineage (`dbt_ls`), and populates `model.compiled_sql`, `model.upstream_models`, and `model.downstream_models`
3. Rule-based analysis — each rule runs independently against the SQL text (or compiled SQL when MCP is active); failures never block other rules
4. AI analysis — models sent to Claude in priority order (most issues first), results parsed from JSON response
5. Filtering and output — results filtered by `--min-severity`, rendered to terminal or JSON

## Contributing

```bash
git clone https://github.com/your-org/dbt-optimizer
cd dbt-optimizer
pip install -e ".[dev]"
pytest
```

To add a new rule: subclass `BaseRule` in `dbt_optimizer/rules/sql_rules.py`, implement `check(model) -> list[Suggestion]`, and add an instance to `ALL_RULES`. Add a corresponding test in `tests/test_rules.py`.
