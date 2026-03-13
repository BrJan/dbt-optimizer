"""CLI entry point for dbt-optimizer."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from .models import AnalysisResult
from .project import DbtProjectParser, DbtProjectError
from .rules import ALL_RULES
from .reporter import ConsoleReporter, JsonReporter

_MCP_HELP = (
    "Connect to the dbt MCP server for compiled SQL and lineage data. "
    "Requires the dbt MCP server to be installed (e.g. pip install dbt-mcp)."
)

console = Console()


def _run_rules(models, enabled_rules=None) -> list:
    rules = enabled_rules or ALL_RULES
    suggestions = []
    for model in models:
        for rule in rules:
            try:
                suggestions.extend(rule.check(model))
            except Exception:
                pass  # Never let a buggy rule crash the whole analysis
    return suggestions


@click.group()
@click.version_option()
def cli():
    """dbt-optimizer: AI-powered SQL optimization advisor for dbt projects."""


@cli.command()
@click.argument("project_path", default=".", type=click.Path(exists=True))
@click.option("--ai/--no-ai", default=True, show_default=True,
              help="Enable AI-powered analysis via Claude (requires ANTHROPIC_API_KEY).")
@click.option("--ai-max-models", default=20, show_default=True, metavar="N",
              help="Maximum number of models to send for AI analysis (0 = unlimited).")
@click.option("--min-severity", default="info",
              type=click.Choice(["high", "medium", "low", "info"], case_sensitive=False),
              show_default=True, help="Minimum severity level to display.")
@click.option("--output", "-o", default=None, metavar="FILE",
              help="Write results to a JSON file.")
@click.option("--format", "output_format", default="terminal",
              type=click.Choice(["terminal", "json"], case_sensitive=False),
              show_default=True, help="Output format.")
@click.option("--group-by-model", is_flag=True, default=False,
              help="Group terminal output by model instead of severity.")
@click.option("--select", default=None, metavar="PATTERN",
              help="Only analyze models whose name contains this string.")
@click.option("--skip-rules", default=None, metavar="RULE_IDS",
              help="Comma-separated rule IDs to skip (e.g. OPT001,OPT010).")
@click.option("--fail-on-severity", default=None, metavar="LEVEL",
              type=click.Choice(["high", "medium", "low"], case_sensitive=False),
              help="Exit with code 1 if suggestions at this level or higher are found.")
@click.option("--mcp/--no-mcp", default=False, show_default=True,
              help=_MCP_HELP)
@click.option("--mcp-command", default="dbt-mcp", show_default=True, metavar="CMD",
              help="Command to launch the dbt MCP server.")
@click.option("--mcp-args", default=None, metavar="ARGS",
              help="Space-separated extra arguments passed to the MCP server command.")
@click.option("--mcp-no-lineage", is_flag=True, default=False,
              help="Skip lineage fetching when using MCP (faster for large projects).")
def analyze(
    project_path,
    ai,
    ai_max_models,
    min_severity,
    output,
    output_format,
    group_by_model,
    select,
    skip_rules,
    fail_on_severity,
    mcp,
    mcp_command,
    mcp_args,
    mcp_no_lineage,
):
    """Analyze a dbt project and surface SQL optimization suggestions.

    PROJECT_PATH is the root directory of the dbt project (default: current directory).

    Examples:

    \b
        # Analyze current directory
        dbt-optimizer analyze

    \b
        # Analyze a specific project without AI
        dbt-optimizer analyze ~/projects/my_dbt_project --no-ai

    \b
        # Only show high/medium issues, write JSON output
        dbt-optimizer analyze . --min-severity medium -o report.json

    \b
        # Only analyze models matching a pattern
        dbt-optimizer analyze . --select orders

    \b
        # Fail CI if any high-severity issues found
        dbt-optimizer analyze . --fail-on-severity high
    """
    # 1. Parse project
    reporter = ConsoleReporter(console)

    with console.status("[bold blue]Loading dbt project...[/bold blue]"):
        try:
            parser = DbtProjectParser(project_path).load()
            models = parser.discover_models()
        except DbtProjectError as e:
            console.print(f"[red]Error:[/red] {e}")
            sys.exit(1)

    if not models:
        console.print(f"[yellow]No SQL models found in {project_path}[/yellow]")
        sys.exit(0)

    # 2. Filter by --select
    if select:
        models = [m for m in models if select.lower() in m.name.lower()]
        if not models:
            console.print(f"[yellow]No models matched the pattern '{select}'[/yellow]")
            sys.exit(0)

    # 3. Filter rules by --skip-rules
    skip_set: set[str] = set()
    if skip_rules:
        skip_set = {r.strip().upper() for r in skip_rules.split(",")}
    active_rules = [r for r in ALL_RULES if r.rule_id not in skip_set]

    if output_format != "json":
        console.print(
            f"[bold]dbt-optimizer[/bold]  project=[cyan]{parser.project_name}[/cyan]  "
            f"models=[cyan]{len(models)}[/cyan]  rules=[cyan]{len(active_rules)}[/cyan]"
        )

    # 4. MCP enrichment (optional)
    mcp_active = False
    if mcp:
        from .mcp_client import DbtMcpClient, McpNotAvailableError

        extra_args = mcp_args.split() if mcp_args else []
        try:
            fetch_lineage = not mcp_no_lineage
            label = "compiled SQL + lineage" if fetch_lineage else "compiled SQL"
            total_steps = len(models) * (3 if fetch_lineage else 1)

            with console.status("[bold blue]Connecting to dbt MCP server...[/bold blue]"):
                mcp_client_obj = DbtMcpClient(command=mcp_command, args=extra_args)

            with mcp_client_obj as mcp_client:
                mcp_active = True

                with Progress(
                    SpinnerColumn(),
                    TextColumn(
                        f"[bold blue]MCP enrichment ({label})[/bold blue] {{task.description}}"
                    ),
                    BarColumn(),
                    TaskProgressColumn(),
                    console=console,
                    transient=True,
                ) as progress:
                    task = progress.add_task("", total=total_steps)
                    step = [0]

                    def mcp_cb(msg: str) -> None:
                        step[0] += 1
                        progress.update(task, completed=step[0], description=f"[dim]{msg}[/dim]")

                    parser.enrich_models_from_mcp(
                        models,
                        mcp_client,
                        fetch_lineage=fetch_lineage,
                        progress_callback=mcp_cb,
                    )
                    progress.update(task, completed=total_steps)

            compiled_count = sum(1 for m in models if m.compiled_sql)
            if output_format != "json":
                console.print(
                    f"[dim]MCP: compiled SQL for {compiled_count}/{len(models)} models"
                    + (", lineage fetched" if fetch_lineage else "")
                    + "[/dim]"
                )

        except McpNotAvailableError as e:
            console.print(f"[yellow]MCP skipped:[/yellow] {e}")
        except Exception as e:
            console.print(f"[yellow]MCP enrichment failed:[/yellow] {e}")
            console.print("[dim]Falling back to file-based analysis.[/dim]")

    # 5. Rule-based analysis
    with console.status("[bold blue]Running rule-based analysis...[/bold blue]"):
        suggestions = _run_rules(models, active_rules)

    result = AnalysisResult(
        project_name=parser.project_name,
        project_path=str(Path(project_path).resolve()),
        models_analyzed=len(models),
        suggestions=suggestions,
    )

    # 6. AI analysis
    ai_analyzed = 0
    if ai:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            console.print(
                "[yellow]ANTHROPIC_API_KEY not set — skipping AI analysis. "
                "Set the env var or use --no-ai to suppress this message.[/yellow]"
            )
        else:
            from .ai_analyzer import AIAnalyzer
            analyzer = AIAnalyzer(api_key=api_key)

            # Prioritize models with existing suggestions, then by complexity
            candidates = sorted(models, key=lambda m: (-len(result.by_model(m.name)), -m.line_count))
            if ai_max_models > 0:
                candidates = candidates[:ai_max_models]

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]AI analysis[/bold blue] {task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
                transient=True,
            ) as progress:
                task = progress.add_task("", total=len(candidates))

                def cb(i, total, name):
                    progress.update(task, completed=i, description=f"[dim]{name}[/dim]")

                ai_suggestions, ai_errors = analyzer.analyze_models(candidates, progress_callback=cb)
                progress.update(task, completed=len(candidates))

            result.suggestions.extend(ai_suggestions)
            result.errors.extend(ai_errors)
            result.ai_analyzed_models = len(candidates) - len(ai_errors)
            ai_analyzed = result.ai_analyzed_models

    # 6. Filter by min_severity
    severity_order = {"high": 0, "medium": 1, "low": 2, "info": 3}
    min_rank = severity_order[min_severity.lower()]
    result.suggestions = [
        s for s in result.suggestions if severity_order[s.severity.value] <= min_rank
    ]

    # 7. Output
    if output_format == "json":
        jr = JsonReporter()
        data = jr.write(result, output)
        if not output:
            print(data)
    else:
        reporter.print_summary(result)
        if result.suggestions:
            if len(models) > 5:
                reporter.print_model_table(result)
                console.print()
            reporter.print_suggestions(result, group_by_model=group_by_model)
        else:
            console.print("\n[green]No suggestions found. Your models look great![/green]\n")

        if output:
            jr = JsonReporter()
            jr.write(result, output)
            console.print(f"[dim]JSON report written to {output}[/dim]")

    # 8. Exit code
    if fail_on_severity:
        fail_rank = severity_order[fail_on_severity.lower()]
        has_violations = any(
            severity_order[s.severity.value] <= fail_rank for s in result.suggestions
        )
        if has_violations:
            console.print(
                f"\n[red]Exiting with code 1[/red]: suggestions at '{fail_on_severity}' or higher found."
            )
            sys.exit(1)


@cli.command("list-rules")
def list_rules():
    """List all available rule-based checks."""
    from rich import box
    from rich.table import Table

    from .rules import ALL_SQL_RULES, LINEAGE_RULES

    table = Table(title="Available Rules", box=box.SIMPLE_HEAVY)
    table.add_column("Rule ID", style="bold cyan")
    table.add_column("Title")
    table.add_column("Requires", style="dim")

    for rule in ALL_SQL_RULES:
        table.add_row(rule.rule_id, rule.title, "—")
    for rule in LINEAGE_RULES:
        table.add_row(rule.rule_id, rule.title, "MCP (lineage)")
    table.add_row("AI", "AI-powered deep analysis (Claude)", "ANTHROPIC_API_KEY")

    console.print(table)
    console.print(
        f"\n[dim]{len(ALL_SQL_RULES)} static rules  |  "
        f"{len(LINEAGE_RULES)} lineage rules (require --mcp)  |  "
        "1 AI analyzer\n"
        "Use --skip-rules RULE_ID,... to disable specific rules.[/dim]"
    )
