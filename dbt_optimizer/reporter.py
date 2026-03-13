"""Output formatting for analysis results."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

from .models import AnalysisResult, Severity, Suggestion

if TYPE_CHECKING:
    pass

_SEVERITY_STYLES = {
    Severity.HIGH: ("bold red", "HIGH"),
    Severity.MEDIUM: ("bold yellow", "MEDIUM"),
    Severity.LOW: ("bold cyan", "LOW"),
    Severity.INFO: ("dim", "INFO"),
}

_SEVERITY_EMOJI = {
    Severity.HIGH: "[red]●[/red]",
    Severity.MEDIUM: "[yellow]●[/yellow]",
    Severity.LOW: "[cyan]●[/cyan]",
    Severity.INFO: "[dim]●[/dim]",
}


class ConsoleReporter:
    """Rich terminal output reporter."""

    def __init__(self, console: Console | None = None) -> None:
        self.console = console or Console()

    def print_summary(self, result: AnalysisResult) -> None:
        total = result.suggestion_count
        high = len(result.by_severity(Severity.HIGH))
        medium = len(result.by_severity(Severity.MEDIUM))
        low = len(result.by_severity(Severity.LOW))
        info = len(result.by_severity(Severity.INFO))
        ai_note = f", {result.ai_analyzed_models} analyzed by AI" if result.ai_analyzed_models else ""

        self.console.print()
        self.console.print(Panel(
            f"[bold]Project:[/bold] {result.project_name}\n"
            f"[bold]Path:[/bold] {result.project_path}\n"
            f"[bold]Models analyzed:[/bold] {result.models_analyzed}{ai_note}\n\n"
            f"[bold]Suggestions found:[/bold] {total}  "
            f"[red]{high} high[/red]  "
            f"[yellow]{medium} medium[/yellow]  "
            f"[cyan]{low} low[/cyan]  "
            f"[dim]{info} info[/dim]",
            title="[bold]dbt-optimizer Analysis Summary[/bold]",
            border_style="blue",
        ))

        if result.errors:
            for err in result.errors:
                self.console.print(f"[red]Warning:[/red] {err}")

    def print_suggestions(self, result: AnalysisResult, group_by_model: bool = False) -> None:
        if not result.suggestions:
            self.console.print("\n[green]No suggestions found. Your models look clean![/green]")
            return

        if group_by_model:
            self._print_grouped_by_model(result)
        else:
            self._print_flat(result)

    def _print_flat(self, result: AnalysisResult) -> None:
        for suggestion in result.sorted_suggestions():
            self._print_suggestion(suggestion)

    def _print_grouped_by_model(self, result: AnalysisResult) -> None:
        # Get unique model names in sorted order
        model_names = sorted(set(s.model_name for s in result.suggestions))
        for model_name in model_names:
            suggestions = result.by_model(model_name)
            model_suggestions = sorted(suggestions, key=lambda s: _severity_rank(s.severity))
            model_path = model_suggestions[0].model_path if model_suggestions else ""

            self.console.print(f"\n[bold blue]{model_name}[/bold blue] [dim]{model_path}[/dim]")
            for s in model_suggestions:
                self._print_suggestion(s, show_model=False)

    def _print_suggestion(self, s: Suggestion, show_model: bool = True) -> None:
        style, label = _SEVERITY_STYLES[s.severity]
        dot = _SEVERITY_EMOJI[s.severity]
        source_tag = r"[dim]\[AI][/dim]" if s.source == "ai" else f"[dim]\\[{s.rule_id}][/dim]"

        header_parts = [f"{dot} [{style}]{label}[/{style}]"]
        if show_model:
            header_parts.append(f"[bold]{s.model_name}[/bold]")
        header_parts.append(f"[bold]{s.title}[/bold] {source_tag}")

        self.console.print("  " + "  ".join(header_parts))
        self.console.print(f"     [dim]{s.description}[/dim]")
        if s.context:
            self.console.print(f"     [italic dim]Context:[/italic dim] [italic]{s.context}[/italic]")
        if s.recommendation:
            self.console.print(f"     [green]Fix:[/green] {s.recommendation}")
        self.console.print()

    def print_model_table(self, result: AnalysisResult) -> None:
        """Print a compact per-model summary table."""
        table = Table(
            title="Suggestions by Model",
            box=box.SIMPLE_HEAVY,
            show_header=True,
            header_style="bold",
        )
        table.add_column("Model", style="bold")
        table.add_column("HIGH", justify="center", style="red")
        table.add_column("MED", justify="center", style="yellow")
        table.add_column("LOW", justify="center", style="cyan")
        table.add_column("INFO", justify="center", style="dim")
        table.add_column("Total", justify="center")
        table.add_column("Path", style="dim")

        model_names = sorted(set(s.model_name for s in result.suggestions))
        for name in model_names:
            sug = result.by_model(name)
            h = sum(1 for s in sug if s.severity == Severity.HIGH)
            m = sum(1 for s in sug if s.severity == Severity.MEDIUM)
            lo = sum(1 for s in sug if s.severity == Severity.LOW)
            i = sum(1 for s in sug if s.severity == Severity.INFO)
            path = sug[0].model_path if sug else ""
            table.add_row(name, str(h) if h else "-", str(m) if m else "-",
                          str(lo) if lo else "-", str(i) if i else "-",
                          str(len(sug)), path)

        self.console.print(table)


def _severity_rank(s: Severity) -> int:
    return {Severity.HIGH: 0, Severity.MEDIUM: 1, Severity.LOW: 2, Severity.INFO: 3}[s]


class JsonReporter:
    """Outputs analysis results as JSON."""

    def write(self, result: AnalysisResult, output_path: str | None = None) -> str:
        data = json.dumps(result.as_dict(), indent=2)
        if output_path:
            with open(output_path, "w") as f:
                f.write(data)
        return data
