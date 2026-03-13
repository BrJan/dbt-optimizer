"""dbt MCP client — synchronous wrapper around the async MCP protocol.

Connects to the dbt MCP server (e.g. dbt-mcp) via stdio and exposes
synchronous helpers for the tools most useful during optimization:

  - get_compiled_sql(model_name)  → fully compiled SQL (Jinja resolved)
  - get_model_lineage(model_name) → upstream / downstream model names
  - list_tools()                  → available MCP tool names
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
from typing import Any

logger = logging.getLogger(__name__)

try:
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    _MCP_AVAILABLE = True
except ImportError:  # pragma: no cover
    _MCP_AVAILABLE = False


class McpNotAvailableError(Exception):
    """Raised when the mcp package is not installed."""


class DbtMcpClient:
    """Synchronous wrapper around the dbt MCP server.

    Usage::

        with DbtMcpClient(command="dbt-mcp") as client:
            sql = client.get_compiled_sql("stg_orders")
            lineage = client.get_model_lineage("fct_orders")
    """

    def __init__(
        self,
        command: str = "dbt-mcp",
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        if not _MCP_AVAILABLE:
            raise McpNotAvailableError(
                "The 'mcp' package is required for MCP integration.\n"
                "Install it with:  pip install 'dbt-optimizer[mcp]'"
            )
        self._command = command
        self._args = args or []
        self._env = env  # extra env vars to pass; None means inherit
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._session: Any = None  # mcp.ClientSession
        self._stop_event: asyncio.Event | None = None  # set on async side
        self._ready = threading.Event()
        self._connect_error: Exception | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "DbtMcpClient":
        self._loop = asyncio.new_event_loop()

        def _run_loop() -> None:
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._async_run())

        self._thread = threading.Thread(target=_run_loop, daemon=True, name="dbt-mcp")
        self._thread.start()

        if not self._ready.wait(timeout=30):
            raise TimeoutError(
                "Timed out waiting for dbt MCP server to start. "
                f"Command: {self._command} {' '.join(self._args)}"
            )
        if self._connect_error:
            raise self._connect_error
        return self

    def __exit__(self, *_: Any) -> None:
        if self._loop and self._stop_event:
            self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._thread:
            self._thread.join(timeout=10)

    # ------------------------------------------------------------------
    # Async session management
    # ------------------------------------------------------------------

    async def _async_run(self) -> None:
        env = {**os.environ, **(self._env or {})}
        try:
            params = StdioServerParameters(
                command=self._command,
                args=self._args,
                env=env,
            )
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    self._session = session
                    self._stop_event = asyncio.Event()
                    self._ready.set()
                    await self._stop_event.wait()  # keep session alive until __exit__
        except Exception as exc:
            self._connect_error = exc
            self._ready.set()

    # ------------------------------------------------------------------
    # Low-level tool invocation
    # ------------------------------------------------------------------

    def call_tool(self, name: str, arguments: dict[str, Any], timeout: float = 120) -> Any:
        """Synchronously invoke an MCP tool and return the raw result."""
        if self._session is None:
            raise RuntimeError("DbtMcpClient is not connected; use it as a context manager.")
        future = asyncio.run_coroutine_threadsafe(
            self._session.call_tool(name, arguments),
            self._loop,  # type: ignore[arg-type]
        )
        return future.result(timeout=timeout)

    def list_tools(self) -> list[str]:
        """Return the names of all tools exposed by the dbt MCP server."""
        if self._session is None:
            raise RuntimeError("Not connected.")
        future = asyncio.run_coroutine_threadsafe(
            self._session.list_tools(),
            self._loop,  # type: ignore[arg-type]
        )
        result = future.result(timeout=15)
        return [t.name for t in result.tools]

    # ------------------------------------------------------------------
    # High-level helpers
    # ------------------------------------------------------------------

    def get_compiled_sql(self, model_name: str) -> str | None:
        """Return the compiled (Jinja-resolved) SQL for *model_name*.

        Returns ``None`` if the tool is unavailable or compilation fails.
        """
        try:
            result = self.call_tool("dbt_compile", {"select": model_name})
            return _extract_compiled_sql(result, model_name)
        except Exception as exc:
            logger.debug("get_compiled_sql(%s) failed: %s", model_name, exc)
            return None

    def get_compiled_sql_bulk(self, model_names: list[str]) -> dict[str, str]:
        """Compile multiple models in one call.  Returns {name: sql}."""
        if not model_names:
            return {}
        select = " ".join(model_names)
        try:
            result = self.call_tool("dbt_compile", {"select": select})
            return _extract_all_compiled_sql(result)
        except Exception as exc:
            logger.debug("get_compiled_sql_bulk failed: %s", exc)
            return {}

    def get_model_lineage(self, model_name: str) -> dict[str, list[str]]:
        """Return ``{"upstream": [...], "downstream": [...]}`` for *model_name*.

        Uses two ``dbt_ls`` calls:
          - ``+model_name`` → all ancestors
          - ``model_name+`` → all descendants
        """
        upstream: list[str] = []
        downstream: list[str] = []
        try:
            up_result = self.call_tool("dbt_ls", {"select": f"+{model_name}", "output": "name"})
            all_up = _parse_ls_names(up_result)
            upstream = [n for n in all_up if n != model_name]
        except Exception as exc:
            logger.debug("get_model_lineage upstream(%s) failed: %s", model_name, exc)

        try:
            dn_result = self.call_tool("dbt_ls", {"select": f"{model_name}+", "output": "name"})
            all_dn = _parse_ls_names(dn_result)
            downstream = [n for n in all_dn if n != model_name]
        except Exception as exc:
            logger.debug("get_model_lineage downstream(%s) failed: %s", model_name, exc)

        return {"upstream": upstream, "downstream": downstream}

    def get_all_model_lineage(self, model_names: list[str]) -> dict[str, dict[str, list[str]]]:
        """Return lineage for every model in *model_names*.  Returns {name: {upstream, downstream}}."""
        results: dict[str, dict[str, list[str]]] = {}
        for name in model_names:
            results[name] = self.get_model_lineage(name)
        return results


# ---------------------------------------------------------------------------
# Output parsing helpers
# ---------------------------------------------------------------------------

def _extract_text(result: Any) -> str:
    """Pull plain text out of an MCP tool result."""
    parts: list[str] = []
    for item in getattr(result, "content", []):
        text = getattr(item, "text", None)
        if text:
            parts.append(text)
    return "\n".join(parts)


def _extract_compiled_sql(result: Any, model_name: str) -> str | None:
    """Parse a single compiled SQL block from a ``dbt_compile`` result."""
    text = _extract_text(result)
    if not text.strip():
        return None

    # Pattern: "Compiled node 'model.project.name' is:\n<SQL>"
    pattern = rf"Compiled node '[^']*\.{re.escape(model_name)}' is:\n([\s\S]+?)(?=\nCompiled node '|$)"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    # Fallback: if there's only one model in the output, return everything
    # after the last "Compiled node ... is:" line
    last_match = None
    for m in re.finditer(r"Compiled node '[^']+' is:\n", text, re.IGNORECASE):
        last_match = m
    if last_match:
        sql_start = last_match.end()
        return text[sql_start:].strip()

    # Last resort: return the full text if it looks like SQL
    if re.search(r"\b(SELECT|WITH|INSERT|CREATE)\b", text, re.IGNORECASE):
        return text.strip()

    return None


def _extract_all_compiled_sql(result: Any) -> dict[str, str]:
    """Parse ALL compiled SQL blocks from a bulk ``dbt_compile`` result.

    Returns ``{model_name: compiled_sql}``.
    """
    text = _extract_text(result)
    compiled: dict[str, str] = {}

    # Find all "Compiled node 'model.project.name' is:\n<SQL>" blocks
    for match in re.finditer(
        r"Compiled node '(?:[^']*\.)?([^'.]+)' is:\n([\s\S]+?)(?=\nCompiled node '|$)",
        text,
        re.IGNORECASE,
    ):
        name = match.group(1)
        sql = match.group(2).strip()
        if sql:
            compiled[name] = sql

    return compiled


def _parse_ls_names(result: Any) -> list[str]:
    """Parse model names from a ``dbt_ls`` text result.

    dbt ls output lines look like:
      my_project.model_name
      my_project.staging.stg_orders
    """
    text = _extract_text(result)
    names: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Skip dbt status lines
        if any(line.startswith(p) for p in ("Running", "Found", "Done", "Completed", "WARNING")):
            continue
        # Strip project/resource-type prefix: "project.model.name" → "name"
        parts = line.split(".")
        names.append(parts[-1])
    return names
