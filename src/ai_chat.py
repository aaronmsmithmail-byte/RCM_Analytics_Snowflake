"""
AI Chat Module for Healthcare RCM Analytics
============================================

Provides the backend for the AI Assistant tab.  The system prompt is built
dynamically from the four meta_* tables in SQLite so it stays automatically
in sync with the data model without any manual maintenance.

Architecture:
    1. build_system_prompt()  — queries meta_kpi_catalog, meta_semantic_layer,
                                meta_kg_nodes, meta_kg_edges and formats them
                                into a rich context string.  Optional live KPI
                                values from the active dashboard filters are
                                appended so the AI can answer "what is our
                                current denial rate?" accurately.
    2. stream_chat()          — calls the OpenRouter API (OpenAI-compatible)
                                with streaming enabled; yields text chunks so
                                the Streamlit UI can update incrementally.

Configuration (via .env file):
    OPENROUTER_API_KEY  — required
    OPENROUTER_MODEL    — optional, defaults to openai/gpt-4o-mini
"""

import os
from typing import Iterator

# Load .env file if present — no-op when the file is missing.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed; env vars must be set another way


# ---------------------------------------------------------------------------
# Available models offered in the UI selectbox
# ---------------------------------------------------------------------------
AVAILABLE_MODELS = [
    ("GPT-4o mini  (fast · cheap)",          "openai/gpt-4o-mini"),
    ("GPT-4o  (most capable OpenAI)",         "openai/gpt-4o"),
    ("Claude 3.5 Haiku  (fast · Anthropic)",  "anthropic/claude-3-5-haiku"),
    ("Claude 3.5 Sonnet  (smart · Anthropic)","anthropic/claude-3-5-sonnet"),
    ("Gemini Flash 1.5  (fast · Google)",     "google/gemini-flash-1.5"),
]

DEFAULT_MODEL = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini")


# ---------------------------------------------------------------------------
# Internal: build context string from meta_* tables
# ---------------------------------------------------------------------------

def _get_meta_context(db_path=None) -> str:
    """Query all four meta_* tables and format them as a markdown context block."""
    from src.database import get_connection
    conn = get_connection(db_path)

    kpis = conn.execute(
        "SELECT metric_name, category, definition, formula, benchmark "
        "FROM meta_kpi_catalog ORDER BY category, metric_name"
    ).fetchall()

    semantic = conn.execute(
        "SELECT business_concept, kpi_name, silver_columns, formula, business_rule "
        "FROM meta_semantic_layer ORDER BY business_concept"
    ).fetchall()

    nodes = conn.execute(
        "SELECT entity_id, entity_group, silver_table, description "
        "FROM meta_kg_nodes ORDER BY entity_group, entity_id"
    ).fetchall()

    edges = conn.execute(
        "SELECT parent_entity, child_entity, join_column, cardinality, business_meaning "
        "FROM meta_kg_edges"
    ).fetchall()

    conn.close()

    lines: list[str] = []

    # ── Database tables ────────────────────────────────────────────────
    lines.append("## Silver-Layer Tables (primary query targets)")
    for entity_id, group, silver_table, desc in nodes:
        lines.append(f"- **{silver_table}** ({group}): {desc}")

    # ── Relationships ──────────────────────────────────────────────────
    lines.append("\n## Table Relationships (foreign keys)")
    for parent, child, join_col, cardinality, meaning in edges:
        lines.append(
            f"- silver_{parent} → silver_{child} "
            f"ON {join_col} ({cardinality}): {meaning}"
        )

    # ── KPI definitions ────────────────────────────────────────────────
    lines.append("\n## KPI Definitions")
    current_cat = None
    for metric, cat, defn, formula, benchmark in kpis:
        if cat != current_cat:
            lines.append(f"\n### {cat}")
            current_cat = cat
        bench_str = f" | Benchmark: {benchmark}" if benchmark else ""
        lines.append(f"- **{metric}**: {defn}")
        lines.append(f"  Formula: `{formula}`{bench_str}")

    # ── Semantic mappings ──────────────────────────────────────────────
    lines.append("\n## Semantic Mappings  (business concept → KPI → source columns)")
    current_concept = None
    for concept, kpi, cols, formula, rule in semantic:
        if concept != current_concept:
            lines.append(f"\n### {concept}")
            current_concept = concept
        lines.append(f"- **{kpi}**")
        lines.append(f"  Columns: `{cols}`")
        lines.append(f"  Formula: `{formula}`")
        lines.append(f"  Business rule: {rule}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public: system prompt builder
# ---------------------------------------------------------------------------

def build_system_prompt(live_kpis: dict | None = None, db_path=None) -> str:
    """
    Build the AI system prompt.

    Combines a static role description with dynamically-generated content
    from the meta_* tables so the AI always has an accurate picture of the
    data model.

    Args:
        live_kpis: Optional dict of current KPI values from the active
                   dashboard filters, e.g.::

                       {"Days in A/R": 32.5, "Denial Rate": 8.2, ...}

        db_path:   Optional SQLite path override (defaults to DB_PATH).

    Returns:
        System prompt string ready for the messages list.
    """
    meta = _get_meta_context(db_path)

    kpi_snapshot = ""
    if live_kpis:
        kpi_lines = "\n".join(f"  - {k}: {v}" for k, v in live_kpis.items())
        kpi_snapshot = (
            "\n## Live KPI Snapshot  (from active sidebar filters)\n"
            + kpi_lines
        )

    return f"""You are an AI analyst embedded in a Healthcare Revenue Cycle Management (RCM) \
Analytics dashboard built on a SQLite Medallion-Architecture database \
(Bronze → Silver → Gold layers).

Your role:
- Answer natural-language questions about RCM performance.
- Explain KPIs, formulas, and industry benchmarks in plain language.
- Identify potential issues and suggest actions based on the numbers.
- Describe what SQL would be needed to answer deeper questions.

{meta}
{kpi_snapshot}

## Response guidelines
- Be concise.  Healthcare finance professionals are busy.
- Always state the relevant benchmark when discussing a KPI value.
- Format numbers as "$1.2M", "8.3%", "34 days", etc.
- If asked about a value not in the snapshot, say you don't have that data \
and explain which table/column to query.
- Never fabricate specific dollar or percentage figures.
- Industry benchmarks (unless overridden above):
    DAR < 35 days | NCR > 95% | GCR > 70% | Clean Claim Rate > 90%
    Denial Rate < 10% | First-Pass Rate > 85% | Cost to Collect < 3%
"""


# ---------------------------------------------------------------------------
# Public: streaming chat
# ---------------------------------------------------------------------------

def stream_chat(
    messages: list[dict],
    model: str | None = None,
) -> Iterator[str]:
    """
    Stream a chat response from OpenRouter.

    OpenRouter exposes an OpenAI-compatible endpoint so we use the ``openai``
    Python SDK with a custom ``base_url``.

    Args:
        messages: Full conversation history as a list of
                  ``{"role": "system"|"user"|"assistant", "content": "..."}``
                  dicts.  The system message should be the first entry.
        model:    OpenRouter model ID string.  Defaults to the
                  ``OPENROUTER_MODEL`` env var or ``openai/gpt-4o-mini``.

    Yields:
        Text chunks from the streaming response.

    Raises:
        ValueError:  If ``OPENROUTER_API_KEY`` is not configured.
        ImportError: If the ``openai`` package is not installed.
    """
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise ImportError(
            "The 'openai' package is required for the AI tab.  "
            "Run: pip install openai"
        ) from exc

    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key or api_key.strip() in ("", "your_api_key_here"):
        raise ValueError(
            "OPENROUTER_API_KEY is not configured.  "
            "Add it to the .env file in the project root and restart the app."
        )

    selected_model = model or DEFAULT_MODEL

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
        default_headers={
            "HTTP-Referer": "https://rcm-analytics.local",
            "X-Title": "Healthcare RCM Analytics",
        },
    )

    stream = client.chat.completions.create(
        model=selected_model,
        messages=messages,
        stream=True,
        max_tokens=1500,
        temperature=0.3,
    )

    for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content
