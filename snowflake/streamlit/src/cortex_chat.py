"""
Cortex Analyst Integration for Healthcare RCM Analytics
========================================================

Replaces the OpenRouter-based AI chat with Snowflake Cortex Analyst.
Cortex Analyst uses a staged semantic model YAML to understand the data
model and generate SQL queries against the Silver layer.

Usage in Streamlit in Snowflake:
    from src.cortex_chat import send_analyst_message, get_session

    session = get_session()
    response = send_analyst_message(session, user_question, message_history)
"""

import json

import streamlit as st
from snowflake.snowpark.context import get_active_session

# Stage path where the semantic model YAML is uploaded
SEMANTIC_MODEL_STAGE = "@RCM_ANALYTICS.STAGING.RCM_STAGE/cortex/rcm_semantic_model.yaml"

SUGGESTED_QUESTIONS = [
    "Which payer has the highest denial rate? Show me the breakdown.",
    "What are the top 5 denial reason codes by total denied amount?",
    "Show me monthly denial rate trend for the past 12 months.",
    "Which department generates the most revenue per encounter?",
    "What is the average days in A/R by payer?",
    "Show total charges and payments by month for the past year.",
]


def _safe_rerun():
    """Call st.rerun() with fallback to st.experimental_rerun() for SiS compatibility."""
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()


def get_session():
    """Get the active Snowpark session (provided by SiS runtime)."""
    return get_active_session()


def send_analyst_message(session, user_question, message_history=None):
    """
    Send a question to Cortex Analyst and return the response.

    Args:
        session: Active Snowpark session.
        user_question: The user's natural language question.
        message_history: List of prior message dicts for conversation context.
            Each dict has 'role' ('user' or 'analyst') and 'content'.

    Returns:
        dict with keys:
            - 'type': 'sql' | 'text' | 'error'
            - 'content': The response text or SQL query
            - 'sql': The generated SQL (if type == 'sql')
            - 'results': DataFrame of query results (if type == 'sql')
            - 'explanation': Natural language explanation of the answer
    """
    # Build the message payload for Cortex Analyst
    messages = []
    if message_history:
        for msg in message_history:
            role = msg["role"] if msg["role"] == "user" else "analyst"
            messages.append(
                {
                    "role": role,
                    "content": [{"type": "text", "text": msg["content"]}],
                }
            )

    # Add the current user question
    messages.append(
        {
            "role": "user",
            "content": [{"type": "text", "text": user_question}],
        }
    )

    # Call Cortex Analyst via the _snowflake REST API (primary path in SiS)
    try:
        return _call_analyst_via_rest(session, messages)
    except Exception as rest_err:
        # Fallback: try the SQL function approach
        try:
            request_body = {
                "messages": messages,
                "semantic_model_file": SEMANTIC_MODEL_STAGE,
            }
            json_str = json.dumps(request_body).replace("\\", "\\\\").replace("'", "''")
            resp = session.sql(
                f"""
                SELECT SNOWFLAKE.CORTEX.ANALYST(
                    PARSE_JSON('{json_str}')
                ) AS RESPONSE
                """
            ).collect()

            if not resp:
                return {"type": "error", "content": "No response from Cortex Analyst"}

            response_json = json.loads(resp[0]["RESPONSE"])
            return _parse_analyst_response(session, response_json)
        except Exception as sql_err:
            return {
                "type": "error",
                "content": f"Cortex Analyst error: {rest_err} (fallback: {sql_err})",
            }


def _call_analyst_via_rest(session, messages):
    """
    Call Cortex Analyst via the internal REST API available in SiS.

    This uses the _snowflake module which provides access to Snowflake
    internal APIs from within Streamlit in Snowflake.
    """
    import _snowflake

    request_body = {
        "messages": messages,
        "semantic_model_file": SEMANTIC_MODEL_STAGE,
    }

    resp = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )

    if resp["status"] < 400:
        response_json = json.loads(resp["content"])
        return _parse_analyst_response(session, response_json)
    else:
        # Surface the actual error body so users can diagnose the issue
        error_detail = ""
        try:
            error_body = json.loads(resp.get("content", "{}"))
            error_detail = error_body.get("message", json.dumps(error_body))
        except Exception:
            error_detail = str(resp.get("content", ""))[:500]
        return {
            "type": "error",
            "content": (f"Cortex Analyst returned status {resp['status']}. {error_detail}"),
        }


def _parse_analyst_response(session, response_json):
    """
    Parse the Cortex Analyst response into a structured dict.

    Cortex Analyst responses contain a 'message' with 'content' blocks
    that can be 'text' or 'sql' type.
    """
    result = {
        "type": "text",
        "content": "",
        "sql": None,
        "results": None,
        "explanation": "",
    }

    message = response_json.get("message", response_json)
    content_blocks = message.get("content", [])

    text_parts = []
    for block in content_blocks:
        if block.get("type") == "sql":
            result["type"] = "sql"
            result["sql"] = block.get("statement", block.get("sql", ""))
            # Execute the generated SQL
            try:
                df = session.sql(result["sql"]).to_pandas()
                result["results"] = df
            except Exception as e:
                result["type"] = "error"
                result["content"] = f"SQL execution error: {str(e)}"
                return result
        elif block.get("type") == "text":
            text = block.get("text", "")
            if text:
                text_parts.append(text)

    # Build combined content for display
    if text_parts:
        result["content"] = "\n\n".join(text_parts)
    elif result["type"] == "sql" and result["results"] is not None:
        # Cortex Analyst returned SQL only with no explanation —
        # generate a brief summary from the results
        df = result["results"]
        n_rows = len(df)
        n_cols = len(df.columns)
        col_list = ", ".join(df.columns[:5])
        if n_cols > 5:
            col_list += f", ... ({n_cols - 5} more)"
        summary = f"Query returned **{n_rows} row(s)** with columns: {col_list}."
        # Add quick numeric highlights if available
        numeric_cols = df.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 0 and n_rows > 0:
            highlights = []
            for col in numeric_cols[:3]:
                val = df[col].iloc[0]
                if abs(val) >= 1000:
                    highlights.append(f"{col}: {val:,.2f}")
                else:
                    highlights.append(f"{col}: {val}")
            if highlights:
                summary += " Top row: " + ", ".join(highlights) + "."
        result["content"] = summary
    else:
        result["content"] = "Here are the results:"

    return result


def render_chat_ui():
    """
    Render the Cortex Analyst chat interface in a Streamlit tab.

    Uses st.text_input + st.button instead of st.chat_input/st.chat_message
    for compatibility with Streamlit in Snowflake (SiS), which runs an older
    Streamlit version that lacks the chat_* widgets.

    Manages conversation history in st.session_state and displays
    messages with SQL results inline.
    """
    session = get_session()

    # Initialize chat history
    if "analyst_messages" not in st.session_state:
        st.session_state.analyst_messages = []

    st.subheader("AI Assistant")
    st.caption(
        "Ask natural-language questions about your RCM data. "
        "The AI can query the database directly to answer questions "
        "about specific payers, departments, denial codes, and more."
    )

    # ── Model selector + clear button ─────────────────────────────────
    _col_model, _col_clear = st.columns([3, 1])
    with _col_model:
        st.selectbox(
            "Model",
            ["Cortex Analyst (Snowflake)"],
            index=0,
            key="ai_model_display",
            disabled=True,
        )
    with _col_clear:
        st.markdown("<div style='margin-top:28px'></div>", unsafe_allow_html=True)
        if st.button("Clear chat", key="ai_clear_chat", use_container_width=True):
            st.session_state.analyst_messages = []
            _safe_rerun()

    # ── Suggested questions (shown when chat is empty) ────────────────
    if not st.session_state.analyst_messages:
        st.markdown("**Suggested questions:**")
        _sug_cols_row1 = st.columns(3)
        for i in range(3):
            with _sug_cols_row1[i]:
                if st.button(
                    SUGGESTED_QUESTIONS[i],
                    key=f"ai_sug_{i}",
                    use_container_width=True,
                ):
                    st.session_state["ai_pending_input"] = SUGGESTED_QUESTIONS[i]
                    _safe_rerun()
        _sug_cols_row2 = st.columns(3)
        for i in range(3, 6):
            with _sug_cols_row2[i - 3]:
                if st.button(
                    SUGGESTED_QUESTIONS[i],
                    key=f"ai_sug_{i}",
                    use_container_width=True,
                ):
                    st.session_state["ai_pending_input"] = SUGGESTED_QUESTIONS[i]
                    _safe_rerun()

    # Display chat history
    for msg in st.session_state.analyst_messages:
        if msg["role"] == "user":
            st.markdown(f"**You:** {msg['content']}")
        else:
            st.markdown(f"**Analyst:** {msg['content']}")
            if msg.get("sql"):
                with st.expander("View SQL", expanded=False):
                    st.code(msg["sql"], language="sql")
            if msg.get("results") is not None:
                st.dataframe(msg["results"], use_container_width=True)
        st.divider()

    # Chat input -- compatible with older SiS Streamlit versions
    input_col, btn_col = st.columns([5, 1])
    with input_col:
        user_input = st.text_input(
            "Ask a question about your RCM data...",
            key="analyst_input",
            label_visibility="collapsed",
            placeholder="Ask a question about your RCM data...",
        )
    with btn_col:
        send_clicked = st.button("Send", type="primary", use_container_width=True)

    # Determine effective input: from text box or from a suggested question click
    effective_input = st.session_state.pop("ai_pending_input", None) or (user_input if send_clicked else None)

    if effective_input:
        st.session_state.analyst_messages.append(
            {
                "role": "user",
                "content": effective_input,
            }
        )

        # Build conversation history for context
        history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.analyst_messages[:-1]]

        # Get Cortex Analyst response
        with st.spinner("Analyzing..."):
            response = send_analyst_message(session, effective_input, history)

        if response["type"] == "error":
            st.session_state.analyst_messages.append(
                {
                    "role": "analyst",
                    "content": f"Error: {response['content']}",
                }
            )
        elif response["type"] == "sql":
            st.session_state.analyst_messages.append(
                {
                    "role": "analyst",
                    "content": response["content"],
                    "sql": response["sql"],
                    "results": response["results"],
                }
            )
        else:
            st.session_state.analyst_messages.append(
                {
                    "role": "analyst",
                    "content": response["content"],
                }
            )

        _safe_rerun()
