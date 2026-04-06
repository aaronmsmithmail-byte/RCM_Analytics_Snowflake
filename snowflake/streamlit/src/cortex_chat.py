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
            messages.append(
                {
                    "role": msg["role"],
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

    # Call Cortex Analyst via the REST API (available within SiS)
    try:
        request_body = {
            "messages": messages,
            "semantic_model_file": SEMANTIC_MODEL_STAGE,
        }

        resp = session.sql(
            f"""
            SELECT SNOWFLAKE.CORTEX.ANALYST(
                PARSE_JSON('{json.dumps(request_body).replace("'", "''")}')
            ) AS RESPONSE
            """
        ).collect()

        if not resp:
            return {"type": "error", "content": "No response from Cortex Analyst"}

        response_json = json.loads(resp[0]["RESPONSE"])
        return _parse_analyst_response(session, response_json)

    except Exception as e:
        # Fallback: try the _snowflake module approach (available in SiS)
        try:
            return _call_analyst_via_rest(session, messages)
        except Exception:
            return {"type": "error", "content": f"Cortex Analyst error: {str(e)}"}


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
        return {
            "type": "error",
            "content": f"Cortex Analyst returned status {resp['status']}",
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
            if result["sql"]:
                result["explanation"] = text
            else:
                result["content"] = text

    # Build combined content for display
    if result["type"] == "sql" and result["explanation"]:
        result["content"] = result["explanation"]
    elif result["type"] == "sql" and not result["content"]:
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

    st.subheader("AI Assistant (Cortex Analyst)")
    st.caption(
        "Ask questions about your RCM data in natural language. "
        "Cortex Analyst will generate and execute SQL queries against your data."
    )

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

    # Chat input — compatible with older SiS Streamlit versions
    input_col, btn_col = st.columns([5, 1])
    with input_col:
        user_input = st.text_input(
            "Ask about your RCM data",
            key="analyst_input",
            label_visibility="collapsed",
            placeholder="Ask about your RCM data...",
        )
    with btn_col:
        send_clicked = st.button("Send", type="primary", use_container_width=True)

    if send_clicked and user_input:
        st.session_state.analyst_messages.append(
            {
                "role": "user",
                "content": user_input,
            }
        )

        # Build conversation history for context
        history = [{"role": m["role"], "content": m["content"]} for m in st.session_state.analyst_messages[:-1]]

        # Get Cortex Analyst response
        with st.spinner("Analyzing..."):
            response = send_analyst_message(session, user_input, history)

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

        st.experimental_rerun()
