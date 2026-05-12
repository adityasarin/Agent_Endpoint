from __future__ import annotations

import json
import os
import pathlib
from typing import Any, Optional

from google import genai
from google.genai import types
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown

from apiconsumer.agent.prompts import SYSTEM_PROMPT, learning_phase_prompt, copilot_review_prompt
from apiconsumer.agent.tools import TOOL_SCHEMAS, dispatch
from apiconsumer.models.pipeline import PipelineConfig

console = Console()

_SESSIONS_DIR = pathlib.Path(".claude/sessions")
_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

# Cached in-memory — never written to environment or disk.
_cached_api_key: Optional[str] = None


def _get_client() -> genai.Client:
    global _cached_api_key
    api_key = os.environ.get("Gemini_API_Key") or _cached_api_key
    if not api_key:
        import click
        api_key = click.prompt("Enter Gemini API key", hide_input=True)
        _cached_api_key = api_key
    return genai.Client(api_key=api_key)


def _build_gemini_tools(schemas: list[dict]) -> list[types.Tool]:
    declarations = []
    for schema in schemas:
        kwargs: dict[str, Any] = {
            "name": schema["name"],
            "description": schema["description"],
        }
        if schema.get("parameters"):
            kwargs["parameters"] = schema["parameters"]
        declarations.append(types.FunctionDeclaration(**kwargs))
    return [types.Tool(function_declarations=declarations)]


def _content_to_serialisable(content: types.Content) -> dict:
    parts = []
    for part in content.parts:
        if hasattr(part, "text") and part.text:
            parts.append({"type": "text", "text": part.text})
        elif hasattr(part, "function_call") and part.function_call:
            parts.append({
                "type": "function_call",
                "name": part.function_call.name,
                "args": dict(part.function_call.args) if part.function_call.args else {},
            })
        elif hasattr(part, "function_response") and part.function_response:
            parts.append({
                "type": "function_response",
                "name": part.function_response.name,
            })
    return {"role": content.role, "parts": parts}


def _save_session(pipeline_id: str, messages: list[types.Content]) -> None:
    _SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    session_file = _SESSIONS_DIR / f"{pipeline_id}.jsonl"
    with session_file.open("a", encoding="utf-8") as f:
        for msg in messages:
            f.write(json.dumps(_content_to_serialisable(msg), default=str) + "\n")


def _render_assistant_text(content: types.Content) -> None:
    for part in content.parts:
        if hasattr(part, "text") and part.text and part.text.strip():
            console.print(Panel(Markdown(part.text), border_style="blue", title="[blue]Agent[/blue]"))


def _render_tool_call(name: str, tool_input: dict) -> None:
    console.print(f"  [dim]-> calling [bold]{name}[/bold] ...[/dim]")


def _render_tool_result(name: str, result: dict) -> None:
    if result.get("success") is False:
        console.print(f"  [red]x {name}: {result.get('error', 'failed')}[/red]")
    else:
        if name == "probe_endpoint":
            console.print(f"  [green]ok probe: HTTP {result.get('status_code')} in {result.get('response_time_ms', 0):.0f}ms[/green]")
        elif name == "detect_pagination_pattern":
            console.print(f"  [green]ok pagination: {result.get('strategy')} (confidence {result.get('confidence', 0):.0%})[/green]")
        elif name == "test_auth":
            status = "[green]ok" if result.get("success") else "[red]x"
            console.print(f"  {status} auth: {result.get('message')}[/green]")
        elif name == "finalize_pipeline_config":
            console.print(f"  [green]ok pipeline config saved: {result.get('pipeline_id')}[/green]")
        else:
            console.print(f"  [green]ok {name}[/green]")


def run_learning_phase(
    endpoint_spec_dict: dict,
    sample_provided: bool,
    pipeline_id_hint: Optional[str] = None,
) -> Optional[PipelineConfig]:
    """
    Run the learning phase agent loop.
    Returns the finalised PipelineConfig or None if the agent couldn't complete.
    """
    client = _get_client()
    gemini_tools = _build_gemini_tools(TOOL_SCHEMAS)

    user_content = learning_phase_prompt(
        json.dumps(endpoint_spec_dict, indent=2),
        sample_provided,
    )

    messages: list[types.Content] = [
        types.Content(role="user", parts=[types.Part.from_text(user_content)])
    ]
    finalised_config: Optional[PipelineConfig] = None

    console.print()
    console.print(Panel("[bold cyan]Learning Phase[/bold cyan] — Analysing endpoint...", border_style="cyan"))

    iteration = 0
    max_iterations = 40

    while iteration < max_iterations:
        iteration += 1

        response = client.models.generate_content(
            model=_MODEL,
            contents=messages,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                tools=gemini_tools,
                max_output_tokens=4096,
            ),
        )

        assistant_content = response.candidates[0].content
        messages.append(assistant_content)
        _render_assistant_text(assistant_content)

        function_calls = [
            p.function_call for p in assistant_content.parts
            if hasattr(p, "function_call") and p.function_call is not None
        ]

        if not function_calls:
            break

        tool_result_parts: list[types.Part] = []
        for fc in function_calls:
            name = fc.name
            args = dict(fc.args) if fc.args else {}

            _render_tool_call(name, args)
            result = dispatch(name, args)
            _render_tool_result(name, result)

            if name == "finalize_pipeline_config" and result.get("success"):
                from apiconsumer.models.pipeline import PipelineConfig as PC
                try:
                    finalised_config = PC.model_validate(result["config"])
                except Exception:
                    pass

            tool_result_parts.append(
                types.Part.from_function_response(name=name, response={"result": result})
            )

        messages.append(types.Content(role="user", parts=tool_result_parts))

    pid = (finalised_config.pipeline_id if finalised_config else pipeline_id_hint) or "unknown"
    _save_session(pid, messages)
    return finalised_config


def run_copilot_review(
    pipeline_id: str,
    batch_summary: dict,
) -> dict:
    """
    Run a single co-pilot review turn.
    Returns {"approved": bool, "notes": str, "user_input_needed": bool, "user_question": str}
    """
    client = _get_client()
    review_schemas = [t for t in TOOL_SCHEMAS if t["name"] in ("approve_batch", "ask_user")]
    gemini_tools = _build_gemini_tools(review_schemas)

    messages: list[types.Content] = [
        types.Content(role="user", parts=[types.Part.from_text(copilot_review_prompt(batch_summary))])
    ]

    iteration = 0
    while iteration < 10:
        iteration += 1

        response = client.models.generate_content(
            model=_MODEL,
            contents=messages,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                tools=gemini_tools,
                max_output_tokens=1024,
            ),
        )

        assistant_content = response.candidates[0].content
        messages.append(assistant_content)
        _render_assistant_text(assistant_content)

        function_calls = [
            p.function_call for p in assistant_content.parts
            if hasattr(p, "function_call") and p.function_call is not None
        ]

        if not function_calls:
            return {"approved": True, "notes": "", "user_input_needed": False}

        tool_result_parts: list[types.Part] = []
        for fc in function_calls:
            name = fc.name
            args = dict(fc.args) if fc.args else {}

            if name == "approve_batch":
                dispatch(name, args)
                _save_session(pipeline_id, messages)
                return {
                    "approved": True,
                    "notes": args.get("notes", ""),
                    "user_input_needed": False,
                }

            if name == "ask_user":
                result = dispatch(name, args)
                tool_result_parts.append(
                    types.Part.from_function_response(name=name, response={"result": result})
                )
                _save_session(pipeline_id, messages)
                return {
                    "approved": False,
                    "notes": "",
                    "user_input_needed": True,
                    "user_answers": result.get("answers", {}),
                }

            result = dispatch(name, args)
            tool_result_parts.append(
                types.Part.from_function_response(name=name, response={"result": result})
            )

        messages.append(types.Content(role="user", parts=tool_result_parts))

    return {"approved": True, "notes": "Review loop exhausted — continuing", "user_input_needed": False}
