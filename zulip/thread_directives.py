"""Strict assistant-output protocol for virtual Zulip thread actions.

Only standalone blocks with the exact delimiters below are considered.  The
adapter must call :func:`extract_assistant_directives` on assistant output
only; user messages must never be passed to this parser.

Example::

    [[zulip_thread_action:
    {"op":"split","children":[{"name":"perf","goal":"Benchmark GPU use"}]}
    ]]
"""

from __future__ import annotations

import json
from bisect import bisect_right
from dataclasses import dataclass
from typing import Any, Literal

from .thread_hierarchy import ThreadStatus

DIRECTIVE_OPEN = "[[zulip_thread_action:"
DIRECTIVE_CLOSE = "]]"
DEFAULT_MAX_DIRECTIVES = 8
DEFAULT_MAX_CHILDREN_PER_SPLIT = 6
MAX_DIRECTIVE_BLOCK_CHARS = 16_000
MAX_CHILD_NAME_LENGTH = 100
MAX_GOAL_LENGTH = 2_000
MAX_STATUS_SUMMARY_LENGTH = 4_000


@dataclass(frozen=True)
class SplitChildDirective:
    name: str
    goal: str


@dataclass(frozen=True)
class SplitDirective:
    op: Literal["split"]
    children: tuple[SplitChildDirective, ...]


@dataclass(frozen=True)
class StatusDirective:
    op: Literal["status"]
    status: ThreadStatus
    summary: str


AssistantDirective = SplitDirective | StatusDirective


@dataclass(frozen=True)
class DirectiveRejection:
    """A bounded diagnostic for a removed but invalid directive block."""

    reason: str


@dataclass(frozen=True)
class DirectiveExtraction:
    visible_text: str
    directives: tuple[AssistantDirective, ...]
    rejections: tuple[DirectiveRejection, ...]


class _DuplicateKeyError(ValueError):
    pass


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _strict_json(payload: str) -> Any:
    return json.loads(
        payload,
        object_pairs_hook=_strict_object,
        parse_constant=_reject_constant,
    )


def _required_text(
    value: Any,
    field: str,
    maximum: int,
    *,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized and not allow_empty:
        raise ValueError(f"{field} must not be empty")
    if len(normalized) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters")
    return normalized


def _exact_keys(value: dict[str, Any], expected: set[str]) -> None:
    if set(value) != expected:
        raise ValueError("object keys must be exactly: " + ", ".join(sorted(expected)))


def _parse_directive(
    payload: str, *, max_children_per_split: int
) -> AssistantDirective:
    value = _strict_json(payload)
    if not isinstance(value, dict):
        raise TypeError("directive must be a JSON object")
    op = value.get("op")
    if op == "split":
        _exact_keys(value, {"op", "children"})
        children = value["children"]
        if not isinstance(children, list) or not children:
            raise ValueError("split.children must be a non-empty array")
        if len(children) > max_children_per_split:
            raise ValueError(
                f"split.children exceeds the limit of {max_children_per_split}"
            )
        parsed_children: list[SplitChildDirective] = []
        seen_names: set[str] = set()
        for child in children:
            if not isinstance(child, dict):
                raise TypeError("each split child must be an object")
            _exact_keys(child, {"name", "goal"})
            name = _required_text(child["name"], "child.name", MAX_CHILD_NAME_LENGTH)
            if "/" in name or any(ord(char) < 32 or ord(char) == 127 for char in name):
                raise ValueError("child.name must be one topic segment")
            folded_name = name.casefold()
            if folded_name in seen_names:
                raise ValueError("split child names must be unique")
            seen_names.add(folded_name)
            parsed_children.append(
                SplitChildDirective(
                    name=name,
                    goal=_required_text(child["goal"], "child.goal", MAX_GOAL_LENGTH),
                )
            )
        return SplitDirective(op="split", children=tuple(parsed_children))

    if op == "status":
        _exact_keys(value, {"op", "status", "summary"})
        try:
            status = ThreadStatus(value["status"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "status must be todo, in_progress, blocked, or done"
            ) from exc
        return StatusDirective(
            op="status",
            status=status,
            summary=_required_text(
                value["summary"],
                "summary",
                MAX_STATUS_SUMMARY_LENGTH,
                allow_empty=True,
            ),
        )
    raise ValueError("op must be split or status")


def extract_assistant_directives(
    content: str,
    *,
    max_directives: int = DEFAULT_MAX_DIRECTIVES,
    max_children_per_split: int = DEFAULT_MAX_CHILDREN_PER_SPLIT,
) -> DirectiveExtraction:
    """Remove exact directive blocks and return their validated typed values.

    Closed invalid blocks are removed and reported in ``rejections``.  They
    never raise into the response flow.  Delimiter-like inline text, fenced
    examples, and unclosed blocks remain visible because they are not exact
    assistant protocol blocks.
    """
    if not isinstance(content, str):
        return DirectiveExtraction("", (), (DirectiveRejection("content is not text"),))

    try:
        safe_directive_limit = min(max(int(max_directives), 0), 32)
    except (TypeError, ValueError, OverflowError):
        safe_directive_limit = DEFAULT_MAX_DIRECTIVES
    try:
        safe_children_limit = min(max(int(max_children_per_split), 1), 12)
    except (TypeError, ValueError, OverflowError):
        safe_children_limit = DEFAULT_MAX_CHILDREN_PER_SPLIT
    lines = content.splitlines(keepends=True)
    close_indices = [
        line_index
        for line_index, line in enumerate(lines)
        if line.rstrip("\r\n") == DIRECTIVE_CLOSE
    ]
    visible: list[str] = []
    directives: list[AssistantDirective] = []
    rejections: list[DirectiveRejection] = []
    index = 0
    in_fence = False
    block_count = 0

    while index < len(lines):
        line = lines[index]
        line_body = line.rstrip("\r\n")
        if line_body.startswith(("```", "~~~")):
            in_fence = not in_fence
            visible.append(line)
            index += 1
            continue
        if in_fence or line_body != DIRECTIVE_OPEN:
            visible.append(line)
            index += 1
            continue

        close_position = bisect_right(close_indices, index)
        if close_position >= len(close_indices):
            # An unclosed marker is ordinary visible text, not a protocol block.
            visible.append(line)
            index += 1
            continue
        close_index = close_indices[close_position]

        payload = "".join(lines[index + 1 : close_index]).rstrip("\r\n")
        block_length = sum(len(part) for part in lines[index : close_index + 1])
        block_count += 1
        if block_count > safe_directive_limit:
            rejections.append(DirectiveRejection("too many directive blocks"))
        elif block_length > MAX_DIRECTIVE_BLOCK_CHARS:
            rejections.append(DirectiveRejection("directive block is too large"))
        else:
            try:
                directives.append(
                    _parse_directive(
                        payload,
                        max_children_per_split=safe_children_limit,
                    )
                )
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                # Keep diagnostics bounded; the invalid payload itself is never
                # reflected into the visible assistant response or logs.
                rejections.append(DirectiveRejection(str(exc)[:200]))
        index = close_index + 1

    return DirectiveExtraction(
        visible_text="".join(visible),
        directives=tuple(directives),
        rejections=tuple(rejections),
    )
