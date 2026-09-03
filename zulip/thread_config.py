"""Typed configuration for Zulip's virtual topic hierarchy.

Behavioral settings live under ``PlatformConfig.extra["thread_hierarchy"]``.
The resolver is deliberately tolerant: malformed user configuration falls
back to conservative defaults instead of preventing the Zulip adapter from
starting.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

DEFAULT_MAX_DEPTH = 4
DEFAULT_MAX_CHILDREN_PER_SPLIT = 6
DEFAULT_ROLLUP_DEBOUNCE_SECONDS = 30

MIN_MAX_DEPTH = 1
MAX_MAX_DEPTH = 8
MIN_CHILDREN_PER_SPLIT = 1
MAX_CHILDREN_PER_SPLIT = 12
MIN_ROLLUP_DEBOUNCE_SECONDS = 1
MAX_ROLLUP_DEBOUNCE_SECONDS = 3600


class AutoSplitMode(str, Enum):
    """How much authority the agent has to create virtual subtopics."""

    OFF = "off"
    SUGGEST = "suggest"
    AUTO = "auto"


@dataclass(frozen=True)
class ThreadHierarchyConfig:
    """Validated settings consumed by the Zulip adapter."""

    enabled: bool = True
    auto_split: AutoSplitMode = AutoSplitMode.SUGGEST
    max_depth: int = DEFAULT_MAX_DEPTH
    max_children_per_split: int = DEFAULT_MAX_CHILDREN_PER_SPLIT
    rollup_debounce_seconds: int = DEFAULT_ROLLUP_DEBOUNCE_SECONDS


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"true", "yes", "on", "1"}:
            return True
        if normalized in {"false", "no", "off", "0"}:
            return False
    return default


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    # bool is an int subclass, but accepting it here would turn configuration
    # typos into surprising limits of one.
    if isinstance(value, bool):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return min(max(parsed, minimum), maximum)


def _resolve_mode(value: Any) -> AutoSplitMode:
    if isinstance(value, AutoSplitMode):
        return value
    if isinstance(value, str):
        try:
            return AutoSplitMode(value.strip().casefold())
        except ValueError:
            pass
    return AutoSplitMode.SUGGEST


def resolve_thread_hierarchy_config(platform_config: Any) -> ThreadHierarchyConfig:
    """Resolve safe hierarchy settings from a PlatformConfig-like object.

    Only ``extra.thread_hierarchy`` is consulted.  In particular, this module
    intentionally has no environment-variable fallback.
    """

    extra = getattr(platform_config, "extra", None)
    if not isinstance(extra, dict):
        return ThreadHierarchyConfig()
    raw = extra.get("thread_hierarchy")
    if not isinstance(raw, dict):
        return ThreadHierarchyConfig()

    enabled = _as_bool(raw.get("enabled"), True)
    return ThreadHierarchyConfig(
        enabled=enabled,
        auto_split=(
            _resolve_mode(raw.get("auto_split", raw.get("mode")))
            if enabled
            else AutoSplitMode.OFF
        ),
        max_depth=_bounded_int(
            raw.get("max_depth"), DEFAULT_MAX_DEPTH, MIN_MAX_DEPTH, MAX_MAX_DEPTH
        ),
        max_children_per_split=_bounded_int(
            raw.get("max_children_per_split"),
            DEFAULT_MAX_CHILDREN_PER_SPLIT,
            MIN_CHILDREN_PER_SPLIT,
            MAX_CHILDREN_PER_SPLIT,
        ),
        rollup_debounce_seconds=_bounded_int(
            raw.get("rollup_debounce_seconds"),
            DEFAULT_ROLLUP_DEBOUNCE_SECONDS,
            MIN_ROLLUP_DEBOUNCE_SECONDS,
            MAX_ROLLUP_DEBOUNCE_SECONDS,
        ),
    )
