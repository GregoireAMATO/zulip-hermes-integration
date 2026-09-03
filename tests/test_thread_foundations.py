"""Focused tests for hierarchy configuration, state, and directives."""

import random
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from zulip.thread_config import (
    MAX_CHILDREN_PER_SPLIT,
    MAX_MAX_DEPTH,
    MAX_ROLLUP_DEBOUNCE_SECONDS,
    AutoSplitMode,
    ThreadHierarchyConfig,
    resolve_thread_hierarchy_config,
)
from zulip.thread_directives import (
    DIRECTIVE_CLOSE,
    DIRECTIVE_OPEN,
    MAX_DIRECTIVE_BLOCK_CHARS,
    SplitDirective,
    StatusDirective,
    extract_assistant_directives,
)
from zulip.thread_hierarchy import (
    SCHEMA_VERSION,
    ThreadHierarchyStore,
    ThreadStatus,
)


def test_thread_config_defaults_to_suggest_and_never_reads_environment(monkeypatch):
    monkeypatch.setenv("ZULIP_THREAD_HIERARCHY_MODE", "auto")
    assert resolve_thread_hierarchy_config(SimpleNamespace(extra={})) == (
        ThreadHierarchyConfig(auto_split=AutoSplitMode.SUGGEST)
    )


def test_thread_config_is_typed_bounded_and_disable_wins():
    config = SimpleNamespace(
        extra={
            "thread_hierarchy": {
                "enabled": "false",
                "auto_split": "auto",
                "max_depth": 999,
                "max_children_per_split": "999",
                "rollup_debounce_seconds": 999_999,
            }
        }
    )
    resolved = resolve_thread_hierarchy_config(config)
    assert resolved.enabled is False
    assert resolved.auto_split is AutoSplitMode.OFF
    assert resolved.max_depth == MAX_MAX_DEPTH
    assert resolved.max_children_per_split == MAX_CHILDREN_PER_SPLIT
    assert resolved.rollup_debounce_seconds == MAX_ROLLUP_DEBOUNCE_SECONDS


def test_invalid_mode_and_values_fall_back_safely():
    config = SimpleNamespace(
        extra={
            "thread_hierarchy": {
                "auto_split": "unlimited",
                "max_depth": True,
                "max_children_per_split": object(),
                "rollup_debounce_seconds": None,
            }
        }
    )
    resolved = resolve_thread_hierarchy_config(config)
    assert resolved == ThreadHierarchyConfig(auto_split=AutoSplitMode.SUGGEST)


def test_legacy_database_migrates_without_losing_relations(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE threads (
                channel_id TEXT NOT NULL,
                topic_name TEXT NOT NULL,
                parent_topic_name TEXT,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (channel_id, topic_name),
                FOREIGN KEY (channel_id, parent_topic_name)
                    REFERENCES threads(channel_id, topic_name)
            );
            INSERT INTO threads VALUES ('5', 'training', NULL, 1);
            INSERT INTO threads VALUES ('5', 'training / perf', 'training', 2);
            """
        )

    store = ThreadHierarchyStore(path)
    assert store.parent("5", "training / perf") == "training"
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        columns = {row[1] for row in connection.execute("PRAGMA table_info(threads)")}
    assert {
        "objective",
        "seed_context",
        "seed_consumed_at",
        "source_message_id",
    } <= columns


def test_schema_initialization_is_safe_across_concurrent_store_instances(tmp_path):
    path = tmp_path / "concurrent.sqlite3"
    with ThreadPoolExecutor(max_workers=8) as executor:
        stores = list(executor.map(lambda _: ThreadHierarchyStore(path), range(16)))

    assert len(stores) == 16
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


def test_split_carries_goal_seed_and_source_and_deduplicates(tmp_path):
    store = ThreadHierarchyStore(tmp_path / "threads.sqlite3")
    first = store.split(
        "5",
        "training",
        "perf",
        goal="Benchmark the pipeline",
        seed_context="The parent requested a GPU benchmark.",
        source_message_id=42,
    )
    duplicate = store.split(
        "5",
        "training",
        "perf",
        goal="ignored retry",
        seed_context="ignored retry",
        source_message_id=42,
    )
    details = store.thread_details("5", "training / perf")

    assert first.created is True and first.duplicate is False
    assert duplicate.created is False and duplicate.duplicate is True
    assert details is not None
    assert details.goal == "Benchmark the pipeline"
    assert details.seed_context == "The parent requested a GPU benchmark."
    assert details.source_message_id == "42"


def test_seed_is_consumed_once_under_concurrency(tmp_path):
    store = ThreadHierarchyStore(tmp_path / "threads.sqlite3")
    store.split("5", "training", "perf", seed_context="seed")

    with ThreadPoolExecutor(max_workers=8) as executor:
        seeds = list(
            executor.map(
                lambda _: store.consume_seed("5", "training / perf"), range(20)
            )
        )

    assert [seed.context for seed in seeds if seed is not None] == ["seed"]
    assert store.peek_seed("5", "training / perf") is None
    assert store.mark_seed_consumed("5", "training / perf") is False


def test_status_overview_and_source_deduplication(tmp_path):
    store = ThreadHierarchyStore(tmp_path / "threads.sqlite3")
    store.split("5", "training", "perf", goal="Benchmark")

    first = store.set_status(
        "5", "training / perf", "in_progress", "Running", source_message_id=88
    )
    duplicate = store.set_status(
        "5", "training / perf", "done", "Wrong retry", source_message_id=88
    )

    assert first.changed is True
    assert duplicate.changed is False
    assert store.get_status("5", "training / perf").status is ThreadStatus.IN_PROGRESS
    overview = store.overview("5", "training")
    assert [item.thread.topic_name for item in overview] == [
        "training",
        "training / perf",
    ]
    assert overview[0].status is ThreadStatus.TODO
    assert overview[1].goal == "Benchmark"
    assert overview[1].summary == "Running"


def test_rollup_metadata_and_debounce_survive_reopen(tmp_path):
    path = tmp_path / "threads.sqlite3"
    store = ThreadHierarchyStore(path)
    stored = store.set_rollup("5", "training", 123, "digest-v1", updated_at=1_000)

    assert stored.message_id == "123"
    reopened = ThreadHierarchyStore(path)
    assert reopened.get_rollup("5", "training") == stored
    assert reopened.rollup_due("5", "training", 1, now_ns=500_000_000) is False
    assert reopened.rollup_due("5", "training", 1, now_ns=1_000_001_000) is True


def test_generic_action_claim_is_atomic(tmp_path):
    store = ThreadHierarchyStore(tmp_path / "threads.sqlite3")
    with ThreadPoolExecutor(max_workers=8) as executor:
        claims = list(
            executor.map(
                lambda _: store.claim_action("5", 77, "rollup:training"), range(20)
            )
        )
    assert sum(claims) == 1
    assert store.is_action_processed("5", 77, "rollup:training") is True


def _block(payload: str) -> str:
    return f"{DIRECTIVE_OPEN}\n{payload}\n{DIRECTIVE_CLOSE}"


def test_extracts_typed_split_and_status_and_preserves_visible_text():
    content = (
        "Je découpe ce travail.\n"
        + _block(
            '{"op":"split","children":['
            '{"name":"perf","goal":"Benchmark GPU"},'
            '{"name":"bugs","goal":"Triage failures"}]}'
        )
        + "\nPuis je lance le benchmark.\n"
        + _block('{"op":"status","status":"in_progress","summary":"Started"}')
    )
    result = extract_assistant_directives(content)

    assert (
        result.visible_text == "Je découpe ce travail.\nPuis je lance le benchmark.\n"
    )
    assert isinstance(result.directives[0], SplitDirective)
    assert result.directives[0].children[0].name == "perf"
    assert isinstance(result.directives[1], StatusDirective)
    assert result.directives[1].status is ThreadStatus.IN_PROGRESS
    assert result.rejections == ()


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        '{"op":"status","status":"done","summary":"ok","extra":1}',
        '{"op":"status","status":"done","status":"todo","summary":"ok"}',
        '{"op":"status","status":"unknown","summary":"ok"}',
        '{"op":"split","children":[]}',
        '{"op":"split","children":[{"name":"a/b","goal":"bad"}]}',
        '{"op":"status","status":"done","summary":"ok","n":NaN}',
    ],
)
def test_invalid_exact_blocks_are_removed_without_raising(payload):
    result = extract_assistant_directives(f"before\n{_block(payload)}\nafter")
    assert result.visible_text == "before\nafter"
    assert result.directives == ()
    assert len(result.rejections) == 1


def test_inline_fenced_and_unclosed_markers_are_not_protocol_blocks():
    content = (
        f"inline {DIRECTIVE_OPEN}\n"
        f'```text\n{DIRECTIVE_OPEN}\n{{"op":"status"}}\n{DIRECTIVE_CLOSE}\n```\n'
        f"{DIRECTIVE_OPEN}\nunclosed"
    )
    result = extract_assistant_directives(content)
    assert result.visible_text == content
    assert result.directives == ()
    assert result.rejections == ()


def test_parser_enforces_block_children_and_count_limits():
    oversized = _block(" " * MAX_DIRECTIVE_BLOCK_CHARS)
    too_many_children = _block(
        '{"op":"split","children":['
        + ",".join(f'{{"name":"c{i}","goal":"g"}}' for i in range(7))
        + "]}"
    )
    valid = _block('{"op":"status","status":"done","summary":"ok"}')
    result = extract_assistant_directives(
        f"{oversized}\n{too_many_children}\n{valid}\n{valid}", max_directives=3
    )
    assert len(result.directives) == 1
    assert len(result.rejections) == 3


def test_parser_fuzz_never_raises_or_grows_directive_count():
    rng = random.Random(20260830)
    alphabet = '{}[],:"abcdefghijklmnopqrstuvwxyz0123456789\n '
    for _ in range(200):
        payload = "".join(rng.choice(alphabet) for _ in range(rng.randrange(500)))
        result = extract_assistant_directives(_block(payload), max_directives=2)
        assert len(result.directives) <= 2
        assert len(result.rejections) <= 1
