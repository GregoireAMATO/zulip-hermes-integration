"""End-to-end behavior tests for assistant-driven Zulip topic hierarchy."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from zulip.thread_hierarchy import ThreadStatus


def _directive(payload: str) -> str:
    return f"[[zulip_thread_action:\n{payload}\n]]"


@pytest.fixture
def make_adapter(monkeypatch, tmp_path):
    import zulip.adapter as adapter_module
    from tests.conftest import MockZulipClient

    monkeypatch.setattr(adapter_module, "ZULIP_AVAILABLE", True)
    monkeypatch.setenv("HERMES_DATA_DIR", str(tmp_path))

    class MockZulipModule:
        class Client:
            def __init__(self, **kwargs):
                self._client = MockZulipClient(**kwargs)

            def __getattr__(self, name):
                return getattr(self._client, name)

    monkeypatch.setattr(adapter_module, "zulip", MockZulipModule())

    def factory(mode="suggest", **overrides):
        hierarchy = {"auto_split": mode, **overrides}
        config = SimpleNamespace(
            extra={
                "api_key": "fake-key",
                "email": "bot@test.zulipchat.com",
                "site": "https://test.zulipchat.com",
                "thread_hierarchy": hierarchy,
            }
        )
        adapter = adapter_module.ZulipAdapter(config)
        adapter._stream_name_cache["5"] = "ml-platform"
        adapter._schedule_thread_rollup = MagicMock()
        return adapter

    return factory


@pytest.mark.asyncio
async def test_suggest_mode_strips_directive_and_requires_confirmation(make_adapter):
    adapter = make_adapter("suggest")
    content = "Je vois deux chantiers.\n" + _directive(
        '{"op":"split","children":['
        '{"name":"perf","goal":"Mesurer le débit"},'
        '{"name":"bugs","goal":"Trier les erreurs"}]}'
    )

    result = await adapter.send("5", content, metadata={"topic": "training"})

    assert result.success is True
    sent = adapter.client._client._sent_messages
    assert sent[0]["content"] == "Je vois deux chantiers.\n"
    assert "zulip_thread_action" not in "\n".join(item["content"] for item in sent)
    assert "@thread split perf" in sent[1]["content"]
    assert "@thread split bugs" in sent[1]["content"]
    assert adapter._thread_store.children("5", "training") == []


@pytest.mark.asyncio
async def test_auto_mode_creates_children_goals_and_seed_without_leaking_block(
    make_adapter,
):
    adapter = make_adapter("auto")
    content = "Je lance les deux pistes.\n" + _directive(
        '{"op":"split","children":['
        '{"name":"perf","goal":"Mesurer le débit"},'
        '{"name":"bugs","goal":"Trier les erreurs"}]}'
    )

    await adapter.send("5", content, metadata={"topic": "training"})

    assert adapter._thread_store.children("5", "training") == [
        "training / bugs",
        "training / perf",
    ]
    perf = adapter._thread_store.thread_details("5", "training / perf")
    assert perf.goal == "Mesurer le débit"
    assert perf.seed_context == "Je lance les deux pistes."
    sent = adapter.client._client._sent_messages
    assert all("zulip_thread_action" not in item["content"] for item in sent)
    assert {item["topic"] for item in sent} >= {
        "training",
        "training / perf",
        "training / bugs",
    }
    adapter._schedule_thread_rollup.assert_called_once_with(
        "5", "training", "ml-platform"
    )


@pytest.mark.asyncio
async def test_auto_split_is_idempotent_for_retried_assistant_output(make_adapter):
    adapter = make_adapter("auto")
    content = "Découpage.\n" + _directive(
        '{"op":"split","children":[{"name":"perf","goal":"Benchmark"}]}'
    )

    await adapter.send("5", content, metadata={"topic": "training"})
    first_count = len(adapter.client._client._sent_messages)
    await adapter.send("5", content, metadata={"topic": "training"})

    # The visible answer is retried by the caller, but structural messages and
    # database mutations are not duplicated.
    assert len(adapter.client._client._sent_messages) == first_count + 1
    assert adapter._thread_store.children("5", "training") == ["training / perf"]


@pytest.mark.asyncio
async def test_off_mode_strips_and_ignores_automatic_action(make_adapter):
    adapter = make_adapter("off")
    content = "Réponse normale.\n" + _directive(
        '{"op":"split","children":[{"name":"perf","goal":"Benchmark"}]}'
    )

    await adapter.send("5", content, metadata={"topic": "training"})

    assert [item["content"] for item in adapter.client._client._sent_messages] == [
        "Réponse normale.\n"
    ]
    assert adapter._thread_store.children("5", "training") == []


@pytest.mark.asyncio
async def test_status_directive_updates_current_topic_and_schedules_rollup(
    make_adapter,
):
    adapter = make_adapter("suggest")
    content = "Le benchmark démarre.\n" + _directive(
        '{"op":"status","status":"in_progress","summary":"GPU réservé"}'
    )

    await adapter.send("5", content, metadata={"topic": "training / perf"})

    status = adapter._thread_store.get_status("5", "training / perf")
    assert status.status is ThreadStatus.IN_PROGRESS
    assert status.summary == "GPU réservé"
    adapter._schedule_thread_rollup.assert_called_once_with(
        "5", "training / perf", "ml-platform"
    )


@pytest.mark.asyncio
async def test_max_depth_blocks_auto_split(make_adapter):
    adapter = make_adapter("auto", max_depth=1)
    adapter._thread_store.split("5", "training", "perf")
    content = "Encore un niveau.\n" + _directive(
        '{"op":"split","children":[{"name":"gpu","goal":"Profiler"}]}'
    )

    await adapter.send("5", content, metadata={"topic": "training / perf"})

    assert adapter._thread_store.children("5", "training / perf") == []
    assert "profondeur maximale" in adapter.client._client._sent_messages[-1]["content"]


@pytest.mark.asyncio
async def test_dm_never_executes_but_still_hides_control_protocol(make_adapter):
    adapter = make_adapter("auto")
    content = "Réponse privée.\n" + _directive(
        '{"op":"split","children":[{"name":"perf","goal":"Benchmark"}]}'
    )

    await adapter.send("dm:42", content)

    assert adapter.client._client._sent_messages[0]["content"] == "Réponse privée.\n"
    assert adapter._thread_store.children("dm:42", "general") == []


def test_platform_hint_exposes_stable_bounded_protocol():
    import zulip.adapter as adapter_module

    context = MagicMock()
    adapter_module.register(context)
    hint = context.register_platform.call_args.kwargs["platform_hint"]

    assert "[[zulip_thread_action:" in hint
    assert '"op":"split"' in hint
    assert '"op":"status"' in hint
    assert "do not split ordinary conversations" in hint
    assert "control blocks" in hint
