"""Empty/refused runs stay in the audit; error reactions remain accurate."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

SUCCESS = SimpleNamespace(value="success")
FAILURE = SimpleNamespace(value="failure")
CANCELLED = SimpleNamespace(value="cancelled")

TOPIC = "api-review"
CHAT_ID = "573423"
MESSAGE_ID = 4242


@pytest.fixture
def adapter(mock_platform_config, monkeypatch, tmp_path):
    import zulip.adapter as adapter_module
    import zulip.zulip_client as zulip_client_module

    monkeypatch.setattr(zulip_client_module, "ZULIP_AVAILABLE", True)
    monkeypatch.setenv("ZULIP_SITE", "https://test.zulipchat.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@test.com")
    monkeypatch.setenv("ZULIP_API_KEY", "k" * 32)
    monkeypatch.setenv("HERMES_DATA_DIR", str(tmp_path))
    # Traces off: this file is about the room-facing notice, and a trace status
    # message would land in the same list the notice does.
    monkeypatch.delenv("ZULIP_ACTIVITY_TRACE", raising=False)

    class MockZulipModule:
        class Client:
            def __init__(self, **kwargs):
                self.sent = []
                self.reactions_added = []
                self.reactions_removed = []
                self._next_id = 900

            def send_message(self, request):
                self.sent.append(request)
                self._next_id += 1
                return {"result": "success", "id": self._next_id}

            def add_reaction(self, request):
                self.reactions_added.append(request)
                return {"result": "success"}

            def remove_reaction(self, request):
                self.reactions_removed.append(request)
                return {"result": "success"}

            def set_typing_status(self, *a, **k):
                return {"result": "success"}

    monkeypatch.setattr(zulip_client_module, "zulip", MockZulipModule())
    # The live-adapter registry is module-level, so adapters built by earlier
    # tests in the same process stay registered and can answer for this one.
    monkeypatch.setattr(
        adapter_module, "_LIVE_ADAPTERS", type(adapter_module._LIVE_ADAPTERS)()
    )
    adapter_module._clear_caches()

    from zulip.adapter import ZulipAdapter

    return ZulipAdapter(mock_platform_config)


def stream_event(topic=TOPIC, chat_id=CHAT_ID, message_id=MESSAGE_ID):
    # The source carries the session's thread_id and the send metadata is
    # derived from it in the real gateway, so a fixture with one and not the
    # other models a state the host cannot produce -- the shape that let #272
    # hide (dispatch read the source, delivery read the send metadata).
    return SimpleNamespace(
        source=SimpleNamespace(chat_id=chat_id, thread_id=topic),
        metadata={"thread_id": topic},
        message_id=message_id,
    )


def notice_texts(adapter):
    """Only the silent-run notices, never a reply or a trace status message."""
    return [
        m["content"] for m in adapter.client.sent if "without a reply" in str(m.get("content"))
    ]


def emojis(adapter, which):
    return [r["emoji_name"] for r in getattr(adapter.client, which)]


async def run_to_completion(adapter, event, outcome, delivered=False):
    """Mirror the real call order: arm, optionally deliver, then complete."""
    await adapter.on_processing_start(event)
    if delivered:
        adapter._note_delivery_attempt(event.source.chat_id, event.metadata)
    await adapter.on_processing_complete(event, outcome)
    # Reaction placement goes through asyncio.to_thread, so give the worker
    # thread a real moment rather than a bare yield.
    await asyncio.sleep(0.1)


class TestErrorReaction:
    @pytest.mark.asyncio
    async def test_failure_after_a_delivered_reply_marks_the_message(self, adapter):
        """✅ already answered the dispatch; ⚠️ has to replace it, not join it."""
        await run_to_completion(adapter, stream_event(), FAILURE, delivered=True)

        assert adapter._reaction_cfg.on_error in emojis(adapter, "reactions_added")
        assert adapter._reaction_cfg.on_success in emojis(adapter, "reactions_removed"), (
            "a failed run must not keep the ✅ that its dispatch return placed"
        )

    @pytest.mark.asyncio
    async def test_a_delivered_success_keeps_todays_reaction(self, adapter):
        """The common case must be untouched: no extra marker, no extra message."""
        await run_to_completion(adapter, stream_event(), SUCCESS, delivered=True)

        assert adapter.client.reactions_added == []
        assert adapter.client.reactions_removed == []
        assert notice_texts(adapter) == []

    @pytest.mark.asyncio
    async def test_never_dispatched_turn_is_untouched(self, adapter):
        """A native command the host answers itself: no arming, so no report."""
        event = stream_event()
        await adapter.on_processing_complete(event, FAILURE)
        await asyncio.sleep(0.1)

        assert adapter.client.reactions_added == []
        assert notice_texts(adapter) == []


class TestSilentRunNotice:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("outcome", [SUCCESS, FAILURE, CANCELLED,
        SimpleNamespace(value="failure", reason="authorization refused")])
    async def test_empty_or_refused_run_is_audit_only(self, adapter, monkeypatch, outcome):
        sent = AsyncMock()
        monkeypatch.setattr(adapter, "send", sent)
        audit = AsyncMock()
        monkeypatch.setattr(adapter._audit_logger, "log_deliver_empty", audit)
        event = stream_event()
        await run_to_completion(adapter, event, outcome)
        sent.assert_not_awaited()
        audit.assert_awaited_once()
        await adapter.on_processing_complete(event, outcome)
        audit.assert_awaited_once()
        sent.assert_not_awaited()
