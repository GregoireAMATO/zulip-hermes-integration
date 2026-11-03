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
        """Remove a possible fallback ✅ and mark the failure directly."""
        await run_to_completion(adapter, stream_event(), FAILURE, delivered=True)

        assert adapter._reaction_cfg.on_error in emojis(adapter, "reactions_added")
        assert adapter._reaction_cfg.on_success in emojis(adapter, "reactions_removed"), (
            "a failed run must not keep a possible dispatch fallback ✅"
        )

    @pytest.mark.asyncio
    async def test_a_delivered_success_places_the_finished_reaction(self, adapter):
        """Run end now owns the success marker; no extra message is sent."""
        await run_to_completion(adapter, stream_event(), SUCCESS, delivered=True)

        assert emojis(adapter, "reactions_added") == ["check_mark"]
        assert emojis(adapter, "reactions_removed") == ["eyes"]
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


class TestSuccessAfterDelivery:
    @staticmethod
    async def dispatch(adapter, event):
        from zulip.reactions import ReactionLifecycle

        reactions = ReactionLifecycle(
            adapter.client, str(event.message_id), adapter._reaction_cfg
        )
        adapter._mark_read = AsyncMock()
        await reactions.start()
        await adapter._dispatch_turn(event, reactions, None, event.message_id)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("clear", [True, False])
    async def test_dispatch_waits_for_delivered_run_end(self, adapter, clear):
        from zulip.reactions import ReactionLifecycle

        adapter._reaction_cfg.clear_on_finish = clear
        event = stream_event()
        reactions = ReactionLifecycle(adapter.client, str(event.message_id), adapter._reaction_cfg)
        adapter._mark_read = AsyncMock()
        adapter.handle_message = AsyncMock(side_effect=adapter.on_processing_start)
        await reactions.start()
        await adapter._dispatch_turn(event, reactions, None, event.message_id)

        assert emojis(adapter, "reactions_added") == ["eyes"]
        assert emojis(adapter, "reactions_removed") == []
        adapter._note_delivery_attempt(event.source.chat_id, event.metadata)
        await adapter.on_processing_complete(event, SUCCESS)
        assert emojis(adapter, "reactions_added") == ["eyes", "check_mark"]
        assert emojis(adapter, "reactions_removed") == (["eyes"] if clear else [])

    @pytest.mark.asyncio
    @pytest.mark.parametrize("clear", [True, False])
    @pytest.mark.parametrize("outcome,delivered", [
        (FAILURE, True), (CANCELLED, True), (SUCCESS, False),
        (FAILURE, False), (CANCELLED, False),
    ])
    async def test_error_never_flashes_success(self, adapter, clear, outcome, delivered):
        adapter._reaction_cfg.clear_on_finish = clear
        event = stream_event()
        adapter.handle_message = AsyncMock(side_effect=adapter.on_processing_start)
        await self.dispatch(adapter, event)
        assert emojis(adapter, "reactions_added") == ["eyes"]

        if delivered:
            adapter._note_delivery_attempt(event.source.chat_id, event.metadata)
        await adapter.on_processing_complete(event, outcome)

        assert emojis(adapter, "reactions_added") == ["eyes", "warning"]
        removed = emojis(adapter, "reactions_removed")
        assert ("eyes" in removed) is clear

    @pytest.mark.asyncio
    async def test_start_hook_after_handle_returns_claims_reactions(self, adapter):
        returned = asyncio.Event()

        async def handle(event):
            returned.set()

        adapter.handle_message = handle
        event = stream_event()
        dispatch = asyncio.create_task(self.dispatch(adapter, event))
        try:
            await returned.wait()
            # handle has returned, but the background task hasn't started yet.
            assert emojis(adapter, "reactions_added") == ["eyes"]
            await adapter.on_processing_start(event)
            await dispatch
            assert emojis(adapter, "reactions_added") == ["eyes"]
            adapter._note_delivery_attempt(event.source.chat_id, event.metadata)
            await adapter.on_processing_complete(event, SUCCESS)
            assert emojis(adapter, "reactions_added") == ["eyes", "check_mark"]
        finally:
            dispatch.cancel()
            await asyncio.gather(dispatch, return_exceptions=True)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("outcome", [SUCCESS, FAILURE])
    async def test_run_end_before_dispatch_returns_is_not_overwritten(self, adapter, outcome):
        async def handle(event):
            await adapter.on_processing_start(event)
            adapter._note_delivery_attempt(event.source.chat_id, event.metadata)
            await adapter.on_processing_complete(event, outcome)

        adapter.handle_message = handle
        await self.dispatch(adapter, stream_event())
        assert not adapter._audit_turns
        assert emojis(adapter, "reactions_added") == [
            "eyes", "check_mark" if outcome is SUCCESS else "warning"
        ]
        assert adapter._queue_controller._reaction_starts == {}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("clear", [True, False])
    @pytest.mark.parametrize("complete", [True, False])
    async def test_unarmed_command_finishes_with_or_without_run_end(
        self, adapter, monkeypatch, clear, complete
    ):
        monkeypatch.setattr("zulip.inbound_queue.REACTION_START_GRACE_SECONDS", 0.01)
        adapter._reaction_cfg.clear_on_finish = clear
        event = stream_event()
        event.text = "/status"

        async def handle(event):
            if complete:
                await adapter.on_processing_complete(event, SUCCESS)

        adapter.handle_message = handle
        await asyncio.wait_for(self.dispatch(adapter, event), timeout=1)
        assert emojis(adapter, "reactions_added") == ["eyes", "check_mark"]
        assert emojis(adapter, "reactions_removed") == (["eyes"] if clear else [])
        assert adapter._queue_controller._reaction_starts == {}

    @pytest.mark.asyncio
    async def test_unarmed_command_cannot_claim_another_runs_latch(self, adapter, monkeypatch):
        monkeypatch.setattr("zulip.inbound_queue.REACTION_START_GRACE_SECONDS", 0.01)
        run = stream_event(message_id=101)
        adapter.handle_message = AsyncMock(side_effect=adapter.on_processing_start)
        await self.dispatch(adapter, run)
        adapter.handle_message = AsyncMock()
        await self.dispatch(adapter, stream_event(message_id=102))

        assert adapter.client.reactions_added == [
            {"message_id": "101", "emoji_name": "eyes"},
            {"message_id": "102", "emoji_name": "eyes"},
            {"message_id": "102", "emoji_name": "check_mark"},
        ]
        adapter._note_delivery_attempt(run.source.chat_id, run.metadata)
        await adapter.on_processing_complete(run, SUCCESS)
        assert adapter.client.reactions_added[-1] == {
            "message_id": "101", "emoji_name": "check_mark"
        }

    @pytest.mark.asyncio
    async def test_dispatch_error_finishes_and_releases_the_latch(self, adapter):
        adapter.handle_message = AsyncMock(side_effect=RuntimeError("dispatch failed"))
        with pytest.raises(RuntimeError, match="dispatch failed"):
            await self.dispatch(adapter, stream_event())
        assert emojis(adapter, "reactions_added") == ["eyes", "warning"]
        assert adapter._queue_controller._reaction_starts == {}

    @pytest.mark.asyncio
    async def test_hook_beyond_grace_cannot_leave_fallback_success_after_error(
        self, adapter, monkeypatch
    ):
        from zulip import reactions as reaction_module

        monkeypatch.setattr("zulip.inbound_queue.REACTION_START_GRACE_SECONDS", 0.01)
        entered = asyncio.Event()
        release = asyncio.Event()
        add = reaction_module.add_reaction

        async def delayed_add(client, message_id, emoji, *args, **kwargs):
            if emoji == "check_mark":
                entered.set()
                await release.wait()
            await add(client, message_id, emoji, *args, **kwargs)

        monkeypatch.setattr(reaction_module, "add_reaction", delayed_add)
        adapter.handle_message = AsyncMock()
        event = stream_event()
        dispatch = asyncio.create_task(self.dispatch(adapter, event))
        complete = None
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)
            # The grace expired, but the fallback SDK call is still in flight.
            await adapter.on_processing_start(event)
            complete = asyncio.create_task(adapter.on_processing_complete(event, FAILURE))
            await asyncio.sleep(0)
            assert not complete.done()
            release.set()
            await asyncio.gather(dispatch, complete)
            assert emojis(adapter, "reactions_added") == ["eyes", "check_mark", "warning"]
            assert "check_mark" in emojis(adapter, "reactions_removed")
            assert adapter._queue_controller._reaction_starts == {}
        finally:
            release.set()
            dispatch.cancel()
            if complete is not None:
                complete.cancel()
            await asyncio.gather(
                dispatch, *([complete] if complete is not None else []),
                return_exceptions=True,
            )
