"""Production transport invariants: no bot turns, strict configured sender list."""
from unittest.mock import AsyncMock
import pytest
from tests.test_integration import TestFullFlow as _FullFlow


class TestSenderIsolation:
    adapter = _FullFlow.adapter

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["stream", "private"])
    @pytest.mark.parametrize("mode,content,soft", [
        ("onmessage", "hello", False), ("oncall", "@bot hello", False),
        ("onchar", "! hello", False), ("oncall", "/help", False),
        ("oncall", "hello", True), ("oncall", "reaction request", False),
    ])
    @pytest.mark.parametrize("sender,email,bot,listed", [
        (19, "other-bot@example.test", True, True),
        (20, "custom@example.test", True, True),
        (99, "outsider@example.test", False, False),
        (18, "bot@zulip.com", True, True),
    ])
    async def test_rejected_sender_never_reaches_hooks(self, adapter, monkeypatch,
            kind, mode, content, soft, sender, email, bot, listed):
        monkeypatch.setenv("ZULIP_CHATMODE", mode)
        adapter._soft_gate = soft
        adapter._bot_user_id = "18"
        adapter._allowed_senders = {email} if listed else {"human@example.test"}
        adapter.get_user_info = AsyncMock(return_value={"is_bot": bot})
        await adapter._handle_message({
            "id": 101, "type": kind, "stream_id": 1, "subject": "test",
            "display_recipient": "test", "sender_id": sender,
            "sender_email": email, "content": content,
            "_reaction_trigger": content == "reaction request",
        })
        adapter.handle_message.assert_not_awaited()
        assert adapter.client.calls["send_message"] == []
        assert adapter.client.calls["add_reaction"] == []
        assert adapter.client.calls["set_typing_status"] == []

    @pytest.mark.asyncio
    async def test_three_bots_produce_no_messages(self, adapter):
        adapter._allowed_senders = {str(i) for i in (18, 19, 20)}
        adapter.get_user_info = AsyncMock(return_value={"is_bot": True})
        for recipient in (18, 19, 20):
            adapter._bot_user_id = str(recipient)
            for sender in (18, 19, 20):
                await adapter._handle_message({
                    "id": recipient * 100 + sender, "type": "stream",
                    "stream_id": 1, "subject": "test", "display_recipient": "test",
                    "sender_id": sender, "sender_email": f"actor{sender}-bot@example.test",
                    "content": "@bot run ended without a reply",
                })
        adapter.handle_message.assert_not_awaited()
        assert adapter.client.calls["send_message"] == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("allowed", [{"human@example.test"}, {"99"}])
    async def test_allowed_human_still_reaches_gateway(self, adapter, allowed):
        adapter._allowed_senders = allowed
        adapter.get_user_info = AsyncMock(return_value={"is_bot": False})
        await adapter._handle_message({"id": 201, "type": "stream", "stream_id": 1,
            "subject": "test", "display_recipient": "test", "sender_id": 99,
            "sender_email": "human@example.test", "content": "hello"})
        adapter.handle_message.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("allowed", [set(), {"human@example.test"}])
    async def test_empty_list_or_unresolved_identity_fails_closed(self, adapter, allowed):
        adapter._allowed_senders = allowed
        adapter.get_user_info = AsyncMock(return_value=None)
        await adapter._handle_message({"id": 202, "type": "stream", "stream_id": 1,
            "sender_id": 99, "sender_email": "human@example.test", "content": "hello"})
        adapter.handle_message.assert_not_awaited()
        assert adapter.client.calls["send_message"] == []
