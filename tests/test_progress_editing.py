"""Progress transport contract, including the installed native consumer method."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from zulip.adapter import ZulipAdapter


@pytest.fixture
def adapter(mock_platform_config, monkeypatch):
    import zulip.adapter as module
    monkeypatch.setattr(module, "ZULIP_AVAILABLE", True)
    client = MagicMock()
    client.send_message.side_effect = lambda request: {"result": "success", "id": 100 + client.send_message.call_count}
    client.update_message.return_value = {"result": "success"}
    monkeypatch.setattr(module, "zulip", SimpleNamespace(Client=lambda **kw: client))
    obj = ZulipAdapter(mock_platform_config)
    obj.send_typing = AsyncMock()
    return obj


@pytest.mark.asyncio
@pytest.mark.parametrize("chat_id", ["dm:42", "17"])
async def test_edit_content_only(adapter, chat_id):
    sent = await adapter.send(chat_id, "tool: skill_view", metadata={"thread_id": "original"})
    adapter._topic_cache[chat_id] = "another topic"
    for finalize in (False, True):
        result = await adapter.edit_message(chat_id, sent.message_id, "tool: skill_view\ntool: test", finalize=finalize, metadata={"topic": "ignore"})
        assert result.success and result.message_id == sent.message_id
    assert adapter.client.update_message.call_args.args[0] == {"message_id": int(sent.message_id), "content": "tool: skill_view\ntool: test"}
    assert adapter.client.send_message.call_count == 1
    assert adapter.SUPPORTS_MESSAGE_EDITING is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [{"result": "error", "msg": "permission denied"}, {"result": "error", "msg": "rate limited", "retry_after": 99}, TimeoutError(), OSError()])
async def test_failure_freezes_bubble_preserves_final(adapter, failure):
    if isinstance(failure, Exception):
        adapter.client.update_message.side_effect = failure
    else:
        adapter.client.update_message.return_value = failure
    for _ in range(5):
        result = await adapter.edit_message("dm:42", "123", "activity")
        assert not result.success and result.retryable
    assert adapter.client.update_message.call_count == 1
    assert (await adapter.send("dm:42", "final answer")).success
    assert adapter.client.send_message.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("content", ["[[zulip_thread_action:", "[[zulip_topic: secret]]", "x" * 10001, " "])
async def test_edits_reject_controls_and_overflow(adapter, content):
    adapter._execute_thread_directives = AsyncMock()
    result = await adapter.edit_message("17", "123", content)
    assert not result.success
    adapter.client.update_message.assert_not_called()
    adapter._execute_thread_directives.assert_not_called()


@pytest.mark.asyncio
async def test_prefix_budget(adapter, monkeypatch):
    monkeypatch.setenv("ZULIP_TEXT_CHUNK_LIMIT", "100")
    adapter._response_prefix = "Bot: "
    assert adapter.MAX_MESSAGE_LENGTH == 95
    assert (await adapter.edit_message("17", "123", "x" * 95)).success
    assert len(adapter.client.update_message.call_args.args[0]["content"]) == 100


@pytest.mark.asyncio
async def test_interruption_freezes_bubble(adapter):
    adapter._sdk_call = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await adapter.edit_message("17", "123", "activity")
    assert not (await adapter.edit_message("17", "123", "again")).success
    assert adapter._sdk_call.call_count == 1


