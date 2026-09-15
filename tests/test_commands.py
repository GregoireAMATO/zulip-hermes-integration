"""Tests for admin command framework."""

import pytest
from unittest.mock import MagicMock, patch

from zulip.commands import (
    register_command,
    handle_command,
    is_command,
    _extract_command,
    _COMMANDS,
)


@pytest.fixture(autouse=True)
def native_recognition(monkeypatch):
    """Unit-test the ownership contract; engine integration tests use real registry."""
    import sys
    from types import ModuleType
    native = ModuleType("hermes_cli.commands")
    native.resolve_command = lambda name: object() if name in {
        "help", "commands", "status", "model", "verbose", "reset"
    } else None
    native.is_gateway_known_command = lambda name: name == "native_plugin"
    monkeypatch.setitem(sys.modules, "hermes_cli.commands", native)


class TestCommandParsing:
    def test_extract_command_basic(self):
        assert _extract_command("/help") == ("help", "")

    def test_extract_command_with_args(self):
        assert _extract_command("/model gpt4") == ("model", "gpt4")

    def test_extract_command_with_spaces(self):
        assert _extract_command("/status all") == ("status", "all")

    def test_extract_command_case_insensitive(self):
        assert _extract_command("/HELP") == ("help", "")

    def test_extract_not_command(self):
        assert _extract_command("hello world") is None

    def test_extract_not_command_slash_in_middle(self):
        assert _extract_command("path/to/file") is None

    def test_is_command_true(self):
        assert is_command("/help") is True

    def test_is_command_false(self):
        assert is_command("hello") is False


class TestCommandRegistration:
    def test_register_and_run(self):
        # Clear any existing test command
        if "testcmd" in _COMMANDS:
            del _COMMANDS["testcmd"]

        @register_command("testcmd")
        def _handler(args, chat_id, sender_email, sender_name):
            return f"ok {args}"

        result = handle_command("/testcmd hello", "dm:1", "a@x.com", "Alice")
        assert result.handled is True
        assert result.reply == "ok hello"

        # Cleanup
        del _COMMANDS["testcmd"]

    def test_register_as_function_call(self):
        if "directcmd" in _COMMANDS:
            del _COMMANDS["directcmd"]

        def _handler(args, chat_id, sender_email, sender_name):
            return "direct"

        register_command("directcmd", _handler)
        result = handle_command("/directcmd", "dm:1", "a@x.com", "Alice")
        assert result.reply == "direct"

        del _COMMANDS["directcmd"]

    def test_unknown_command_falls_through(self):
        result = handle_command("/nonexistent xyz", "dm:1", "a@x.com", "Alice")
        assert result.handled is False
        assert result.reply == ""


class TestBuiltInCommands:
    @pytest.mark.parametrize("content", ["/help", "/HELP", "/commands 2", "/status", "/model", "/model test", "/verbose", "/reset"])
    def test_native_commands_fall_through(self, content):
        assert not handle_command(content, "dm:1", "a@x.com", "Alice").handled

    @pytest.mark.parametrize("content", ["/", "/  ", "  /\t\n"])
    def test_bare_slash_gives_help(self, content):
        assert _extract_command(content) == ("", "")
        result = handle_command(content, "dm:1", "a@x.com", "Alice")
        assert result.handled and "/help" in result.reply

    @pytest.mark.parametrize("content", ["", "  ", "\n\t"])
    def test_whitespace(self, content):
        assert not handle_command(content, "dm:1", "a@x.com", "Alice").handled

    def test_native_collision_never_runs_local_handler(self, monkeypatch):
        handler = MagicMock(return_value="wrong")
        monkeypatch.setitem(_COMMANDS, "verbose", handler)
        assert not handle_command("/verbose", "dm:1", "a@x.com", "Alice").handled
        handler.assert_not_called()

    @pytest.mark.parametrize("name", ["streams", "user", "pin", "unpin"])
    def test_local_commands_remain_guidance(self, name):
        result = handle_command("/" + name + " 42", "dm:1", "a@x.com", "Alice")
        assert result.handled and "AI agent" in result.reply


class TestCommandErrorHandling:
    def test_handler_exception_returns_error(self):
        if "badcmd" in _COMMANDS:
            del _COMMANDS["badcmd"]

        @register_command("badcmd")
        def _bad(args, chat_id, sender_email, sender_name):
            raise RuntimeError("boom")

        result = handle_command("/badcmd", "dm:1", "a@x.com", "Alice")
        assert result.handled is True
        assert "Error processing /badcmd" in result.reply

        del _COMMANDS["badcmd"]


class TestAdapterIntegration:
    """Test that adapter properly intercepts commands."""

    @pytest.mark.asyncio
    async def test_command_reaches_gateway(self, mock_platform_config, monkeypatch):
        import zulip.adapter as adapter_module
        from zulip.adapter import ZulipAdapter
        from tests.conftest import MockZulipClient

        monkeypatch.setenv("ZULIP_SITE", "https://test.zulipchat.com")
        monkeypatch.setenv("ZULIP_EMAIL", "bot@test.com")
        monkeypatch.setenv("ZULIP_API_KEY", "key")
        monkeypatch.setattr(adapter_module, "ZULIP_AVAILABLE", True)

        class MockZulipModule:
            class Client:
                def __init__(self, **kwargs):
                    self._client = MockZulipClient(**kwargs)
                def __getattr__(self, name):
                    return getattr(self._client, name)

        monkeypatch.setattr(adapter_module, "zulip", MockZulipModule())

        adapter = ZulipAdapter(mock_platform_config)

        message = {
            "id": 123,
            "type": "private",
            "sender_id": 42,
            "sender_email": "user@test.com",
            "sender_full_name": "User",
            "content": "/help",
        }

        from unittest.mock import AsyncMock
        adapter.handle_message = AsyncMock()
        await adapter._handle_message(message)
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.call_args.args[0]
        assert event.text == "/help"
        assert event.source.user_id == "user@test.com"
        assert event.source.chat_id == "dm:42"

    @pytest.mark.asyncio
    async def test_non_command_goes_to_ai(self, mock_platform_config, monkeypatch):
        import zulip.adapter as adapter_module
        from zulip.adapter import ZulipAdapter
        from tests.conftest import MockZulipClient

        monkeypatch.setenv("ZULIP_SITE", "https://test.zulipchat.com")
        monkeypatch.setenv("ZULIP_EMAIL", "bot@test.com")
        monkeypatch.setenv("ZULIP_API_KEY", "key")
        monkeypatch.setattr(adapter_module, "ZULIP_AVAILABLE", True)

        class MockZulipModule:
            class Client:
                def __init__(self, **kwargs):
                    self._client = MockZulipClient(**kwargs)
                def __getattr__(self, name):
                    return getattr(self._client, name)

        monkeypatch.setattr(adapter_module, "zulip", MockZulipModule())

        adapter = ZulipAdapter(mock_platform_config)

        # Track whether handle_message (AI dispatch) is called
        ai_called = False

        async def mock_handle(event):
            nonlocal ai_called
            ai_called = True

        adapter.handle_message = mock_handle

        message = {
            "id": 123,
            "type": "private",
            "sender_id": 42,
            "sender_email": "user@test.com",
            "sender_full_name": "User",
            "content": "Hello bot, how are you?",
        }

        await adapter._handle_message(message)
        assert ai_called is True


@pytest.mark.asyncio
@pytest.mark.parametrize("content", ["/help", "/status", "/model test", "/streams", "/", "@thread tree"])
async def test_dm_policy_precedes_all_commands(content, mock_platform_config, monkeypatch):
    from unittest.mock import AsyncMock
    import zulip.adapter as module
    from tests.conftest import MockZulipClient
    monkeypatch.setattr(module, "ZULIP_AVAILABLE", True)
    monkeypatch.setattr(module, "zulip", MagicMock(Client=MockZulipClient))
    adapter = module.ZulipAdapter(mock_platform_config)
    adapter._policy = MagicMock(mode="disabled")
    adapter._policy.check_dm.return_value = (False, None)
    adapter.handle_message = AsyncMock()
    adapter._handle_thread_command = AsyncMock()
    await adapter._handle_message({
        "id": 124, "type": "private", "sender_id": 42,
        "sender_email": "user@test.com", "sender_full_name": "User",
        "content": content,
    })
    adapter.handle_message.assert_not_awaited()
    adapter._handle_thread_command.assert_not_awaited()
    adapter._policy.check_dm.assert_called_once_with("user@test.com")
    assert "disabled" in adapter.client._sent_messages[-1]["content"]


def test_unavailable_registry_defers_even_local_collision(monkeypatch):
    import sys
    monkeypatch.setitem(sys.modules, "hermes_cli.commands", None)
    handler = MagicMock()
    monkeypatch.setitem(_COMMANDS, "streams", handler)
    assert not handle_command("/streams", "dm:1", "a@x.com", "Alice").handled
    handler.assert_not_called()


def test_native_plugin_collision(monkeypatch):
    handler = MagicMock()
    monkeypatch.setitem(_COMMANDS, "native_plugin", handler)
    assert not handle_command("/native_plugin", "dm:1", "a@x.com", "Alice").handled
    handler.assert_not_called()
