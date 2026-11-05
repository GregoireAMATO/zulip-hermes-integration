"""Command completion: the Telegram command menu for clients without one.

Zulip's mobile app offers no ``/`` autocomplete, so the plugin completes the
*sent* message instead -- and only where the gateway would otherwise refuse it.
The rule that matters most is the negative one: a name the host knows is never
rewritten (#190).
"""

from __future__ import annotations

import pytest

from zulip import commands
from zulip.commands import CommandCatalog, CommandEntry, complete_command

from tests.test_engagement import _build, _dm, _stream_msg


def _catalog(*names: str, known: tuple[str, ...] = ()) -> CommandCatalog:
    entries = tuple(CommandEntry(n, f"does {n}") for n in names)
    return CommandCatalog(entries=entries, known=frozenset(set(names) | set(known)))


CAT = _catalog(
    "help", "new", "stop", "status", "model", "memory", "reasoning", "reset",
    "restart", "resume", "my-skill",
    known=("q", "ctx", "quit"),
)


class TestCompleteCommand:
    def test_bare_slash_gets_the_menu(self):
        c = complete_command("/", CAT)
        assert c.kind == "reply"
        assert "`/help`" in c.reply and "`/model`" in c.reply

    def test_bare_slash_with_spaces_gets_the_menu(self):
        assert complete_command("  /  ", CAT).kind == "reply"

    def test_menu_is_capped(self, monkeypatch):
        monkeypatch.setattr(commands, "COMPLETION_MENU_SIZE", 3)
        reply = complete_command("/", CAT).reply
        assert reply.count("- `/") == 3
        assert f"{len(CAT.entries) - 3} autres" in reply

    @pytest.mark.parametrize("text", ["/model", "/MODEL gpt", "/q hello", "/ctx", "/my_skill"])
    def test_known_names_pass_untouched(self, text):
        assert complete_command(text, CAT) is None

    def test_unique_prefix_is_rewritten_with_its_arguments(self):
        c = complete_command("/mod gpt-5 --global", CAT)
        assert c.kind == "rewrite"
        assert c.content == "/model gpt-5 --global"

    def test_unique_prefix_without_arguments(self):
        assert complete_command("/reas", CAT).content == "/reasoning"

    def test_underscore_prefix_matches_hyphen_names(self):
        assert complete_command("/my_s", CAT).content == "/my-skill"

    def test_ambiguous_prefix_lists_candidates_and_dispatches_nothing(self):
        c = complete_command("/re", CAT)
        assert c.kind == "reply"
        for name in ("reasoning", "reset", "restart", "resume"):
            assert f"`/{name}`" in c.reply
        assert "`/model`" not in c.reply

    def test_near_miss_suggests(self):
        c = complete_command("/modle", CAT)
        assert c.kind == "reply"
        assert "`/model`" in c.reply

    def test_unrelated_unknown_falls_through_to_the_gateway(self):
        assert complete_command("/xyzzy", CAT) is None

    @pytest.mark.parametrize(
        "text", ["/etc/hosts est plein", "/ bonjour", "hello /mod", "", "/1abc", "//mod"]
    )
    def test_non_commands_pass(self, text):
        assert complete_command(text, CAT) is None

    def test_no_catalog_means_no_completion(self):
        assert complete_command("/", None) is None
        assert complete_command("/mod", None) is None


_HOST_CHECK = r"""
import sys
try:
    import hermes_cli.commands as host
except Exception:
    sys.exit(77)
from zulip import commands
catalog = commands.build_command_catalog()
if catalog is None:
    sys.exit(78)
names = {e.name for e in catalog.entries}
assert {"help", "model", "reasoning"} <= names, names
for name in host.GATEWAY_KNOWN_COMMANDS:
    assert commands.complete_command("/" + name, catalog) is None, name
assert catalog.entries[0].name == "help", catalog.entries[0]
print("ok")
"""


class TestHostCatalog:
    def test_builds_from_the_installed_host_and_never_shadows_it(self, tmp_path):
        """Runs in a subprocess: reading the registry loads the host's plugins."""
        import os
        import subprocess
        import sys

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        env = dict(os.environ, HERMES_HOME=str(tmp_path))
        proc = subprocess.run(
            [sys.executable, "-c", _HOST_CHECK],
            cwd=root, env=env, capture_output=True, text=True, timeout=120,
        )
        if proc.returncode == 77:
            pytest.skip("no Hermes host importable")
        if proc.returncode == 78:
            pytest.skip("host registry shape not supported")
        assert proc.returncode == 0, proc.stdout + proc.stderr


class TestInboundWiring:
    @pytest.fixture(autouse=True)
    def _fake_catalog(self, monkeypatch):
        monkeypatch.setattr(commands, "command_catalog", lambda: CAT)

    @pytest.mark.asyncio
    async def test_prefix_reaches_the_gateway_completed(self, mock_platform_config, monkeypatch):
        a = _build(mock_platform_config, monkeypatch)
        await a._handle_message(_dm("/mod gpt-5"))
        assert a.handle_message.call_count == 1
        assert a.handle_message.call_args[0][0].text == "/model gpt-5"

    @pytest.mark.asyncio
    async def test_bare_slash_is_answered_not_dispatched(self, mock_platform_config, monkeypatch):
        a = _build(mock_platform_config, monkeypatch)
        await a._handle_message(_dm("/"))
        assert a.handle_message.call_count == 0
        sent = [c.args[0]["content"] for c in a.client.send_message.call_args_list]
        assert any("`/help`" in s for s in sent)

    @pytest.mark.asyncio
    async def test_ambiguous_prefix_in_a_gated_stream_is_answered(
        self, mock_platform_config, monkeypatch
    ):
        a = _build(
            mock_platform_config, monkeypatch,
            ZULIP_CHATMODE="oncall", ZULIP_REQUIRE_MENTION="true",
        )
        await a._handle_message(_stream_msg("/re"))
        assert a.handle_message.call_count == 0
        sent = [c.args[0] for c in a.client.send_message.call_args_list]
        assert sent and sent[-1]["type"] == "stream"
        assert "`/reset`" in sent[-1]["content"]

    @pytest.mark.asyncio
    async def test_known_command_is_dispatched_unchanged(self, mock_platform_config, monkeypatch):
        a = _build(mock_platform_config, monkeypatch)
        await a._handle_message(_dm("/stop"))
        assert a.handle_message.call_args[0][0].text == "/stop"
