"""Behavior tests for bot-managed hierarchical Zulip topics."""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

import pytest

from zulip.thread_hierarchy import (
    ThreadHierarchyError,
    ThreadHierarchyStore,
    build_child_topic,
    parse_thread_command,
    render_tree,
)


class TestThreadCommandParsing:
    def test_exact_command_is_case_insensitive(self):
        assert parse_thread_command(" @THREAD split data augmentation ").action == "split"
        assert (
            parse_thread_command(" @THREAD split data augmentation ").argument
            == "data augmentation"
        )

    def test_bare_command_opens_help(self):
        assert parse_thread_command("@thread").action == "help"

    def test_does_not_capture_normal_mentions(self):
        assert parse_thread_command("hello @thread split perf") is None

    def test_child_is_one_normalized_segment(self):
        assert build_child_topic("training", " data   augmentation ") == (
            "training / data augmentation"
        )
        with pytest.raises(ThreadHierarchyError, match="sans `/`"):
            build_child_topic("training", "data/perf")

    def test_topic_limit_applies_to_full_path(self):
        with pytest.raises(ThreadHierarchyError, match="limite Zulip"):
            build_child_topic("training", "augmentation", max_topic_length=10)


class TestThreadHierarchyStore:
    def test_persists_and_renders_nested_tree(self, tmp_path):
        path = tmp_path / "threads.sqlite3"
        store = ThreadHierarchyStore(path)

        assert store.split("5", "training", "loss").created is True
        assert store.split("5", "training", "data-augmentation").created is True
        assert store.split(
            "5", "training / data-augmentation", "perf"
        ).created is True
        assert store.split(
            "5", "training / data-augmentation", "bugs"
        ).created is True

        reopened = ThreadHierarchyStore(path)
        assert reopened.parent("5", "training / data-augmentation") == "training"
        assert reopened.root("5", "training / data-augmentation / perf") == "training"
        records = reopened.subtree("5", "training")
        assert render_tree("training", records) == (
            "training\n"
            "├── data-augmentation\n"
            "│   ├── bugs\n"
            "│   └── perf\n"
            "└── loss"
        )

    def test_split_is_idempotent_under_concurrency(self, tmp_path):
        store = ThreadHierarchyStore(tmp_path / "threads.sqlite3")

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    lambda _: store.split("5", "training", "infra"),
                    range(20),
                )
            )

        assert sum(result.created for result in results) == 1
        assert store.children("5", "training") == ["training / infra"]

    def test_same_topic_names_are_isolated_by_stream(self, tmp_path):
        store = ThreadHierarchyStore(tmp_path / "threads.sqlite3")
        store.split("5", "training", "infra")
        store.split("6", "training", "loss")

        assert store.children("5", "training") == ["training / infra"]
        assert store.children("6", "training") == ["training / loss"]


class TestThreadHierarchyAdapter:
    @pytest.fixture
    def adapter(self, mock_platform_config, monkeypatch, tmp_path):
        import zulip.adapter as adapter_module
        from tests.conftest import MockZulipClient

        monkeypatch.setattr(adapter_module, "ZULIP_AVAILABLE", True)
        monkeypatch.setenv("HERMES_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("ZULIP_CHATMODE", "oncall")
        monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "true")

        class MockZulipModule:
            class Client:
                def __init__(self, **kwargs):
                    self._client = MockZulipClient(**kwargs)

                def __getattr__(self, name):
                    return getattr(self._client, name)

        monkeypatch.setattr(adapter_module, "zulip", MockZulipModule())

        instance = adapter_module.ZulipAdapter(mock_platform_config)
        instance.email = "bot@test.zulipchat.com"
        instance._typing_delay = 0
        instance.handle_message = AsyncMock()
        return instance

    @staticmethod
    def stream_message(message_id: int, topic: str, content: str) -> dict:
        return {
            "id": message_id,
            "type": "stream",
            "stream_id": 5,
            "subject": topic,
            "display_recipient": "ml-platform",
            "content": content,
            "sender_email": "user@test.zulipchat.com",
            "sender_full_name": "User",
            "sender_id": 42,
        }

    def test_streaming_is_disabled_until_control_blocks_are_complete(self, adapter):
        assert adapter.SUPPORTS_MESSAGE_EDITING is False

    @pytest.mark.asyncio
    async def test_rollup_edits_existing_message(self, adapter):
        calls = []

        def update_message(request):
            calls.append(request)
            return {"result": "success"}

        adapter.client._client.update_message = update_message
        message_id = await adapter._upsert_thread_rollup_message(
            "5", "training", "nouvel état", message_id="123"
        )

        assert message_id == "123"
        assert calls == [{"message_id": 123, "content": "nouvel état"}]
        assert adapter.client._sent_messages == []

    @pytest.mark.asyncio
    async def test_rollup_recreates_deleted_message(self, adapter):
        adapter.client._client.update_message = lambda request: {
            "result": "error",
            "msg": "message not found",
        }

        message_id = await adapter._upsert_thread_rollup_message(
            "5", "training", "état recréé", message_id="123"
        )

        assert message_id == "1000"
        assert adapter.client._sent_messages[-1] == {
            "id": 1000,
            "type": "stream",
            "to": 5,
            "topic": "training",
            "content": "état recréé",
        }

    @pytest.mark.asyncio
    async def test_split_bypasses_oncall_and_seeds_child_context(self, adapter):
        await adapter._handle_message(
            self.stream_message(
                1,
                "training",
                "On devrait revoir le pipeline de data augmentation.",
            )
        )
        adapter.handle_message.assert_not_awaited()

        await adapter._handle_message(
            self.stream_message(2, "training", "@thread split data-augmentation")
        )

        sent = adapter.client._sent_messages
        assert [item["topic"] for item in sent] == [
            "training",
            "training / data-augmentation",
        ]
        assert "Sous-fil créé" in sent[0]["content"]
        assert "Parent : #**ml-platform>training**" in sent[1]["content"]
        assert "On devrait revoir le pipeline" in sent[1]["content"]
        adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_nested_split_and_tree(self, adapter):
        await adapter._handle_message(
            self.stream_message(1, "training", "@thread split data-augmentation")
        )
        await adapter._handle_message(
            self.stream_message(
                2,
                "training / data-augmentation",
                "@thread split perf",
            )
        )
        await adapter._handle_message(
            self.stream_message(
                3,
                "training / data-augmentation / perf",
                "@thread tree",
            )
        )

        assert adapter.client._sent_messages[-1]["content"] == (
            "```text\n"
            "⚪ training\n"
            "└── ⚪ data-augmentation\n"
            "    └── ⚪ perf\n"
            "```"
        )

    @pytest.mark.asyncio
    async def test_status_overview_and_sync_update_root_rollup(self, adapter):
        adapter._thread_config = adapter._thread_config.__class__(
            enabled=True,
            auto_split=adapter._thread_config.auto_split,
            max_depth=4,
            max_children_per_split=6,
            rollup_debounce_seconds=1,
        )
        await adapter._handle_message(
            self.stream_message(1, "training", "@thread split infra")
        )
        await adapter._handle_message(
            self.stream_message(
                2,
                "training / infra",
                "@thread status blocked accès GPU manquant",
            )
        )
        await adapter._handle_message(
            self.stream_message(3, "training / infra", "@thread overview")
        )
        overview = adapter.client._sent_messages[-1]["content"]
        assert "🔴" in overview
        assert "accès GPU manquant" in overview

        await adapter._handle_message(
            self.stream_message(4, "training / infra", "@thread sync")
        )
        rollups = [
            item
            for item in adapter.client._sent_messages
            if item["topic"] == "training" and "Avancement de" in item["content"]
        ]
        assert len(rollups) == 1
        assert "#**ml-platform>training / infra**" in rollups[0]["content"]
        assert "Rollup actualisé" in adapter.client._sent_messages[-1]["content"]

    @pytest.mark.asyncio
    async def test_navigation_commands_return_native_topic_links(self, adapter):
        await adapter._handle_message(
            self.stream_message(1, "training", "@thread split infra")
        )
        await adapter._handle_message(
            self.stream_message(2, "training", "@thread children")
        )
        assert "#**ml-platform>training / infra**" in (
            adapter.client._sent_messages[-1]["content"]
        )

        await adapter._handle_message(
            self.stream_message(3, "training / infra", "@thread parent")
        )
        assert adapter.client._sent_messages[-1]["content"] == (
            "↰ Parent : #**ml-platform>training**"
        )

        await adapter._handle_message(
            self.stream_message(4, "training / infra", "@thread root")
        )
        assert adapter.client._sent_messages[-1]["content"] == (
            "Racine : #**ml-platform>training**"
        )

    @pytest.mark.asyncio
    async def test_repeated_split_reuses_relation(self, adapter):
        command = self.stream_message(1, "training", "@thread split infra")
        await adapter._handle_message(command)
        command["id"] = 2
        await adapter._handle_message(command)

        assert "Sous-fil créé" in adapter.client._sent_messages[0]["content"]
        assert "Sous-fil existant" in adapter.client._sent_messages[2]["content"]

    @pytest.mark.asyncio
    async def test_split_recovers_context_from_zulip_after_restart(self, adapter):
        adapter._thread_context_cache.clear()
        adapter.client._client.get_messages = lambda request: {
            "result": "success",
            "messages": [
                {
                    "id": 8,
                    "sender_email": "user@test.zulipchat.com",
                    "content": "<p>Contexte conservé après redémarrage.</p>",
                },
                {
                    "id": 9,
                    "sender_email": "user@test.zulipchat.com",
                    "content": "<p>@thread split perf</p>",
                },
            ],
        }

        await adapter._handle_message(
            self.stream_message(9, "training", "@thread split perf")
        )

        assert "Contexte conservé après redémarrage." in (
            adapter.client._sent_messages[-1]["content"]
        )

    @pytest.mark.asyncio
    async def test_child_session_receives_seed_exactly_once(
        self, adapter, monkeypatch
    ):
        monkeypatch.setenv("ZULIP_TOPIC_SESSIONS", "true")
        monkeypatch.setenv("ZULIP_CHATMODE", "onmessage")
        adapter._thread_store.split(
            "5",
            "training",
            "infra",
            goal="Préparer le cluster GPU",
            seed_context="Le pipeline requiert quatre GPU.",
        )

        await adapter._handle_message(
            self.stream_message(10, "training / infra", "On commence.")
        )
        first = adapter.handle_message.await_args.args[0]
        assert first.source.thread_id == "training / infra"
        assert "Objectif du sous-fil : Préparer le cluster GPU" in first.text
        assert "Le pipeline requiert quatre GPU." in first.text
        assert first.metadata["thread_seeded"] is True

        adapter.handle_message.reset_mock()
        await adapter._handle_message(
            self.stream_message(11, "training / infra", "Deuxième message.")
        )
        second = adapter.handle_message.await_args.args[0]
        assert second.text == "Deuxième message."
        assert "thread_seeded" not in second.metadata

    @pytest.mark.asyncio
    async def test_dm_explains_stream_only_scope(self, adapter):
        await adapter._handle_message(
            {
                "id": 1,
                "type": "private",
                "sender_id": 42,
                "sender_email": "user@test.zulipchat.com",
                "sender_full_name": "User",
                "content": "@thread tree",
            }
        )

        assert "uniquement" in adapter.client._sent_messages[-1]["content"]
        adapter.handle_message.assert_not_awaited()
