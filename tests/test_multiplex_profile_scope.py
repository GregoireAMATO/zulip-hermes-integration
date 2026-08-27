"""Profile-scoped configuration for a multiplexed Hermes gateway."""

from types import SimpleNamespace

from agent.secret_scope import reset_secret_scope, set_multiplex_active, set_secret_scope


def test_adapter_uses_profile_scoped_credentials_and_safety_flags(monkeypatch):
    import zulip.adapter as adapter_module

    class Client:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    monkeypatch.setattr(
        adapter_module, "_import_zulip_sdk", lambda: SimpleNamespace(Client=Client)
    )
    adapter_module._client_cache.clear()

    for key in (
        "ZULIP_SITE",
        "ZULIP_EMAIL",
        "ZULIP_API_KEY",
        "ZULIP_REACTIONS_ENABLED",
        "ZULIP_STREAMS",
        "ZULIP_TOPIC_SESSIONS",
    ):
        monkeypatch.delenv(key, raising=False)

    set_multiplex_active(True)
    token = set_secret_scope(
        {
            "ZULIP_SITE": "https://example.zulipchat.com",
            "ZULIP_EMAIL": "kms-bot@example.invalid",
            "ZULIP_API_KEY": "profile-secret",
            "ZULIP_REACTIONS_ENABLED": "false",
            "ZULIP_STREAMS": "kms,kms-archives",
            "ZULIP_TOPIC_SESSIONS": "true",
        }
    )
    try:
        adapter = adapter_module.ZulipAdapter(SimpleNamespace(extra={}))
    finally:
        reset_secret_scope(token)
        set_multiplex_active(False)

    assert adapter.site == "https://example.zulipchat.com"
    assert adapter.email == "kms-bot@example.invalid"
    assert adapter.api_key == "profile-secret"
    assert adapter.api_token == adapter.api_key
    assert adapter._reaction_cfg.enabled is False
    assert adapter._streams_filter == {"kms", "kms-archives"}
