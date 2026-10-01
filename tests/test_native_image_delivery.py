"""Native Hermes image delivery must upload, then publish in the source topic."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def image_adapter(mock_platform_config, mock_zulip_client, monkeypatch, tmp_path):
    import zulip.adapter as module

    monkeypatch.setenv("HERMES_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(
        module,
        "_import_zulip_sdk",
        lambda: SimpleNamespace(Client=lambda **kwargs: mock_zulip_client),
    )
    mock_zulip_client.base_url = "https://test.zulipchat.com/api/"
    mock_zulip_client.upload_file = MagicMock(
        return_value={"result": "success", "uri": "/user_uploads/1/capture.png"}
    )
    adapter = module.ZulipAdapter(mock_platform_config)
    adapter._topic_cache["5"] = "another topic received during the turn"
    return adapter


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chat_id,metadata,caption",
    [
        ("5", {"thread_id": "Essential Energy"}, "Ouvrir son profil"),
        ("5", {"topic": "Essential Energy"}, None),
        ("dm:42", {}, "Profil UMS"),
    ],
)
async def test_native_image_upload_preserves_destination(
    image_adapter, tmp_path, chat_id, metadata, caption
):
    image = tmp_path / "capture.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\n")
    uploaded = []

    def upload(file):
        uploaded.append(file.read())
        return {"result": "success", "uri": "/user_uploads/1/capture.png"}

    image_adapter.client.upload_file.side_effect = upload
    result = await image_adapter.send_image_file(
        chat_id, str(image), caption=caption, metadata=metadata
    )

    assert result.success
    assert result.message_id == "1000"
    assert uploaded == [b"\x89PNG\r\n\x1a\n"]
    message = image_adapter.client._sent_messages[0]
    assert "https://test.zulipchat.com/user_uploads/1/capture.png" in message["content"]
    assert "Couldn't deliver" not in message["content"]
    assert str(image) not in message["content"]
    if caption:
        assert caption in message["content"]
    if chat_id == "5":
        assert message["type"] == "stream"
        assert message["to"] == 5
        assert message["topic"] == "Essential Energy"
    else:
        assert message["type"] == "private"
        assert message["to"] == [42]
    assert not image.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["rejected", "timeout", "missing"])
async def test_failed_native_upload_does_not_report_success(
    image_adapter, monkeypatch, tmp_path, failure
):
    image = tmp_path / "capture.png"
    if failure != "missing":
        image.write_bytes(b"png")
    if failure == "rejected":
        image_adapter.client.upload_file.return_value = {
            "result": "error", "msg": "upload refused"
        }
    elif failure == "timeout":
        monkeypatch.setattr(
            "zulip.adapter.upload_file_to_zulip",
            AsyncMock(side_effect=asyncio.TimeoutError),
        )

    result = await image_adapter.send_image_file(
        "5", str(image), metadata={"thread_id": "Essential Energy"}
    )

    assert not result.success
    assert result.error
    assert str(image) not in result.error
    assert image_adapter.client._sent_messages == []
    if failure != "missing":
        assert image.exists()


@pytest.mark.asyncio
async def test_native_image_send_failure_retains_file(image_adapter, tmp_path):
    image = tmp_path / "capture.png"
    image.write_bytes(b"png")
    image_adapter.client.send_message = MagicMock(
        return_value={"result": "error", "msg": "topic unavailable"}
    )

    result = await image_adapter.send_image_file(
        "5", str(image), metadata={"thread_id": "Essential Energy"}
    )

    assert not result.success
    assert result.error
    assert image.exists()
