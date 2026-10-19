"""Pin the registration used by the real E2E subscriber."""
import json
from unittest.mock import Mock
from scripts.zulip_transport_probe import register_transport_observer


def test_stream_typing_observer_requests_required_capability():
    api = Mock(return_value={"queue_id": "observer", "last_event_id": -1})
    assert register_transport_observer(api)["queue_id"] == "observer"
    args, kwargs = api.call_args
    assert args == ("POST", "register")
    assert "typing" in json.loads(kwargs["event_types"])
    assert json.loads(kwargs["client_capabilities"]) == {"stream_typing_notifications": True}
