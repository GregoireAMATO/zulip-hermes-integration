"""Subscriber-side queue used by the live transport acceptance probe.

The gateway sends typing through set_typing_status; the observer must opt
into stream typing at registration, or Zulip silently withholds those events.
See https://zulip.com/api/register-queue (stream_typing_notifications).
"""
import json


def register_transport_observer(api):
    return api("POST", "register",
        event_types=json.dumps(["message", "reaction", "typing", "update_message"]),
        client_capabilities=json.dumps({"stream_typing_notifications": True}),
        apply_markdown="false")
