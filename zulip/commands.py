"""Admin command framework for Zulip Hermes integration.

Commands are parsed BEFORE messages reach the AI agent.
Unknown commands fall through to the AI.
"""

from __future__ import annotations

from typing import Callable
from dataclasses import dataclass

from .logger import mask_pii

logger = __import__("logging").getLogger(__name__)

CommandHandler = Callable[[str, str, str, str], str]

_COMMANDS: dict[str, CommandHandler] = {}


@dataclass
class CommandResult:
    """Result of command parsing."""

    handled: bool
    reply: str = ""


def register_command(name: str, handler: CommandHandler | None = None):
    """Register a command handler.

    Can be used as a decorator:
        @register_command("help")
        def _cmd_help(args, chat_id, sender_email, sender_name) -> str:
            return "Help text..."
    """

    def decorator(func: CommandHandler) -> CommandHandler:
        _COMMANDS[name] = func
        return func

    if handler is not None:
        _COMMANDS[name] = handler
        return handler
    return decorator


def _extract_command(content: str) -> tuple[str, str] | None:
    """Parse \"/command arg1 arg2\" from message text.

    Returns (cmd, args) or None if not a command.
    """
    stripped = content.strip()
    if stripped.startswith("/"):
        parts = stripped[1:].split(maxsplit=1)
        if not parts:
            return "", ""
        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""
        return cmd, args
    return None


def handle_command(
    content: str,
    chat_id: str,
    sender_email: str,
    sender_name: str,
    version: str = "unknown",
) -> CommandResult:
    """Try to parse and execute a command.

    Returns CommandResult(handled=True, reply=...) if a known command matched.
    Returns CommandResult(handled=False) to fall through to AI agent.
    """
    parsed = _extract_command(content)
    if not parsed:
        return CommandResult(handled=False)

    cmd, args = parsed
    if not cmd:
        return CommandResult(handled=True, reply="Use `/help` for Hermes help or `/commands 1` for its command catalogue.")

    # Recognition must not use the visible help menu: disabled commands and
    # aliases still belong to Hermes, whose normal dispatch applies its ACLs.
    try:
        from hermes_cli.commands import resolve_command, is_gateway_known_command
    except ImportError:
        # Without native recognition, do not risk executing a local collision.
        return CommandResult(handled=False)
    if resolve_command(cmd) is not None or is_gateway_known_command(cmd):
        return CommandResult(handled=False)

    handler = _COMMANDS.get(cmd)
    if handler is None:
        return CommandResult(handled=False)

    try:
        reply = handler(args, chat_id, sender_email, sender_name)
        return CommandResult(handled=True, reply=reply)
    except Exception as e:
        logger.warning("command error [cmd=%s sender=%s]: %s", cmd, mask_pii(sender_email), e)
        return CommandResult(handled=True, reply=f"❌ Error processing /{cmd}. Please try again later.")


def is_command(content: str) -> bool:
    """Check if content looks like a command (starts with /)."""
    return content.strip().startswith("/")


# ------------------------------------------------------------------
# Admin commands (stream CRUD, user info)
# ------------------------------------------------------------------

@register_command("streams")
def _cmd_streams(
    args: str, chat_id: str, sender_email: str, sender_name: str
) -> str:
    """List streams the bot can see.

    Usage: /streams [--all]
    """
    return (
        "Stream management is available through the AI agent.\n"
        "Ask the bot to list, create, or manage streams "
        "using natural language."
    )


@register_command("user")
def _cmd_user(
    args: str, chat_id: str, sender_email: str, sender_name: str
) -> str:
    """Get user information.

    Usage: /user <email_or_id>
    """
    if not args:
        return "Usage: `/user <email_or_id>`"
    return (
        "User information is available through the AI agent.\n"
        f"Ask the bot about user `{args.strip()}`."
    )


@register_command("pin")
def _cmd_pin(
    args: str, chat_id: str, sender_email: str, sender_name: str
) -> str:
    """Star/pin a message.

    Usage: /pin <message_id>
    """
    if not args:
        return "Usage: `/pin <message_id>`"
    return (
        "Message pinning is available through the AI agent.\n"
        f"Ask the bot to pin message `{args.strip()}`."
    )


@register_command("unpin")
def _cmd_unpin(
    args: str, chat_id: str, sender_email: str, sender_name: str
) -> str:
    """Unstar/unpin a message.

    Usage: /unpin <message_id>
    """
    if not args:
        return "Usage: `/unpin <message_id>`"
    return (
        "Message unpinning is available through the AI agent.\n"
        f"Ask the bot to unpin message `{args.strip()}`."
    )
