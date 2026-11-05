"""Admin command framework for Zulip Hermes integration.

Commands are parsed BEFORE messages reach the AI agent.
Unknown commands fall through to the AI.

Gateway-native slash commands (``/help``, ``/status``, ``/model``, ``/stop``,
``/new``, ...) are deliberately **not** registered here: the Hermes gateway
implements them natively, so registering them in the plugin would shadow the
gateway and swallow the command (issue #190). The set is recorded once, as
``GATEWAY_NATIVE_COMMANDS`` -- complete, with its host source -- and
``scripts/check_compat.py`` compares it against the host's own registry (#251).
Only plugin-specific commands are registered below; everything else falls
through to the gateway.
"""

from __future__ import annotations

import re
from typing import Any, Callable
from dataclasses import dataclass

from .logger import mask_pii

logger = __import__("logging").getLogger(__name__)


# The host's own exec-approval decision commands. Recognised (not consumed) so
# the plugin can record *who* decided an approval (issue #222): the gateway
# resolves the approval from this message and its ``post_approval_response``
# hook carries no human identity on the gateway surface. ``/approvals`` — the
# command that shows or sets the approval *mode* — must not match, so the
# pattern requires a word boundary after the verb.
_APPROVAL_DECISION_RE = re.compile(r"^/(?:approve|deny)\b", re.IGNORECASE)


def is_approval_decision(content: str) -> bool:
    """Whether ``content`` is the host's ``/approve`` / ``/deny`` command.

    Covers the modifiers the host accepts (``/approve session``, ``/approve
    always``, ``/approve all``, ``/deny all``). The message is never consumed or
    answered here: it still reaches the gateway, which owns slash authorization
    and the decision itself.
    """
    text = (content or "").strip()
    if not text:
        return False
    return bool(_APPROVAL_DECISION_RE.match(text))

CommandHandler = Callable[[str, str, str, str], str]

_COMMANDS: dict[str, CommandHandler] = {}


@dataclass
class CommandResult:
    """Result of command parsing."""

    handled: bool
    reply: str = ""


# ------------------------------------------------------------------
# The gateway-owned command namespace
#
# A plugin command with one of these names SHADOWS the gateway's own command:
# the plugin's router answers it and the host never sees it (#43 ``/version``,
# #190/#191 ``/help``/``/status``/``/model``). The set used to live here as three
# prose copies, one ending in a literal "...", so it was hand-maintained,
# duplicated and incomplete by admission (#251) -- the ``PLUGIN_FILES`` drift
# mechanism (CONTRIBUTING row 1) applied to command names.
#
# It is recorded here so a host that does not ship its registry can still be
# checked, and ``scripts/check_compat.py`` prefers the host's own
# ``hermes_cli.commands.GATEWAY_KNOWN_COMMANDS`` whenever it is importable --
# derivation first, this constant as the floor.
_GATEWAY_NATIVE_SOURCE = (
    "hermes_cli.commands.GATEWAY_KNOWN_COMMANDS at host v2026.9.24; the same "
    "127 names are present at v2026.9.14 (the declared __min_hermes__ floor). "
    "Names and aliases read from COMMAND_REGISTRY, so this is a superset of the "
    "names the gateway dispatches (host-cli-only commands included): a superset "
    "can only produce a needless refusal, never a missed collision."
)

GATEWAY_NATIVE_COMMANDS: frozenset[str] = frozenset({
    "agents", "approvals", "approve", "battery", "bg", "blueprint", "bp", "branch",
    "browser", "btw", "bundles", "busy", "clear", "codex-runtime", "codex_runtime",
    "commands", "compact", "compose", "compress", "config", "context", "copy", "cron",
    "ctx", "curator", "debug", "deny", "diff", "egress", "exit", "export", "fast",
    "focus", "footer", "fork", "gateway", "generate-pet", "goal", "handoff", "hatch",
    "hb", "heartbeat", "help", "history", "image", "import", "indicator", "init",
    "insights", "journey", "kanban", "learn", "learning", "login", "loop", "memory",
    "memory-graph", "moa", "model", "new", "palette", "paste", "pause", "personality",
    "pet", "plan", "platform", "platforms", "plugins", "proactive", "profile", "prompt",
    "q", "queue", "quit", "reasoning", "redraw", "refine", "reload", "reload-mcp",
    "reload-skills", "reload_mcp", "reload_skills", "reset", "restart", "resume",
    "retry", "review", "rollback", "save", "sb", "sessions", "set-home", "sethome",
    "skills", "skin", "snap", "snapshot", "start", "status", "statusbar", "steer",
    "stop", "subgoal", "subscription", "suggest", "suggestions", "tasks", "timestamps",
    "title", "tools", "toolsets", "topic", "topup", "ts", "undo", "update", "upgrade",
    "usage", "v", "verbose", "version", "voice", "wake", "whoami", "worktree", "yolo",
})

# The provenance above, exposed so a test can assert the set says where it came
# from instead of asking the reader to trust it.
GATEWAY_NATIVE_SOURCE = _GATEWAY_NATIVE_SOURCE


def command_collisions(plugin_names: Any, gateway_names: Any) -> list[str]:
    def _norm(name) -> str:
        return str(name).strip().lower().lstrip("/")

    plugin = {_norm(n) for n in plugin_names or ()} - {""}
    gateway = {_norm(n) for n in gateway_names or ()} - {""}
    return sorted(plugin & gateway)


def plugin_command_names() -> frozenset[str]:
    """The names this plugin answers itself.

    The comparison side of :data:`GATEWAY_NATIVE_COMMANDS`; ``check_compat``
    reads it so a future registration colliding with a host command fails the
    gate rather than a user's slash command.
    """
    return frozenset(_COMMANDS)


def register_command(name: str, handler: CommandHandler | None = None):
    """Register a command handler.

    Can be used as a decorator:
        @register_command("ping")
        def _cmd_ping(args, chat_id, sender_email, sender_name) -> str:
            return "🏓 Pong!"

    Do not register a name in ``GATEWAY_NATIVE_COMMANDS``; that shadows the
    gateway command (issue #190), and ``check_compat`` fails the build when the
    installed host owns it (#251).
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
    handler = _COMMANDS.get(cmd)
    if handler is None:
        return CommandResult(handled=False)

    # One line, so a plugin command is visible at all. A command is answered before
    # the gateway dispatches, so it produces none of the ``inbound message`` /
    # ``response ready`` lines a turn does -- which made a working /streams reply
    # indistinguishable from a message that had never arrived. The name and a masked
    # sender only: arguments can carry an email address or a key, and this file is
    # durable.
    logger.info("zulip command [name=%s sender=%s]", cmd, mask_pii(sender_email))

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
# Plugin-specific commands
#
# Nothing here may share a name with a gateway-native command. The complete
# set -- derived from the host, not remembered -- is GATEWAY_NATIVE_COMMANDS;
# those must reach the gateway, not be absorbed by this router (issue #190).
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


# ------------------------------------------------------------------
# Command completion (the Telegram command menu, for clients without one)
#
# Telegram shows the bot's commands while ``/`` is being typed. Zulip's web
# client can be taught the same (a server-side typeahead patch), but the mobile
# app only autocompletes ``@``, ``#`` and ``:`` and offers no hook at all. So the
# completion happens on the *sent* message instead, and only where the gateway
# would otherwise refuse it:
#
# * ``/`` alone            -> the command menu (Telegram's priority order);
# * a unique prefix        -> rewritten to the full command, then dispatched
#                             (``/mod gpt-5`` runs ``/model gpt-5``);
# * an ambiguous prefix    -> the matching commands, nothing dispatched;
# * a near miss (``/modle``)-> "did you mean", nothing dispatched.
#
# A name the host already knows (built-in, alias, plugin command or skill) is
# never touched, and anything that does not look like a command name (a path
# such as ``/etc/hosts``) passes through. If the host registry cannot be read,
# completion is off and every message passes through unchanged: shadowing a
# gateway command is the failure this file exists to prevent (#190).
# ------------------------------------------------------------------

import difflib
import time as _time

# ``/`` then an optional command-shaped word, then whitespace or the end.
_COMPLETION_TOKEN_RE = re.compile(r"^/([A-Za-z][A-Za-z0-9_-]*)?(?=\s|$)")

# Commands the plugin answers itself (outside the ``_COMMANDS`` registry).
_PLUGIN_EXTRA_COMMANDS: dict[str, str] = {
    "pick-model": "Pick a model from a list of buttons",
    "continue": "Re-bind this topic to an earlier session",
    "topic-sessions": "List this topic's sessions",
    "unlisten": "Stop following this topic without a mention",
}

# Known to the host (so never completed into something else) but not worth a
# menu line: ``/start`` only acknowledges Telegram's start ping, and listing it
# makes ``/sta`` ambiguous with ``/status``.
_MENU_HIDDEN: frozenset[str] = frozenset({"start"})

COMPLETION_MENU_SIZE = 25
COMPLETION_LIST_SIZE = 20
_DESC_LIMIT = 70
_CATALOG_TTL_SECONDS = 60.0


@dataclass(frozen=True)
class CommandEntry:
    """One command a user may type, as shown in a menu."""

    name: str
    description: str = ""


@dataclass(frozen=True)
class CommandCatalog:
    """What the host dispatches, read from its own registry.

    ``entries`` are the canonical, menu-worthy names in menu order (the
    Telegram priority first). ``known`` holds every name the host or the plugin
    answers -- aliases, CLI-only names and skills included -- normalised with
    :func:`_norm_command`; a typed name in it is never completed.
    """

    entries: tuple[CommandEntry, ...]
    known: frozenset[str]


@dataclass(frozen=True)
class Completion:
    """``kind`` is ``"rewrite"`` (dispatch ``content``) or ``"reply"`` (answer
    ``reply`` and dispatch nothing)."""

    kind: str
    content: str = ""
    reply: str = ""


def _norm_command(name: Any) -> str:
    return str(name).strip().lower().lstrip("/").replace("_", "-")


def _short(text: Any) -> str:
    text = " ".join(str(text or "").split())
    if len(text) > _DESC_LIMIT:
        text = text[: _DESC_LIMIT - 1].rstrip() + "…"
    return text


def build_command_catalog(platform: str = "zulip") -> CommandCatalog | None:
    """Read the host's command registry; ``None`` when it cannot be read.

    Uses the host helpers the Telegram menu is built from, so the two menus
    agree. Every helper but ``COMMAND_REGISTRY`` and ``GATEWAY_KNOWN_COMMANDS``
    is private to the host, hence the defensive import: a host that renames
    one turns completion off, never the bot.
    """
    try:
        from hermes_cli import commands as host  # type: ignore[import-not-found]

        registry = list(host.COMMAND_REGISTRY)
        known: set[str] = {_norm_command(n) for n in host.GATEWAY_KNOWN_COMMANDS}
        overrides = host._resolve_config_gates()
        available = {
            c.name: c for c in registry if host._is_gateway_available(c, overrides)
        }
        for cmd in registry:
            known.add(_norm_command(cmd.name))
            known.update(_norm_command(a) for a in (cmd.aliases or ()))
        # Plugin slash commands and skills, every one of them (no cap), so a
        # typed skill name is recognised rather than "completed" into another.
        extra_entries, _hidden = host._collect_gateway_skill_entries(
            platform=platform,
            max_slots=None,
            reserved_names=set(available),
            desc_limit=_DESC_LIMIT,
            sanitize_name=lambda raw: raw,
        )
        try:
            priority = [_norm_command(n) for n in host._telegram_effective_priority()]
        except Exception:
            priority = [_norm_command(n) for n in getattr(host, "_TELEGRAM_MENU_PRIORITY", ())]
    except Exception as exc:  # pragma: no cover - depends on the installed host
        logger.info("zulip command completion off: host registry unreadable (%s)", exc)
        return None

    by_name: dict[str, CommandEntry] = {}
    for name, cmd in available.items():
        by_name[_norm_command(name)] = CommandEntry(
            _norm_command(name), _short(getattr(cmd, "description", ""))
        )
    for name, desc, _cmd_key, _raw in extra_entries:
        key = _norm_command(name)
        known.add(key)
        by_name.setdefault(key, CommandEntry(key, _short(desc)))
    for name, desc in _PLUGIN_EXTRA_COMMANDS.items():
        known.add(name)
        by_name.setdefault(name, CommandEntry(name, desc))
    for name in _COMMANDS:
        key = _norm_command(name)
        known.add(key)
        by_name.setdefault(key, CommandEntry(key, ""))

    for name in _MENU_HIDDEN:
        by_name.pop(name, None)

    rank = {name: i for i, name in enumerate(priority)}
    ordered = sorted(
        by_name.values(), key=lambda e: (rank.get(e.name, len(rank)), e.name)
    )
    return CommandCatalog(entries=tuple(ordered), known=frozenset(known))


_catalog_cache: tuple[float, CommandCatalog | None] | None = None


def command_catalog() -> CommandCatalog | None:
    """:func:`build_command_catalog`, cached briefly (skills are read from disk)."""
    global _catalog_cache
    now = _time.monotonic()
    if _catalog_cache is None or now - _catalog_cache[0] > _CATALOG_TTL_SECONDS:
        _catalog_cache = (now, build_command_catalog())
    return _catalog_cache[1]


def _entry_line(entry: CommandEntry) -> str:
    if entry.description:
        return f"- `/{entry.name}` — {entry.description}"
    return f"- `/{entry.name}`"


def command_menu(catalog: CommandCatalog) -> str:
    """The answer to a bare ``/``."""
    shown = catalog.entries[:COMPLETION_MENU_SIZE]
    lines = ["**Commandes**"]
    lines += [_entry_line(e) for e in shown]
    rest = len(catalog.entries) - len(shown)
    tail = (
        "Tape le début d'une commande pour la lancer ou filtrer : `/mod` lance "
        "`/model`, `/re` liste celles qui commencent par « re »."
    )
    if rest > 0:
        tail = f"… et {rest} autres. " + tail
    lines += ["", tail]
    return "\n".join(lines)


def complete_command(content: str, catalog: CommandCatalog | None) -> Completion | None:
    """Complete a typed command, or ``None`` to let the message pass unchanged."""
    if catalog is None or not catalog.entries:
        return None
    text = (content or "").strip()
    match = _COMPLETION_TOKEN_RE.match(text)
    if not match:
        return None
    token = match.group(1)
    rest = text[match.end():]
    if token is None:
        return Completion("reply", reply=command_menu(catalog)) if not rest.strip() else None

    typed = _norm_command(token)
    if typed in catalog.known:
        return None

    candidates = [e for e in catalog.entries if e.name.startswith(typed)]
    if len(candidates) == 1:
        return Completion("rewrite", content=f"/{candidates[0].name}{rest}")
    if candidates:
        shown = candidates[:COMPLETION_LIST_SIZE]
        lines = [f"Plusieurs commandes commencent par `/{token}` :"]
        lines += [_entry_line(e) for e in shown]
        if len(candidates) > len(shown):
            lines.append(f"… et {len(candidates) - len(shown)} autres.")
        lines += ["", "Renvoie la commande voulue (le début suffit s'il n'y en a qu'une)."]
        return Completion("reply", reply="\n".join(lines))

    names = [e.name for e in catalog.entries]
    close = difflib.get_close_matches(typed, names, n=5, cutoff=0.6)
    if not close:
        return None  # the gateway answers its own "unknown command"
    by_name = {e.name: e for e in catalog.entries}
    lines = [f"Commande inconnue `/{token}`. Tu voulais dire :"]
    lines += [_entry_line(by_name[n]) for n in close]
    lines += ["", "Tape `/` seul pour voir le menu."]
    return Completion("reply", reply="\n".join(lines))
