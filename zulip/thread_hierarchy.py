"""Virtual topic hierarchy for Zulip streams.

Zulip topics remain flat.  This module stores only parent/child relationships
and renders them as a tree for the ``@thread`` adapter commands.
"""

from __future__ import annotations

import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

TOPIC_SEPARATOR = " / "
DEFAULT_MAX_TOPIC_LENGTH = 60
MAX_TREE_NODES = 500
SCHEMA_VERSION = 4
MAX_GOAL_LENGTH = 2_000
MAX_SEED_LENGTH = 16_000
MAX_STATUS_SUMMARY_LENGTH = 4_000
MAX_DIGEST_LENGTH = 64_000
MAX_SOURCE_MESSAGE_ID_LENGTH = 200


class ThreadHierarchyError(ValueError):
    """Raised when a virtual-thread operation is invalid."""


@dataclass(frozen=True)
class ThreadCommand:
    action: str
    argument: str = ""


@dataclass(frozen=True)
class SplitResult:
    topic_name: str
    created: bool
    duplicate: bool = False


@dataclass(frozen=True)
class ThreadRecord:
    topic_name: str
    parent_topic_name: str | None
    created_at: int


class ThreadStatus(str, Enum):
    """Lifecycle states supported by virtual threads."""

    TODO = "todo"
    IN_PROGRESS = "in_progress"
    BLOCKED = "blocked"
    DONE = "done"


@dataclass(frozen=True)
class ThreadDetails:
    topic_name: str
    parent_topic_name: str | None
    created_at: int
    goal: str | None
    seed_context: str | None
    seed_consumed_at: int | None
    source_message_id: str | None


@dataclass(frozen=True)
class ThreadSeed:
    topic_name: str
    goal: str | None
    context: str


@dataclass(frozen=True)
class ThreadStatusRecord:
    topic_name: str
    status: ThreadStatus
    summary: str
    source_message_id: str | None
    updated_at: int


@dataclass(frozen=True)
class StatusUpdateResult:
    record: ThreadStatusRecord
    changed: bool


@dataclass(frozen=True)
class ThreadOverviewRecord:
    thread: ThreadRecord
    goal: str | None
    status: ThreadStatus
    summary: str
    status_updated_at: int | None


@dataclass(frozen=True)
class RollupRecord:
    root_topic_name: str
    message_id: str | None
    digest: str
    updated_at: int


_THREAD_COMMAND_RE = re.compile(
    r"^\s*@thread(?:\s+(?P<action>\S+))?(?:\s+(?P<argument>.*?))?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def parse_thread_command(content: str) -> ThreadCommand | None:
    """Parse an exact ``@thread ...`` command, or return ``None``."""
    match = _THREAD_COMMAND_RE.fullmatch(content)
    if match is None:
        return None
    return ThreadCommand(
        action=(match.group("action") or "help").casefold(),
        argument=(match.group("argument") or "").strip(),
    )


def build_child_topic(
    parent_topic: str,
    child_segment: str,
    *,
    max_topic_length: int = DEFAULT_MAX_TOPIC_LENGTH,
) -> str:
    """Validate one child segment and return its full Zulip topic name."""
    parent = parent_topic.strip()
    segment = " ".join(child_segment.split())
    if not parent:
        raise ThreadHierarchyError("Le topic courant n'a pas de nom.")
    if not segment:
        raise ThreadHierarchyError("Usage : `@thread split <nom>`.")
    if "/" in segment:
        raise ThreadHierarchyError("Donne un seul niveau à la fois (sans `/`).")
    if any(ord(char) < 32 or ord(char) == 127 for char in segment):
        raise ThreadHierarchyError("Le nom contient un caractère non autorisé.")

    topic_name = f"{parent}{TOPIC_SEPARATOR}{segment}"
    if len(topic_name) > max_topic_length:
        raise ThreadHierarchyError(
            f"Le topic ferait {len(topic_name)} caractères, au-delà de la limite "
            f"Zulip ({max_topic_length})."
        )
    return topic_name


class ThreadHierarchyStore:
    """Small SQLite store scoped to one Zulip bot account."""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser()
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    @staticmethod
    def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
        return {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }

    def _migrate_v1(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS threads (
                channel_id TEXT NOT NULL,
                topic_name TEXT NOT NULL,
                parent_topic_name TEXT,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (channel_id, topic_name),
                FOREIGN KEY (channel_id, parent_topic_name)
                    REFERENCES threads(channel_id, topic_name)
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_threads_parent
                ON threads(channel_id, parent_topic_name, created_at)
            """
        )

    def _migrate_v2(self, connection: sqlite3.Connection) -> None:
        columns = self._columns(connection, "threads")
        additions = {
            "objective": "TEXT",
            "seed_context": "TEXT",
            "seed_consumed_at": "INTEGER",
            "source_message_id": "TEXT",
        }
        for name, sql_type in additions.items():
            if name not in columns:
                connection.execute(f"ALTER TABLE threads ADD COLUMN {name} {sql_type}")

    def _migrate_v3(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS thread_status (
                channel_id TEXT NOT NULL,
                topic_name TEXT NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('todo', 'in_progress', 'blocked', 'done')
                ),
                summary TEXT NOT NULL DEFAULT '',
                source_message_id TEXT,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (channel_id, topic_name),
                FOREIGN KEY (channel_id, topic_name)
                    REFERENCES threads(channel_id, topic_name) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_thread_status_updated
                ON thread_status(channel_id, updated_at)
            """
        )

    def _migrate_v4(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS thread_rollups (
                channel_id TEXT NOT NULL,
                root_topic_name TEXT NOT NULL,
                rollup_message_id TEXT,
                digest TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (channel_id, root_topic_name),
                FOREIGN KEY (channel_id, root_topic_name)
                    REFERENCES threads(channel_id, topic_name) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_thread_actions (
                channel_id TEXT NOT NULL,
                source_message_id TEXT NOT NULL,
                action_key TEXT NOT NULL,
                processed_at INTEGER NOT NULL,
                PRIMARY KEY (channel_id, source_message_id, action_key)
            )
            """
        )

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"Thread database schema {version} is newer than supported "
                    f"version {SCHEMA_VERSION}."
                )
            # Legacy stores had the v1 table but no user_version.  Every
            # migration is idempotent, so running the chain also upgrades them.
            migrations = (
                self._migrate_v1,
                self._migrate_v2,
                self._migrate_v3,
                self._migrate_v4,
            )
            for target_version, migrate in enumerate(migrations, start=1):
                if version < target_version:
                    migrate(connection)
                    connection.execute(f"PRAGMA user_version = {target_version}")
        try:
            self.path.chmod(0o600)
        except OSError:
            pass

    def split(
        self,
        channel_id: str,
        parent_topic: str,
        child_segment: str,
        *,
        max_topic_length: int = DEFAULT_MAX_TOPIC_LENGTH,
        goal: str | None = None,
        seed_context: str | None = None,
        source_message_id: str | int | None = None,
    ) -> SplitResult:
        """Create a child relation, optionally seeding its future session.

        A source message makes the operation idempotent at directive level as
        well as at topic level.  Multiple children from one source remain
        valid because each child gets its own action key.
        """
        child_topic = build_child_topic(
            parent_topic,
            child_segment,
            max_topic_length=max_topic_length,
        )
        parent = parent_topic.strip()
        normalized_goal = self._optional_text(goal, "goal", MAX_GOAL_LENGTH)
        normalized_seed = self._optional_text(
            seed_context, "seed_context", MAX_SEED_LENGTH
        )
        source_id = self._source_id(source_message_id)
        now = time.time_ns()
        action_key = f"split:{child_topic}"

        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if source_id is not None and self._action_exists(
                connection, channel_id, source_id, action_key
            ):
                return SplitResult(
                    topic_name=child_topic, created=False, duplicate=True
                )
            connection.execute(
                """
                INSERT OR IGNORE INTO threads(
                    channel_id, topic_name, parent_topic_name, created_at
                ) VALUES (?, ?, NULL, ?)
                """,
                (channel_id, parent, now),
            )
            existing = connection.execute(
                """
                SELECT parent_topic_name
                FROM threads
                WHERE channel_id = ? AND topic_name = ?
                """,
                (channel_id, child_topic),
            ).fetchone()
            if existing is not None:
                if existing["parent_topic_name"] != parent:
                    raise ThreadHierarchyError(
                        "Ce topic existe déjà sous un autre parent."
                    )
                connection.execute(
                    """
                    UPDATE threads
                    SET objective = COALESCE(objective, ?),
                        seed_context = COALESCE(seed_context, ?),
                        source_message_id = COALESCE(source_message_id, ?)
                    WHERE channel_id = ? AND topic_name = ?
                    """,
                    (
                        normalized_goal,
                        normalized_seed,
                        source_id,
                        channel_id,
                        child_topic,
                    ),
                )
                if source_id is not None:
                    self._record_action(
                        connection, channel_id, source_id, action_key, now
                    )
                return SplitResult(topic_name=child_topic, created=False)

            connection.execute(
                """
                INSERT INTO threads(
                    channel_id, topic_name, parent_topic_name, created_at,
                    objective, seed_context, source_message_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    channel_id,
                    child_topic,
                    parent,
                    now + 1,
                    normalized_goal,
                    normalized_seed,
                    source_id,
                ),
            )
            if source_id is not None:
                self._record_action(connection, channel_id, source_id, action_key, now)
        return SplitResult(topic_name=child_topic, created=True)

    @staticmethod
    def _optional_text(value: str | None, field: str, maximum: int) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ThreadHierarchyError(f"{field} must be a string.")
        normalized = value.strip()
        if len(normalized) > maximum:
            raise ThreadHierarchyError(
                f"{field} exceeds the {maximum}-character limit."
            )
        return normalized or None

    @staticmethod
    def _source_id(source_message_id: str | int | None) -> str | None:
        if source_message_id is None:
            return None
        if isinstance(source_message_id, bool) or not isinstance(
            source_message_id, (str, int)
        ):
            raise ThreadHierarchyError("source_message_id must be a string or integer.")
        normalized = str(source_message_id).strip()
        if not normalized or len(normalized) > MAX_SOURCE_MESSAGE_ID_LENGTH:
            raise ThreadHierarchyError("source_message_id is empty or too long.")
        return normalized

    @staticmethod
    def _action_exists(
        connection: sqlite3.Connection,
        channel_id: str,
        source_message_id: str,
        action_key: str,
    ) -> bool:
        return (
            connection.execute(
                """
            SELECT 1 FROM processed_thread_actions
            WHERE channel_id = ? AND source_message_id = ? AND action_key = ?
            """,
                (channel_id, source_message_id, action_key),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _record_action(
        connection: sqlite3.Connection,
        channel_id: str,
        source_message_id: str,
        action_key: str,
        processed_at: int,
    ) -> bool:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO processed_thread_actions(
                channel_id, source_message_id, action_key, processed_at
            ) VALUES (?, ?, ?, ?)
            """,
            (channel_id, source_message_id, action_key, processed_at),
        )
        return cursor.rowcount == 1

    @staticmethod
    def _ensure_thread(
        connection: sqlite3.Connection,
        channel_id: str,
        topic_name: str,
        created_at: int,
    ) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO threads(
                channel_id, topic_name, parent_topic_name, created_at
            ) VALUES (?, ?, NULL, ?)
            """,
            (channel_id, topic_name, created_at),
        )

    def thread_details(self, channel_id: str, topic_name: str) -> ThreadDetails | None:
        """Return hierarchy and seed metadata for one known topic."""
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT topic_name, parent_topic_name, created_at, objective,
                       seed_context, seed_consumed_at, source_message_id
                FROM threads
                WHERE channel_id = ? AND topic_name = ?
                """,
                (channel_id, topic_name),
            ).fetchone()
        if row is None:
            return None
        return ThreadDetails(
            topic_name=row["topic_name"],
            parent_topic_name=row["parent_topic_name"],
            created_at=row["created_at"],
            goal=row["objective"],
            seed_context=row["seed_context"],
            seed_consumed_at=row["seed_consumed_at"],
            source_message_id=row["source_message_id"],
        )

    def peek_seed(self, channel_id: str, topic_name: str) -> ThreadSeed | None:
        """Return an unconsumed session seed without changing it."""
        details = self.thread_details(channel_id, topic_name)
        if (
            details is None
            or details.seed_context is None
            or details.seed_consumed_at is not None
        ):
            return None
        return ThreadSeed(details.topic_name, details.goal, details.seed_context)

    def consume_seed(self, channel_id: str, topic_name: str) -> ThreadSeed | None:
        """Atomically return a seed once, including across concurrent callers."""
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT objective, seed_context
                FROM threads
                WHERE channel_id = ? AND topic_name = ?
                      AND seed_context IS NOT NULL
                      AND seed_consumed_at IS NULL
                """,
                (channel_id, topic_name),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """
                UPDATE threads SET seed_consumed_at = ?
                WHERE channel_id = ? AND topic_name = ?
                      AND seed_consumed_at IS NULL
                """,
                (time.time_ns(), channel_id, topic_name),
            )
            return ThreadSeed(topic_name, row["objective"], row["seed_context"])

    def mark_seed_consumed(self, channel_id: str, topic_name: str) -> bool:
        """Mark an existing seed consumed without returning its contents."""
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE threads SET seed_consumed_at = ?
                WHERE channel_id = ? AND topic_name = ?
                      AND seed_context IS NOT NULL
                      AND seed_consumed_at IS NULL
                """,
                (time.time_ns(), channel_id, topic_name),
            )
        return cursor.rowcount == 1

    def is_action_processed(
        self, channel_id: str, source_message_id: str | int, action_key: str
    ) -> bool:
        """Check directive deduplication state without modifying it."""
        source_id = self._source_id(source_message_id)
        if not action_key or len(action_key) > 500:
            raise ThreadHierarchyError("action_key is empty or too long.")
        assert source_id is not None
        with self._lock, self._connect() as connection:
            return self._action_exists(connection, channel_id, source_id, action_key)

    def claim_action(
        self, channel_id: str, source_message_id: str | int, action_key: str
    ) -> bool:
        """Atomically claim a source/action pair; return false for duplicates.

        Prefer ``split(..., source_message_id=...)`` and
        ``set_status(..., source_message_id=...)`` where possible because those
        methods record the claim in the same transaction as the state change.
        """
        source_id = self._source_id(source_message_id)
        if not action_key or len(action_key) > 500:
            raise ThreadHierarchyError("action_key is empty or too long.")
        assert source_id is not None
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            return self._record_action(
                connection, channel_id, source_id, action_key, time.time_ns()
            )

    @staticmethod
    def _status_record(row: sqlite3.Row) -> ThreadStatusRecord:
        return ThreadStatusRecord(
            topic_name=row["topic_name"],
            status=ThreadStatus(row["status"]),
            summary=row["summary"],
            source_message_id=row["source_message_id"],
            updated_at=row["updated_at"],
        )

    def get_status(self, channel_id: str, topic_name: str) -> ThreadStatusRecord | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT topic_name, status, summary, source_message_id, updated_at
                FROM thread_status
                WHERE channel_id = ? AND topic_name = ?
                """,
                (channel_id, topic_name),
            ).fetchone()
        return None if row is None else self._status_record(row)

    def set_status(
        self,
        channel_id: str,
        topic_name: str,
        status: ThreadStatus | str,
        summary: str = "",
        *,
        source_message_id: str | int | None = None,
    ) -> StatusUpdateResult:
        """Set thread status, atomically deduplicating an optional source."""
        try:
            normalized_status = (
                status if isinstance(status, ThreadStatus) else ThreadStatus(status)
            )
        except (TypeError, ValueError) as exc:
            raise ThreadHierarchyError(f"Unknown thread status: {status!r}.") from exc
        normalized_summary = (
            self._optional_text(summary, "summary", MAX_STATUS_SUMMARY_LENGTH) or ""
        )
        source_id = self._source_id(source_message_id)
        action_key = f"status:{topic_name}"
        now = time.time_ns()

        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT topic_name, status, summary, source_message_id, updated_at
                FROM thread_status
                WHERE channel_id = ? AND topic_name = ?
                """,
                (channel_id, topic_name),
            ).fetchone()
            if (
                source_id is not None
                and existing is not None
                and self._action_exists(connection, channel_id, source_id, action_key)
            ):
                return StatusUpdateResult(self._status_record(existing), changed=False)

            self._ensure_thread(connection, channel_id, topic_name, now)
            connection.execute(
                """
                INSERT INTO thread_status(
                    channel_id, topic_name, status, summary,
                    source_message_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(channel_id, topic_name) DO UPDATE SET
                    status = excluded.status,
                    summary = excluded.summary,
                    source_message_id = excluded.source_message_id,
                    updated_at = excluded.updated_at
                """,
                (
                    channel_id,
                    topic_name,
                    normalized_status.value,
                    normalized_summary,
                    source_id,
                    now,
                ),
            )
            if source_id is not None:
                self._record_action(connection, channel_id, source_id, action_key, now)
            row = connection.execute(
                """
                SELECT topic_name, status, summary, source_message_id, updated_at
                FROM thread_status
                WHERE channel_id = ? AND topic_name = ?
                """,
                (channel_id, topic_name),
            ).fetchone()
            assert row is not None
            return StatusUpdateResult(self._status_record(row), changed=True)

    def set_rollup(
        self,
        channel_id: str,
        root_topic_name: str,
        message_id: str | int | None,
        digest: str,
        *,
        updated_at: int | None = None,
    ) -> RollupRecord:
        """Create or update the stable rollup message metadata for a root."""
        if message_id is None:
            normalized_message_id = None
        else:
            normalized_message_id = self._source_id(message_id)
        normalized_digest = (
            self._optional_text(digest, "digest", MAX_DIGEST_LENGTH) or ""
        )
        timestamp = time.time_ns() if updated_at is None else int(updated_at)
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._ensure_thread(connection, channel_id, root_topic_name, timestamp)
            connection.execute(
                """
                INSERT INTO thread_rollups(
                    channel_id, root_topic_name, rollup_message_id,
                    digest, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(channel_id, root_topic_name) DO UPDATE SET
                    rollup_message_id = excluded.rollup_message_id,
                    digest = excluded.digest,
                    updated_at = excluded.updated_at
                """,
                (
                    channel_id,
                    root_topic_name,
                    normalized_message_id,
                    normalized_digest,
                    timestamp,
                ),
            )
        return RollupRecord(
            root_topic_name, normalized_message_id, normalized_digest, timestamp
        )

    def get_rollup(self, channel_id: str, root_topic_name: str) -> RollupRecord | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT root_topic_name, rollup_message_id, digest, updated_at
                FROM thread_rollups
                WHERE channel_id = ? AND root_topic_name = ?
                """,
                (channel_id, root_topic_name),
            ).fetchone()
        if row is None:
            return None
        return RollupRecord(
            root_topic_name=row["root_topic_name"],
            message_id=row["rollup_message_id"],
            digest=row["digest"],
            updated_at=row["updated_at"],
        )

    def rollup_due(
        self,
        channel_id: str,
        root_topic_name: str,
        debounce_seconds: int,
        *,
        now_ns: int | None = None,
    ) -> bool:
        """Return whether a root has no rollup or its debounce elapsed."""
        record = self.get_rollup(channel_id, root_topic_name)
        if record is None:
            return True
        now = time.time_ns() if now_ns is None else now_ns
        return now - record.updated_at >= max(0, debounce_seconds) * 1_000_000_000

    def parent(self, channel_id: str, topic_name: str) -> str | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT parent_topic_name
                FROM threads
                WHERE channel_id = ? AND topic_name = ?
                """,
                (channel_id, topic_name),
            ).fetchone()
        return None if row is None else row["parent_topic_name"]

    def children(self, channel_id: str, topic_name: str) -> list[str]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT topic_name
                FROM threads
                WHERE channel_id = ? AND parent_topic_name = ?
                ORDER BY lower(topic_name), topic_name
                """,
                (channel_id, topic_name),
            ).fetchall()
        return [row["topic_name"] for row in rows]

    def root(self, channel_id: str, topic_name: str) -> str:
        current = topic_name
        seen: set[str] = set()
        with self._lock, self._connect() as connection:
            while current not in seen:
                seen.add(current)
                row = connection.execute(
                    """
                    SELECT parent_topic_name
                    FROM threads
                    WHERE channel_id = ? AND topic_name = ?
                    """,
                    (channel_id, current),
                ).fetchone()
                if row is None or row["parent_topic_name"] is None:
                    return current
                current = row["parent_topic_name"]
        raise ThreadHierarchyError("La hiérarchie contient une boucle.")

    def subtree(self, channel_id: str, root_topic: str) -> list[ThreadRecord]:
        """Return a bounded root-first subtree without recursive SQL."""
        records: list[ThreadRecord] = []
        pending = [root_topic]
        seen: set[str] = set()
        with self._lock, self._connect() as connection:
            while pending and len(records) < MAX_TREE_NODES:
                topic = pending.pop(0)
                if topic in seen:
                    continue
                seen.add(topic)
                row = connection.execute(
                    """
                    SELECT topic_name, parent_topic_name, created_at
                    FROM threads
                    WHERE channel_id = ? AND topic_name = ?
                    """,
                    (channel_id, topic),
                ).fetchone()
                if row is None:
                    if topic == root_topic:
                        records.append(ThreadRecord(topic, None, 0))
                    continue
                records.append(
                    ThreadRecord(
                        topic_name=row["topic_name"],
                        parent_topic_name=row["parent_topic_name"],
                        created_at=row["created_at"],
                    )
                )
                children = connection.execute(
                    """
                    SELECT topic_name
                    FROM threads
                    WHERE channel_id = ? AND parent_topic_name = ?
                    ORDER BY lower(topic_name), topic_name
                    """,
                    (channel_id, topic),
                ).fetchall()
                pending.extend(child["topic_name"] for child in children)
        return records

    def overview(self, channel_id: str, root_topic: str) -> list[ThreadOverviewRecord]:
        """Return the bounded subtree enriched with goals and current status."""
        records = self.subtree(channel_id, root_topic)
        result: list[ThreadOverviewRecord] = []
        with self._lock, self._connect() as connection:
            for record in records:
                row = connection.execute(
                    """
                    SELECT t.objective, s.status, s.summary, s.updated_at
                    FROM threads AS t
                    LEFT JOIN thread_status AS s
                      ON s.channel_id = t.channel_id
                     AND s.topic_name = t.topic_name
                    WHERE t.channel_id = ? AND t.topic_name = ?
                    """,
                    (channel_id, record.topic_name),
                ).fetchone()
                result.append(
                    ThreadOverviewRecord(
                        thread=record,
                        goal=None if row is None else row["objective"],
                        status=(
                            ThreadStatus.TODO
                            if row is None or row["status"] is None
                            else ThreadStatus(row["status"])
                        ),
                        summary=(
                            ""
                            if row is None or row["summary"] is None
                            else row["summary"]
                        ),
                        status_updated_at=(None if row is None else row["updated_at"]),
                    )
                )
        return result


def render_tree(root_topic: str, records: list[ThreadRecord]) -> str:
    """Render records as a compact Unicode tree using relative labels."""
    children_by_parent: dict[str, list[str]] = {}
    for record in records:
        if record.parent_topic_name is not None:
            children_by_parent.setdefault(record.parent_topic_name, []).append(
                record.topic_name
            )
    for children in children_by_parent.values():
        children.sort(key=lambda value: (value.casefold(), value))

    lines = [root_topic]

    def label(topic: str, parent: str) -> str:
        prefix = f"{parent}{TOPIC_SEPARATOR}"
        return topic.removeprefix(prefix)

    def walk(parent: str, prefix: str, ancestors: set[str]) -> None:
        children = children_by_parent.get(parent, [])
        for index, child in enumerate(children):
            last = index == len(children) - 1
            lines.append(f"{prefix}{'└── ' if last else '├── '}{label(child, parent)}")
            if child not in ancestors:
                walk(
                    child,
                    f"{prefix}{'    ' if last else '│   '}",
                    ancestors | {child},
                )

    walk(root_topic, "", {root_topic})
    if len(records) >= MAX_TREE_NODES:
        lines.append(f"… arbre limité à {MAX_TREE_NODES} topics")
    return "\n".join(lines)
