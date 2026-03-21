"""
Persistent Event Store — SQLite-backed queue for webhook events.

Provides:
  - Idempotency via unique messageId constraint
  - Status tracking (pending, processing, completed, failed, dead)
  - Retry count and next_retry_at scheduling
  - Full event payload persistence for recovery
"""

import sqlite3
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from enum import Enum


class EventStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"  # exceeded max retries


DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "event_queue.db")

_local = threading.local()


def _get_connection() -> sqlite3.Connection:
    """Get a thread-local SQLite connection."""
    if not hasattr(_local, "connection") or _local.connection is None:
        db_dir = os.path.dirname(os.path.abspath(DB_PATH))
        os.makedirs(db_dir, exist_ok=True)
        _local.connection = sqlite3.connect(os.path.abspath(DB_PATH))
        _local.connection.row_factory = sqlite3.Row
        _local.connection.execute("PRAGMA journal_mode=WAL")
        _local.connection.execute("PRAGMA busy_timeout=5000")
    return _local.connection


def init_db():
    """Initialize the event queue database schema."""
    conn = _get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS webhook_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id      TEXT UNIQUE NOT NULL,
            history_id      TEXT,
            email_address   TEXT,
            payload         TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'pending',
            retry_count     INTEGER NOT NULL DEFAULT 0,
            max_retries     INTEGER NOT NULL DEFAULT 5,
            next_retry_at   TEXT,
            error_message   TEXT,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            completed_at    TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_status ON webhook_events(status)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_next_retry ON webhook_events(next_retry_at)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_gmail_ids (
            gmail_msg_id    TEXT PRIMARY KEY,
            event_id        INTEGER,
            processed_at    TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_state (
            key     TEXT PRIMARY KEY,
            value   TEXT NOT NULL
        )
    """)
    conn.commit()
    print("✅ [EVENT_STORE] Database initialized", flush=True)


# ──────────────────────────────────────────────
# IDEMPOTENCY CHECK
# ──────────────────────────────────────────────

def is_duplicate(message_id: str) -> bool:
    """Check if a webhook event with this messageId was already received."""
    conn = _get_connection()
    row = conn.execute(
        "SELECT id FROM webhook_events WHERE message_id = ?",
        (message_id,)
    ).fetchone()
    return row is not None


# ──────────────────────────────────────────────
# EVENT INSERTION
# ──────────────────────────────────────────────

def store_event(message_id: str, history_id: str, email_address: str,
                payload: dict, max_retries: int = 5) -> int:
    """
    Store a new webhook event. Returns the event ID.
    Raises sqlite3.IntegrityError if duplicate messageId.
    """
    conn = _get_connection()
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute("""
        INSERT INTO webhook_events
            (message_id, history_id, email_address, payload, status,
             retry_count, max_retries, next_retry_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
    """, (
        message_id, history_id, email_address,
        json.dumps(payload), EventStatus.PENDING,
        max_retries, now, now, now
    ))
    conn.commit()
    event_id = cursor.lastrowid
    print(f"📥 [EVENT_STORE] Stored event #{event_id} | messageId={message_id}", flush=True)
    return event_id


# ──────────────────────────────────────────────
# STATUS UPDATES
# ──────────────────────────────────────────────

def mark_processing(event_id: int):
    conn = _get_connection()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        UPDATE webhook_events SET status = ?, updated_at = ?
        WHERE id = ?
    """, (EventStatus.PROCESSING, now, event_id))
    conn.commit()


def mark_completed(event_id: int):
    conn = _get_connection()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        UPDATE webhook_events SET status = ?, updated_at = ?, completed_at = ?
        WHERE id = ?
    """, (EventStatus.COMPLETED, now, now, event_id))
    conn.commit()
    print(f"✅ [EVENT_STORE] Event #{event_id} marked COMPLETED", flush=True)


def mark_failed(event_id: int, error_msg: str):
    """Mark event as failed and schedule next retry with exponential backoff."""
    conn = _get_connection()
    now = datetime.now(timezone.utc)

    row = conn.execute(
        "SELECT retry_count, max_retries FROM webhook_events WHERE id = ?",
        (event_id,)
    ).fetchone()

    if not row:
        return

    new_retry_count = row["retry_count"] + 1

    if new_retry_count >= row["max_retries"]:
        # Exceeded max retries → mark as dead
        conn.execute("""
            UPDATE webhook_events
            SET status = ?, retry_count = ?, error_message = ?, updated_at = ?
            WHERE id = ?
        """, (EventStatus.DEAD, new_retry_count, error_msg, now.isoformat(), event_id))
        conn.commit()
        print(f"💀 [EVENT_STORE] Event #{event_id} marked DEAD after {new_retry_count} retries", flush=True)
        return

    # Exponential backoff: 2^retry * 30 seconds (30s, 60s, 120s, 240s, 480s)
    backoff_seconds = (2 ** row["retry_count"]) * 30
    next_retry = now + timedelta(seconds=backoff_seconds)

    conn.execute("""
        UPDATE webhook_events
        SET status = ?, retry_count = ?, error_message = ?,
            next_retry_at = ?, updated_at = ?
        WHERE id = ?
    """, (
        EventStatus.FAILED, new_retry_count, error_msg,
        next_retry.isoformat(), now.isoformat(), event_id
    ))
    conn.commit()
    print(f"⚠ [EVENT_STORE] Event #{event_id} FAILED (retry {new_retry_count}/{row['max_retries']}) "
          f"| Next retry at {next_retry.isoformat()} | Error: {error_msg}", flush=True)


# ──────────────────────────────────────────────
# QUERY HELPERS
# ──────────────────────────────────────────────

def get_pending_events() -> list:
    """Get all events that are pending or failed and due for retry."""
    conn = _get_connection()
    now = datetime.now(timezone.utc).isoformat()
    rows = conn.execute("""
        SELECT * FROM webhook_events
        WHERE status = ?
           OR (status = ? AND next_retry_at <= ?)
        ORDER BY created_at ASC
    """, (EventStatus.PENDING, EventStatus.FAILED, now)).fetchall()
    return [dict(r) for r in rows]


def get_stale_processing_events(stale_minutes: int = 10) -> list:
    """Get events stuck in 'processing' for more than stale_minutes."""
    conn = _get_connection()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)).isoformat()
    rows = conn.execute("""
        SELECT * FROM webhook_events
        WHERE status = ? AND updated_at < ?
    """, (EventStatus.PROCESSING, cutoff)).fetchall()
    return [dict(r) for r in rows]


def get_event_stats() -> dict:
    """Get summary counts of events by status."""
    conn = _get_connection()
    rows = conn.execute("""
        SELECT status, COUNT(*) as count FROM webhook_events GROUP BY status
    """).fetchall()
    return {row["status"]: row["count"] for row in rows}


# ──────────────────────────────────────────────
# GMAIL MESSAGE ID TRACKING
# ──────────────────────────────────────────────

def is_gmail_msg_processed(gmail_msg_id: str) -> bool:
    """Check if a specific Gmail message was already processed."""
    conn = _get_connection()
    row = conn.execute(
        "SELECT gmail_msg_id FROM processed_gmail_ids WHERE gmail_msg_id = ?",
        (gmail_msg_id,)
    ).fetchone()
    return row is not None


def track_gmail_msg(gmail_msg_id: str, event_id: int = None):
    """Record that a Gmail message has been processed."""
    conn = _get_connection()
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO processed_gmail_ids (gmail_msg_id, event_id, processed_at) VALUES (?, ?, ?)",
            (gmail_msg_id, event_id, now)
        )
        conn.commit()
    except Exception:
        pass  # Ignore duplicate inserts


# ──────────────────────────────────────────────
# SYSTEM STATE (last known historyId, etc.)
# ──────────────────────────────────────────────

def get_system_state(key: str) -> str | None:
    conn = _get_connection()
    row = conn.execute("SELECT value FROM system_state WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_system_state(key: str, value: str):
    conn = _get_connection()
    conn.execute("""
        INSERT INTO system_state (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = ?
    """, (key, value, value))
    conn.commit()
