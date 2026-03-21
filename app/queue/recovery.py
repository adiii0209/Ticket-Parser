"""
Startup Recovery — Runs once at server boot to ensure no events are lost.

Performs three recovery actions:
  1. Reset stale 'processing' events → 'failed' (server crashed mid-process)
  2. Re-queue all pending/failed events for immediate retry
  3. Fetch missed emails from Gmail using stored historyId (gap detection)
"""

import traceback
from datetime import datetime, timezone

from app.queue.event_store import (
    init_db,
    get_pending_events,
    get_stale_processing_events,
    get_event_stats,
    mark_failed,
    get_system_state,
    set_system_state,
    store_event,
    is_duplicate,
    is_gmail_msg_processed,
    track_gmail_msg,
)
from app.gmail.auth import get_gmail_service
from app.gmail.gmail_client import (
    extract_text_from_message,
    is_flight_email,
)
from app.services.email_processor import process_single_email


def run_startup_recovery():
    """
    Full startup recovery sequence.
    Called once during FastAPI lifespan startup.
    """
    print("\n" + "=" * 60, flush=True)
    print("🔧 [RECOVERY] Starting system recovery...", flush=True)
    print("=" * 60, flush=True)

    # Step 0: Initialize database
    init_db()

    # Step 1: Reset stale processing events
    _recover_stale_events()

    # Step 2: Show queue statistics
    stats = get_event_stats()
    if stats:
        print(f"📊 [RECOVERY] Queue stats: {stats}", flush=True)
    else:
        print("📊 [RECOVERY] Queue is empty — fresh start", flush=True)

    # Step 3: Check for processing gaps via Gmail historyId
    pending = get_pending_events()
    if pending:
        print(f"📋 [RECOVERY] {len(pending)} event(s) pending for (re)processing — "
              "retry worker will handle them", flush=True)

    # Step 4: Fetch any missed emails using Gmail history
    _fetch_missed_emails()

    print("=" * 60, flush=True)
    print("✅ [RECOVERY] Startup recovery complete!", flush=True)
    print("=" * 60 + "\n", flush=True)


def _recover_stale_events():
    """Reset events stuck in 'processing' back to 'failed' for retry."""
    stale = get_stale_processing_events(stale_minutes=0)  # Any 'processing' on startup is stale
    if stale:
        print(f"🔧 [RECOVERY] Found {len(stale)} event(s) stuck in 'processing' "
              "— resetting to 'failed'", flush=True)
        for ev in stale:
            mark_failed(ev["id"], "Server restarted — recovered from stale processing state")
    else:
        print("✅ [RECOVERY] No stale processing events found", flush=True)


def _fetch_missed_emails():
    """
    Use Gmail History API to fetch emails that arrived while the server was down.
    Uses the last known historyId stored in system_state.
    Falls back to scanning recent unread emails if no historyId is available.
    """
    print("\n📬 [RECOVERY] Checking for missed emails during downtime...", flush=True)

    try:
        service = get_gmail_service()
    except Exception as e:
        print(f"❌ [RECOVERY] Cannot connect to Gmail: {e}", flush=True)
        return

    last_history_id = get_system_state("last_history_id")

    if last_history_id:
        print(f"📌 [RECOVERY] Last known historyId: {last_history_id}", flush=True)
        _fetch_via_history(service, last_history_id)
    else:
        print("⚠ [RECOVERY] No stored historyId — scanning recent unread emails", flush=True)
        _fetch_recent_unread(service)


def _fetch_via_history(service, start_history_id: str):
    """Fetch missed messages using Gmail History API from a given historyId."""
    try:
        response = service.users().history().list(
            userId="me",
            startHistoryId=start_history_id,
            historyTypes=["messageAdded"],
            labelId="INBOX"
        ).execute()

        history_records = response.get("history", [])
        new_history_id = response.get("historyId")

        if new_history_id:
            set_system_state("last_history_id", str(new_history_id))

        if not history_records:
            print("✅ [RECOVERY] No missed emails found via history — all caught up!", flush=True)
            return

        # Collect unique message IDs from history
        missed_msg_ids = set()
        for record in history_records:
            for msg in record.get("messagesAdded", []):
                msg_id = msg.get("message", {}).get("id")
                if msg_id:
                    missed_msg_ids.add(msg_id)

        print(f"📧 [RECOVERY] Found {len(missed_msg_ids)} message(s) in history gap", flush=True)

        processed = 0
        skipped = 0
        for msg_id in missed_msg_ids:
            if is_gmail_msg_processed(msg_id):
                skipped += 1
                continue

            success = _process_missed_email(service, msg_id)
            if success:
                processed += 1

        print(f"📊 [RECOVERY] History scan complete: "
              f"{processed} processed, {skipped} already known", flush=True)

    except Exception as e:
        if "404" in str(e) or "historyId" in str(e).lower():
            print(f"⚠ [RECOVERY] HistoryId {start_history_id} expired — "
                  "falling back to unread scan", flush=True)
            _fetch_recent_unread(service)
        else:
            print(f"❌ [RECOVERY] History fetch failed: {e}", flush=True)
            traceback.print_exc()
            # Still try unread scan as fallback
            _fetch_recent_unread(service)


def _fetch_recent_unread(service):
    """Fallback: scan recent unread emails in inbox."""
    try:
        results = service.users().messages().list(
            userId="me",
            q="is:unread",
            maxResults=20
        ).execute()

        messages = results.get("messages", [])

        if not messages:
            print("✅ [RECOVERY] No unread emails to recover", flush=True)
            return

        print(f"📧 [RECOVERY] Found {len(messages)} unread email(s) to check", flush=True)

        processed = 0
        skipped = 0
        for msg in messages:
            msg_id = msg["id"]

            if is_gmail_msg_processed(msg_id):
                skipped += 1
                continue

            success = _process_missed_email(service, msg_id)
            if success:
                processed += 1

        print(f"📊 [RECOVERY] Unread scan complete: "
              f"{processed} queued, {skipped} already known", flush=True)

        # Update historyId from profile
        profile = service.users().getProfile(userId="me").execute()
        current_history_id = profile.get("historyId")
        if current_history_id:
            set_system_state("last_history_id", str(current_history_id))
            print(f"📌 [RECOVERY] Updated historyId to {current_history_id}", flush=True)

    except Exception as e:
        print(f"❌ [RECOVERY] Unread scan failed: {e}", flush=True)
        traceback.print_exc()


def _process_missed_email(service, gmail_msg_id: str) -> bool:
    """Fetch a single Gmail message by ID and queue it for processing."""
    try:
        message = service.users().messages().get(
            userId="me",
            id=gmail_msg_id,
            format="full"
        ).execute()

        headers = message["payload"].get("headers", [])
        subject = ""
        from_email = ""

        for h in headers:
            if h["name"].lower() == "subject":
                subject = h["value"]
            if h["name"].lower() == "from":
                from_email = h["value"]

        body = extract_text_from_message(message)

        if is_flight_email(subject, body, from_email):
            # Create a unique messageId for this recovered email
            recovery_msg_id = f"recovery_{gmail_msg_id}"

            if not is_duplicate(recovery_msg_id):
                store_event(
                    message_id=recovery_msg_id,
                    history_id=None,
                    email_address=from_email,
                    payload={
                        "source": "recovery",
                        "gmail_msg_id": gmail_msg_id,
                        "subject": subject,
                        "from": from_email,
                    }
                )
                print(f"📥 [RECOVERY] Queued missed flight email: {subject}", flush=True)
                return True
            else:
                return False
        else:
            # Not a flight email — still track it so we don't re-scan
            track_gmail_msg(gmail_msg_id)
            return False

    except Exception as e:
        print(f"❌ [RECOVERY] Failed to fetch message {gmail_msg_id}: {e}", flush=True)
        return False
