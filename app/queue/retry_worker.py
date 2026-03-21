"""
Async Event Processor — Event-driven, no polling.

Instead of polling every N seconds, events are processed immediately:
  - Webhook arrives → store in DB → trigger async processing instantly
  - On failure → schedule a delayed retry via asyncio.create_task with sleep
  - On startup → process all pending/failed events in one shot
"""

import asyncio
import json
import traceback
from datetime import datetime, timezone

from app.queue.event_store import (
    get_pending_events,
    get_stale_processing_events,
    mark_processing,
    mark_completed,
    mark_failed,
    track_gmail_msg,
    EventStatus,
)
from app.gmail.gmail_client import (
    fetch_and_process_by_history_id,
    process_inbox_now,
)

# Track active retry tasks so they can be cancelled on shutdown
_active_tasks: set = set()


async def process_event_now(event_id: int, message_id: str, history_id: str = None,
                            payload: dict = None, retry_count: int = 0, max_retries: int = 5):
    """
    Process a single event immediately (no polling).
    Called directly from the webhook handler or from startup recovery.
    On failure, schedules a delayed retry task automatically.
    """
    label = "NEW" if retry_count == 0 else f"RETRY #{retry_count}"
    print(f"\n⚡ [PROCESSOR] Processing event #{event_id} ({label}) "
          f"| messageId={message_id}", flush=True)

    mark_processing(event_id)

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, _do_gmail_processing, event_id, history_id
        )

        if result:
            mark_completed(event_id)
        else:
            error_msg = "Processing returned False — no flight email or forwarding failed"
            _handle_failure(event_id, message_id, history_id, payload,
                            retry_count, max_retries, error_msg)

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(f"❌ [PROCESSOR] Event #{event_id} failed: {error_msg}", flush=True)
        traceback.print_exc()
        _handle_failure(event_id, message_id, history_id, payload,
                        retry_count, max_retries, error_msg)


def _handle_failure(event_id: int, message_id: str, history_id: str,
                    payload: dict, retry_count: int, max_retries: int, error_msg: str):
    """Mark as failed and schedule a delayed retry if retries remain."""
    mark_failed(event_id, error_msg)

    new_retry_count = retry_count + 1
    if new_retry_count < max_retries:
        # Exponential backoff: 30s, 60s, 120s, 240s
        delay = (2 ** retry_count) * 30
        print(f"⏳ [PROCESSOR] Scheduling retry #{new_retry_count} for event #{event_id} "
              f"in {delay}s", flush=True)

        task = asyncio.create_task(
            _delayed_retry(delay, event_id, message_id, history_id,
                           payload, new_retry_count, max_retries)
        )
        _active_tasks.add(task)
        task.add_done_callback(_active_tasks.discard)
    else:
        print(f"💀 [PROCESSOR] Event #{event_id} exhausted all {max_retries} retries", flush=True)


async def _delayed_retry(delay: float, event_id: int, message_id: str,
                         history_id: str, payload: dict,
                         retry_count: int, max_retries: int):
    """Wait for the backoff delay, then re-process the event."""
    print(f"💤 [PROCESSOR] Retry #{retry_count} for event #{event_id} sleeping {delay}s...",
          flush=True)
    await asyncio.sleep(delay)
    await process_event_now(event_id, message_id, history_id, payload,
                            retry_count, max_retries)


def _do_gmail_processing(event_id: int, history_id: str = None) -> bool:
    """
    Synchronous Gmail processing — runs in thread pool.
    Always uses unread inbox scan (proven reliable).
    History-based fetching is only used for startup recovery gap detection.
    """
    try:
        result = process_inbox_now(event_id=event_id)
        return result
    except Exception as e:
        print(f"❌ [PROCESSOR] Gmail processing error: {e}", flush=True)
        raise


async def process_all_pending():
    """
    Process ALL pending/failed events in one shot.
    Called once at startup to recover from any downtime.
    """
    # Reset stale 'processing' events first
    stale = get_stale_processing_events(stale_minutes=0)
    if stale:
        print(f"🔧 [PROCESSOR] Resetting {len(stale)} stale event(s)", flush=True)
        for ev in stale:
            mark_failed(ev["id"], "Server restarted — recovered from stale state")

    events = get_pending_events()
    if not events:
        print("✅ [PROCESSOR] No pending events to recover", flush=True)
        return

    print(f"🚀 [PROCESSOR] Processing {len(events)} pending event(s) from queue...", flush=True)

    # Fire all of them concurrently
    tasks = []
    for ev in events:
        task = asyncio.create_task(
            process_event_now(
                event_id=ev["id"],
                message_id=ev["message_id"],
                history_id=ev.get("history_id"),
                payload=json.loads(ev["payload"]) if isinstance(ev["payload"], str) else ev.get("payload"),
                retry_count=ev["retry_count"],
                max_retries=ev["max_retries"],
            )
        )
        tasks.append(task)
        _active_tasks.add(task)
        task.add_done_callback(_active_tasks.discard)

    # Wait for all to complete
    await asyncio.gather(*tasks, return_exceptions=True)
    print(f"✅ [PROCESSOR] Startup recovery batch complete", flush=True)


def cancel_all_tasks():
    """Cancel all active retry tasks on shutdown."""
    if _active_tasks:
        print(f"🛑 [PROCESSOR] Cancelling {len(_active_tasks)} active task(s)...", flush=True)
        for task in _active_tasks:
            task.cancel()
    _active_tasks.clear()
