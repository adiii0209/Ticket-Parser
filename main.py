import base64
import json
import asyncio

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from app.gmail.auth import get_gmail_service
from app.queue.event_store import init_db, is_duplicate, store_event, get_event_stats, set_system_state
from app.queue.retry_worker import process_event_now, process_all_pending, cancel_all_tasks
from app.queue.recovery import run_startup_recovery
from dotenv import load_dotenv

import sys
sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

import os


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting Server...", flush=True)

    # ──────────────────────────────────────
    # PHASE 1: Database + Recovery
    # ──────────────────────────────────────
    print("🔧 Running startup recovery...", flush=True)
    run_startup_recovery()

    # ──────────────────────────────────────
    # PHASE 2: Gmail Webhook Registration
    # ──────────────────────────────────────
    print("📬 Configuring Gmail Webhook...", flush=True)
    try:
        service = get_gmail_service()
        response = service.users().watch(
            userId='me',
            body={
                'topicName': os.getenv('GCP_PUB_SUB_TOPIC', 'projects/ticket-parser-488310/topics/gmail_ticket'),
                'labelIds': ['INBOX']
            }
        ).execute()
        print(f"✅ Gmail Watch active. Debug info: {response}", flush=True)

        watch_history_id = response.get("historyId")
        if watch_history_id:
            set_system_state("last_history_id", str(watch_history_id))
            print(f"📌 Stored historyId: {watch_history_id}", flush=True)

    except Exception as e:
        print(f"❌ Gmail Webhook config failed: {e}", flush=True)

    # ──────────────────────────────────────
    # PHASE 3: Process all pending events from DB
    # ──────────────────────────────────────
    asyncio.create_task(process_all_pending())

    yield

    # ──────────────────────────────────────
    # SHUTDOWN
    # ──────────────────────────────────────
    print("🛑 Shutting down Server...", flush=True)
    cancel_all_tasks()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"status": "online", "message": "Ticket Parser System is running"}


@app.post("/gmail/webhook")
async def gmail_webhook(request: Request):
    """
    Gmail Pub/Sub Webhook.

    Flow: Receive → Dedup → Store → ACK → Process immediately (async).
    No polling — processing fires the instant the event is stored.
    """
    print("\n🔔 [WEBHOOK] Request received from Google Pub/Sub!", flush=True)

    try:
        envelope = await request.json()
    except Exception as e:
        print(f"❌ [WEBHOOK] Failed to parse JSON: {e}", flush=True)
        return {"status": "ack"}

    if "message" not in envelope:
        print("⚠ [WEBHOOK] No 'message' in envelope.", flush=True)
        return {"status": "ack"}

    pubsub_message = envelope["message"]
    message_id = pubsub_message.get("messageId", pubsub_message.get("message_id", ""))

    if not message_id:
        print("⚠ [WEBHOOK] No messageId found in Pub/Sub message", flush=True)
        return {"status": "ack"}

    # ── IDEMPOTENCY CHECK ──
    if is_duplicate(message_id):
        print(f"⏭ [WEBHOOK] Duplicate messageId={message_id} — skipping", flush=True)
        return {"status": "ack"}

    # ── DECODE & STORE ──
    history_id = None
    email_address = None

    if "data" in pubsub_message:
        try:
            decoded_data = base64.b64decode(pubsub_message["data"]).decode("utf-8")
            message_data = json.loads(decoded_data)
            history_id = message_data.get("historyId")
            email_address = message_data.get("emailAddress")
            print(f"📩 [WEBHOOK] Gmail Update: User={email_address} | HistoryId={history_id}",
                  flush=True)
        except Exception as e:
            print(f"⚠ [WEBHOOK] Error decoding data (will still queue): {e}", flush=True)

    try:
        event_id = store_event(
            message_id=message_id,
            history_id=str(history_id) if history_id else None,
            email_address=email_address,
            payload=envelope
        )
        print(f"✅ [WEBHOOK] Event #{event_id} queued", flush=True)

        # 🔥 Fire processing immediately — no polling, no waiting
        asyncio.create_task(
            process_event_now(
                event_id=event_id,
                message_id=message_id,
                history_id=str(history_id) if history_id else None,
                payload=envelope,
            )
        )

    except Exception as e:
        print(f"❌ [WEBHOOK] Failed to store event: {e}", flush=True)

    return {"status": "ack"}


# ── MONITORING ──

@app.get("/queue/stats")
async def queue_stats():
    """Get current queue statistics."""
    stats = get_event_stats()
    return {"status": "ok", "queue": stats}