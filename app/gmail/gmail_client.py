import base64
import re
from .auth import get_gmail_service
from app.services.email_processor import process_single_email
from app.queue.event_store import (
    is_gmail_msg_processed,
    track_gmail_msg,
    set_system_state,
)


# -----------------------------
# 🔥 STRONGER FLIGHT DETECTION
# -----------------------------

AIRLINE_DOMAINS = [
    "airindia",
    "indigo",
    "spicejet",
    "vistara",
    "emirates",
    "qatarairways",
    "etihad",
    "lufthansa",
    "airasia",
    "goair"
]

STRONG_TERMS = [
    "passenger name record",
    "booking reference",
    "e-ticket",
    "flight itinerary",
    "boarding pass",
    "departure",
    "arrival",
    "terminal",
    "gate",
    "seat",
    "flight",
    "ticket",
    "itinerary",
    "pnr"
]

STRICT_PNR_REGEX = r"\b[A-Z0-9]{6}\b"
FLIGHT_NUMBER_REGEX = r"\b[A-Z]{2}\s?\d{2,4}\b"
AIRPORT_CODE_REGEX = r"\b[A-Z]{3}\b"


def is_flight_email(subject: str, body: str, from_email: str) -> bool:
    combined = f"{subject} {body}".lower()
    score = 0

    if any(domain in from_email.lower() for domain in AIRLINE_DOMAINS):
        score += 3

    if any(term in combined for term in STRONG_TERMS):
        score += 2

    if re.search(FLIGHT_NUMBER_REGEX, combined.upper()):
        score += 2

    airport_matches = re.findall(AIRPORT_CODE_REGEX, combined.upper())
    if len(airport_matches) >= 2:
        score += 1

    if re.search(STRICT_PNR_REGEX, combined.upper()):
        score += 2

    return score >= 2


# -------------------------------------------
# 📩 HISTORY-BASED FETCH (webhook triggered)
# -------------------------------------------
def fetch_and_process_by_history_id(history_id: str, event_id: int = None) -> bool:
    """
    Fetch new messages since the given historyId and process flight emails.
    Used by the retry worker to process webhook events precisely.
    Returns True if at least one email was processed (or no new emails found).
    """
    service = get_gmail_service()

    print(f"🔎 [GMAIL] Fetching history since historyId={history_id}", flush=True)

    try:
        response = service.users().history().list(
            userId="me",
            startHistoryId=history_id,
            historyTypes=["messageAdded"],
            labelId="INBOX"
        ).execute()

        # Update last known historyId
        new_history_id = response.get("historyId")
        if new_history_id:
            set_system_state("last_history_id", str(new_history_id))

        history_records = response.get("history", [])

        if not history_records:
            print("💤 [GMAIL] No new messages in history range", flush=True)
            return True  # Nothing to process is not a failure

        # Collect unique message IDs
        msg_ids = set()
        for record in history_records:
            for msg in record.get("messagesAdded", []):
                msg_id = msg.get("message", {}).get("id")
                if msg_id:
                    msg_ids.add(msg_id)

        print(f"📧 [GMAIL] Found {len(msg_ids)} new message(s) via history", flush=True)

        any_processed = False
        for msg_id in msg_ids:
            # Skip if already processed
            if is_gmail_msg_processed(msg_id):
                print(f"⏭ [GMAIL] Skipping already-processed message {msg_id}", flush=True)
                continue

            success = _fetch_and_process_single(service, msg_id, event_id)
            if success:
                any_processed = True

        return True  # Successfully handled (even if no flight emails)

    except Exception as e:
        if "404" in str(e) or "notFound" in str(e):
            print(f"⚠ [GMAIL] HistoryId {history_id} expired — falling back to inbox scan",
                  flush=True)
            return _fallback_inbox_scan(event_id)
        raise


def _fetch_and_process_single(service, msg_id: str, event_id: int = None) -> bool:
    """Fetch and process a single Gmail message by ID."""
    try:
        message = service.users().messages().get(
            userId="me",
            id=msg_id,
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

        print(f"📨 [GMAIL] Scanned: {subject} | From: {from_email}", flush=True)

        if is_flight_email(subject, body, from_email):
            print("✈ [GMAIL] Flight email detected", flush=True)
            success = process_single_email({
                "id": msg_id,
                "subject": subject,
                "from": from_email,
                "body": body,
                "raw_message": message
            })

            if success:
                mark_as_read(msg_id)
                track_gmail_msg(msg_id, event_id)
                return True
            else:
                return False
        else:
            print("❌ [GMAIL] Not classified as flight email", flush=True)
            track_gmail_msg(msg_id, event_id)  # Track even non-flight to avoid rescanning
            return False

    except Exception as e:
        print(f"❌ [GMAIL] Error processing message {msg_id}: {e}", flush=True)
        raise


def _fallback_inbox_scan(event_id: int = None) -> bool:
    """Fallback when historyId is expired — scan recent unread inbox."""
    print("🔄 [GMAIL] Running fallback inbox scan...", flush=True)
    process_inbox_now(event_id=event_id)
    return True


# -----------------------------
# 📩 PROCESS INBOX (FULL SCAN)
# -----------------------------
def process_inbox_now(event_id: int = None) -> bool:
    """
    Scan unread inbox for flight emails, parse and forward them.
    Returns True if processing was successful (or no unread emails).
    Returns False only if a flight email was found but failed to process.
    """
    service = get_gmail_service()

    print("🔎 [GMAIL] Checking unread inbox...", flush=True)

    results = service.users().messages().list(
        userId="me",
        q="is:unread",
        maxResults=5
    ).execute()

    messages = results.get("messages", [])

    if not messages:
        print("💤 [GMAIL] No unread emails found.", flush=True)
        return True  # Nothing to process is not a failure

    print(f"📧 [GMAIL] Found {len(messages)} unread emails", flush=True)

    # Update historyId from profile
    try:
        profile = service.users().getProfile(userId="me").execute()
        current_hid = profile.get("historyId")
        if current_hid:
            set_system_state("last_history_id", str(current_hid))
    except Exception:
        pass

    any_flight_found = False
    any_flight_failed = False

    for msg in messages:
        msg_id = msg["id"]

        # Skip already processed
        if is_gmail_msg_processed(msg_id):
            print(f"⏭ [GMAIL] Skipping already-processed message {msg_id}", flush=True)
            continue

        message = service.users().messages().get(
            userId="me",
            id=msg_id,
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

        print(f"📨 [GMAIL] Scanned: {subject} | From: {from_email}", flush=True)

        if is_flight_email(subject, body, from_email):
            print("✈ [GMAIL] Flight email detected", flush=True)
            any_flight_found = True
            success = process_single_email({
                "id": msg_id,
                "subject": subject,
                "from": from_email,
                "body": body,
                "raw_message": message
            })

            if success:
                mark_as_read(msg_id)
                track_gmail_msg(msg_id, event_id)
            else:
                any_flight_failed = True
        else:
            print("❌ [GMAIL] Not classified as flight email", flush=True)
            track_gmail_msg(msg_id, event_id)

    # Return False only if a flight email was found but failed
    if any_flight_failed:
        return False
    return True


# -----------------------------
# 📦 BODY EXTRACTION
# -----------------------------
def extract_text_from_message(message):
    payload = message.get("payload", {})

    def decode_base64(data):
        try:
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
        except Exception:
            return ""

    def extract_parts(parts):
        text_content = ""
        for part in parts:
            mime_type = part.get("mimeType", "")
            body = part.get("body", {})

            if "parts" in part:
                text_content += extract_parts(part["parts"])
            elif mime_type == "text/plain":
                data = body.get("data")
                if data:
                    text_content += decode_base64(data)
            elif mime_type == "text/html":
                data = body.get("data")
                if data:
                    html_content = decode_base64(data)
                    text_content += strip_html_tags(html_content)

        return text_content

    if "parts" in payload:
        return extract_parts(payload["parts"])

    if payload.get("mimeType") in ["text/plain", "text/html"]:
        data = payload.get("body", {}).get("data")
        if data:
            content = decode_base64(data)
            if payload.get("mimeType") == "text/html":
                return strip_html_tags(content)
            return content

    return ""


def strip_html_tags(html):
    return re.sub(r"<.*?>", "", html)


def mark_as_read(msg_id):
    service = get_gmail_service()

    service.users().messages().modify(
        userId="me",
        id=msg_id,
        body={"removeLabelIds": ["UNREAD"]}
    ).execute()

    print("✅ [GMAIL] Marked as read", flush=True)