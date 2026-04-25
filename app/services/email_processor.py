import re
from copy import deepcopy

import fitz  # PyMuPDF

from app.forwarder.query_client import notify_processing_batch, send_to_query_system
from app.gmail.auth import get_gmail_service
from app.parser.attachment_extractor import extract_pdf_attachment
from indigo_parser import try_indigo_parse
from llm_extractor import extract as parse_ticket_llm, _extract_pnr
from gds_parser import try_gds_parse

def extract_text_from_pdf(pdf_bytes):
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        return text
    except Exception as e:
        print(f"[PROCESSOR] Error extracting PDF: {e}", flush=True)
        return None


def extract_pnr(text):
    if not text:
        return None
    return _extract_pnr(text)


def _build_batch_id(email):
    message_id = str(email.get("id") or "").strip()
    if message_id:
        return f"mail-{message_id}"
    return "mail-unknown"


def _build_batch_label(email):
    subject = str(email.get("subject") or "").strip()
    sender = str(email.get("from") or "").strip()

    if subject and sender:
        return f"{subject} | {sender}"
    return subject or sender or "email"


def _normalize_ticket_payloads(parsed_result):
    if isinstance(parsed_result, list):
        return [ticket for ticket in parsed_result if isinstance(ticket, dict)]

    if isinstance(parsed_result, dict):
        tickets = parsed_result.get("tickets")
        if isinstance(tickets, list):
            return [ticket for ticket in tickets if isinstance(ticket, dict)]
        return [parsed_result]

    return []


def _inject_batch_metadata(ticket_payload, batch_id):
    enriched_payload = deepcopy(ticket_payload)
    metadata = enriched_payload.get("metadata")

    if not isinstance(metadata, dict):
        metadata = {}

    parser_version = metadata.get("parser_version") or metadata.get("version")
    if parser_version:
        metadata["parser_version"] = parser_version

    metadata["processing_batch_id"] = batch_id
    enriched_payload["metadata"] = metadata
    return enriched_payload


def process_single_email(email):
    service = get_gmail_service()
    user_id = "me"

    print("\n--- PROCESSING EMAIL ---", flush=True)
    print(f"From: {email['from']}", flush=True)
    print(f"Subject: {email['subject']}", flush=True)

    raw_text = None

    pdf_bytes = extract_pdf_attachment(
        service,
        user_id,
        email["raw_message"]
    )

    if pdf_bytes:
        print("[PROCESSOR] PDF attachment detected", flush=True)
        raw_text = extract_text_from_pdf(pdf_bytes)

        if not raw_text:
            print("[PROCESSOR] PDF extraction failed. Falling back to body.", flush=True)

    if not raw_text:
        print("[PROCESSOR] Using email body.", flush=True)
        raw_text = email.get("body")

        if not raw_text:
            print("[PROCESSOR] No readable content found", flush=True)
            return None

    subject_text = str(email.get("subject") or "")
    pnr_scan_text = f"{subject_text}\n{raw_text}"
    pnr = extract_pnr(pnr_scan_text)

    if pnr:
        print(f"[PROCESSOR] Valid PNR detected: {pnr}", flush=True)
    else:
        print(
            f"[PROCESSOR] No valid PNR found in subject/body precheck for {email['from']}; continuing parse.",
            flush=True,
        )

    batch_id = _build_batch_id(email)
    batch_label = _build_batch_label(email)

    print(
        f"[PROCESSOR] Announcing batch {batch_id} with 1 ticket",
        flush=True,
    )

    if not notify_processing_batch(batch_id, 1, batch_label):
        print("[PROCESSOR] Failed to announce processing batch.", flush=True)
        return False

    try:
        parsed_ticket = try_indigo_parse(raw_text)
        if parsed_ticket is not None:
            print("[PROCESSOR] Ticket parsed successfully via IndiGo parser (no LLM)", flush=True)
        else:
            parsed_ticket = try_gds_parse(raw_text)
            if parsed_ticket is not None:
                print("[PROCESSOR] Ticket parsed successfully via GDS parser (no LLM)", flush=True)
        if parsed_ticket is None:
            parsed_ticket = parse_ticket_llm(raw_text)
            print("[PROCESSOR] Ticket parsed successfully via LLM (or fallback)", flush=True)

        bk = parsed_ticket.get("booking", {})
        if isinstance(parsed_ticket, dict) and not bk.get("pnr") and pnr:
            if "booking" not in parsed_ticket:
                parsed_ticket["booking"] = {}
            parsed_ticket["booking"]["pnr"] = pnr

    except Exception as e:
        print(f"[PROCESSOR] Global parsing failed: {e}", flush=True)
        return False

    ticket_payloads = _normalize_ticket_payloads(parsed_ticket)
    if not ticket_payloads:
        print("[PROCESSOR] Parser returned no ticket payloads.", flush=True)
        return False

    if not any((ticket.get("booking", {}) or {}).get("pnr") not in (None, "", "N/A") for ticket in ticket_payloads):
        print(f"[PROCESSOR] Parsed payload has no valid PNR for {email['from']}.", flush=True)
        return None

    print(
        f"[PROCESSOR] Forwarding {len(ticket_payloads)} parsed ticket(s) for batch {batch_id}",
        flush=True,
    )

    for index, ticket_payload in enumerate(ticket_payloads, start=1):
        success = send_to_query_system(_inject_batch_metadata(ticket_payload, batch_id))
        if not success:
            print(
                f"[PROCESSOR] Failed to forward ticket {index}/{len(ticket_payloads)}.",
                flush=True,
            )
            return False

    print("[PROCESSOR] Ticket batch forwarded successfully!", flush=True)
    return True
