import re
import fitz  # PyMuPDF

from app.parser.attachment_extractor import extract_pdf_attachment
from llm_extractor import extract as parse_ticket_llm
from app.forwarder.query_client import send_to_query_system
from app.gmail.auth import get_gmail_service

# 🔥 PNR regex:
PNR_REGEX = r"\b[A-Z0-9]{6}\b"

def extract_text_from_pdf(pdf_bytes):
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        return text
    except Exception as e:
        print(f"❌ [PROCESSOR] Error extracting PDF: {e}", flush=True)
        return None

def extract_pnr(text):
    if not text:
        return None
    matches = re.findall(PNR_REGEX, text.upper())
    return matches[0] if matches else None

def process_single_email(email):
    service = get_gmail_service()
    user_id = "me"

    print("\n--- PROCESSING EMAIL ---", flush=True)
    print(f"From: {email['from']}", flush=True)
    print(f"Subject: {email['subject']}", flush=True)

    raw_text = None

    # 🔹 Step 1: Try PDF first
    pdf_bytes = extract_pdf_attachment(
        service,
        user_id,
        email["raw_message"]
    )

    if pdf_bytes:
        print("✅ [PROCESSOR] PDF attachment detected", flush=True)
        raw_text = extract_text_from_pdf(pdf_bytes)

        if not raw_text:
            print("❌ [PROCESSOR] PDF extraction failed. Falling back to body.", flush=True)

    if not raw_text:
        print("⚠ [PROCESSOR] Using email body.", flush=True)
        raw_text = email.get("body")

        if not raw_text:
            print("❌ [PROCESSOR] No readable content found", flush=True)
            return False

    # 🔥 Step 2: STRICT PNR CHECK
    pnr = extract_pnr(raw_text)

    if not pnr:
        print(f"❌ [PROCESSOR] No valid PNR found in content from {email['from']}.", flush=True)
        return False

    print(f"✅ [PROCESSOR] Valid PNR detected: {pnr}", flush=True)

    # 🔹 Step 3: LLM Parsing
    try:
        parsed_ticket = parse_ticket_llm(raw_text)

        if not parsed_ticket.get("pnr"):
            parsed_ticket["pnr"] = pnr

        print("✅ [PROCESSOR] Ticket parsed successfully via LLM (or fallback)", flush=True)

    except Exception as e:
        print(f"❌ [PROCESSOR] Global Parsing failed: {e}", flush=True)
        return False

    # 🔹 Step 4: Send to Query System
    success = send_to_query_system(parsed_ticket)

    if success:
        print("✅ [PROCESSOR] Ticket forwarded successfully!", flush=True)
        return True
    else:
        print("❌ [PROCESSOR] Failed to forward ticket.", flush=True)
        return False
