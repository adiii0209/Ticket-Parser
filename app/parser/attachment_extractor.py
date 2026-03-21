import base64


def extract_pdf_attachment(service, user_id, message):
    """
    Extract first PDF attachment from Gmail message.
    Returns PDF bytes or None.
    """

    payload = message.get("payload", {})

    if "parts" not in payload:
        return None

    for part in payload["parts"]:
        filename = part.get("filename")
        body = part.get("body", {})

        if filename and filename.lower().endswith(".pdf"):
            attachment_id = body.get("attachmentId")

            if attachment_id:
                attachment = service.users().messages().attachments().get(
                    userId=user_id,
                    messageId=message["id"],
                    id=attachment_id
                ).execute()

                data = attachment.get("data")
                if data:
                    return base64.urlsafe_b64decode(data)

    return None