import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

QUERY_SYSTEM_URL = os.getenv("QUERY_SYSTEM_URL")
QUERY_API_KEY = os.getenv("QUERY_API_KEY")


def send_to_query_system(ticket_data: dict) -> bool:
    """
    Sends parsed ticket JSON to Query System (Flask backend).
    Synchronous call. Waits for response.
    Returns True if accepted, False otherwise.
    """

    print(f"📡 [FORWARDER] Sending ticket to Query System: {QUERY_SYSTEM_URL}", flush=True)
    print(f"📊 [FORWARDER] Data: {json.dumps(ticket_data, indent=2)}", flush=True)
    try:
        response = requests.post(
            QUERY_SYSTEM_URL,
            json=ticket_data,
            headers={
                "Authorization": f"Bearer {QUERY_API_KEY}",
                "Content-Type": "application/json"
            },
            timeout=15  # Prevent hanging forever
        )

        # Raise error for non-200 responses
        response.raise_for_status()

        result = response.json()

        print("✅ Query System Accepted Ticket")
        print("Response:", result)

        return True

    except requests.exceptions.Timeout:
        print("❌ Query System Timeout")
        return False

    except requests.exceptions.HTTPError as e:
        print("❌ HTTP Error:", e)
        print("Response:", response.text)
        return False

    except Exception as e:
        print("❌ Failed to Send Ticket:", str(e))
        return False