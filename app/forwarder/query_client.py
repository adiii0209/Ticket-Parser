import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

QUERY_SYSTEM_URL = os.getenv("QUERY_SYSTEM_URL")
QUERY_API_KEY = os.getenv("QUERY_API_KEY")


def _build_processing_url() -> str | None:
    if not QUERY_SYSTEM_URL:
        return None

    normalized_url = QUERY_SYSTEM_URL.strip().rstrip("/")
    return f"{normalized_url}/processing"


def _auth_headers() -> dict:
    return {
        "Authorization": f"Bearer {QUERY_API_KEY}",
        "Content-Type": "application/json",
    }


def notify_processing_batch(batch_id: str, ticket_count: int, label: str = "") -> bool:
    processing_url = _build_processing_url()
    payload = {
        "batch_id": batch_id,
        "ticket_count": ticket_count,
        "source": "email",
        "label": label or "",
    }

    print(f"Sending processing batch to: {processing_url}")
    print(f"Batch data:\n{json.dumps(payload, indent=2)}")

    try:
        response = requests.post(
            processing_url,
            json=payload,
            headers=_auth_headers(),
            timeout=15,
        )
        response.raise_for_status()

        try:
            result = response.json()
            print("Processing batch sent successfully")
            print(json.dumps(result, indent=2))
        except ValueError:
            print("Processing batch sent successfully")
            print(response.text)

        return True

    except requests.exceptions.Timeout:
        print("Processing batch timeout")
        return False

    except requests.exceptions.HTTPError as e:
        print("Processing batch HTTP Error:", e)
        print("Response:", response.text)
        return False

    except Exception as e:
        print("Processing batch failed:", str(e))
        return False


def send_to_query_system(ticket_data: dict) -> bool:
    print(f"Sending ticket to: {QUERY_SYSTEM_URL}")
    print(f"Data:\n{json.dumps(ticket_data, indent=2)}")

    try:
        response = requests.post(
            QUERY_SYSTEM_URL,
            json=ticket_data,
            headers=_auth_headers(),
            timeout=15,
        )
        response.raise_for_status()

        try:
            result = response.json()
            print("Sent successfully")
            print(json.dumps(result, indent=2))
        except ValueError:
            print("Sent successfully")
            print(response.text)

        return True

    except requests.exceptions.Timeout:
        print("Timeout")
        return False

    except requests.exceptions.HTTPError as e:
        print("HTTP Error:", e)
        print("Response:", response.text)
        return False

    except Exception as e:
        print("Failed:", str(e))
        return False


def main():
    raw_json = r'''
{
  "pnr": "INDIGO",
  "passengers": [
    {
      "name": "Mrs Vrinda Bagri",
      "pax_type": "ADT",
      "ticket_number": "N/A",
      "frequent_flyer_number": "N/A",
      "baggage": "7 Kg Cabin, 15 Kg Check-in",
      "meals": [],
      "ancillaries": [],
      "fare": {
        "base_fare": 35152,
        "k3_gst": 746,
        "other_taxes": 6660.0,
        "total_fare": 42558
      },
      "seats": []
    },
    {
      "name": "Mr Giriraj Bagri",
      "pax_type": "ADT",
      "ticket_number": "N/A",
      "frequent_flyer_number": "N/A",
      "baggage": "7 Kg Cabin, 15 Kg Check-in",
      "meals": [],
      "ancillaries": [],
      "fare": {
        "base_fare": 35152,
        "k3_gst": 746,
        "other_taxes": 6660.0,
        "total_fare": 42558
      },
      "seats": []
    }
  ],
  "segments": [
    {
      "airline": "Indigo",
      "flight_number": "6E 798",
      "booking_class": "N/A",
      "departure": {
        "city": "Kolkata",
        "airport": "CCU",
        "date": "24 Apr 26",
        "time": "18:55",
        "terminal": "Netaji Subhash Chandra Bose International Airport"
      },
      "arrival": {
        "city": "Indore",
        "airport": "IDR",
        "date": "24 Apr 26",
        "time": "21:20",
        "terminal": "Devi Ahilya Bai Holkar Airport"
      },
      "duration_extracted": "2h 25m",
      "duration_calculated": "2h 25m"
    },
    {
      "airline": "Indigo",
      "flight_number": "6E 6566",
      "booking_class": "N/A",
      "departure": {
        "city": "Indore",
        "airport": "IDR",
        "date": "26 Apr 26",
        "time": "20:30",
        "terminal": "Devi Ahilya Bai Holkar Airport"
      },
      "arrival": {
        "city": "Kolkata",
        "airport": "CCU",
        "date": "26 Apr 26",
        "time": "22:40",
        "terminal": "Netaji Subhash Chandra Bose International Airport"
      },
      "duration_extracted": "2h 10m",
      "duration_calculated": "2h 10m"
    }
  ],
  "journey": {
    "trip_type": "round_trip",
    "trip_type_display": "Round Trip",
    "total_duration": "4h 35m",
    "legs": [
      {
        "from": "CCU",
        "to": "IDR",
        "total_duration": "2h 25m",
        "layovers": [],
        "has_layovers": false,
        "segments": [
          {
            "flight_number": "6E 798"
          }
        ]
      },
      {
        "from": "IDR",
        "to": "CCU",
        "total_duration": "2h 10m",
        "layovers": [],
        "has_layovers": false,
        "segments": [
          {
            "flight_number": "6E 6566"
          }
        ]
      }
    ]
  }
}
'''

    try:
        ticket_data = json.loads(raw_json)
    except json.JSONDecodeError as e:
        print("Invalid JSON")
        print("Error:", e)
        return

    send_to_query_system(ticket_data)


if __name__ == "__main__":
    main()
