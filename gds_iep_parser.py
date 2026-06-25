"""
gds_iep_parser.py - Parser for the GDS-IEP itinerary receipt layout.

This parser is intentionally narrow: it targets the exact IEP format used in
SriLankan-style itinerary receipts and returns the same high-level schema as
the existing GDS parser.
"""

import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

from gds_parser import _dedupe_segments_with_remap, _remap_segment_refs, _pax, _seg
from llm_extractor import build_journey, normalize_baggage, normalize_data, PARSER_VERSION
from mappings import AIRLINE_CODES, AIRPORT_CODES, resolve_booking_class


IEP_SOURCE = "gds_iep"
IEP_SOURCE_NAME = "GDS-IEP"

_RE_ISSUING_AIRLINE = re.compile(r"^\s*([A-Z][A-Z\s().,&'-]+?)\s+\(PB\s*\d+\)", re.MULTILINE)
_RE_PNR = re.compile(r"BOOKING\s+REF:\s*([A-Z0-9]{5,8})", re.IGNORECASE)
_RE_BOOKING_DATE = re.compile(r"DATE:\s*(\d{2}\s+[A-Z]{3}\s+\d{4})", re.IGNORECASE)
_RE_PHONE = re.compile(r"TELEPHONE:\s*([+\d][\d\s()\-]+)", re.IGNORECASE)
_name_pattern = r"[A-Z][A-Z.-]*(?: [A-Z.-]+)*"
_RE_PAX_LINE = re.compile(rf"\b({_name_pattern}/{_name_pattern}(?:\s+(?:MR|MRS|MS|MISS|MSTR))?)$", re.IGNORECASE | re.MULTILINE)
_RE_TICKET = re.compile(
    rf"TICKET:\s*[A-Z0-9]+/ETKT\s*(\d{{3}})\s*(\d{{10}})(?:\s+FOR\s+({_name_pattern}/{_name_pattern}(?:\s+(?:MR|MRS|MS|MISS|MSTR))?))?$",
    re.IGNORECASE | re.MULTILINE,
)
_RE_FLIGHT_HEADER = re.compile(
    r"^FLIGHT\s+([A-Z0-9]{2})\s+(\d{1,4})\s*-\s*([A-Z][A-Z\s().,&'-]+?)\s+"
    r"(?:MON|TUE|WED|THU|FRI|SAT|SUN)\s+(\d{2}\s+[A-Z]{3,9}\s+\d{4})\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_RE_LOCATION = re.compile(
    r"([A-Z][A-Z\s'.-]+),\s*([A-Z]{2})\s*\(([^)]+)\)(?:,\s*TERMINAL\s*([A-Z0-9]+))?",
    re.IGNORECASE,
)
_RE_DEPARTURE = re.compile(r"DEPARTURE:\s*(.+?)\s+(\d{2}\s+[A-Z]{3})\s+(\d{2}:\d{2})", re.IGNORECASE)
_RE_ARRIVAL = re.compile(r"ARRIVAL:\s*(.+?)\s+(\d{2}\s+[A-Z]{3})\s+(\d{2}:\d{2})", re.IGNORECASE)
_RE_RESERVATION = re.compile(
    r"RESERVATION\s+CONFIRMED,\s*([A-Z\s]+?)\s*\(([A-Z])\)\s*DURATION:\s*([0-9]{2}:[0-9]{2})",
    re.IGNORECASE,
)
_RE_BAGGAGE = re.compile(r"BAGGAGE\s+ALLOWANCE:\s*([A-Z0-9]+)", re.IGNORECASE)
_RE_MEAL = re.compile(r"MEAL:\s*(.+)", re.IGNORECASE)


def is_gds_iep_format(text: str) -> bool:
    required_patterns = [
        r"BOOKING\s+REF:",
        r"FLIGHT\s+TICKET\(S\)",
        r"TICKET:\s*[A-Z0-9]+/ETKT",
        r"RESERVATION\s+CONFIRMED,\s*[A-Z\s]+\([A-Z]\)",
        r"BAGGAGE\s+ALLOWANCE:",
    ]
    return all(re.search(pattern, text, re.IGNORECASE) for pattern in required_patterns)


def _to_dd_mon_yy(raw: str) -> str:
    dt = datetime.strptime(raw.strip(), "%d %b %Y")
    return dt.strftime("%d %b %y")


def _normalize_phone(raw: str | None) -> str:
    if not raw:
        return "N/A"
    raw = re.split(r"\s+-\s+", raw.strip(), maxsplit=1)[0]
    digits = re.sub(r"[^\d+]", "", raw)
    return digits or "N/A"


def _format_passenger_name(raw: str) -> str:
    raw = re.sub(r"\s+", " ", raw.strip())
    if "/" not in raw:
        return raw.title()
        
    last, rest = raw.split("/", 1)
    rest_parts = rest.split()
    title = ""
    if len(rest_parts) > 1 and rest_parts[-1].upper() in ("MR", "MRS", "MS", "MISS", "MSTR"):
        title = rest_parts.pop(-1).title()
    
    first = " ".join(rest_parts)
    return " ".join(part for part in [title, first.title(), last.title()] if part).strip()


def _passenger_name_key(raw: str) -> str:
    return re.sub(r"\s+", " ", raw.strip().upper())


def _parse_passenger_names(text: str) -> List[str]:
    passenger_section = text.split("FLIGHT", 1)[0]
    names: List[str] = []
    seen = set()
    for match in _RE_PAX_LINE.finditer(passenger_section):
        raw_name = _passenger_name_key(match.group(1))
        if raw_name in seen:
            continue
        seen.add(raw_name)
        names.append(_format_passenger_name(match.group(1)))
    return names


def _parse_location(raw: str) -> Dict[str, str]:
    match = _RE_LOCATION.search(raw.strip())
    if not match:
        return {
            "city": "N/A",
            "airport": "N/A",
            "terminal": "N/A",
        }

    city = match.group(1).strip().title()
    terminal = match.group(4).strip().upper() if match.group(4) else "N/A"
    airport_name = match.group(3).strip()

    airport_code = "N/A"
    target = airport_name.upper()
    city_target = city.upper()
    for iata, name in AIRPORT_CODES.items():
        name_upper = name.upper()
        if target in name_upper or name_upper in target or city_target in name_upper:
            airport_code = iata
            break

    return {
        "city": city,
        "airport": airport_code,
        "terminal": terminal,
    }


def _iter_flight_blocks(text: str) -> List[re.Match]:
    return list(_RE_FLIGHT_HEADER.finditer(text))


def _extract_iep(text: str) -> Dict:
    issuing_airline_match = _RE_ISSUING_AIRLINE.search(text)
    issuing_airline = issuing_airline_match.group(1).strip().title() if issuing_airline_match else "N/A"

    pnr_match = _RE_PNR.search(text)
    pnr = pnr_match.group(1).upper() if pnr_match else "N/A"

    booking_date_match = _RE_BOOKING_DATE.search(text)
    booking_date = _to_dd_mon_yy(booking_date_match.group(1)) if booking_date_match else "N/A"

    phone_match = _RE_PHONE.search(text)
    phone = _normalize_phone(phone_match.group(1)) if phone_match else "N/A"

    passenger_names = _parse_passenger_names(text)
    tickets_by_name: Dict[str, str] = {}
    unnamed_tickets: List[str] = []
    for match in _RE_TICKET.finditer(text):
        ticket_number = f"{match.group(1)}{match.group(2)}"
        raw_name = match.group(3)
        if raw_name:
            ticket_name_key = _format_passenger_name(raw_name)
            tickets_by_name[ticket_name_key] = ticket_number
        else:
            unnamed_tickets.append(ticket_number)

    segments: List[Dict] = []
    segment_meals: List[str] = []
    baggage = "N/A"

    flight_headers = _iter_flight_blocks(text)
    for index, match in enumerate(flight_headers):
        airline_code = match.group(1).upper()
        flight_number_numeric = match.group(2)
        airline_name = AIRLINE_CODES.get(airline_code, match.group(3).strip().title())
        block_start = match.start()
        block_end = flight_headers[index + 1].start() if index + 1 < len(flight_headers) else text.find("FLIGHT TICKET(S)", match.end())
        if block_end == -1:
            block_end = len(text)
        block = text[block_start:block_end]

        departure_match = _RE_DEPARTURE.search(block)
        arrival_match = _RE_ARRIVAL.search(block)
        reservation_match = _RE_RESERVATION.search(block)
        baggage_match = _RE_BAGGAGE.search(block)
        meal_match = _RE_MEAL.search(block)

        if not (departure_match and arrival_match and reservation_match):
            continue

        dep_info = _parse_location(departure_match.group(1))
        arr_info = _parse_location(arrival_match.group(1))
        dep_date = _to_dd_mon_yy(f"{departure_match.group(2)} {match.group(4).split()[-1]}")
        arr_date = _to_dd_mon_yy(f"{arrival_match.group(2)} {match.group(4).split()[-1]}")

        booking_class_code = reservation_match.group(2).upper()
        resolved_booking_class = resolve_booking_class(booking_class_code, airline_code=airline_code)
        booking_class_label = (
            resolved_booking_class.get("cabin")
            or resolved_booking_class.get("full_form")
            or reservation_match.group(1).strip().title()
        )

        if baggage_match:
            baggage = normalize_baggage(baggage_match.group(1).strip()) or baggage
        meal_name = re.sub(r"\s+", " ", meal_match.group(1).strip()).title() if meal_match else "N/A"
        segment_meals.append(meal_name)

        segments.append(
            _seg(
                airline=airline_name,
                flight_number=f"{airline_code} {flight_number_numeric}",
                booking_class=booking_class_label,
                dep_city=dep_info["city"],
                dep_airport=dep_info["airport"],
                dep_date=dep_date,
                dep_time=departure_match.group(3),
                dep_terminal=dep_info["terminal"],
                arr_city=arr_info["city"],
                arr_airport=arr_info["airport"],
                arr_date=arr_date,
                arr_time=arrival_match.group(3),
                arr_terminal=arr_info["terminal"],
                duration_extracted=reservation_match.group(3),
            )
        )

    passengers: List[Dict] = []
    if not passenger_names and tickets_by_name:
        passenger_names = [_format_passenger_name(name_key) for name_key in tickets_by_name.keys()]

    fallback_tickets = iter(unnamed_tickets)
    for passenger_name in passenger_names or ["N/A"]:
        ticket_number = tickets_by_name.get(passenger_name)
        if not ticket_number:
            ticket_number = next(fallback_tickets, "N/A")

        passenger = _pax(
            name=passenger_name,
            ticket_number=ticket_number,
            frequent_flyer_number="N/A",
            baggage=baggage,
        )
        for segment_index, meal_name in enumerate(segment_meals):
            if meal_name and meal_name != "N/A":
                passenger["meals"].append({
                    "segment_index": segment_index,
                    "code": "N/A",
                    "name": meal_name,
                })
        passengers.append(passenger)

    return {
        "source": IEP_SOURCE,
        "source_name": IEP_SOURCE_NAME,
        "booking": {
            "pnr": pnr,
            "booking_date": booking_date,
            "phone": phone,
            "currency": "N/A",
            "grand_total": None,
            "class_of_travel": "",
        },
        "gst_details": {"gst_number": "N/A", "company_name": "N/A"},
        "passengers": passengers,
        "segments": segments,
        "barcode": None,
        "extra": {
            "issuing_airline": issuing_airline,
        },
    }


def try_gds_iep_parse(raw_text: str) -> Optional[Dict]:
    if not is_gds_iep_format(raw_text):
        return None

    print("[GDS_IEP_PARSER] GDS-IEP format detected - bypassing LLM", flush=True)

    try:
        extracted = _extract_iep(raw_text)
    except Exception as exc:
        print(f"[GDS_IEP_PARSER] Extraction failed: {exc} - falling back", flush=True)
        return None

    if not extracted.get("segments"):
        print("[GDS_IEP_PARSER] No segments found - falling back", flush=True)
        return None

    unique_segments, index_remap = _dedupe_segments_with_remap(extracted["segments"])
    _remap_segment_refs(extracted["passengers"], index_remap)

    data = {
        "booking": extracted["booking"],
        "gst_details": extracted["gst_details"],
        "passengers": extracted["passengers"],
        "segments": unique_segments,
        "barcode": extracted.get("barcode"),
    }
    data = normalize_data(data)
    data = build_journey(data)

    warnings = []
    errors = []
    if data.get("booking", {}).get("pnr") == "N/A":
        warnings.append("PNR not found")
    for index, passenger in enumerate(data.get("passengers", [])):
        if passenger.get("name") == "N/A":
            errors.append(f"Passenger {index}: name missing")

    return {
        "metadata": {
            "version": PARSER_VERSION,
            "source": IEP_SOURCE,
            "source_name": IEP_SOURCE_NAME,
            "llm_status": "gds_iep_regex_only",
            "parsed_at": datetime.now(timezone.utc).isoformat() + "Z",
            "warnings": warnings,
            "errors": errors,
        },
        "booking": data.get("booking", {}),
        "gst_details": data.get("gst_details", {"gst_number": "N/A", "company_name": "N/A"}),
        "passengers": data.get("passengers", []),
        "segments": data.get("segments", []),
        "journey": data.get("journey", {}),
        "barcode": data.get("barcode"),
    }
