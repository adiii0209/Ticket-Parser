"""
gds_parser.py — Airline-Grade GDS Ticket Parser
=================================================
Parses GDS-format tickets (Amadeus PIR, Sabre, Galileo, Worldspan, Apollo)
directly via regex, bypassing the LLM entirely.

Handles:
  - One-way, round-trip, multi-city itineraries
  - Layovers and connections
  - Timezone-aware duration calculations
  - Multi-line city names (e.g. CASABLANCA / MOHAMMED V)
  - Terminal info, seat assignments, baggage
  - Fare breakdowns (base fare, taxes, surcharges, total)
  - Multiple passengers
  - All major GDS output formats

Integration:
  from gds_parser import try_gds_parse
  result = try_gds_parse(raw_text)
  if result is not None:
      # GDS format detected and parsed — skip LLM
      return result
  # else: not a GDS ticket, fall through to LLM
"""

import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

from mappings import (AIRPORT_CODES, AIRLINE_CODES, AIRPORT_TZ_MAP,
                      MEAL_CODES, ANCILLARY_CODES, resolve_booking_class)
from llm_extractor import (build_journey, normalize_data, normalize_name,
                           normalize_phone, normalize_baggage, PARSER_VERSION)

# ─────────────────────────────────────────────────────────────────────────────
# Detection patterns — must match ANY major GDS PIR or cryptic format
# ─────────────────────────────────────────────────────────────────────────────

_RE_GDS_DETECT = [
    # Amadeus PIR
    re.compile(r'ELECTRONIC\s+TICKET\s+PASSENGER\s+ITINERARY\s+RECEIPT', re.I),
    re.compile(r'PASSENGER\s+ITINERARY\s+RECEIPT', re.I),
    re.compile(r'BOOKING\s+REF\s*:\s*AMADEUS', re.I),
    re.compile(r'ETKT\s+\d{3}\s+\d{10}', re.I),
    re.compile(r'FARE\s+CALCULATION\s*:', re.I),
    # Sabre
    re.compile(r'\bSABRE\b.*(?:ITINERARY|RECEIPT|TICKET)', re.I),
    # Galileo / Apollo / Worldspan / Travelport
    re.compile(r'(?:GALILEO|APOLLO|TRAVELPORT|WORLDSPAN).*(?:ITINERARY|RECEIPT)', re.I),
    # Generic PIR with FROM/TO + FLIGHT + CL + DATE pattern
    re.compile(
        r'FROM\s*/\s*TO\s+FLIGHT\s+CL\s+DATE\s+DEP\s+FARE\s*BASIS', re.I),
]


def is_gds_format(text: str) -> bool:
    """Return True if text looks like a GDS-format ticket."""
    for pat in _RE_GDS_DETECT:
        if pat.search(text):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

_MONTH_MAP = {
    "jan":"Jan","feb":"Feb","mar":"Mar","apr":"Apr","may":"May","jun":"Jun",
    "jul":"Jul","aug":"Aug","sep":"Sep","oct":"Oct","nov":"Nov","dec":"Dec",
    "january":"Jan","february":"Feb","march":"Mar","april":"Apr","june":"Jun",
    "july":"Jul","august":"Aug","september":"Sep","october":"Oct",
    "november":"Nov","december":"Dec",
}

def _nd(day, mon, yr) -> str:
    m = _MONTH_MAP.get(str(mon).lower().rstrip("."), str(mon)[:3].capitalize())
    y = str(yr).strip()
    if len(y) == 4: y = y[2:]
    if not y: y = "26"
    return f"{int(day):02d} {m} {y}"

def _hhmm(raw: str) -> str:
    raw = raw.strip().replace(":", "")
    if len(raw) == 4 and raw.isdigit():
        return f"{raw[:2]}:{raw[2:]}"
    return raw

def _num(s: str) -> Optional[float]:
    try: return float(re.sub(r"[,\s]", "", s))
    except: return None

def _city(iata: str) -> str:
    return AIRPORT_CODES.get(iata.upper(), "N/A")

def _airline_name(code: str) -> str:
    return AIRLINE_CODES.get(code.upper(), "N/A")


# ─────────────────────────────────────────────────────────────────────────────
# City name → IATA resolution (comprehensive)
# ─────────────────────────────────────────────────────────────────────────────

_CITY_IATA: Dict[str, str] = {
    # India
    "KOLKATA":"CCU","CALCUTTA":"CCU","KOLKATA SUBHAS CHANDRA BOSE":"CCU","MUMBAI":"BOM","BOMBAY":"BOM",
    "DELHI":"DEL","NEW DELHI":"DEL","CHENNAI":"MAA","MADRAS":"MAA",
    "BENGALURU":"BLR","BANGALORE":"BLR","HYDERABAD":"HYD","KOCHI":"COK",
    "COCHIN":"COK","AHMEDABAD":"AMD","GOA":"GOI","JAIPUR":"JAI",
    "PUNE":"PNQ","AMRITSAR":"ATQ","CHANDIGARH":"IXC","LUCKNOW":"LKO",
    "GUWAHATI":"GAU","BHUBANESWAR":"BBI","VISAKHAPATNAM":"VTZ",
    "BAGDOGRA":"IXB","VARANASI":"VNS","PATNA":"PAT","NAGPUR":"NAG",
    "INDORE":"IDR","BHOPAL":"BHO","COIMBATORE":"CJB","MANGALORE":"IXE",
    "THIRUVANANTHAPURAM":"TRV","TRIVANDRUM":"TRV","SRINAGAR":"SXR",
    # Middle East
    "DOHA":"DOH","DUBAI":"DXB","ABU DHABI":"AUH","ABU DHABI ZAYED":"AUH","SHARJAH":"SHJ",
    "MUSCAT":"MCT","BAHRAIN":"BAH","KUWAIT":"KWI","RIYADH":"RUH",
    "JEDDAH":"JED","DAMMAM":"DMM",
    # Europe
    "LONDON HEATHROW":"LHR","LONDON GATWICK":"LGW","LONDON":"LHR",
    "PARIS CHARLES":"CDG","PARIS":"CDG","FRANKFURT":"FRA",
    "AMSTERDAM":"AMS","LISBON":"LIS","MADRID":"MAD","BARCELONA":"BCN",
    "ROME":"FCO","MILAN":"MXP","MUNICH":"MUC","VIENNA":"VIE",
    "ZURICH":"ZRH","BRUSSELS":"BRU","STOCKHOLM":"ARN","OSLO":"OSL",
    "COPENHAGEN":"CPH","HELSINKI":"HEL","ATHENS":"ATH",
    "ISTANBUL":"IST","ISTANBUL AIRPORT":"IST","ISTANBUL ATATURK":"IST",
    "ISTANBUL SABIHA":"SAW",
    # Africa
    "CASABLANCA":"CMN","CASABLANCA MOHAMMED":"CMN","MOHAMMED V":"CMN",
    "JOHANNESBURG":"JNB","NAIROBI":"NBO","ADDIS ABABA":"ADD",
    "CAIRO":"CAI","LAGOS":"LOS","ACCRA":"ACC","DAKAR":"DKR",
    "MARRAKECH":"RAK","FEZ":"FEZ","TANGIER":"TNG","ALGIERS":"ALG",
    "TUNIS":"TUN",
    # Asia/Pacific
    "SINGAPORE":"SIN","KUALA LUMPUR":"KUL","BANGKOK":"BKK",
    "HONG KONG":"HKG","TOKYO NARITA":"NRT","TOKYO HANEDA":"HND",
    "TOKYO":"NRT","SEOUL INCHEON":"ICN","SEOUL":"ICN",
    "SYDNEY":"SYD","MELBOURNE":"MEL","BRISBANE":"BNE",
    "COLOMBO":"CMB","KATHMANDU":"KTM","DHAKA":"DAC",
    "DENPASAR":"DPS","DENPASAR-BALI":"DPS","BALI":"DPS",
    "DENPASAR BALI":"DPS","NGURAH RAI":"DPS",
    "TACLOBAN":"TAC","TACLOBAN D Z":"TAC",
    "TACLOBAN DANIEL Z":"TAC","TACLOBAN DANIEL Z ROMUALDEZ":"TAC",
    "JAKARTA":"CGK","SURABAYA":"SUB","YOGYAKARTA":"JOG",
    "MANILA":"MNL","CEBU":"CEB","HANOI":"HAN",
    "HO CHI MINH":"SGN","SAIGON":"SGN",
    "TAIPEI":"TPE","SHANGHAI":"PVG","BEIJING":"PEK",
    "GUANGZHOU":"CAN","CHENGDU":"CTU","KUNMING":"KMG",
    # Americas
    "NEW YORK JFK":"JFK","NEW YORK NEWARK":"EWR","NEW YORK":"JFK",
    "LOS ANGELES":"LAX","CHICAGO":"ORD","CHICAGO O HARE":"ORD","SAN FRANCISCO":"SFO",
    "TORONTO":"YYZ","VANCOUVER":"YVR","MONTREAL":"YUL",
    "SAO PAULO":"GRU","RIO DE JANEIRO":"GIG","BUENOS AIRES":"EZE",
    "BOGOTA":"BOG","LIMA":"LIM","SANTIAGO":"SCL","MEXICO CITY":"MEX",
    "CANCUN":"CUN","PANAMA CITY":"PTY",
}


def _resolve_city_name(raw_city: str) -> str:
    """Map city name to IATA code. Handles multi-word, partial matches."""
    city = re.sub(r'\s+', ' ', raw_city.strip()).upper()
    # Remove noise words
    city = re.sub(
        r'\b(?:INTERNATIONAL|AIRPORT|TERMINAL|SUBHAS|CHANDRA|BOSE|HAMAD|'
        r'INDIRA|GANDHI|RAJIV|NETAJI|CHHATRAPATI|SHIVAJI|MAHARAJ|'
        r'KEMPEGOWDA|KING|QUEEN|ALIA|KHALID|FAHD|PRINCE|MOHAMED|'
        r'BEN GURION|ATATURK|SABIHA GOKCEN)\b', '', city).strip()
    city = re.sub(r'\s+', ' ', city).strip()

    # Direct match
    if city in _CITY_IATA:
        return _CITY_IATA[city]

    # Prefix match (longest first)
    for key in sorted(_CITY_IATA.keys(), key=len, reverse=True):
        if city.startswith(key) or key.startswith(city):
            return _CITY_IATA[key]

    # Try lookup in AIRPORT_CODES values
    for iata, name in AIRPORT_CODES.items():
        name_upper = name.upper()
        if city in name_upper or name_upper.startswith(city[:5]):
            return iata

    return "N/A"


def _normalize_city_for_match(raw_city: str) -> str:
    city = re.sub(r'\s+', ' ', raw_city.strip()).upper()
    city = re.sub(
        r'\b(?:INTERNATIONAL|AIRPORT|TERMINAL|INTL|SUBHAS|CHANDRA|BOSE|HAMAD|'
        r'INDIRA|GANDHI|RAJIV|NETAJI|CHHATRAPATI|SHIVAJI|MAHARAJ|'
        r'KEMPEGOWDA|KING|QUEEN|ALIA|KHALID|FAHD|PRINCE|MOHAMED|'
        r'BEN|GURION|ATATURK|SABIHA|GOKCEN|NINOY|AQUINO|DANIEL|ROMUALDEZ|D|Z)\b',
        '',
        city,
    )
    return re.sub(r'\s+', ' ', city).strip()


def _iata_matches_city(raw_city: str, iata: str) -> bool:
    """Return True when the candidate IATA is consistent with the parsed city text."""
    if not raw_city or not iata or iata == "N/A":
        return False

    resolved = _resolve_city_name(raw_city)
    if resolved != "N/A":
        return resolved == iata

    city_norm = _normalize_city_for_match(raw_city)
    airport_name = _normalize_city_for_match(AIRPORT_CODES.get(iata.upper(), ""))
    if not city_norm or not airport_name:
        return False

    return city_norm in airport_name or airport_name in city_norm


# ─────────────────────────────────────────────────────────────────────────────
# Schema templates
# ─────────────────────────────────────────────────────────────────────────────

def _seg(airline="N/A", flight_number="N/A", booking_class="N/A",
         dep_city="N/A", dep_airport="N/A", dep_date="N/A", dep_time="N/A", dep_terminal="N/A",
         arr_city="N/A", arr_airport="N/A", arr_date="N/A", arr_time="N/A", arr_terminal="N/A",
         duration_extracted="N/A") -> Dict:
    return {
        "airline": airline, "flight_number": flight_number, "booking_class": booking_class,
        "departure": {"city": dep_city, "airport": dep_airport, "date": dep_date,
                      "time": dep_time, "terminal": dep_terminal},
        "arrival":   {"city": arr_city, "airport": arr_airport, "date": arr_date,
                      "time": arr_time, "terminal": arr_terminal},
        "duration_extracted": duration_extracted,
    }

def _pax(name="N/A", pax_type="ADT", ticket_number="N/A",
         frequent_flyer_number="N/A", baggage="N/A") -> Dict:
    return {
        "name": name, "pax_type": pax_type, "ticket_number": ticket_number,
        "frequent_flyer_number": frequent_flyer_number, "baggage": baggage,
        "meals": [], "ancillaries": [],
        "fare": {"base_fare": None, "k3_gst": None, "other_taxes": None, "total_fare": None},
        "seats": [],
    }


def _dedupe_segments_with_remap(segments: List[Dict]) -> Tuple[List[Dict], Dict[int, int]]:
    """Return unique segments plus old_index -> new_index remap."""
    unique_segments: List[Dict] = []
    seen_segs: Dict[Tuple[str, str, str, str], int] = {}
    index_remap: Dict[int, int] = {}

    for old_idx, seg in enumerate(segments):
        key = (
            seg.get("flight_number"),
            seg.get("departure", {}).get("date"),
            seg.get("departure", {}).get("airport"),
            seg.get("arrival", {}).get("airport"),
        )
        new_idx = seen_segs.get(key)
        if new_idx is None:
            new_idx = len(unique_segments)
            seen_segs[key] = new_idx
            unique_segments.append(seg)
        index_remap[old_idx] = new_idx

    return unique_segments, index_remap


def _remap_segment_refs(passengers: List[Dict], index_remap: Dict[int, int]) -> None:
    """Collapse duplicate segment references after segment deduplication."""
    for pax in passengers:
        for field, identity_keys in (
            ("seats", ("seat_number",)),
            ("meals", ("code", "name")),
            ("ancillaries", ("code", "name")),
        ):
            items = pax.get(field, [])
            remapped_items = []
            seen_items = set()

            for item in items:
                old_idx = item.get("segment_index")
                if old_idx not in index_remap:
                    continue

                new_item = dict(item)
                new_item["segment_index"] = index_remap[old_idx]

                dedupe_key = (new_item["segment_index"],) + tuple(
                    new_item.get(key) for key in identity_keys
                )
                if dedupe_key in seen_items:
                    continue

                seen_items.add(dedupe_key)
                remapped_items.append(new_item)

            pax[field] = remapped_items


# ─────────────────────────────────────────────────────────────────────────────
# PIR Segment Parser (state-machine approach)
# ─────────────────────────────────────────────────────────────────────────────

_MONTHS = r'(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)'

# DEP line pattern — matches lines like:
#  CASABLANCA      TK 618  C  27MAR  1645     CBRT                      2PC  OK
_RE_DEP = re.compile(
    r'^[ \t]{1,3}'
    r'([A-Z][A-Z \'\-]{2,45}?)'         # dep city
    r'[ \t]+'                            # gap (some PIRs use a single space here)
    r'([A-Z0-9]{2})\s+'                 # airline code
    r'(\d{1,4}[A-Z]?)\s+'              # flight number
    r'([A-Z])\s+'                       # booking class
    r'(\d{1,2}(?:' + _MONTHS + r'))\s+' # date DDMON
    r'(\d{4})\s+'                       # dep time HHMM
    r'([A-Z0-9]+)'                      # fare basis
    r'(?:\s+.*?)?'                      # NVB/NVA (optional, greedy)
    r'(\d+(?:K[Gg]?|PC|pc))\s+'        # baggage
    r'(OK|HK|RR|HL|WL|TK|KL|DK|UN)',  # status
    re.MULTILINE | re.I
)

# ARR line — matches:
#  ISTANBUL           SEAT: 03B     ARRIVAL TIME: 2325   ARRIVAL DATE: 27MAR
_RE_ARR = re.compile(
    r'^[ \t]{1,3}'
    r'([A-Z][A-Z \'\-]{2,45}?)'
    r'(?:[ \t]{3,}(?:SEAT:\s*(\w+)\s+)?)?'
    r'ARRIVAL\s+TIME:\s*(\d{4})'
    r'.*?ARRIVAL\s+DATE:\s*(\d{1,2}(?:' + _MONTHS + r'))',
    re.MULTILINE | re.I
)

# SEAT line inside ARR context — SEAT: 03B
_RE_SEAT_INLINE = re.compile(r'SEAT:\s*(\d{1,3}[A-Z])', re.I)

# Terminal
_RE_TERMINAL = re.compile(r'TERMINAL\s*[:\s]+([A-Z0-9]{1,3})', re.I)

# Date parser for DDMON format
_RE_ADATE = re.compile(r'(\d{1,2})(' + _MONTHS + r')(\d{0,4})', re.I)
_RE_CITY_CONTINUATION = re.compile(r'^[A-Z][A-Z \'\-/0-9]+$', re.I)
_RE_CITY_CONTINUATION_SKIP = re.compile(
    r'^(?:'
    r'FLIGHT OPERATED BY|MARKETED BY|AT CHECK|BAGGAGE POLICY|CARRY-ON BAG|'
    r'FROM /TO|ISSUING AIRLINE|TICKET NUMBER|BOOKING REF|DATE|AGENT|NAME|IATA|'
    r'TELEPHONE|PAYMENT|ENDORSEMENTS?|FARE CALCULATION|AIR FARE|TAX|TOTAL|'
    r'AIRLINE SURCHARGES|NOTICE|SOURCE|THIS DOCUMENT|PLEASE DO NOT|'
    r'FLIGHT\(S\) EMISSIONS|GST[A-Z]'
    r')\b',
    re.I,
)


def _parse_adate(raw: str, default_yr: str = "26") -> str:
    raw = raw.strip()
    m = _RE_ADATE.match(raw)
    if not m:
        return "N/A"
    y = m.group(3).strip() if m.group(3).strip() else default_yr
    return _nd(m.group(1), m.group(2), y)


def _infer_year(text: str) -> str:
    """Extract default year from DATE: DD MON YYYY in header."""
    m = re.search(r'DATE:\s*\d{1,2}\s+\w+\s+(\d{4})', text, re.I)
    if m:
        return m.group(1)[2:]
    return "26"


def _is_city_continuation(line: str) -> bool:
    """Return True for uppercase city/airport continuation lines inside PIR blocks."""
    nln = line.strip()
    if not nln or len(nln) > 40:
        return False
    if ":" in nln:
        return False
    if not _RE_CITY_CONTINUATION.match(nln):
        return False
    if _RE_CITY_CONTINUATION_SKIP.match(nln):
        return False
    return True


def _parse_pir_segments(text: str) -> Tuple[List[Dict], List[str]]:
    """
    Parse PIR-format segments using a line-by-line state machine.
    Returns (segments, seat_assignments) where seat_assignments[i] is the
    seat for segment i (or None).
    """
    segments: List[Dict] = []
    seats: List[Optional[str]] = []
    lines = text.split('\n')
    default_yr = _infer_year(text)
    header_routes = [(m.group(1).upper(), m.group(2).upper()) for m in _RE_HEADER_ROUTE.finditer(text)]

    i = 0
    while i < len(lines):
        ln = lines[i]
        m = _RE_DEP.match(ln)
        if not m:
            i += 1
            continue

        dep_city_raw = m.group(1).strip()
        al_code      = m.group(2).upper()
        flt_no       = f"{al_code} {m.group(3)}"
        bk_class     = m.group(4).upper()
        dep_date     = _parse_adate(m.group(5), default_yr)
        dep_time     = _hhmm(m.group(6))
        baggage_raw  = m.group(8)
        baggage = re.sub(r'(\d+)K[Gg]?$', r'\1 Kg', baggage_raw, flags=re.I)
        baggage = re.sub(r'(\d+)(?:PC|pc)$', r'\1 Piece', baggage, flags=re.I)

        dep_terminal = "N/A"
        arr_city_raw = ""
        arr_time     = "N/A"
        arr_date     = dep_date
        arr_terminal = "N/A"
        seat_num     = None

        # Scan ahead
        j = i + 1
        found_arr = False
        while j < len(lines):
            nln = lines[j].strip()

            # Empty line after arrival block = end of this segment
            if not nln and found_arr:
                j += 1
                break

            # Terminal before arrival = departure terminal
            tm = _RE_TERMINAL.search(lines[j])
            if tm and not found_arr:
                dep_terminal = tm.group(1).upper()
                j += 1
                continue

            # Terminal after arrival = arrival terminal
            if tm and found_arr:
                arr_terminal = tm.group(1).upper()
                j += 1
                continue

            # Arrival line
            am = _RE_ARR.match(lines[j])
            if am:
                arr_city_raw = am.group(1).strip()
                if am.group(2):
                    seat_num = am.group(2)
                arr_time = _hhmm(am.group(3))
                arr_date = _parse_adate(am.group(4), default_yr)
                found_arr = True
                j += 1
                continue

            # Seat inline on a separate line near arrival
            if found_arr:
                sm = _RE_SEAT_INLINE.search(lines[j])
                if sm and not seat_num:
                    seat_num = sm.group(1)

            # City continuation line (e.g. "MOHAMMED V" or "NGURAH RAI")
            if (_is_city_continuation(lines[j])
                    and not _RE_DEP.match(lines[j])
                    and not _RE_ARR.match(lines[j])):
                if found_arr:
                    arr_city_raw = f"{arr_city_raw} {nln}".strip()
                else:
                    dep_city_raw = f"{dep_city_raw} {nln}".strip()
                j += 1
                continue

            # Next DEP line = stop
            if _RE_DEP.match(lines[j]):
                break

            j += 1

        i = j

        dep_ap = _resolve_city_name(dep_city_raw)
        arr_ap = _resolve_city_name(arr_city_raw)

        route_hint = header_routes[len(segments)] if len(segments) < len(header_routes) else None
        if route_hint:
            hinted_dep, hinted_arr = route_hint
            if dep_ap == "N/A" and _iata_matches_city(dep_city_raw, hinted_dep):
                dep_ap = hinted_dep
            if arr_ap == "N/A" and _iata_matches_city(arr_city_raw, hinted_arr):
                arr_ap = hinted_arr

        segments.append(_seg(
            airline=_airline_name(al_code),
            flight_number=flt_no,
            booking_class=bk_class,
            dep_city=_city(dep_ap) if dep_ap != "N/A" else dep_city_raw.strip().title(),
            dep_airport=dep_ap,
            dep_date=dep_date,
            dep_time=dep_time,
            dep_terminal=dep_terminal,
            arr_city=_city(arr_ap) if arr_ap != "N/A" else arr_city_raw.strip().title(),
            arr_airport=arr_ap,
            arr_date=arr_date,
            arr_time=arr_time,
            arr_terminal=arr_terminal,
        ))
        seats.append(seat_num)

    return segments, seats


# ─────────────────────────────────────────────────────────────────────────────
# Cryptic GDS segment parser (single-line format)
# ─────────────────────────────────────────────────────────────────────────────

_RE_CRYPTIC_SEG = re.compile(
    r'^\s*\d+\s+([A-Z0-9]{2})\s*(\d{1,4}[A-Z]?)\s+([A-Z])\s+'
    r'(\d{1,2}(?:' + _MONTHS + r')\d{0,4})\s+'
    r'([A-Z]{3})([A-Z]{3})\w*\s+'
    r'(?:[A-Z]{2}\d\s+)?'
    r'(\d{4})\s+(\d{4})'
    r'(?:\s+(\d{1,2}(?:' + _MONTHS + r')\d{0,4}))?',
    re.MULTILINE | re.I
)


def _parse_cryptic_segments(text: str) -> List[Dict]:
    """Parse cryptic GDS single-line segments."""
    segments = []
    default_yr = _infer_year(text)
    for m in _RE_CRYPTIC_SEG.finditer(text):
        dep_ap, arr_ap = m.group(5).upper(), m.group(6).upper()
        segments.append(_seg(
            airline=_airline_name(m.group(1)),
            flight_number=f"{m.group(1).upper()} {m.group(2)}",
            booking_class=m.group(3).upper(),
            dep_city=_city(dep_ap), dep_airport=dep_ap,
            dep_date=_parse_adate(m.group(4), default_yr),
            dep_time=_hhmm(m.group(7)),
            arr_city=_city(arr_ap), arr_airport=arr_ap,
            arr_date=_parse_adate(m.group(9), default_yr) if m.group(9) else _parse_adate(m.group(4), default_yr),
            arr_time=_hhmm(m.group(8)),
        ))
    return segments


# ─────────────────────────────────────────────────────────────────────────────
# Field extractors
# ─────────────────────────────────────────────────────────────────────────────

_RE_PNR_AMADEUS = re.compile(r'AMADEUS\s*:\s*([A-Z0-9]{6})\b', re.I)
_RE_PNR_AIRLINE = re.compile(r'AIRLINE\s*:\s*[A-Z]{2}/([A-Z0-9]{6})\b', re.I)
_RE_PNR_GENERIC = re.compile(
    r'(?:BOOKING\s*REF(?:ERENCE)?|RECORD\s*LOCATOR|PNR|RECLOC)\s*[:\s/\-]*([A-Z0-9]{6})\b', re.I)

_RE_BK_DATE = re.compile(
    r'DATE\s*[:\s]+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{4})', re.I)

_RE_TICKET = re.compile(r'ETKT\s+(\d{3})\s+(\d{10})(-\d+)?', re.I)
_RE_TICKET_13 = re.compile(
    r'TICKET\s+NUMBER\s*[:\s]*(?:ETKT\s+)?(\d{3})\s*(\d{10})(-\d+)?',
    re.I,
)

_RE_PAX_LABEL = re.compile(
    r'NAME\s*[:\s]+([A-Z]+)/([A-Z][A-Z\s]+?)(?:\s+(MR|MRS|MS|MISS|DR|MSTR))?'
    r'(?=\s*\n|\s{2,}|$)', re.I)
_RE_PAX_BARE = re.compile(
    r'^\s*([A-Z]{2,})/([A-Z][A-Z\s]+?)\s+(MR|MRS|MS|MISS|DR|MSTR)\b', re.MULTILINE)
_RE_PAX_HEADER = re.compile(
    r'^([A-Z]{2,})/([A-Z][A-Z\s]+?)\s+(MR|MRS|MS|MISS|DR|MSTR)\s+\d{1,2}(?:' + _MONTHS + r')',
    re.MULTILINE | re.I)

_RE_PHONE = re.compile(r'TELEPHONE\s*[:\s]+(\(?\d{2,4}\)?[\d\s\-]{6,20}\d)', re.I)
_RE_PHONE2 = re.compile(r'(?:CTCM|CTCH|P-|PHONE|CONTACT)\s*[:\s]*([+\d][\d\s\-]{7,20}\d)', re.I)
_RE_GSTN = re.compile(r'^\s*GSTN\b.*?/([0-9A-Z]{15})/([^\n]+)$', re.MULTILINE | re.I)

_RE_AGENCY = re.compile(
    r'^[ \t]+([A-Z][A-Z\s&.,]+?(?:LTD|PVT|INC|CORP|CO\.?|TRAVELS?|TOURS?))[ \t]+DATE',
    re.MULTILINE | re.I)

_RE_ISSUING_AIRLINE = re.compile(r'ISSUING\s+AIRLINE\s*[:\s]+(.+?)(?:\s*\n)', re.I)

_RE_AIR_FARE = re.compile(r'AIR\s*FARE\s*[:\s]+(?:(\w{3})\s+)?(\d[\d,]+)', re.I)
_RE_TOTAL = re.compile(r'^\s*TOTAL\s*[:\s]+(?:(\w{3})\s+)?(\d[\d,]+)', re.MULTILINE | re.I)
_RE_EQUIV_FARE = re.compile(r'EQUIV\s+FARE\s+PAID\s*[:\s]+(?:(\w{3})\s+)?(\d[\d,]+)', re.I)
_RE_PAYMENT = re.compile(r'PAYMENT\s*[:\s]+(\w+)', re.I)
_RE_ENDORSEMENT = re.compile(r'ENDORSEMENTS?\s*[:\s]+(.+?)(?:\n\s*(?:EXCHANGE|PAYMENT|FARE\s+CALC))', re.I | re.S)
_RE_EXCHANGE_RATE = re.compile(r'EXCHANGE\s+RATE\s*[:\s]+([\d.]+)\s*(\w+)?', re.I)

# Tax lines: INR 4009A9 INR 1343MA etc.
_RE_TAX_ITEMS = re.compile(r'(?:INR|USD|EUR|GBP|MAD|AED)\s+(\d[\d,]*)([A-Z0-9]{2,4})', re.I)
_RE_YQ = re.compile(r'(\d[\d,]*)YQ\b')
_RE_YR = re.compile(r'(\d[\d,]*)YR\b')
_RE_K3 = re.compile(r'(?:INR|USD)\s+(\d[\d,]*)K3\b', re.I)

_RE_IATA_NUM = re.compile(r'IATA\s*[:\s]*([\d\s]+)', re.I)
_RE_HEADER_ROUTE = re.compile(
    r'^\s*[A-Z]{2,}/[A-Z][A-Z\s]+?\s+(?:MR|MRS|MS|MISS|DR|MSTR)\s+'
    r'\d{1,2}(?:' + _MONTHS + r')\s+([A-Z]{3})\s+([A-Z]{3})\s*$',
    re.MULTILINE | re.I,
)

# Seat from arrival line
_RE_SEAT_ARR = re.compile(r'SEAT:\s*(\d{1,3}[A-Z])', re.I)

_RE_FF = re.compile(r'(?:FQTV|FFN|FF\s+NO|LOYALTY)\s+([A-Z0-9]{5,15})\b', re.I)


# ─────────────────────────────────────────────────────────────────────────────
# Main GDS extraction
# ─────────────────────────────────────────────────────────────────────────────

def _extract_gds(text: str) -> Dict:
    """Full GDS extraction pipeline."""

    # ── PNR ──
    m = _RE_PNR_AMADEUS.search(text)
    pnr = m.group(1).upper() if m else None
    if not pnr:
        m = _RE_PNR_GENERIC.search(text)
        pnr = m.group(1).upper() if m else "N/A"

    # ── Airline PNR ──
    airline_pnr = None
    m = _RE_PNR_AIRLINE.search(text)
    if m:
        airline_pnr = m.group(1).upper()

    # ── Booking date ──
    m = _RE_BK_DATE.search(text)
    bk_date = _nd(m.group(1), m.group(2), m.group(3)) if m else "N/A"

    # ── Issuing airline ──
    m = _RE_ISSUING_AIRLINE.search(text)
    issuing_airline = m.group(1).strip().title() if m else "N/A"

    # ── Tickets ──
    tickets = []
    for m in _RE_TICKET.finditer(text):
        tickets.append(m.group(1) + m.group(2) + (m.group(3) or ""))
    if not tickets:
        for m in _RE_TICKET_13.finditer(text):
            tickets.append(m.group(1) + m.group(2) + (m.group(3) or ""))

    # ── Passenger names ──
    pax_names = []
    seen = set()
    # Try header line first (NAME/SURNAME FIRST)
    for m in _RE_PAX_HEADER.finditer(text):
        last = m.group(1).strip().title()
        first = m.group(2).strip().title()
        title = (m.group(3) or "").strip().title()
        name = f"{title} {first} {last}".strip()
        if name.lower() not in seen:
            seen.add(name.lower()); pax_names.append(name)
    if not pax_names:
        for m in _RE_PAX_LABEL.finditer(text):
            last = m.group(1).strip().title()
            first = m.group(2).strip().title()
            title = (m.group(3) or "").strip().title()
            name = f"{title} {first} {last}".strip()
            if name.lower() not in seen:
                seen.add(name.lower()); pax_names.append(name)
    if not pax_names:
        for m in _RE_PAX_BARE.finditer(text):
            last = m.group(1).strip().title()
            first = m.group(2).strip().title()
            title = (m.group(3) or "").strip().title()
            name = f"{title} {first} {last}".strip()
            if name.lower() not in seen:
                seen.add(name.lower()); pax_names.append(name)

    # ── Phone ──
    m = _RE_PHONE.search(text) or _RE_PHONE2.search(text)
    phone = re.sub(r'[\s\-()]', '', m.group(1)) if m else "N/A"
    if phone != "N/A" and not phone.startswith("+"):
        if phone.startswith("0") and len(phone) >= 11:
            phone = f"+91{phone[1:]}"
        elif len(phone) == 10:
            phone = f"+91{phone}"

    # ── Agency ──
    m = _RE_AGENCY.search(text)
    agency = m.group(1).strip().title() if m else "N/A"

    # ── GST ──
    m = _RE_GSTN.search(text)
    gst_number = m.group(1).strip().upper() if m else "N/A"
    gst_company = re.sub(r'\s+', ' ', m.group(2).strip()) if m else "N/A"

    # ── Fares ──
    fare_currency = None

    m = _RE_AIR_FARE.search(text)
    base_fare_raw = m.group(2) if m else None
    if m and m.group(1): fare_currency = m.group(1).upper()
    base_f = _num(base_fare_raw) if base_fare_raw else None

    m = _RE_EQUIV_FARE.search(text)
    equiv_fare = None
    if m:
        equiv_fare = _num(m.group(2))
        if m.group(1): fare_currency = m.group(1).upper()

    m = _RE_TOTAL.search(text)
    total_raw = m.group(2) if m else None
    if m and m.group(1): fare_currency = m.group(1).upper()
    grand_t = _num(total_raw) if total_raw else None

    # Currency detection
    if not fare_currency:
        cur_m = re.search(r'\b(INR|USD|EUR|GBP|AED|SGD|MAD)\b', text)
        fare_currency = cur_m.group(1) if cur_m else "INR"

    # K3
    m = _RE_K3.search(text)
    k3 = _num(m.group(1)) if m else None

    # YQ + YR surcharges
    yq_total = sum(_num(m.group(1)) or 0 for m in _RE_YQ.finditer(text))
    yr_total = sum(_num(m.group(1)) or 0 for m in _RE_YR.finditer(text))
    surcharges = (yq_total + yr_total) if (yq_total or yr_total) else None

    # Tax items
    tax_sum = 0
    for m in _RE_TAX_ITEMS.finditer(text):
        code = m.group(2).upper()
        if code not in ('YQ', 'YR'):
            tax_sum += _num(m.group(1)) or 0
    other_taxes = tax_sum if tax_sum > 0 else None

    # Payment
    m = _RE_PAYMENT.search(text)
    payment = m.group(1).strip().title() if m else "N/A"

    # Endorsement
    m = _RE_ENDORSEMENT.search(text)
    endorsement = re.sub(r'\s+', ' ', m.group(1).strip()) if m else "N/A"

    # ── Segments ──
    segments, seat_list = _parse_pir_segments(text)
    if not segments:
        segments = _parse_cryptic_segments(text)
        seat_list = [None] * len(segments)
        # Try extracting seats from SEAT: lines for cryptic format
        seat_matches = [m.group(1) for m in _RE_SEAT_ARR.finditer(text)]
        for si in range(min(len(seat_matches), len(segments))):
            seat_list[si] = seat_matches[si]

    # ── Determine baggage from segments if not found ──
    baggage = "N/A"
    if segments:
        # Get baggage from first segment's raw parsing (stored during parse)
        bag_matches = re.findall(r'\b(\d+(?:K[Gg]?|PC|pc))\b.*?(?:OK|HK|RR|HL|WL|TK)', text, re.I)
        if bag_matches:
            raw_b = bag_matches[0]
            baggage = re.sub(r'(\d+)K[Gg]?$', r'\1 Kg', raw_b, flags=re.I)
            baggage = re.sub(r'(\d+)(?:PC|pc)$', r'\1 Piece', baggage, flags=re.I)

    # FF
    ff_nums = [m.group(1) for m in _RE_FF.finditer(text)]

    # ── Determine class of travel from booking class ──
    class_of_travel = "N/A"
    if segments:
        bk_cls = segments[0].get("booking_class", "N/A")
        if bk_cls and bk_cls != "N/A" and len(bk_cls) == 1:
            from mappings import BOOKING_CLASS_GENERIC
            if bk_cls in BOOKING_CLASS_GENERIC:
                class_of_travel = BOOKING_CLASS_GENERIC[bk_cls][0]

    # ── GDS source detection ──
    source = "gds_generic"
    source_name = "GDS"
    if re.search(r'AMADEUS', text, re.I):
        source, source_name = "amadeus", "Amadeus GDS"
    elif re.search(r'SABRE', text, re.I):
        source, source_name = "sabre", "Sabre GDS"
    elif re.search(r'GALILEO|APOLLO|TRAVELPORT', text, re.I):
        source, source_name = "galileo", "Galileo/Apollo GDS"
    elif re.search(r'WORLDSPAN', text, re.I):
        source, source_name = "worldspan", "Worldspan GDS"

    # ── Build passengers ──
    n_pax = max(len(pax_names), 1)
    passengers: List[Dict] = []
    for pi in range(n_pax):
        p = _pax(
            name=pax_names[pi] if pi < len(pax_names) else "N/A",
            ticket_number=tickets[pi] if pi < len(tickets) else "N/A",
            frequent_flyer_number=ff_nums[pi] if pi < len(ff_nums) else "N/A",
            baggage=baggage,
        )
        p["fare"] = {
            "base_fare": equiv_fare or base_f,
            "k3_gst": k3,
            "other_taxes": other_taxes,
            "total_fare": grand_t,
        }
        # Assign seats
        for si, seat in enumerate(seat_list):
            if seat:
                p["seats"].append({"segment_index": si, "seat_number": seat})
        passengers.append(p)

    return {
        "source": source,
        "source_name": source_name,
        "booking": {
            "pnr": pnr,
            "booking_date": bk_date,
            "phone": phone,
            "currency": fare_currency,
            "grand_total": grand_t,
            "class_of_travel": class_of_travel,
        },
        "gst_details": {"gst_number": gst_number, "company_name": gst_company},
        "passengers": passengers,
        "segments": segments,
        "barcode": None,
        "extra": {
            "issuing_airline": issuing_airline,
            "agency_name": agency,
            "airline_pnr": airline_pnr,
            "payment": payment,
            "endorsement": endorsement,
            "surcharges": surcharges,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def try_gds_parse(raw_text: str) -> Optional[Dict]:
    """
    Attempt to parse raw_text as a GDS ticket.

    Returns:
        Full parsed result dict (same schema as llm_extractor.extract())
        if GDS format is detected, or None if not a GDS ticket.
    """
    if not is_gds_format(raw_text):
        return None

    print("[GDS_PARSER] GDS format detected — bypassing LLM", flush=True)

    try:
        extracted = _extract_gds(raw_text)
    except Exception as e:
        print(f"[GDS_PARSER] Extraction failed: {e} — falling back to LLM", flush=True)
        return None

    # Check if we got meaningful data
    if not extracted.get("segments"):
        print("[GDS_PARSER] No segments found — falling back to LLM", flush=True)
        return None

    source = extracted.get("source", "gds_generic")
    source_name = extracted.get("source_name", "GDS")

    # Deduplicate segments (e.g., if PDF has multiple pages with the same itinerary)
    unique_segments, index_remap = _dedupe_segments_with_remap(extracted["segments"])
    _remap_segment_refs(extracted["passengers"], index_remap)

    data = {
        "booking":     extracted["booking"],
        "gst_details": extracted["gst_details"],
        "passengers":  extracted["passengers"],
        "segments":    unique_segments,
        "barcode":     extracted.get("barcode"),
    }

    # Normalize
    data = normalize_data(data)

    # Journey pipeline
    data = build_journey(data)

    # Validation
    warnings, errors = [], []
    bk = data.get("booking", {})
    if not bk.get("pnr") or bk["pnr"] == "N/A":
        warnings.append("PNR not found")
    if not bk.get("grand_total"):
        warnings.append("grand_total not found")
    for i, seg in enumerate(data.get("segments", [])):
        dep, arr = seg.get("departure", {}), seg.get("arrival", {})
        if dep.get("airport") == "N/A": warnings.append(f"Segment {i}: departure airport missing")
        if arr.get("airport") == "N/A": warnings.append(f"Segment {i}: arrival airport missing")
        if dep.get("airport") == arr.get("airport") != "N/A":
            errors.append(f"Segment {i}: departure == arrival ({dep['airport']})")
    for i, pax in enumerate(data.get("passengers", [])):
        if not pax.get("name") or pax["name"] == "N/A":
            errors.append(f"Passenger {i}: name missing")

    n_segs = len(data.get("segments", []))
    n_pax = len(data.get("passengers", []))
    jrn = data.get("journey", {})

    print(f"[GDS_PARSER] OK Parsed: {source_name} | {n_segs} segments | "
          f"{n_pax} pax | {jrn.get('trip_type_display', '?')}", flush=True)

    return {
        "metadata": {
            "version": PARSER_VERSION,
            "source": source,
            "source_name": source_name,
            "llm_status": "gds_regex_only",
            "parsed_at": datetime.now(timezone.utc).isoformat() + "Z",
            "warnings": warnings,
            "errors": errors,
        },
        "booking":     data.get("booking", {}),
        "gst_details": data.get("gst_details", {"gst_number": "N/A", "company_name": "N/A"}),
        "passengers":  data.get("passengers", []),
        "segments":    data.get("segments", []),
        "journey":     data.get("journey", {}),
        "barcode":     data.get("barcode"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Self-test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    SAMPLE = """
BAJEDDOUB/AHMED MR 27MAR CMN IST

------------------------------------------------------------------------------

This document is automatically generated.
Please do not respond to this mail.


                            ELECTRONIC TICKET
                       PASSENGER ITINERARY RECEIPT

 TIME TRAVELS LTD                   DATE: 23 MAR 2026
 13 CAMAC STREET                   AGENT: 1212
                                    NAME: BAJEDDOUB/AHMED MR
 CALCUTTA 700 017
 IATA       : 143 27821
 TELEPHONE  : (033)40011333

 ISSUING AIRLINE                        : TURKISH AIRLINES
 TICKET NUMBER                          : ETKT 235 5895018854
 BOOKING REF : AMADEUS: 7W59DQ, AIRLINE: TK/VV7D3K

 FROM /TO        FLIGHT  CL DATE   DEP      FARE BASIS    NVB   NVA   BAG  ST

 CASABLANCA      TK 618  C  27MAR  1645     CBRT                      2PC  OK
 MOHAMMED V
 TERMINAL:2
 ISTANBUL           SEAT: 03B     ARRIVAL TIME: 2325   ARRIVAL DATE: 27MAR
 ISTANBUL
 AIRPORT

 ISTANBUL        TK 310  C  29MAR  0220     CBRT                      2PC  OK
 ISTANBUL
 AIRPORT
 DENPASAR-BALI      SEAT: 02G     ARRIVAL TIME: 1955   ARRIVAL DATE: 29MAR
 NGURAH RAI

 DENPASAR-BALI   TK 311  C  06APR  2145     CBRT                      2PC  OK
 NGURAH RAI
 ISTANBUL           SEAT: 03E     ARRIVAL TIME: 0545   ARRIVAL DATE: 07APR
 ISTANBUL
 AIRPORT

 ISTANBUL        TK 617  C  07APR  1205     CBRT                      2PC  OK
 ISTANBUL
 AIRPORT
 CASABLANCA         SEAT: 02J     ARRIVAL TIME: 1455   ARRIVAL DATE: 07APR
 MOHAMMED V
 TERMINAL:2


 AT CHECK-IN, PLEASE SHOW A PICTURE IDENTIFICATION AND THE DOCUMENT YOU GAVE
 FOR REFERENCE AT RESERVATION TIME

 ENDORSEMENTS  : NONEND/TK ONLY
 EXCHANGE RATE : 10.02089273 INR
 PAYMENT       : CASH

 FARE CALCULATION   :CMN TK IST TK DPS Q CMNDPS45.00 2850.33TK X/IST Q45.00TK
                     CMN Q DPSCMN45.00 2850.33NUC5835.66END ROE9.255048XT
                     4009A91343MA481MA537M62683TR1321D532D5

 AIR FARE           : MAD     54010
 EQUIV FARE PAID    : INR     541230
 TAX                : INR     4009A9    INR     1343MA    INR     481MA
                      INR     537M6     INR     2683TR    INR     1321D5
                      INR     32D5
 AIRLINE SURCHARGES : INR     2610YQ    INR     73066YR
 TOTAL              : INR     627312


FLIGHT(S) EMISSIONS 1897.97 KG CO2 PER TOTAL NUMBER IN PARTY
SOURCE: THE DYNAMIC FIELDS COMING FROM TRAVEL IMPACT EXPLORER API RESPONSE
"""

    result = try_gds_parse(SAMPLE)
    if result:
        from llm_extractor import print_result
        print_result(result)
    else:
        print("NOT a GDS format ticket")
