"""
IndiGo ticket email parser  —  regex-first, no LLM required.

Handles:
  * One-way, return, multi-city, layover itineraries
  * 1-N passengers (40+ tested)
  * HTML-table-stripped emails (all columns concatenated on one line)
  * Well-formatted plain-text emails (two-line segment format)
  * Compact / PDF-ripped single-line format
  * Seats / meals / ancillaries per passenger per segment
  * GST, contact, baggage, fare extraction

Key design notes
----------------
1.  _normalize_text() is called ONCE at the top of try_indigo_parse().
    The clean string is passed everywhere. No function re-normalises.

2.  Segment extraction has THREE strategies (tried in order):
      S1  Line-pair   - standard plain-text IndiGo emails
      S2  Table-row   - HTML email pasted as text (the 40-pax format)
      S3  Compact     - PDF/forwarded, everything on one joined line

3.  Passenger names are found via standalone titled-name lines.
    Names appearing N times (once per leg) are deduplicated by
    lower-cased key, so round-trips don't produce duplicate passengers.

4.  Round-trip segment deduplication: _seg_key() includes arr_date so
    outbound BOM->CMB and return CMB->BOM are kept as separate segments.
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from mappings import AIRLINE_CODES, AIRPORT_CODES, ANCILLARY_CODES, MEAL_CODES, search_by_name
from llm_extractor import build_journey, normalize_data, normalize_name, _extract_pnr

INDIGO_PARSER_VERSION = "indigo_regex_v3.0"

# ---------------------------------------------------------------------------
# Primitive building-block pattern strings (not compiled here)
# ---------------------------------------------------------------------------
_MONTH = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?"
    r"|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
)
_DATE = r"\d{1,2}\s+" + _MONTH + r"\s+\d{2,4}"
_TIME = r"\d{1,2}:\d{2}"
_FN   = r"6E\s*-?\s*\d{1,4}[A-Z]?"
_TITLE = r"(?:Mr\.|Mrs\.|Ms\.|Miss|Mstr\.|Master|Dr\.|Prof\.|Mr|Mrs|Ms|Mstr|Dr|Prof)"

# City char class: MUST include \d to handle terminal suffixes like "T2" in "Mumbai (T2)"
# Without \d the regex cannot consume the digit in "(T2)" and the whole match fails.
_CITY_CC = r"[A-Za-z\d ()/'&\-\.]"

# ---------------------------------------------------------------------------
# Format-detection markers  (>= 2 must match)
# ---------------------------------------------------------------------------
_INDIGO_MARKERS = [
    re.compile(r"PNR/Booking\s*Ref\.", re.I),
    re.compile(r"IndiGo\s+(?:Flight|Booking|Confirmation|Passenger)", re.I),
    re.compile(r"Interglobe\s+Aviation", re.I),
    re.compile(r"\b6E\s*[-\s]?\d{3,4}\b"),
]

# ---------------------------------------------------------------------------
# Booking / meta regexes
# ---------------------------------------------------------------------------
_RE_PNR = re.compile(
    r"PNR\s*/\s*Booking\s*Ref\.?\s*:\s*([A-Z0-9]{5,8})\b",
    re.I,
)
_RE_BOOKING_ROW = re.compile(
    r"^\s*(CONFIRMED|CANCELLED|WAITLISTED|HOLD)\s+(" + _DATE + r")"
    r"(?:\s+\d{2}:\d{2}:\d{2})?\s*\(UTC\)?\s+([A-Za-z]+)\s*$",
    re.I | re.M,
)
_RE_TOTAL_PAX  = re.compile(r"IndiGo\s+Passenger\s*-\s*\d+\s*/\s*(\d+)", re.I)
_RE_FARE_TYPE  = re.compile(r"Fare\s*Type\s*:\s*([A-Za-z0-9 +/&-]+)", re.I)
_RE_BAGGAGE    = re.compile(r"Check-?in\s*Baggage\s*:\s*(\d+)\s*[Kk][Gg]", re.I)
_RE_HAND_BAGGAGE = re.compile(r"Hand\s*Baggage\s*:\s*.*?up to\s*(\d+)\s*[Kk][Gg]", re.I)
_RE_CURRENCY   = re.compile(r"\b(INR|USD|AED|SAR|EUR|GBP)\b")
_RE_TOTAL_FARE = re.compile(
    r"(?:Grand\s*)?Total\s*(?:Fare|Amount|Charges)?\s*[:\-]?\s*(?:INR|Rs\.?)?\s*([\d,]+(?:\.\d{1,2})?)",
    re.I,
)
_RE_GST_CO  = re.compile(r"GST\s*Company\s*Name\s*:\s*(.+)", re.I)
_RE_GST_NUM = re.compile(
    r"GST\s*(?:Number|No\.?)\s*:\s*([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z])",
    re.I,
)
_RE_COMPANY = re.compile(r"(?:Company|Agency)\s*Name\s*:\s*(.+)", re.I)
_RE_PHONE   = re.compile(r"(?:Home\s*)?Phone\s*:\s*([+\d*][\d*\- ]{7,20})", re.I)
_RE_EMAIL   = re.compile(r"Email\s*:\s*([^\s]+@[^\s]+)", re.I)
_RE_ADDRESS = re.compile(r"Address\s*:\s*(.+)", re.I)

# ---------------------------------------------------------------------------
# Passenger name: a line containing ONLY a titled name, nothing else
# ---------------------------------------------------------------------------
_RE_NAME_LINE = re.compile(
    r"^\s*(" + _TITLE + r"\s+[A-Za-z][A-Za-z \.'\\-]{1,60}?)\s*$",
    re.I,
)

# ---------------------------------------------------------------------------
# Segment patterns
# ---------------------------------------------------------------------------

# S1: two-line format
#   Line A: <date>  <city>  <dep_time>  <flight>
#   Line B: <class> <iata>  <???> <arr_city> <arr_time>
_RE_S1A = re.compile(
    r"^(" + _DATE + r")\s+(.+?)\s+(" + _TIME + r")\s+(" + _FN + r")\s*$",
    re.I,
)
_RE_S1B = re.compile(
    r"^\(?([A-Z0-9]{1,3})\)?\s+([A-Z]{3})\s+(" + _TIME + r")\s+(.+?)\s+(" + _TIME + r"(?:\+\d+)?)\s*$",
    re.I,
)

# S2: HTML-table-stripped format — ALL columns run together with no separator.
#
# Real example (after _normalize_text which converts \xa0 -> space):
#   "DateFrom (Terminal)DepartsFlight Number (Aircraft type)Check-in/Bag drop closesTo (Terminal)ArrivesVia
#    03 Apr 26Mumbai (T2)06:156E1185   (A320)05:00Colombo08:45"
#
# IndiGo table column order:
#   Date | From(Terminal) | Departs | FlightNo(Aircraft) | Checkin-close | To(Terminal) | Arrives
#
# The aircraft type "(A320)" and check-in-close time are consumed and discarded.
# Both city groups use _CITY_CC which includes \d (mandatory for "Mumbai (T2)").
_RE_S2 = re.compile(
    r"(?P<date>" + _DATE + r")"
    r"(?P<dep_city>" + _CITY_CC + r"{1,50}?)"
    r"(?P<dep_time>" + _TIME + r")"
    r"(?P<flight>" + _FN + r")"
    r"\s*\([^)]{2,20}\)"                   # (A320) / (ATR 72) — discard
    r"\s*(?P<checkin>" + _TIME + r")"      # check-in close time — discard
    r"(?P<arr_city>" + _CITY_CC + r"{1,50}?)"
    r"(?P<arr_time>" + _TIME + r"(?:\+\d+)?)",
    re.I,
)

# S2b: wrapped table row format where aircraft/check-in fields may spill onto
# the next line(s), e.g.:
#   03 May 26 Visakhapatnam 10:40 6E6294
#   (A321) 09:40 Hyderabad 11:55
_RE_S2_WRAPPED = re.compile(
    r"(?P<date>" + _DATE + r")\s+"
    r"(?P<dep_city>" + _CITY_CC + r"{1,50}?)\s+"
    r"(?P<dep_time>" + _TIME + r")\s+"
    r"(?P<flight>" + _FN + r")"
    r"(?:\s*\([^)]{2,20}\))?"
    r"(?:\s+(?P<checkin>" + _TIME + r"))?\s+"
    r"(?P<arr_city>" + _CITY_CC + r"{1,50}?)\s+"
    r"(?P<arr_time>" + _TIME + r"(?:\+\d+)?)",
    re.I,
)

# S3: compact single-line fallback
_RE_S3 = re.compile(
    r"(" + _DATE + r")\s+"
    r"(" + _CITY_CC + r"{1,60}?)\s+"
    r"(" + _TIME + r")\s+"
    r"(" + _FN + r")\s+"
    r"(?:\([A-Z0-9]{1,3}\)\s+)?"          # optional booking class
    r"([A-Z]{3})?\s*"                      # optional dep IATA
    r"(" + _TIME + r")\s+"
    r"(" + _CITY_CC + r"{1,60}?)\s+"
    r"(" + _TIME + r"(?:\+\d+)?)",
    re.I,
)

# ---------------------------------------------------------------------------
# Seat / service
# ---------------------------------------------------------------------------
_RE_ROUTE_HDR    = re.compile(r"^(?:([A-Z]{3})\s*[-]?\s*([A-Z]{3})|([A-Z]{3})([A-Z]{3}))$")
_RE_SEAT         = re.compile(r"\b(\d{1,3}[A-HJ-Z])\b")
_RE_SERVICE_CODE = re.compile(r"\b([A-Z]{4})\b")


def _parse_service_payload(payload: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    seen = set()

    parts = [part.strip(" .:-") for part in re.split(r"[,|\t]+", payload or "") if part.strip(" .:-")]
    for part in parts:
        token = _c(part)
        upper = token.upper()
        item = None

        if upper in MEAL_CODES:
            item = {"code": upper, "name": MEAL_CODES[upper], "type": "meal"}
        elif upper in ANCILLARY_CODES:
            item = {"code": upper, "name": ANCILLARY_CODES[upper], "type": "ancillary"}
        elif re.search(r"\b(?:bag|baggage|travell?er)\b", token, re.I):
            item = {"code": "N/A", "name": token, "type": "ancillary"}

        if not item:
            continue

        dedupe_key = (item["type"], item["code"], item["name"].lower())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        items.append(item)

    return items


def _extract_seat_service_pairs(line: str) -> List[Dict[str, str]]:
    """Extract one or more seat/service cells from a single line."""
    pairs: List[Dict[str, str]] = []
    matches = list(_RE_SEAT.finditer(line or ""))
    if not matches:
        return pairs

    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(line)
        payload = _c(line[start:end])
        pairs.append({"seat": match.group(1), "payload": payload})
    return pairs

# ---------------------------------------------------------------------------
# City -> IATA lookup table
# ---------------------------------------------------------------------------
_CITY_IATA: Dict[str, str] = {
    "AGARTALA": "IXA", "AGRA": "AGR", "AHMEDABAD": "AMD", "AIZAWL": "AJL",
    "AMRITSAR": "ATQ", "AURANGABAD": "IXU", "BAGDOGRA": "IXB",
    "BANGALORE": "BLR", "BENGALURU": "BLR", "BHOPAL": "BHO",
    "BHUBANESWAR": "BBI", "BOMBAY": "BOM", "CALCUTTA": "CCU",
    "CHANDIGARH": "IXC", "CHENNAI": "MAA", "COIMBATORE": "CJB",
    "COCHIN": "COK", "DELHI": "DEL", "DIBRUGARH": "DIB", "DIMAPUR": "DMU",
    "GOA": "GOI", "GORAKHPUR": "GOP", "GUWAHATI": "GAU",
    "HUBLI": "HBX", "HYDERABAD": "HYD", "IMPHAL": "IMF", "INDORE": "IDR",
    "JAIPUR": "JAI", "JAMMU": "IXJ", "JODHPUR": "JDH",
    "KOCHI": "COK", "KOLKATA": "CCU", "KOZHIKODE": "CCJ", "CALICUT": "CCJ",
    "LEH": "IXL", "LUCKNOW": "LKO", "MADURAI": "IXM",
    "MANGALORE": "IXE", "MUMBAI": "BOM", "MYSORE": "MYQ",
    "NAGPUR": "NAG", "NEW DELHI": "DEL", "PATNA": "PAT",
    "PORT BLAIR": "IXZ", "PUNE": "PNQ", "RAIPUR": "RPR",
    "RAJKOT": "RAJ", "RANCHI": "IXR", "SILCHAR": "IXS",
    "SRINAGAR": "SXR", "SURAT": "STV", "THIRUVANANTHAPURAM": "TRV",
    "TIRUCHIRAPPALLI": "TRZ", "TRIVANDRUM": "TRV", "TIRUPATI": "TIR",
    "UDAIPUR": "UDR", "VADODARA": "BDQ", "VARANASI": "VNS",
    "VIJAYAWADA": "VGA", "VISAKHAPATNAM": "VTZ",
    # International
    "COLOMBO": "CMB", "DUBAI": "DXB", "ABU DHABI": "AUH", "DOHA": "DOH",
    "MUSCAT": "MCT", "BANGKOK": "BKK", "SINGAPORE": "SIN",
    "KUALA LUMPUR": "KUL", "KATHMANDU": "KTM", "MALE": "MLE",
    "DHAKA": "DAC", "KARACHI": "KHI",
}


# ===========================================================================
# Public API
# ===========================================================================

def is_indigo_format(text: str) -> bool:
    return sum(1 for p in _INDIGO_MARKERS if p.search(text)) >= 2


def try_indigo_parse(raw_text: str) -> Optional[Dict]:
    """
    Parse an IndiGo confirmation email.
    Returns None if not IndiGo format or no segments found (triggers LLM fallback).
    """
    if not is_indigo_format(raw_text):
        return None

    _log("IndiGo format detected — regex parser v3")

    # Normalise ONCE; pass the clean string everywhere
    text = _normalize_text(raw_text)

    booking, b_meta = _extract_booking(text)
    gst             = _extract_gst(text)
    contact         = _extract_contact(text)
    baggage         = _extract_baggage(text)
    segments        = _extract_segments(text)

    if not segments:
        _log("No segments found — falling back to LLM")
        return None

    passengers = _extract_passengers(text, baggage, booking.get("grand_total"))

    r2s         = _route_map(segments)
    if _has_compact_multicolumn_services_table(text):
        compact_assignments = _parse_seats_and_services_compact(text, r2s)
        sequential_assignments = _parse_seats_and_services_sequential(text, segments)
        assignments = _merge_assignments(compact_assignments, sequential_assignments)
    else:
        assignments = _parse_seats_and_services(text, r2s)
        sequential_assignments = _parse_seats_and_services_sequential(text, segments)
        assignments = _merge_assignments(assignments, sequential_assignments)
    _apply_assignments(passengers, assignments)

    booking["phone"] = contact.get("phone", "N/A")

    data = normalize_data({
        "booking":     booking,
        "gst_details": gst,
        "passengers":  passengers,
        "segments":    segments,
        "barcode":     None,
        "extra":       _build_extra(b_meta, contact),
    })
    data = build_journey(data)

    warnings, errors = _validate(data)
    _log(
        "OK -- %d segment(s), %d passenger(s), %dW %dE" %
        (len(data["segments"]), len(data["passengers"]), len(warnings), len(errors))
    )

    return {
        "metadata": {
            "version":        INDIGO_PARSER_VERSION,
            "parser_version": INDIGO_PARSER_VERSION,
            "source":         "indigo_email",
            "source_name":    "IndiGo Email",
            "llm_status":     "indigo_regex_only",
            "parsed_at":      datetime.now(timezone.utc).isoformat() + "Z",
            "warnings":       warnings,
            "errors":         errors,
        },
        "booking":     data.get("booking", {}),
        "gst_details": data.get("gst_details", {"gst_number": "N/A", "company_name": "N/A"}),
        "passengers":  data.get("passengers", []),
        "segments":    data.get("segments", []),
        "journey":     data.get("journey", {}),
        "barcode":     data.get("barcode"),
        "extra":       data.get("extra", {}),
    }


# ===========================================================================
# Internal utilities
# ===========================================================================

def _log(msg):
    print("[INDIGO_PARSER] " + msg, flush=True)


def _c(s):
    """Collapse all whitespace to single space and strip."""
    return re.sub(r"\s+", " ", s or "").strip()


def _normalize_text(text):
    """
    Convert all Unicode whitespace variants to plain ASCII space.
    Remove zero-width / BOM characters.
    Normalise line endings to LF.
    """
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    for ch in (
        "\u00a0", "\u2007", "\u202f", "\u2009", "\u200a",
        "\u205f", "\u3000", "\u2002", "\u2003", "\u2004",
        "\u2005", "\u2006", "\u2008",
    ):
        t = t.replace(ch, " ")
    for ch in ("\ufeff", "\u200b", "\u200c", "\u200d", "\u2060"):
        t = t.replace(ch, "")
    return t


def _norm_date(raw):
    parts = _c(raw).split()
    if len(parts) != 3:
        return "N/A"
    day, mon, yr = parts
    mon = mon[:3].title()
    if len(yr) == 4:
        yr = yr[2:]
    try:
        return "%02d %s %s" % (int(day), mon, yr)
    except ValueError:
        return "N/A"


def _parse_arrival(dep_date, dep_time, raw_arr):
    arr = _c(raw_arr)
    offset = 0
    pm = re.match(r"^(\d{1,2}:\d{2})\+(\d+)$", arr)
    if pm:
        arr    = pm.group(1)
        offset = int(pm.group(2))
    arr_date = dep_date
    fmt = "%d %b %y"
    if offset > 0:
        try:
            arr_date = (datetime.strptime(dep_date, fmt) + timedelta(days=offset)).strftime(fmt)
        except ValueError:
            pass
    elif arr < dep_time:
        try:
            arr_date = (datetime.strptime(dep_date, fmt) + timedelta(days=1)).strftime(fmt)
        except ValueError:
            pass
    return arr_date, arr


def _duration(dep_date, dep_time, arr_date, arr_time):
    fmt = "%d %b %y %H:%M"
    try:
        dep = datetime.strptime(dep_date + " " + dep_time, fmt)
        arr = datetime.strptime(arr_date + " " + arr_time, fmt)
    except ValueError:
        return "N/A"
    if arr < dep:
        arr += timedelta(days=1)
    mins = int((arr - dep).total_seconds() // 60)
    h, m = divmod(mins, 60)
    return "%dh %dm" % (h, m)


def _split_terminal(raw):
    v = _c(raw)
    t = "N/A"
    pm = re.search(r"\((T\d+[A-Z]?|Terminal\s*\d+[A-Z]?)\)\s*$", v, re.I)
    if pm:
        t = pm.group(1).upper().replace("TERMINAL ", "T").replace(" ", "")
        v = v[:pm.start()].strip()
    return v or "N/A", t


def _resolve_iata(city):
    if not city or city == "N/A":
        return "N/A"
    s = _c(city)
    u = s.upper()
    if re.fullmatch(r"[A-Z]{3}", u):
        return u
    if u in _CITY_IATA:
        return _CITY_IATA[u]
    hits = search_by_name(s)
    if not hits:
        return "N/A"
    exact = next((h for h in hits if h["name"].lower() == s.lower()), None)
    if exact:
        return exact["code"]
    prefix = [h for h in hits if h["name"].lower().startswith(s.lower())]
    pool = prefix or hits
    return sorted(pool, key=lambda h: len(h["name"]))[0]["code"]


def _city_name(iata):
    return AIRPORT_CODES.get((iata or "").upper(), "N/A")


# ===========================================================================
# Section extractors
# ===========================================================================

def _extract_booking(text):
    pm = _RE_PNR.search(text)
    rm = _RE_BOOKING_ROW.search(text)
    fm = _RE_FARE_TYPE.search(text)
    cm = _RE_CURRENCY.search(text)
    tm = _RE_TOTAL_FARE.search(text)

    pnr    = pm.group(1).upper() if pm else (_extract_pnr(text) or "N/A")
    status = rm.group(1).title()      if rm else "N/A"
    bdate  = _norm_date(rm.group(2))  if rm else "N/A"
    pay    = rm.group(3).title()      if rm else "N/A"
    ftype  = _c(fm.group(1))          if fm else "N/A"
    curr   = cm.group(1).upper()      if cm else "N/A"
    total  = float(tm.group(1).replace(",", "")) if tm else None

    if curr == "N/A" and total is not None:
        curr = "INR"
    cot = ("Business" if ftype != "N/A" and "business" in ftype.lower()
           else ("Economy" if ftype != "N/A" else "N/A"))

    return (
        {"pnr": pnr, "booking_date": bdate, "phone": "N/A",
         "currency": curr, "grand_total": total, "class_of_travel": cot},
        {"booking_status": status, "payment_status": pay, "fare_type": ftype},
    )


def _extract_gst(text):
    cn = _RE_GST_CO.search(text)
    gn = _RE_GST_NUM.search(text)
    return {
        "gst_number":   gn.group(1).upper()   if gn else "N/A",
        "company_name": cn.group(1).strip()   if cn else "N/A",
    }


def _extract_contact(text):
    co = _RE_COMPANY.search(text)
    ph = _RE_PHONE.search(text)
    em = _RE_EMAIL.search(text)
    ad = _RE_ADDRESS.search(text)
    return {
        "phone":        ph.group(1)         if ph else "N/A",
        "agency_name":  co.group(1).strip() if co else "N/A",
        "agency_email": em.group(1).strip() if em else "N/A",
        "address":      ad.group(1).strip() if ad else "N/A",
    }


def _extract_baggage(text):
    checkin = _RE_BAGGAGE.search(text)
    hand = _RE_HAND_BAGGAGE.search(text)

    checkin_text = f"{checkin.group(1)} Kg Check-in Baggage" if checkin else None
    hand_text = f"{hand.group(1)} Kg Cabin Baggage" if hand else None

    if hand_text and checkin_text:
        return f"{hand_text} + {checkin_text}"
    if checkin_text:
        return checkin_text
    if hand_text:
        return hand_text
    return "N/A"


# ===========================================================================
# Segment extraction — THREE strategies
# ===========================================================================

def _make_segment(dep_date_raw, dep_city_raw, dep_time_raw,
                  flight_raw, arr_city_raw, arr_time_raw,
                  booking_class="N/A"):
    dep_date               = _norm_date(dep_date_raw)
    dep_city, dep_terminal = _split_terminal(dep_city_raw)
    arr_city, arr_terminal = _split_terminal(arr_city_raw)
    dep_time               = _c(dep_time_raw)
    arr_date, arr_time     = _parse_arrival(dep_date, dep_time, arr_time_raw)
    dep_iata               = _resolve_iata(dep_city)
    arr_iata               = _resolve_iata(arr_city)

    # Normalise flight number -> "6E NNNN"
    fn = re.sub(r"\s+", "", _c(flight_raw))
    fn = re.sub(r"^6[Ee]-?", "6E", fn)
    fn = fn[:2].upper() + " " + fn[2:]

    return {
        "airline":       AIRLINE_CODES.get("6E", "IndiGo"),
        "flight_number": fn,
        "booking_class": booking_class,
        "departure": {
            "city":     dep_city if dep_city != "N/A" else _city_name(dep_iata),
            "airport":  dep_iata,
            "date":     dep_date,
            "time":     dep_time,
            "terminal": dep_terminal,
        },
        "arrival": {
            "city":     arr_city if arr_city != "N/A" else _city_name(arr_iata),
            "airport":  arr_iata,
            "date":     arr_date,
            "time":     arr_time,
            "terminal": arr_terminal,
        },
        "duration_extracted": _duration(dep_date, dep_time, arr_date, arr_time),
    }


def _seg_key(seg):
    d, a = seg["departure"], seg["arrival"]
    return (seg["flight_number"],
            d["date"], d["time"], d["airport"],
            a["date"], a["time"], a["airport"])


def _extract_segments(text):
    """
    Three strategies tried in order. `text` must already be normalised.
    Returns a deduplicated list of segment dicts.
    """
    segments = []
    seen     = set()

    def add(seg):
        k = _seg_key(seg)
        if k not in seen:
            seen.add(k)
            segments.append(seg)

    lines = [ln.rstrip() for ln in text.split("\n")]

    # -- S1: line-pair -------------------------------------------------------
    i = 0
    while i < len(lines):
        la = _c(lines[i])
        ma = _RE_S1A.match(la)
        if not ma:
            i += 1
            continue
        j, mb = i + 1, None
        while j < len(lines) and j <= i + 5:
            lb = _c(lines[j])
            if lb:
                mb = _RE_S1B.match(lb)
                if mb:
                    break
                break
            j += 1
        if mb:
            add(_make_segment(ma.group(1), ma.group(2), ma.group(3), ma.group(4),
                              mb.group(4), mb.group(5), mb.group(1)))
            i = j + 1
        else:
            i += 1

    if segments:
        _log("S1 (line-pair): %d segment(s)" % len(segments))
        return segments

    # -- S2: table-row (HTML-stripped, the 40-pax format) --------------------
    # Per-line scan first (each original table row is one text line)
    for line in lines:
        cl = _c(line)
        if not cl:
            continue
        for m in _RE_S2.finditer(cl):
            add(_make_segment(
                m.group("date"),    m.group("dep_city"), m.group("dep_time"),
                m.group("flight"),  m.group("arr_city"), m.group("arr_time"),
            ))

    if not segments:
        # Sliding-window join for wrapped table rows where the flight row spills
        # across the next line (aircraft type / check-in close / arrival city-time).
        for i in range(len(lines)):
            chunk_parts = []
            for j in range(i, min(i + 4, len(lines))):
                cl = _c(lines[j])
                if not cl:
                    continue
                if j > i and re.search(r"^(Seats\s+and\s+Additional\s+Services|IndiGo\s+Passenger|Travel\s+and\s+Baggage)", cl, re.I):
                    break
                chunk_parts.append(cl)
            if not chunk_parts:
                continue
            chunk = _c(" ".join(chunk_parts))
            for m in _RE_S2_WRAPPED.finditer(chunk):
                add(_make_segment(
                    m.group("date"),    m.group("dep_city"), m.group("dep_time"),
                    m.group("flight"),  m.group("arr_city"), m.group("arr_time"),
                ))

    if not segments:
        # Fallback: join all lines (handles wrapped rows)
        joined = _c(text.replace("\n", " "))
        for m in _RE_S2.finditer(joined):
            add(_make_segment(
                m.group("date"),    m.group("dep_city"), m.group("dep_time"),
                m.group("flight"),  m.group("arr_city"), m.group("arr_time"),
            ))
        for m in _RE_S2_WRAPPED.finditer(joined):
            add(_make_segment(
                m.group("date"),    m.group("dep_city"), m.group("dep_time"),
                m.group("flight"),  m.group("arr_city"), m.group("arr_time"),
            ))

    if segments:
        _log("S2 (table-row): %d segment(s)" % len(segments))
        return segments

    # -- S3: compact single-line fallback ------------------------------------
    joined = _c(text.replace("\n", " "))
    for m in _RE_S3.finditer(joined):
        add(_make_segment(m.group(1), m.group(2), m.group(3), m.group(4),
                          m.group(7), m.group(8)))

    if segments:
        _log("S3 (compact): %d segment(s)" % len(segments))
    else:
        _log("WARNING: no segments found in any mode")

    return segments


# ===========================================================================
# Passenger extraction
# ===========================================================================

def _infer_pax_type(name):
    u = (name or "").upper()
    if any(k in u for k in ("INF", "INFANT")):
        return "INF"
    if any(k in u for k in ("CHD", "CHILD", "MSTR", "MASTER")):
        return "CHD"
    return "ADT"


def _blank_passenger(name, baggage):
    return {
        "name":                  name,
        "pax_type":              _infer_pax_type(name),
        "ticket_number":         "N/A",
        "frequent_flyer_number": "N/A",
        "baggage":               baggage,
        "meals":                 [],
        "ancillaries":           [],
        "fare": {
            "base_fare": None, "k3_gst": None,
            "other_taxes": None, "total_fare": None,
        },
        "seats": [],
    }


def _extract_passenger_names(text):
    """
    Scan every line for standalone titled-name lines.
    Deduplicate by lower-cased full name so round-trip emails
    (where each name appears once per leg) produce one entry each.
    """
    names = []

    for raw_line in text.split("\n"):
        m = _RE_NAME_LINE.match(raw_line)
        if not m:
            continue
        raw_name = m.group(1).strip()
        # Require at least two words (title + surname minimum)
        if len(raw_name.split()) < 2:
            continue
        # Reject if digits appear after the title (table data leak)
        after_title = re.sub(r"^" + _TITLE + r"\s*", "", raw_name, flags=re.I)
        if re.search(r"\d", after_title):
            continue
        cleaned = normalize_name(raw_name)
        if cleaned == "N/A":
            continue
        key = cleaned.lower()

        replaced = False
        skip = False
        for idx, existing in enumerate(names):
            ex_key = existing.lower()
            if key == ex_key:
                skip = True
                break
            if key.startswith(ex_key) and len(key) > len(ex_key):
                names[idx] = cleaned
                replaced = True
                break
            if ex_key.startswith(key) and len(ex_key) > len(key):
                skip = True
                break
        if not skip and not replaced:
            names.append(cleaned)

    return names


def _extract_passengers(text, baggage, total_fare):
    names    = _extract_passenger_names(text)
    counts   = [int(v) for v in _RE_TOTAL_PAX.findall(text)]
    expected = max(counts) if counts else (len(names) or 1)

    while len(names) < expected:
        names.append("Passenger %d" % (len(names) + 1))

    passengers = [_blank_passenger(n, baggage) for n in names]
    if total_fare is not None and len(passengers) == 1:
        passengers[0]["fare"]["total_fare"] = total_fare
    return passengers


# ===========================================================================
# Seat / service assignment
# ===========================================================================

def _route_map(segments):
    routes = {}
    for idx, seg in enumerate(segments):
        key = (seg["departure"]["airport"], seg["arrival"]["airport"])
        routes.setdefault(key, idx)
    return routes


def _store(assignments, name, route, payload, r2s):
    if route is None:
        return
    idx = r2s.get(route)
    if idx is None:
        return
    sm = _RE_SEAT.search(payload)
    service_items = _parse_service_payload(payload)
    assignments.setdefault(name.lower(), []).append({
        "segment_index": idx,
        "seat":          sm.group(1) if sm else None,
        "service_items": service_items,
    })


def _parse_seats_and_services(text, r2s):
    assignments = {}
    lines       = text.split("\n")
    in_section  = False
    cur_route   = None
    pending     = None

    for raw in lines:
        line = _c(raw)
        if not line:
            continue
        if re.search(r"Seats\s+and\s+Additional\s+Services", line, re.I):
            in_section = True
            continue
        if not in_section:
            continue
        rh = _RE_ROUTE_HDR.match(line.replace(" ", "").replace("-", ""))
        if rh:
            dep = rh.group(1) or rh.group(3)
            arr = rh.group(2) or rh.group(4)
            cur_route = (dep, arr)
            pending   = None
            continue
        if re.match(r"Passenger\s+Name", line, re.I):
            pending = None
            continue
        mi = re.match(
            r"^(" + _TITLE + r"\s+[A-Za-z][A-Za-z \.'\\-]{1,60}?)\s+(\d{1,3}[A-HJ-Z])(?:\s+(.*))?$",
            line, re.I,
        )
        if mi:
            n = normalize_name(mi.group(1))
            if n != "N/A":
                _store(assignments, n, cur_route,
                       mi.group(2) + " " + (mi.group(3) or ""), r2s)
            pending = None
            continue
        mn = _RE_NAME_LINE.match(line)
        if mn:
            pending = normalize_name(mn.group(1))
            continue
        if pending:
            _store(assignments, pending, cur_route, line, r2s)
            pending = None

    return assignments


def _parse_seats_and_services_compact(text, r2s):
    """Parse compact multi-column IndiGo service tables grouped by route headers."""
    assignments = {}
    lines = text.split("\n")
    in_section = False
    current_routes = []
    in_rows = False
    pending_name = None

    def add_pairs(name: str, line: str):
        pairs = _extract_seat_service_pairs(line)
        if not pairs:
            return False
        for idx, pair in enumerate(pairs):
            if idx >= len(current_routes):
                continue
            route = current_routes[idx]
            seg_idx = r2s.get(route)
            if seg_idx is None:
                continue
            assignments.setdefault(name.lower(), []).append({
                "segment_index": seg_idx,
                "seat": pair.get("seat"),
                "service_items": _parse_service_payload(pair.get("payload", "")),
            })
        return True

    i = 0
    while i < len(lines):
        raw = lines[i]
        line = _c(raw)
        if not line:
            i += 1
            continue

        if re.search(r"Seats\s+and\s+Additional\s+Services", line, re.I):
            in_section = True
            current_routes = []
            in_rows = False
            pending_name = None
            i += 1
            continue

        if not in_section:
            i += 1
            continue

        if re.search(
            r"^(?:Status|IndiGo\s+Passenger|Check-in\s+now|Flight\s+Status|IndiGo\s+Flight\(s\)|Fare\s+Type|Travel\s+and\s+Baggage|Terms\s+&\s+Conditions|For\s+Your\s+Benefits)\b",
            line,
            re.I,
        ):
            in_section = False
            current_routes = []
            in_rows = False
            pending_name = None
            continue

        compact = line.replace(" ", "").replace("-", "")
        rh = _RE_ROUTE_HDR.match(compact)
        if rh:
            dep = rh.group(1) or rh.group(3)
            arr = rh.group(2) or rh.group(4)
            if in_rows:
                current_routes = []
                in_rows = False
                pending_name = None
            current_routes.append((dep, arr))
            i += 1
            continue

        if re.match(r"Passenger\s+name", line, re.I):
            in_rows = True
            pending_name = None
            i += 1
            continue
        if re.match(r"Seat$", line, re.I) or re.match(r"Services\s+Purchased", line, re.I):
            i += 1
            continue

        if not in_rows:
            i += 1
            continue

        m = _RE_NAME_LINE.match(raw)
        if m:
            candidate = normalize_name(m.group(1))
            if candidate != "N/A":
                if i + 1 < len(lines):
                    nxt_raw = lines[i + 1].strip()
                    if nxt_raw and re.fullmatch(r"[A-Za-z][A-Za-z .'\-]{1,60}", nxt_raw) and not _RE_NAME_LINE.match(nxt_raw):
                        merged = normalize_name(m.group(1) + " " + nxt_raw)
                        if merged != "N/A":
                            candidate = merged
                            i += 1
                pending_name = candidate
            i += 1
            continue

        if pending_name and add_pairs(pending_name, line):
            pending_name = None
            i += 1
            continue

        i += 1

    return assignments


def _parse_seats_and_services_sequential(text, segments):
    """Fallback for PDF-extracted multi-segment seat tables with scrambled route headers."""
    assignments = {}
    lines = text.split("\n")
    in_section = False
    pending_name = None
    seat_service_pairs = []

    def flush_pending():
        nonlocal pending_name, seat_service_pairs
        if pending_name and seat_service_pairs:
            assignments.setdefault(pending_name.lower(), [])
            for idx, pair in enumerate(seat_service_pairs[:len(segments)]):
                assignments[pending_name.lower()].append({
                    "segment_index": idx,
                    "seat": pair.get("seat"),
                    "service_items": _parse_service_payload(pair.get("payload", "")),
                })
        pending_name = None
        seat_service_pairs = []

    i = 0
    while i < len(lines):
        line = _c(lines[i])
        if not line:
            i += 1
            continue
        if re.search(r"Seats\s+and\s+Additional\s+Services", line, re.I):
            in_section = True
            i += 1
            continue
        if in_section and re.search(
            r"^(?:Status|IndiGo\s+Passenger|Check-in\s+now|Flight\s+Status|IndiGo\s+Flight\(s\)|Fare\s+Type|Travel\s+and\s+Baggage)\b",
            line,
            re.I,
        ):
            flush_pending()
            in_section = False
            continue
        if not in_section:
            i += 1
            continue
        if re.search(r"^(?:Travel\s+and\s+Baggage|Fare\s+Type|Terms\s+&\s+Conditions|For\s+Your\s+Benefits)\b", line, re.I):
            flush_pending()
            break
        if re.match(r"Passenger\s+name", line, re.I) or re.match(r"Seat$", line, re.I) or re.match(r"Services\s+Purchased", line, re.I):
            i += 1
            continue

        if pending_name:
            inline_pairs = _extract_seat_service_pairs(line)
            if inline_pairs:
                if len(inline_pairs) == 1 and not inline_pairs[0]["payload"]:
                    payload = ""
                    if i + 1 < len(lines):
                        nxt = _c(lines[i + 1])
                        if nxt and not _RE_NAME_LINE.match(nxt) and not _extract_seat_service_pairs(nxt):
                            payload = nxt
                            i += 1
                    inline_pairs[0]["payload"] = payload
                seat_service_pairs.extend(inline_pairs)
                i += 1
                continue

        m = _RE_NAME_LINE.match(lines[i])
        if m:
            candidate = normalize_name(m.group(1))
            if candidate != "N/A":
                flush_pending()
                # Merge a split surname line like "Mr. Ajit kumar Yugal" + "kishor"
                if i + 1 < len(lines):
                    nxt_raw = lines[i + 1].strip()
                    if nxt_raw and re.fullmatch(r"[A-Za-z][A-Za-z .'\-]{1,40}", nxt_raw) and not _RE_NAME_LINE.match(nxt_raw):
                        merged = normalize_name(m.group(1) + " " + nxt_raw)
                        if merged != "N/A":
                            candidate = merged
                            i += 1
                pending_name = candidate
            i += 1
            continue

        i += 1

    flush_pending()
    return assignments


def _has_compact_multicolumn_services_table(text: str) -> bool:
    header_repeats = re.search(
        r"Passenger\s+name\s+Seat\s+Services\s+Purchased\s+Seat\s+Services\s+Purchased",
        _c(text),
        re.I,
    )
    compact_routes = re.search(r"^\s*[A-Z]{6}\s*$", text, re.M)
    return bool(header_repeats or compact_routes)


def _merge_assignments(primary, secondary):
    merged = {k: list(v) for k, v in (primary or {}).items()}
    for name, items in (secondary or {}).items():
        bucket = merged.setdefault(name, [])
        for item in items:
            seat = item.get("seat")
            service_items = item.get("service_items", [])
            existing = None
            for candidate in bucket:
                if candidate.get("segment_index") == item.get("segment_index"):
                    existing = candidate
                    break
            if existing is None:
                bucket.append({
                    "segment_index": item.get("segment_index", 0),
                    "seat": seat,
                    "service_items": list(service_items),
                })
                continue

            if not existing.get("seat") and seat:
                existing["seat"] = seat

            seen = {
                (svc.get("type"), svc.get("code"), svc.get("name"))
                for svc in existing.get("service_items", [])
            }
            for svc in service_items:
                key = (svc.get("type"), svc.get("code"), svc.get("name"))
                if key not in seen:
                    existing.setdefault("service_items", []).append(svc)
                    seen.add(key)
    return merged


def _apply_assignments(passengers, assignments):
    for pax in passengers:
        key = pax["name"].lower()
        for item in assignments.get(key, []):
            sidx = item["segment_index"]
            seat = item.get("seat")
            if seat and not any(
                e["segment_index"] == sidx and e["seat_number"] == seat
                for e in pax["seats"]
            ):
                pax["seats"].append({"segment_index": sidx, "seat_number": seat})
            for service in item.get("service_items", []):
                entry  = {
                    "segment_index": sidx,
                    "code":          service.get("code", "N/A"),
                    "name":          service.get("name", "N/A"),
                }
                target = pax["meals"] if service.get("type") == "meal" else pax["ancillaries"]
                if not any(
                    e["segment_index"] == sidx
                    and e.get("code") == entry["code"]
                    and e.get("name") == entry["name"]
                    for e in target
                ):
                    target.append(entry)


# ===========================================================================
# Helpers
# ===========================================================================

def _build_extra(b_meta, contact):
    return {
        "source":         "indigo_email",
        "source_name":    "IndiGo Email",
        "booking_status": b_meta.get("booking_status", "N/A"),
        "payment_status": b_meta.get("payment_status", "N/A"),
        "fare_type":      b_meta.get("fare_type",      "N/A"),
        "agency_name":    contact.get("agency_name",   "N/A"),
        "agency_email":   contact.get("agency_email",  "N/A"),
        "address":        contact.get("address",       "N/A"),
    }


def _validate(data):
    warnings, errors = [], []
    if data.get("booking", {}).get("pnr", "N/A") == "N/A":
        warnings.append("PNR not found")
    if not data.get("segments"):
        errors.append("No flight segments found")
    for i, seg in enumerate(data.get("segments", [])):
        if seg["departure"]["airport"] == "N/A":
            warnings.append("Segment %d: departure airport unknown" % i)
        if seg["arrival"]["airport"] == "N/A":
            warnings.append("Segment %d: arrival airport unknown" % i)
        if seg["departure"]["date"] == "N/A":
            warnings.append("Segment %d: departure date unknown" % i)
    for i, pax in enumerate(data.get("passengers", [])):
        if not pax.get("name") or pax["name"] == "N/A":
            errors.append("Passenger %d: name missing" % i)
    return warnings, errors
