"""
source_extractors.py  v2.1
===========================
Source-specific regex extraction for:
  IndiGo · Air India Express · Amadeus · Galileo · Sabre · TBO · Riya

Each extractor produces dicts that map EXACTLY onto the final output schema:

  booking        → pnr, booking_date, phone, currency, grand_total, class_of_travel
  gst_details    → gst_number, company_name
  passengers[]   → name, pax_type, ticket_number, frequent_flyer_number,
                   baggage, meals[], ancillaries[], fare{}, seats[]
  segments[]     → airline, flight_number, booking_class,
                   departure{city,airport,date,time,terminal},
                   arrival{city,airport,date,time,terminal},
                   duration_extracted
  barcode

Integration (one-line change in llm_extractor.py extract()):
─────────────────────────────────────────────────────────────
    from source_extractors import enrich_regex_hints

    def extract(raw_text):
        rx       = regex_extract(raw_text)
        rx       = enrich_regex_hints(raw_text, rx)   # add this line
        llm_data = llm_extract(raw_text, rx)
        ...
"""

import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from mappings import (AIRPORT_CODES, AIRLINE_CODES, AIRPORT_TZ_MAP,
                      MEAL_CODES, ANCILLARY_CODES, resolve_booking_class)
from llm_extractor import (build_journey, normalize_data, normalize_name,
                           normalize_phone, normalize_baggage, PARSER_VERSION)

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
    return f"{int(day):02d} {m} {y}"

def _hhmm(raw: str) -> str:
    raw = raw.strip().replace(":", "")
    if len(raw) == 4 and raw.isdigit():
        return f"{raw[:2]}:{raw[2:]}"
    return raw

def _num(s: str) -> Optional[float]:
    try: return float(re.sub(r"[,\s]", "", s))
    except: return None

MONTH_PAT = (r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?"
             r"|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)")

def _city(iata: str) -> str:
    return AIRPORT_CODES.get(iata.upper(), "N/A")

def _airline_name(code: str) -> str:
    return AIRLINE_CODES.get(code.upper(), "N/A")

def _resolve_code(code: str) -> Dict:
    c = code.upper().strip()
    if c in MEAL_CODES:      return {"code": c, "name": MEAL_CODES[c],      "type": "meal"}
    if c in ANCILLARY_CODES: return {"code": c, "name": ANCILLARY_CODES[c], "type": "ancillary"}
    return {"code": c, "name": c, "type": "ancillary"}

# ── Schema templates ──────────────────────────────────────────────────────────

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

def _add_ssr(passengers, segments, ssr_iter):
    """Distribute SSR (meal/ancillary) items to all passengers per segment."""
    flt_to_seg = {}
    for si, seg in enumerate(segments):
        route = seg["departure"]["airport"] + seg["arrival"]["airport"]
        flt_to_seg[route] = si
    for code, route_dep, route_arr in ssr_iter:
        si    = flt_to_seg.get(route_dep + route_arr, 0)
        res   = _resolve_code(code)
        entry = {"segment_index": si, "code": res["code"], "name": res["name"]}
        for p in passengers:
            lst = p["meals"] if res["type"] == "meal" else p["ancillaries"]
            if not any(x["code"] == code and x["segment_index"] == si for x in lst):
                lst.append(entry)

def _flat_hints(src, pnr, bk_date, phone, currency, grand_total, class_of_travel,
                gst_number, gst_company, baggage, base_fare, k3_gst, other_taxes,
                ticket_numbers, ff_numbers, seats_raw, segments, passengers, **extra) -> Dict:
    """Build the flat hint keys that base merge() consumes."""
    d = {
        "source": src[0], "source_name": src[1],
        "pnr": pnr, "booking_date": bk_date, "phone": phone, "currency": currency,
        "grand_total": grand_total, "class_of_travel": class_of_travel,
        "gst_number": gst_number, "gst_company_name": gst_company,
        "baggage": baggage, "base_fare": base_fare, "k3_gst": k3_gst,
        "other_taxes": other_taxes, "total_fare": grand_total,
        "ticket_numbers": ticket_numbers, "frequent_flyer_numbers": ff_numbers,
        "seats_raw": seats_raw,
        "passenger_names": [p["name"] for p in passengers],
        "parsed_segments": segments,
        "flight_numbers": [s["flight_number"] for s in segments],
        "iata_pairs": [(s["departure"]["airport"], s["arrival"]["airport"])
                       for s in segments
                       if s["departure"]["airport"] != "N/A" and
                          s["arrival"]["airport"] != "N/A"],
        # Structured output fields
        "booking": {"pnr": pnr, "booking_date": bk_date, "phone": phone,
                    "currency": currency, "grand_total": grand_total,
                    "class_of_travel": class_of_travel},
        "gst_details": {"gst_number": gst_number, "company_name": gst_company},
        "passengers": passengers, "segments": segments, "barcode": None,
    }
    d.update(extra)
    return d


# ══════════════════════════════════════════════════════════════════════════════
# 1. IndiGo
# ══════════════════════════════════════════════════════════════════════════════
class IndiGoExtractor:
    _RE_DETECT = re.compile(r"IndiGo|6E\s*\d{3,4}|Interglobe\s*Aviation|goindigo\.in|indigo\.in", re.I)
    _RE_PNR    = re.compile(r"PNR\s*/\s*Booking\s*Ref\.?\s*[:\s]*([A-Z0-9]{5,8})", re.I)
    _RE_BKDATE = re.compile(rf"(\d{{1,2}})({MONTH_PAT})(\d{{2,4}})\s+\d{{2}}:\d{{2}}:\d{{2}}", re.I)
    _RE_PHONE  = re.compile(r"(?:Home\s*Phone|Phone|Mobile)\s*[:\s]*(\d[\d*\s\-]{6,20}\d)", re.I)
    _RE_EMAIL  = re.compile(r"Email\s*[:\s]*([a-zA-Z0-9_.+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z]{2,})", re.I)
    _RE_AGENCY = re.compile(r"Company\s*Name\s*[:\s]*(.+?)(?=\s*(?:Home|Phone|Email|GST|\n|$))", re.I)
    _RE_GSTNAM = re.compile(r"GST\s*Company\s*Name\s*[:\s]*(.+?)(?=\s*(?:GST\s*Number|\n|$))", re.I)
    _RE_GSTNUM = re.compile(r"GST\s*Number\s*[:\s]*([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z])\b", re.I)
    _RE_FARTYP = re.compile(r"Fare\s*Type\s*[:\s]*(Corporate\s*Fare|Flexi\s*Fare|Super\s*Saver|Lite\s*Fare|Saver\s*Fare|Business\s*Fare|Special\s*Fare)", re.I)
    _RE_TOTAL  = re.compile(r"Total\s*Fare\s*[:\s]*(?:INR|[^0-9\s])?[:\s]*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_BASE   = re.compile(r"Base\s*Fare\s*[:\s]*(?:INR|[^0-9\s])?[:\s]*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_K3     = re.compile(r"(?:\bK3\b|GST\s+on\s+fare)\s*[:\s]*(?:INR|[^0-9\s])?[:\s]*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_OTHTAX = re.compile(r"(?:Other\s*Taxes?|Surcharge|YQ|YR)\s*[:\s]*(?:INR|[^0-9\s])?[:\s]*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_BAG    = re.compile(r"Check-?in\s*Baggage\s*[:\s]*(\d+\s*(?:kg|kgs?))\s*per\s*person", re.I)
    _RE_TICKET = re.compile(r"(?:Ticket\s*(?:No\.?|Number)|E-?Ticket)\s*[:\s]*(\d{3}[\-\s]?\d{7,11})", re.I)
    _RE_FF     = re.compile(r"(?:Frequent\s*Fli(?:er|ght)|FFN?\b|BluChip)\s*[:\s#]*([A-Z0-9]{5,15})\b", re.I)
    _RE_STATUS = re.compile(r"\b(CONFIRMED|CANCELLED|WAITLISTED|HOLD)\b", re.I)
    # Flight row: "05 Mar 26  CityName  20:30  6E 617  (A320)  19:30  CityName  22:00"
    _RE_FLTROW = re.compile(
        rf"(\d{{1,2}})\s+({MONTH_PAT})\s+(\d{{2,4}})"
        r"\s+([\w\s]+?)\s+"
        r"(\d{1,2}:\d{2})"
        r"\s+(6E)\s*(\d{1,4}[A-Z]?)"
        r"(?:\s+\([A-Z0-9]+\))?"
        r"(?:\s+\d{1,2}:\d{2})?"
        r"\s+([\w\s]+?)\s+"
        r"(\d{1,2}:\d{2})",
        re.I)
    # Seat row: "Mr. Name  10C  CPTR,VCSW  10C  CPTR,VCSW"
    _RE_SEATROW = re.compile(
        r"(?:Mr|Mrs|Ms|Miss|Dr|Prof)\.?\s+\S.*?"
        r"(\d{1,3}[A-Z])\s+([\w,]+)"
        r"\s+(\d{1,3}[A-Z])\s+([\w,]+)",
        re.I)
    _RE_SEATSNGL= re.compile(r"Seat\s*(?:No\.?)?\s*[:\s]*(\d{1,3}[A-Z])\b", re.I)
    # Segment-ref block headers: "VTZCCU" on its own line
    _RE_SEGREF  = re.compile(r"^([A-Z]{3})([A-Z]{3})\s*$", re.MULTILINE)
    # PAX name
    _RE_PAXNAME = re.compile(
        r"((?:Mr|Mrs|Ms|Miss|Dr|Prof)\.?\s+[A-Za-z][A-Za-z\s.]{2,60}?)(?=\s*\n|\s{2,})",
        re.I)

    def detect(self, t): return bool(self._RE_DETECT.search(t))

    def _phone(self, raw):
        c = re.sub(r"[*\s\-()]", "", raw)
        if c.startswith("0") and len(c) == 11: return f"+91{c[1:]}"
        if c.startswith("91") and len(c) == 12: return f"+{c}"
        if len(c) == 10 and c.isdigit(): return f"+91{c}"
        return f"+{c}" if not c.startswith("+") else c

    def extract(self, text: str) -> Dict:
        g = lambda pat: (m := pat.search(text)) and m.group(1) or None

        pnr       = (g(self._RE_PNR) or "N/A").upper()
        m         = self._RE_BKDATE.search(text)
        bk_date   = _nd(m.group(1), m.group(2), m.group(3)) if m else "N/A"
        phone_raw = g(self._RE_PHONE)
        phone     = self._phone(phone_raw) if phone_raw else "N/A"
        grand_t   = _num(g(self._RE_TOTAL) or "")
        base_f    = _num(g(self._RE_BASE)  or "")
        k3        = _num(g(self._RE_K3)    or "")
        othtax    = _num(g(self._RE_OTHTAX)or "")
        ft_raw    = g(self._RE_FARTYP) or ""
        class_ot  = "Business" if "business" in ft_raw.lower() else "Economy"
        gst_num   = (g(self._RE_GSTNUM) or "N/A").upper()
        gst_co    = (g(self._RE_GSTNAM) or "N/A").strip().title()
        bag_raw   = g(self._RE_BAG) or "N/A"
        baggage   = re.sub(r"(\d+)\s*kg[s]?", r"\1 Kg", bag_raw, flags=re.I)
        tickets   = [re.sub(r'[\-\s]', '', m.group(1)) for m in self._RE_TICKET.finditer(text)]
        ff_nums   = [m.group(1) for m in self._RE_FF.finditer(text)]
        email     = g(self._RE_EMAIL) or "N/A"
        agency    = (g(self._RE_AGENCY) or "N/A").strip()

        # Seg-refs (airport-pair headers)
        seg_refs = [(m.group(1).upper(), m.group(2).upper())
                    for m in self._RE_SEGREF.finditer(text)]

        # Flight rows
        segments: List[Dict] = []
        for i, m in enumerate(self._RE_FLTROW.finditer(text)):
            dep_ap = seg_refs[i][0] if i < len(seg_refs) else "N/A"
            arr_ap = seg_refs[i][1] if i < len(seg_refs) else "N/A"
            segments.append(_seg(
                airline=_airline_name("6E"),
                flight_number=f"6E {m.group(7)}",
                dep_city=_city(dep_ap) if dep_ap != "N/A" else m.group(4).strip().title(),
                dep_airport=dep_ap,
                dep_date=_nd(m.group(1), m.group(2), m.group(3)),
                dep_time=m.group(5),
                arr_city=_city(arr_ap) if arr_ap != "N/A" else m.group(8).strip().title(),
                arr_airport=arr_ap,
                arr_date=_nd(m.group(1), m.group(2), m.group(3)),
                arr_time=m.group(9),
            ))

        # PAX names (deduplicated)
        pax_names = []
        seen = set()
        for m in self._RE_PAXNAME.finditer(text):
            n = m.group(1).strip()
            k = re.sub(r"\s+", " ", n.lower())
            if k not in seen:
                seen.add(k); pax_names.append(n)

        # Seat / service rows
        seat_rows = []
        for m in self._RE_SEATROW.finditer(text):
            seat_rows.append([
                {"seat": m.group(1), "svcs": [c.strip() for c in m.group(2).split(",") if c.strip()]},
                {"seat": m.group(3), "svcs": [c.strip() for c in m.group(4).split(",") if c.strip()]},
            ])
        if not seat_rows:
            ss = [m.group(1) for m in self._RE_SEATSNGL.finditer(text)]
            if ss: seat_rows = [[{"seat": s, "svcs": []} for s in ss]]

        n_pax = max(len(pax_names), 1)
        passengers: List[Dict] = []
        for pi in range(n_pax):
            p = _pax(
                name=pax_names[pi] if pi < len(pax_names) else "N/A",
                ticket_number=tickets[pi] if pi < len(tickets) else "N/A",
                frequent_flyer_number=ff_nums[pi] if pi < len(ff_nums) else "N/A",
                baggage=baggage,
            )
            p["fare"] = {"base_fare": base_f, "k3_gst": k3,
                         "other_taxes": othtax, "total_fare": grand_t}
            if pi < len(seat_rows):
                for si, sd in enumerate(seat_rows[pi]):
                    p["seats"].append({"segment_index": si, "seat_number": sd["seat"]})
                    for code in sd["svcs"]:
                        res = _resolve_code(code)
                        entry = {"segment_index": si, "code": res["code"], "name": res["name"]}
                        if res["type"] == "meal":
                            p["meals"].append(entry)
                        else:
                            p["ancillaries"].append(entry)
            passengers.append(p)

        seats_raw = [s["seat_number"] for p in passengers for s in p["seats"]]
        return _flat_hints(
            ("indigo", "IndiGo Direct"),
            pnr, bk_date, phone, "INR", grand_t, class_ot,
            gst_num, gst_co, baggage, base_f, k3, othtax,
            tickets, ff_nums, seats_raw, segments, passengers,
            agency_name=agency, email=email,
        )


# ══════════════════════════════════════════════════════════════════════════════
# 2. Air India Express
# ══════════════════════════════════════════════════════════════════════════════
class AirIndiaExpressExtractor:
    _RE_DETECT = re.compile(r"Air\s*India\s*Express|airindiaexpress\.in|\bIX\s+\d{3,4}\b", re.I)
    _RE_PNR    = re.compile(r"(?:Booking\s*(?:Reference|Ref|Code)|PNR)\s*[:\s/]*([A-Z0-9]{5,8})\b", re.I)
    _RE_BKDATE = re.compile(rf"(?:Booking\s*Date|Date\s*of\s*Booking)\s*[:\s]*(\d{{1,2}})[/\-\s]({MONTH_PAT}|\d{{1,2}})[/\-\s](\d{{2,4}})", re.I)
    _RE_TOTAL  = re.compile(r"(?:Total\s*(?:Amount|Fare|Due)|Amount\s*Payable)\s*[:\s]*(?:INR|AED|USD|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_BASE   = re.compile(r"Base\s*Fare\s*[:\s]*(?:INR|AED|USD|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_TAX    = re.compile(r"(?:Taxes?(?:\s*(?:and|&)\s*Fees?)?|Surcharges?)\s*[:\s]*(?:INR|AED|USD|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_BAG    = re.compile(r"(?:Checked?\s*Baggage|Baggage\s*Allowance)\s*[:\s]*(\d+\s*(?:kg|kgs?))", re.I)
    _RE_SEAT   = re.compile(r"(?:Seat\s*(?:No\.?|Number)?)\s*[:\s]*(\d{1,3}[A-Z])\b", re.I)
    _RE_FARTYP = re.compile(r"(?:Fare\s*(?:Type|Family)|Cabin)\s*[:\s]*(FlySmart|Value|Flex|Business|Economy|Smart\s*Saver|Express\s*(?:Value|Flex))", re.I)
    _RE_TICKET = re.compile(r"(?:E-?Ticket|Ticket\s*No\.?)\s*[:\s]*(\d{3}[\-\s]?\d{7,11})", re.I)
    _RE_FF     = re.compile(r"(?:Frequent\s*Fli(?:er|ght)|FFN?|Loyalty|Flying\s*Returns)\s*[:\s#]*([A-Z0-9]{5,15})\b", re.I)
    _RE_PHONE  = re.compile(r"(?:Phone|Mobile|Contact)\s*[:\s]*([+\d][\d\s\-]{7,20}\d)", re.I)
    _RE_GSTNUM = re.compile(r"(?:GSTIN|GST\s*No\.?)\s*[:\s]*([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z])\b", re.I)
    _RE_GSTNAM = re.compile(r"(?:GST\s*(?:Company\s*)?Name|Company\s*Name)\s*[:\s]*(.+?)(?=\s*(?:GST|\n|$))", re.I)
    _RE_PAX    = re.compile(r"((?:Mr|Mrs|Ms|Miss|Dr|Prof)\.?\s+[A-Za-z][A-Za-z\s.]{2,60}?)(?=\s*\n|\s{2,}|$)", re.I)
    # "IX 344  BOM (Mumbai) → DXB (Dubai)  12 Mar 26  08:45 → 11:15"
    _RE_SEG_FULL = re.compile(
        rf"(IX)\s+(\d{{1,4}}[A-Z]?)[^\n]*?\(([A-Z]{{3}})\)\s*[→\-]\s*[^\n]*?\(([A-Z]{{3}})\)"
        rf"\s+(\d{{1,2}})\s+({MONTH_PAT})\s+(\d{{2,4}})"
        r"\s+(\d{1,2}:\d{2})\s*[→\-]\s*(\d{1,2}:\d{2})",
        re.I)
    # "IX344  BOM  DXB  12Mar26  0845  1115"
    _RE_SEG_SIM  = re.compile(
        rf"(IX)\s*(\d{{1,4}}[A-Z]?)\s+([A-Z]{{3}})\s+([A-Z]{{3}})\s+"
        rf"(\d{{1,2}})\s*({MONTH_PAT})\s*(\d{{2,4}})\s+(\d{{1,2}}:?\d{{2}})\s+(\d{{1,2}}:?\d{{2}})",
        re.I)

    def detect(self, t): return bool(self._RE_DETECT.search(t))

    def extract(self, text: str) -> Dict:
        g = lambda pat: (m := pat.search(text)) and m.group(1) or None

        pnr     = (g(self._RE_PNR) or "N/A").upper()
        m       = self._RE_BKDATE.search(text)
        bk_date = _nd(m.group(1), m.group(2), m.group(3)) if m else "N/A"
        grand_t = _num(g(self._RE_TOTAL) or "")
        base_f  = _num(g(self._RE_BASE)  or "")
        othtax  = _num(g(self._RE_TAX)   or "")
        bag_raw = g(self._RE_BAG) or "N/A"
        baggage = re.sub(r"(\d+)\s*kg[s]?", r"\1 Kg", bag_raw, flags=re.I)
        ft_raw  = g(self._RE_FARTYP) or ""
        class_ot= "Business" if "business" in ft_raw.lower() else "Economy"
        phone_r = g(self._RE_PHONE)
        phone   = re.sub(r"[\s\-()]", "", phone_r) if phone_r else "N/A"
        if phone != "N/A" and not phone.startswith("+") and len(phone) == 10: phone = f"+91{phone}"
        gst_num = (g(self._RE_GSTNUM) or "N/A").upper()
        gst_co  = (g(self._RE_GSTNAM) or "N/A").strip().title()
        tickets = [re.sub(r'[\-\s]', '', m.group(1)) for m in self._RE_TICKET.finditer(text)]
        ff_nums = [m.group(1) for m in self._RE_FF.finditer(text)]
        seats_raw=[m.group(1) for m in self._RE_SEAT.finditer(text)]
        cur     = "AED" if re.search(r"\bAED\b", text) else ("USD" if re.search(r"\bUSD\b", text) else "INR")

        pax_names = list(dict.fromkeys(m.group(1).strip() for m in self._RE_PAX.finditer(text)))

        segments: List[Dict] = []
        for m in self._RE_SEG_FULL.finditer(text):
            dep_ap, arr_ap = m.group(3).upper(), m.group(4).upper()
            dt = _nd(m.group(5), m.group(6), m.group(7))
            segments.append(_seg(airline=_airline_name("IX"), flight_number=f"IX {m.group(2)}",
                dep_city=_city(dep_ap), dep_airport=dep_ap, dep_date=dt, dep_time=m.group(8),
                arr_city=_city(arr_ap), arr_airport=arr_ap, arr_date=dt, arr_time=m.group(9)))
        if not segments:
            for m in self._RE_SEG_SIM.finditer(text):
                dep_ap, arr_ap = m.group(3).upper(), m.group(4).upper()
                dt = _nd(m.group(5), m.group(6), m.group(7))
                segments.append(_seg(airline=_airline_name("IX"), flight_number=f"IX {m.group(2)}",
                    dep_city=_city(dep_ap), dep_airport=dep_ap, dep_date=dt, dep_time=_hhmm(m.group(8)),
                    arr_city=_city(arr_ap), arr_airport=arr_ap, arr_date=dt, arr_time=_hhmm(m.group(9))))

        n_pax = max(len(pax_names), 1)
        passengers = []
        for pi in range(n_pax):
            p = _pax(name=pax_names[pi] if pi < len(pax_names) else "N/A",
                     ticket_number=tickets[pi] if pi < len(tickets) else "N/A",
                     frequent_flyer_number=ff_nums[pi] if pi < len(ff_nums) else "N/A",
                     baggage=baggage)
            p["fare"] = {"base_fare": base_f, "k3_gst": None, "other_taxes": othtax, "total_fare": grand_t}
            if pi < len(seats_raw):
                p["seats"].append({"segment_index": 0, "seat_number": seats_raw[pi]})
            passengers.append(p)

        return _flat_hints(("air_india_express","Air India Express"),
            pnr, bk_date, phone, cur, grand_t, class_ot,
            gst_num, gst_co, baggage, base_f, None, othtax,
            tickets, ff_nums, seats_raw, segments, passengers)


# ══════════════════════════════════════════════════════════════════════════════
# 3. Amadeus GDS
# ══════════════════════════════════════════════════════════════════════════════
class AmadeusExtractor:
    """
    Handles two Amadeus output formats:

    Format A — PIR (Passenger Itinerary Receipt):
      Produced by travel agencies via Amadeus GDS.
      Layout:
        DEP line: ' CITY NAME   QR NNN  CL  DDMON  HHMM   FAREBASIS  NVB NVA  BAG  ST'
        (city may overflow to next line, e.g. 'KOLKATA SUBHAS / CHANDRA BOSE')
        ARR line: ' CITY NAME   ARRIVAL TIME: HHMM   ARRIVAL DATE: DDMON'
        TERMINAL: immediately after DEP (dep terminal) or after ARR (arr terminal)
      No IATA codes in text — resolved via city→IATA map.
      Ticket: 'ETKT NNN NNNNNNNNNN' (split into prefix + 10-digit number)
      PNR:    'BOOKING REF : AMADEUS: XXXXXX, AIRLINE: XX/XXXXXX'
      Fares:  'AIR FARE : INR NNNNN'  /  'TOTAL : INR NNNNN'
              Tax codes inline: 'INR NNNNKs3', 'NNNNNYQs'
      Phone:  'TELEPHONE : (033)40011333'
      Date:   'DATE: 17 FEB 2026'

    Format B — Cryptic segment line (GDS terminal output):
      ' 1 6E 617 Y 05MAR VIZCCU HK1  2030 2200'
      (IATA codes concatenated as 6-char dep+arr)
    """

    _RE_DETECT = re.compile(
        r"AMADEUS|ELECTRONIC\s+TICKET\s+PASSENGER\s+ITINERARY|"
        r"FARE\s+CALCULATION\s*:|PASSENGER\s+ITINERARY\s+RECEIPT|"
        r"ETKT\s+\d{3}\s+\d{10}|BOOKING\s+REF\s*:\s*AMADEUS",
        re.I)

    # ── PNR ───────────────────────────────────────────────────────────────────
    # 'BOOKING REF : AMADEUS: 8QSQWK, AIRLINE: QR/8QSQWK'
    _RE_PNR_AMADEUS = re.compile(r'AMADEUS\s*:\s*([A-Z0-9]{6})\b', re.I)
    _RE_PNR_GENERIC = re.compile(
        r'(?:BOOKING\s*REF(?:ERENCE)?|RECORD\s*LOCATOR|PNR|RECLOC)\s*[:\s/\-]*([A-Z0-9]{6})\b',
        re.I)

    # ── Booking date  'DATE: 17 FEB 2026' ────────────────────────────────────
    _RE_BK_DATE = re.compile(
        r'DATE\s*[:\s]+(\d{1,2})\s+'
        r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+'
        r'(\d{4})',
        re.I)

    # ── Ticket  'ETKT 157 5013854089-90'  (split ticket: two coupons) ────────
    # Full 13-digit = airline_prefix(3) + serial(10)
    _RE_TICKET_ETKT = re.compile(r'ETKT\s+(\d{3})\s+(\d{10})(?:-\d+)?', re.I)
    _RE_TICKET_13   = re.compile(r'\b(\d{13})\b')   # fallback

    # ── Passenger  'NAME: PATNI/ROMY MR'  or bare 'PATNI/ROMY MR' ────────────
    _RE_PAX_NAME_LABEL = re.compile(
        r'NAME\s*[:\s]+([A-Z]+)/([A-Z][A-Z\s]+?)(?:\s+(MR|MRS|MS|MISS|DR|MSTR))?'
        r'(?=\s*\n|\s{2,}|$)',
        re.I)
    _RE_PAX_NAME_BARE  = re.compile(
        r'^\s*([A-Z]{2,})/([A-Z][A-Z\s]+?)\s+(MR|MRS|MS|MISS|DR|MSTR)\b',
        re.MULTILINE)

    # ── Agency/issuer phone  'TELEPHONE : (033)40011333' ──────────────────────
    _RE_PHONE = re.compile(
        r'TELEPHONE\s*[:\s]+(\(?\d{2,4}\)?[\d\s\-]{6,20}\d)',
        re.I)
    _RE_PHONE2 = re.compile(
        r'(?:CTCM|CTCH|P-|PHONE|APE|CONTACT)\s*[:\s]*([+\d][\d\s\-]{7,20}\d)',
        re.I)

    # ── Agency name (first non-blank line after header, or labeled) ───────────
    _RE_AGENCY = re.compile(
        r'^[ \t]+([A-Z][A-Z\s&.,]+?(?:LTD|PVT|INC|CORP|CO\.?|TRAVELS?|TOURS?))'
        r'[ \t]+DATE',
        re.MULTILINE | re.I)

    # ── PIR segment DEP line ─────────────────────────────────────────────────
    # ' KOLKATA SUBHAS  QR 541  O  27MAY  0400  OJINP1RE  ..  25K  OK'
    # Anchored to 1-3 leading spaces (distinguishes from continuation lines)
    _RE_PIR_DEP = re.compile(
        r'^[ \t]{1,3}'                     # 1-3 leading spaces
        r'([A-Z][A-Z\s\-]{2,30}?)'         # dep city (variable width)
        r'[ \t]{2,}'                        # 2+ spaces separator
        r'([A-Z0-9]{2})\s+'                # airline code
        r'(\d{1,4}[A-Z]?)\s+'             # flight number
        r'([A-Z])\s+'                      # booking class
        r'(\d{1,2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))\s+'  # date DDMON
        r'(\d{4})\s+'                      # dep time HHMM
        r'([A-Z0-9]+)\s+'                  # fare basis
        r'(?:[A-Z0-9]*\s+)?'               # NVB (optional)
        r'(?:\d{1,2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+)?'  # NVA
        r'(\d+K|\d+PC?)\s+'               # baggage
        r'(OK|HK|RR|HL|WL)',              # status
        re.MULTILINE | re.I)

    # ── PIR arrival line ──────────────────────────────────────────────────────
    # ' DOHA HAMAD                  ARRIVAL TIME: 0630   ARRIVAL DATE: 27MAY'
    _RE_PIR_ARR = re.compile(
        r'^[ \t]{1,3}'
        r'([A-Z][A-Z\s\-]{2,40}?)'         # arr city
        r'[ \t]{5,}'                        # wide gap (no flight info)
        r'ARRIVAL\s+TIME:\s+(\d{4})'        # arrival time
        r'.*?ARRIVAL\s+DATE:\s+(\d{1,2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))',
        re.MULTILINE | re.I)

    # ── Terminal lines ────────────────────────────────────────────────────────
    _RE_TERMINAL = re.compile(r'TERMINAL\s*[:\s]+([A-Z0-9]{1,3})', re.I)

    # ── Fare lines (PIR format) ───────────────────────────────────────────────
    _RE_AIR_FARE  = re.compile(r'AIR\s*FARE\s*[:\s]+(?:INR|USD|EUR|GBP)\s+([\d,]+)', re.I)
    _RE_TOTAL_PIR = re.compile(r'^[ \t]*TOTAL\s*[:\s]+(?:INR|USD|EUR|GBP)\s+([\d,]+)', re.MULTILINE | re.I)
    _RE_YQ        = re.compile(r'([\d,]+)YQ\b', re.I)
    _RE_YR        = re.compile(r'([\d,]+)YR\b', re.I)
    # K3 tax: 'INR     4735K3'
    _RE_K3_PIR    = re.compile(r'INR\s+([\d,]+)K3\b', re.I)
    # Generic K3
    _RE_K3_GEN    = re.compile(r'\bK3\b\s*(?:INR|USD|EUR)?\s*([\d,.]+)', re.I)

    # ── Format B: cryptic GDS segment line ───────────────────────────────────
    _RE_ADATE  = re.compile(r'(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{0,4})', re.I)
    _RE_SEG_B  = re.compile(
        r'^\s*\d\s+([A-Z0-9]{2})\s+(\d{1,4}[A-Z]?)\s+([A-Z])\s+(\d{1,2}[A-Z]{3}\d{0,4})\s+'
        r'([A-Z]{3})([A-Z]{3})\w*\s+(?:[A-Z]{2}\d\s+)?(\d{4})\s+(\d{4})(?:\s+(\d{1,2}[A-Z]{3}\d{0,4}))?',
        re.MULTILINE)

    # ── SSR ───────────────────────────────────────────────────────────────────
    _RE_SSR = re.compile(r'^SSR\s+(\w{4})\s+([A-Z0-9]{2})\s+\w+\s+([A-Z]{3})([A-Z]{3})', re.MULTILINE)
    _RE_FF  = re.compile(r'(?:FQTV|FFN|MILES|LOYALTY)\s+[A-Z0-9]{2}\s+([A-Z0-9]{5,15})\b', re.I)

    # ── City → IATA mapping (PIR format has no IATA codes in segment lines) ───
    # Keyed on UPPERCASE city name prefix for flexible matching
    _CITY_IATA: Dict[str, str] = {
        # India
        "KOLKATA":"CCU","CALCUTTA":"CCU","MUMBAI":"BOM","BOMBAY":"BOM",
        "DELHI":"DEL","NEW DELHI":"DEL","CHENNAI":"MAA","MADRAS":"MAA",
        "BENGALURU":"BLR","BANGALORE":"BLR","HYDERABAD":"HYD","KOCHI":"COK",
        "COCHIN":"COK","AHMEDABAD":"AMD","GOA":"GOI","JAIPUR":"JAI",
        "PUNE":"PNQ","AMRITSAR":"ATQ","CHANDIGARH":"IXC","LUCKNOW":"LKO",
        "GUWAHATI":"GAU","BHUBANESWAR":"BBI","VISAKHAPATNAM":"VTZ",
        "BAGDOGRA":"IXB","VARANASI":"VNS","PATNA":"PAT","NAGPUR":"NAG",
        "INDORE":"IDR","BHOPAL":"BHO","COIMBATORE":"CJB","MANGALORE":"IXE",
        "THIRUVANANTHAPURAM":"TRV","TRIVANDRUM":"TRV","SRINAGAR":"SXR",
        # Middle East
        "DOHA":"DOH","DUBAI":"DXB","ABU DHABI":"AUH","SHARJAH":"SHJ",
        "MUSCAT":"MCT","BAHRAIN":"BAH","KUWAIT":"KWI","RIYADH":"RUH",
        "JEDDAH":"JED","DAMMAM":"DMM",
        # Europe
        "LONDON HEATHROW":"LHR","LONDON GATWICK":"LGW","LONDON":"LHR",
        "PARIS CHARLES":"CDG","PARIS":"CDG","FRANKFURT":"FRA",
        "AMSTERDAM":"AMS","DUBAI TERMINAL":"DXB","LISBON":"LIS",
        "MADRID":"MAD","BARCELONA":"BCN","ROME":"FCO","MILAN":"MXP",
        "MUNICH":"MUC","VIENNA":"VIE","ZURICH":"ZRH","BRUSSELS":"BRU",
        "STOCKHOLM":"ARN","OSLO":"OSL","COPENHAGEN":"CPH","HELSINKI":"HEL",
        "ATHENS":"ATH","ISTANBUL":"IST",
        # Asia/Pacific
        "SINGAPORE":"SIN","KUALA LUMPUR":"KUL","BANGKOK":"BKK",
        "HONG KONG":"HKG","TOKYO NARITA":"NRT","TOKYO HANEDA":"HND",
        "TOKYO":"NRT","SEOUL INCHEON":"ICN","SEOUL":"ICN",
        "SYDNEY":"SYD","MELBOURNE":"MEL","BRISBANE":"BNE",
        "COLOMBO":"CMB","KATHMANDU":"KTM","DHAKA":"DAC",
        # Americas
        "NEW YORK JFK":"JFK","NEW YORK NEWARK":"EWR","NEW YORK":"JFK",
        "LOS ANGELES":"LAX","CHICAGO":"ORD","SAN FRANCISCO":"SFO",
        "TORONTO":"YYZ","VANCOUVER":"YVR",
        # Africa
        "JOHANNESBURG":"JNB","NAIROBI":"NBO","ADDIS ABABA":"ADD",
    }

    def detect(self, t: str) -> bool:
        return bool(self._RE_DETECT.search(t))

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _resolve_city(self, raw_city: str) -> str:
        """Map a city name (possibly multi-line / partial) to IATA code."""
        # Clean: collapse whitespace, remove continuation words
        city = re.sub(r'\s+', ' ', raw_city.strip()).upper()
        city = re.sub(r'\bINTERNATIONAL\b|\bAIRPORT\b|\bSUBHAS\b|\bCHANDRA\b|\bBOSE\b|\bHAMAD\b', '', city).strip()
        # Direct match
        if city in self._CITY_IATA:
            return self._CITY_IATA[city]
        # Prefix match (longest first)
        for key in sorted(self._CITY_IATA.keys(), key=len, reverse=True):
            if city.startswith(key) or key.startswith(city):
                return self._CITY_IATA[key]
        # Try mappings.py at runtime
        try:
            from mappings import AIRPORT_CODES
            for iata, name in AIRPORT_CODES.items():
                if city in name.upper() or name.upper().startswith(city[:6]):
                    return iata
        except ImportError:
            pass
        return "N/A"

    def _adate(self, raw: str, default_yr: str = "26") -> str:
        raw = raw.strip()
        m = self._RE_ADATE.match(raw)
        if not m: return "N/A"
        y = m.group(3).strip() if m.group(3).strip() else default_yr
        return _nd(m.group(1), m.group(2), y)

    def _parse_pir_segments(self, text: str) -> List[Dict]:
        """
        Parse multi-line PIR segment blocks.
        Strategy: scan lines in order, tracking state machine:
          DEP state  → triggered by flight-code line (has QR NNN or airline code)
          ARR state  → triggered by 'ARRIVAL TIME:' line
          TERM state → 'TERMINAL:' line, assign to most recent DEP or ARR
        """
        segments: List[Dict] = []
        lines = text.split('\n')

        i = 0
        while i < len(lines):
            ln = lines[i]
            m  = self._RE_PIR_DEP.match(ln)
            if m:
                dep_city_raw = m.group(1).strip()
                al_code      = m.group(2).upper()
                flt_no       = f"{al_code} {m.group(3)}"
                bk_class     = m.group(4).upper()
                dep_date     = self._adate(m.group(5))
                dep_time     = _hhmm(m.group(6))
                baggage_raw  = m.group(8)  # e.g. '25K' or '30PC'
                # Normalise baggage: '25K' → '25 Kg', '30PC' → '30 Pieces'
                baggage = re.sub(r'(\d+)K$', r'\1 Kg', baggage_raw, flags=re.I)
                baggage = re.sub(r'(\d+)PC?$', r'\1 Piece', baggage, flags=re.I)

                dep_terminal = "N/A"
                arr_city_raw = ""
                arr_time     = "N/A"
                arr_date     = dep_date
                arr_terminal = "N/A"

                # Scan ahead for optional dep-terminal, then arrival block
                j = i + 1
                while j < len(lines):
                    nln = lines[j].strip()

                    # Dep terminal: appears right after dep line (before arrival)
                    tm = self._RE_TERMINAL.match(lines[j].strip())
                    if tm and not arr_city_raw:
                        dep_terminal = tm.group(1).upper()
                        j += 1; continue

                    # Arrival line
                    am = self._RE_PIR_ARR.match(lines[j])
                    if am:
                        arr_city_raw = am.group(1).strip()
                        arr_time     = _hhmm(am.group(2))
                        arr_date     = self._adate(am.group(3))
                        j += 1
                        # Check immediate next lines for arr-terminal or city continuation
                        while j < len(lines):
                            an = lines[j].strip()
                            if not an:
                                j += 1; break
                            tm2 = self._RE_TERMINAL.match(an)
                            if tm2:
                                arr_terminal = tm2.group(1).upper()
                                j += 1; continue
                            # City continuation (e.g. 'CHANDRA BOSE' or 'INTERNATIONAL')
                            if re.match(r'^[A-Z][A-Z\s\-]+$', an) and len(an) < 40:
                                # Don't append continuation — it's noise for city resolution
                                j += 1; continue
                            break
                        break

                    # City continuation line for DEP (e.g. 'CHANDRA BOSE')
                    if (nln and re.match(r'^[A-Z][A-Z\s\-]+$', nln)
                            and len(nln) < 40 and not arr_city_raw
                            and not self._RE_PIR_DEP.match(lines[j])):
                        dep_city_raw += " " + nln
                        j += 1; continue

                    # Next segment starts → stop
                    if self._RE_PIR_DEP.match(lines[j]):
                        break

                    j += 1

                i = j  # advance outer loop past what we consumed
                dep_ap = self._resolve_city(dep_city_raw)
                arr_ap = self._resolve_city(arr_city_raw)
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
                    duration_extracted="N/A",
                ))
            else:
                i += 1

        return segments

    def extract(self, text: str) -> Dict:
        # ── PNR ───────────────────────────────────────────────────────────────
        m   = self._RE_PNR_AMADEUS.search(text) or self._RE_PNR_GENERIC.search(text)
        pnr = m.group(1).upper() if m else "N/A"

        # ── Booking date ──────────────────────────────────────────────────────
        m       = self._RE_BK_DATE.search(text)
        bk_date = _nd(m.group(1), m.group(2), m.group(3)) if m else "N/A"

        # ── Ticket numbers ────────────────────────────────────────────────────
        tickets = []
        for m in self._RE_TICKET_ETKT.finditer(text):
            # Reconstruct full 13-digit: prefix(3) + serial(10)
            tickets.append(m.group(1) + m.group(2))
        if not tickets:
            tickets = [m.group(1) for m in self._RE_TICKET_13.finditer(text)]

        # ── Fares (PIR style) ─────────────────────────────────────────────────
        m       = self._RE_AIR_FARE.search(text)
        base_f  = _num(m.group(1)) if m else None

        m       = self._RE_TOTAL_PIR.search(text)
        grand_t = _num(m.group(1)) if m else None

        # K3 tax: 'INR     4735K3'
        m    = self._RE_K3_PIR.search(text) or self._RE_K3_GEN.search(text)
        k3   = _num(m.group(1)) if m else None

        # Other taxes = all tax lines total minus K3
        # Sum INR amounts that appear on TAX / AIRLINE SURCHARGES lines
        tax_lines = re.findall(r'INR\s+([\d,]+)(?:[A-Z0-9]+)', text)
        all_tax_sum = sum(_num(v) or 0 for v in tax_lines)
        # Remove base fare itself if it crept in
        other_t = (all_tax_sum - (base_f or 0)) if all_tax_sum > (base_f or 0) else None

        # ── Currency ──────────────────────────────────────────────────────────
        cur_m = re.search(r'\b(INR|USD|EUR|GBP|AED|SGD)\b', text)
        cur   = cur_m.group(1) if cur_m else "INR"

        # ── Phone ─────────────────────────────────────────────────────────────
        m     = self._RE_PHONE.search(text) or self._RE_PHONE2.search(text)
        phone = re.sub(r'[\s\-()]', '', m.group(1)) if m else "N/A"
        if phone != "N/A" and not phone.startswith("+") and len(phone) >= 10:
            if phone.startswith("0"):
                phone = f"+91{phone[1:]}"
            elif len(phone) == 10:
                phone = f"+91{phone}"

        # ── Agency ────────────────────────────────────────────────────────────
        m      = self._RE_AGENCY.search(text)
        agency = m.group(1).strip().title() if m else "N/A"

        # ── Baggage from segment lines ────────────────────────────────────────
        # Prefer the first baggage value found in a DEP line; will be set per-seg below
        bag_codes = re.findall(r'\b(\d+K)\b', text)  # ['25K', '25K', '40K', '40K']
        baggage   = re.sub(r'(\d+)K$', r'\1 Kg', bag_codes[0]) if bag_codes else "N/A"

        # ── FF ────────────────────────────────────────────────────────────────
        ff_nums = [m.group(1) for m in self._RE_FF.finditer(text)]

        # ── Passenger names ───────────────────────────────────────────────────
        pax_names = []
        seen = set()

        # Labeled: 'NAME: PATNI/ROMY MR'
        for m in self._RE_PAX_NAME_LABEL.finditer(text):
            last  = m.group(1).strip().title()
            first = m.group(2).strip().title()
            title = (m.group(3) or "").strip().title()
            name  = f"{title} {first} {last}".strip()
            if name.lower() not in seen:
                seen.add(name.lower()); pax_names.append(name)

        # Bare: 'PATNI/ROMY MR' (first line or header)
        if not pax_names:
            for m in self._RE_PAX_NAME_BARE.finditer(text):
                last  = m.group(1).strip().title()
                first = m.group(2).strip().title()
                title = (m.group(3) or "").strip().title()
                name  = f"{title} {first} {last}".strip()
                if name.lower() not in seen:
                    seen.add(name.lower()); pax_names.append(name)

        # ── Segments ──────────────────────────────────────────────────────────
        # Try PIR format first; fall back to cryptic GDS format
        segments = self._parse_pir_segments(text)
        if not segments:
            # Format B: cryptic single-line GDS
            for m in self._RE_SEG_B.finditer(text):
                dep_ap, arr_ap = m.group(5).upper(), m.group(6).upper()
                segments.append(_seg(
                    airline=_airline_name(m.group(1)),
                    flight_number=f"{m.group(1).upper()} {m.group(2)}",
                    booking_class=m.group(3).upper(),
                    dep_city=_city(dep_ap), dep_airport=dep_ap,
                    dep_date=self._adate(m.group(4)), dep_time=_hhmm(m.group(7)),
                    arr_city=_city(arr_ap), arr_airport=arr_ap,
                    arr_date=self._adate(m.group(9)) if m.group(9) else self._adate(m.group(4)),
                    arr_time=_hhmm(m.group(8))))

        # Per-segment baggage: each segment's baggage from its DEP line
        dep_bags = re.findall(r'\b(\d+K|\d+PC?)\b.*?(?:OK|HK|RR)', text, re.I)
        for si, seg in enumerate(segments):
            if si < len(dep_bags):
                raw_b = dep_bags[si]
                seg["baggage"] = re.sub(r'(\d+)K$', r'\1 Kg', raw_b, flags=re.I)
                seg["baggage"] = re.sub(r'(\d+)PC?$', r'\1 Piece', seg["baggage"], flags=re.I)

        # ── Passengers ───────────────────────────────────────────────────────
        n_pax = max(len(pax_names), 1)
        passengers: List[Dict] = []
        for pi in range(n_pax):
            # Use per-segment baggage if available (segments may have diff allowances)
            pax_bag = baggage
            if segments:
                # Use max baggage across segments for this passenger
                seg_bags = [seg.get("baggage", baggage) for seg in segments if seg.get("baggage")]
                if seg_bags:
                    # Pick highest kg value
                    def _kg_val(s):
                        m2 = re.search(r'(\d+)', s or "")
                        return int(m2.group(1)) if m2 else 0
                    pax_bag = max(seg_bags, key=_kg_val)

            p = _pax(
                name=pax_names[pi] if pi < len(pax_names) else "N/A",
                ticket_number=tickets[pi] if pi < len(tickets) else "N/A",
                frequent_flyer_number=ff_nums[pi] if pi < len(ff_nums) else "N/A",
                baggage=pax_bag,
            )
            p["fare"] = {
                "base_fare": base_f, "k3_gst": k3,
                "other_taxes": other_t, "total_fare": grand_t,
            }
            passengers.append(p)

        # ── SSR distribution ──────────────────────────────────────────────────
        ssr_tuples = [(m.group(1).upper(), m.group(3).upper(), m.group(4).upper())
                      for m in self._RE_SSR.finditer(text)]
        _add_ssr(passengers, segments, ssr_tuples)

        return _flat_hints(
            ("amadeus", "Amadeus GDS"),
            pnr, bk_date, phone, cur, grand_t, "N/A",
            "N/A", "N/A", baggage, base_f, k3, other_t,
            tickets, ff_nums, [], segments, passengers,
            agency_name=agency,
        )


# ══════════════════════════════════════════════════════════════════════════════
# 4. Galileo / Apollo
# ══════════════════════════════════════════════════════════════════════════════
class GalileoExtractor:
    _RE_DETECT = re.compile(r"GALILEO|APOLLO|TRAVELPORT|1G\s|1V\s|WORLDSPAN", re.I)
    _RE_PNR    = re.compile(r"(?:RECORD\s*LOCATOR|PNR|BOOKING\s*REF)\s*[:/\s]+([A-Z0-9]{6})\b", re.I)
    _RE_RECLOC = re.compile(r"(?:^|\s)([A-Z0-9]{6})(?:/[A-Z0-9]{3})?\s+(?:OK|HK|HL|RR)\b", re.MULTILINE)
    _RE_ADATE  = re.compile(r"(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{0,4})", re.I)
    _RE_TICKET = re.compile(r"\b(\d{13})\b")
    _RE_FARE   = re.compile(r"BASE\s+FARE\s+([\d,.]+)(?:.*?TOTAL\s+([\d,.]+))?", re.I|re.S)
    _RE_FF     = re.compile(r"(?:FQTV|FFN|FF\s+NO)\s+([A-Z0-9]{5,15})\b", re.I)
    _RE_PHONE  = re.compile(r"(?:CTCM|CTCH|CTCB|PHONE)\s*[:\s]*([+\d][\d\s\-]{7,20}\d)", re.I)
    _RE_BAG    = re.compile(r"(?:BAGGAGE|BAG)\s*(?:ALLOWANCE)?\s*[:\s]*(\d+\s*(?:KG|PC))", re.I)
    _RE_PAX    = re.compile(r"^\s*\d+\.\s*([A-Z]+)/([A-Z][A-Z\s]*)(?:\s+(MR|MRS|MS|MISS|DR|MSTR))?", re.MULTILINE)
    _RE_SEG    = re.compile(
        r"^\s*\d+\.\s+([A-Z0-9]{2})\s+(\d{1,4}[A-Z]?)\s+([A-Z])\s+(\d{1,2}[A-Z]{3}\d{0,4})\s+"
        r"([A-Z]{3})([A-Z]{3})\s+(?:\w+\s+)?(\d{4})\s+(\d{4})(?:\s+(\d{1,2}[A-Z]{3}\d{0,4}))?",
        re.MULTILINE)
    _RE_SSR    = re.compile(r"^SSR\s+(\w{4})\s+([A-Z0-9]{2})\s+\w+\s+([A-Z]{3})([A-Z]{3})", re.MULTILINE)

    def detect(self, t): return bool(self._RE_DETECT.search(t))

    def _adate(self, raw, yr="26"):
        m = self._RE_ADATE.match(raw.strip())
        if not m: return "N/A"
        y = m.group(3).strip() if m.group(3).strip() else yr
        return _nd(m.group(1), m.group(2), y)

    def extract(self, text: str) -> Dict:
        m   = self._RE_PNR.search(text) or self._RE_RECLOC.search(text)
        pnr = m.group(1).upper() if m else "N/A"
        tickets = [m.group(1) for m in self._RE_TICKET.finditer(text)]
        m = self._RE_FARE.search(text)
        base_f  = _num(m.group(1)) if m and m.group(1) else None
        grand_t = _num(m.group(2)) if m and m.group(2) else None
        cur_m = re.search(r"\b(INR|USD|EUR|GBP|AED)\b", text)
        cur   = cur_m.group(1) if cur_m else "INR"
        m = self._RE_PHONE.search(text)
        phone = re.sub(r"[\s\-()]", "", m.group(1)) if m else "N/A"
        m = self._RE_BAG.search(text)
        baggage = m.group(1).strip().title() if m else "N/A"
        ff_nums = [m.group(1) for m in self._RE_FF.finditer(text)]

        pax_names = []
        seen = set()
        for m in self._RE_PAX.finditer(text):
            last, first = m.group(1).strip().title(), m.group(2).strip().title()
            title = (m.group(3) or "").strip().title()
            name = f"{title} {first} {last}".strip()
            if name.lower() not in seen:
                seen.add(name.lower()); pax_names.append(name)

        segments: List[Dict] = []
        for m in self._RE_SEG.finditer(text):
            dep_ap, arr_ap = m.group(5).upper(), m.group(6).upper()
            segments.append(_seg(
                airline=_airline_name(m.group(1)), flight_number=f"{m.group(1).upper()} {m.group(2)}",
                booking_class=m.group(3).upper(),
                dep_city=_city(dep_ap), dep_airport=dep_ap,
                dep_date=self._adate(m.group(4)), dep_time=_hhmm(m.group(7)),
                arr_city=_city(arr_ap), arr_airport=arr_ap,
                arr_date=self._adate(m.group(9)) if m.group(9) else self._adate(m.group(4)),
                arr_time=_hhmm(m.group(8))))

        n_pax = max(len(pax_names), 1)
        passengers = []
        for pi in range(n_pax):
            p = _pax(name=pax_names[pi] if pi < len(pax_names) else "N/A",
                     ticket_number=tickets[pi] if pi < len(tickets) else "N/A",
                     frequent_flyer_number=ff_nums[pi] if pi < len(ff_nums) else "N/A",
                     baggage=baggage)
            p["fare"] = {"base_fare": base_f, "k3_gst": None, "other_taxes": None, "total_fare": grand_t}
            passengers.append(p)

        ssr_tuples = [(m.group(1).upper(), m.group(3).upper(), m.group(4).upper())
                      for m in self._RE_SSR.finditer(text)]
        _add_ssr(passengers, segments, ssr_tuples)

        return _flat_hints(("galileo","Galileo/Apollo GDS"),
            pnr, "N/A", phone, cur, grand_t, "N/A",
            "N/A", "N/A", baggage, base_f, None, None,
            tickets, ff_nums, [], segments, passengers)


# ══════════════════════════════════════════════════════════════════════════════
# 5. Sabre GDS
# ══════════════════════════════════════════════════════════════════════════════
class SabreExtractor:
    _RE_DETECT = re.compile(r"\bSABRE\b|1S\s+\w|SABRETRAVELNETWORK|SABRE\s+(?:TRAVEL|RED)", re.I)
    _RE_PNR    = re.compile(r"(?:RECORD\s*LOCATOR|PNR|LOCATOR\s*-)\s*[:\-\s]*([A-Z0-9]{6})\b", re.I)
    _RE_ADATE  = re.compile(r"(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{0,2})", re.I)
    _RE_TICKET = re.compile(r"\b(\d{13})\b")
    _RE_TOTAL  = re.compile(r"(?:TOTAL\s*(?:AMOUNT|FARE)?|TTL)\s+(?:INR|USD|EUR|GBP)?\s*(\d[\d,.]+)", re.I)
    _RE_BASE   = re.compile(r"(?:BASE\s*FARE|FARE\s*(?:INR|USD|EUR|GBP))\s*(\d[\d,.]+)", re.I)
    _RE_TAX    = re.compile(r"(?:TOTAL\s*TAX|TAXES?)\s*(\d[\d,.]+)", re.I)
    _RE_K3     = re.compile(r"\bK3\b\s*(\d[\d,.]+)", re.I)
    _RE_FF     = re.compile(r"(?:FQTV|FF)\s+([A-Z]{2})\s+([A-Z0-9]{5,15})\b", re.I)
    _RE_PHONE  = re.compile(r"(?:CTCM|CTCH|P-|PHONE)\s*([+\d][\d\s\-]{7,20}\d)", re.I)
    _RE_BAG    = re.compile(r"(?:BAG|BAGGAGE)\s*(?:ALLOWANCE)?\s*(\d+\s*(?:KG|PC))", re.I)
    _RE_PAX    = re.compile(r"^\s*\d+\.\s*([A-Z]+)/([A-Z][A-Z\s]*)(?:\s*(MR|MRS|MS|MISS|DR|MSTR))?", re.MULTILINE)
    _RE_SEG    = re.compile(
        r"^\s*\d+\s+([A-Z0-9]{2})(\d{1,4}[A-Z]?)\s+([A-Z])\s+(\d{1,2}[A-Z]{3}\d*)\s+"
        r"([A-Z]{3})([A-Z]{3})\s+(?:\w+\s+)?(\d{4})\s+(\d{4})(?:\s+(\d{1,2}[A-Z]{3}\d*))?",
        re.MULTILINE)
    _RE_SSR    = re.compile(r"^SSR\s+(\w{4})\s+([A-Z0-9]{2})\s+\w+\s+([A-Z]{3})([A-Z]{3})", re.MULTILINE)
    _RE_FBASIS = re.compile(r"FARE\s*BASIS\s*[:\s]*([A-Z0-9]+)", re.I)

    def detect(self, t): return bool(self._RE_DETECT.search(t))

    def _sabre_date(self, raw, yr="26"):
        raw = raw.strip()
        # Strip trailing single weekday digit
        if len(raw) in (5,6) and raw[-1].isdigit():
            attempt = self._RE_ADATE.match(raw[:-1])
            if attempt:
                y = attempt.group(3).strip() if attempt.group(3).strip() else yr
                return _nd(attempt.group(1), attempt.group(2), y)
        m = self._RE_ADATE.match(raw)
        if not m: return "N/A"
        y = m.group(3).strip() if m.group(3).strip() else yr
        return _nd(m.group(1), m.group(2), y)

    def extract(self, text: str) -> Dict:
        g = lambda pat: (m := pat.search(text)) and m.group(1) or None

        m   = self._RE_PNR.search(text)
        pnr = m.group(1).upper() if m else "N/A"
        tickets = [m.group(1) for m in self._RE_TICKET.finditer(text)]
        grand_t = _num(g(self._RE_TOTAL) or "")
        base_f  = _num(g(self._RE_BASE)  or "")
        othtax  = _num(g(self._RE_TAX)   or "")
        k3      = _num(g(self._RE_K3)    or "")
        cur_m   = re.search(r"\b(INR|USD|EUR|GBP|AED)\b", text)
        cur     = cur_m.group(1) if cur_m else "INR"
        m = self._RE_PHONE.search(text)
        phone   = re.sub(r"[\s\-()]", "", m.group(1)) if m else "N/A"
        m = self._RE_BAG.search(text)
        baggage = m.group(1).strip().title() if m else "N/A"
        ff_nums = [m.group(2) for m in self._RE_FF.finditer(text)]
        fare_basis = g(self._RE_FBASIS)

        pax_names = []
        seen = set()
        for m in self._RE_PAX.finditer(text):
            last, first = m.group(1).strip().title(), m.group(2).strip().title()
            title = (m.group(3) or "").strip().title()
            name = f"{title} {first} {last}".strip()
            if name.lower() not in seen:
                seen.add(name.lower()); pax_names.append(name)

        segments: List[Dict] = []
        for m in self._RE_SEG.finditer(text):
            dep_ap, arr_ap = m.group(5).upper(), m.group(6).upper()
            segments.append(_seg(
                airline=_airline_name(m.group(1)), flight_number=f"{m.group(1).upper()} {m.group(2)}",
                booking_class=m.group(3).upper(),
                dep_city=_city(dep_ap), dep_airport=dep_ap,
                dep_date=self._sabre_date(m.group(4)), dep_time=_hhmm(m.group(7)),
                arr_city=_city(arr_ap), arr_airport=arr_ap,
                arr_date=self._sabre_date(m.group(9)) if m.group(9) else self._sabre_date(m.group(4)),
                arr_time=_hhmm(m.group(8))))

        n_pax = max(len(pax_names), 1)
        passengers = []
        for pi in range(n_pax):
            p = _pax(name=pax_names[pi] if pi < len(pax_names) else "N/A",
                     ticket_number=tickets[pi] if pi < len(tickets) else "N/A",
                     frequent_flyer_number=ff_nums[pi] if pi < len(ff_nums) else "N/A",
                     baggage=baggage)
            p["fare"] = {"base_fare": base_f, "k3_gst": k3, "other_taxes": othtax, "total_fare": grand_t}
            passengers.append(p)

        ssr_tuples = [(m.group(1).upper(), m.group(3).upper(), m.group(4).upper())
                      for m in self._RE_SSR.finditer(text)]
        _add_ssr(passengers, segments, ssr_tuples)

        return _flat_hints(("sabre","Sabre GDS"),
            pnr, "N/A", phone, cur, grand_t, "N/A",
            "N/A", "N/A", baggage, base_f, k3, othtax,
            tickets, ff_nums, [], segments, passengers,
            fare_basis=fare_basis)


# ══════════════════════════════════════════════════════════════════════════════
# 6. TBO
# ══════════════════════════════════════════════════════════════════════════════
class TBOExtractor:
    _RE_DETECT  = re.compile(r"Travel\s*Boutique\s*Online|tbo\.com|\bTBO\b.*(?:Booking|Travel)|Booking\s*ID\s*[:\s]*TBO", re.I)
    _RE_TBO_ID  = re.compile(r"Booking\s*ID\s*[:\s]*([A-Z0-9\-]{6,20})\b", re.I)
    _RE_PNR     = re.compile(r"(?:Airline\s*PNR|PNR|Ref(?:erence)?)\s*[:\s]*([A-Z0-9]{5,8})\b", re.I)
    _RE_BKDATE  = re.compile(rf"(?:Booking\s*Date|Date\s*of\s*Booking)\s*[:\s]*(\d{{1,2}})[/\-\s]({MONTH_PAT}|\d{{1,2}})[/\-\s](\d{{2,4}})", re.I)
    _RE_TOTAL   = re.compile(r"(?:Total\s*(?:Fare|Amount)|Amount\s*Payable)\s*[:\s]*(?:INR|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_BASE    = re.compile(r"Base\s*Fare\s*[:\s]*(?:INR|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_TAXES   = re.compile(r"(?:Taxes?(?:\s*(?:and|&)\s*Fees?)?|Surcharge)\s*[:\s]*(?:INR|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_K3      = re.compile(r"\bK3\b\s*[:\s]*(?:INR|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_BAGCHK  = re.compile(r"(?:Check-?in\s*Baggage|Checked\s*Baggage|Baggage)\s*[:\s]*(\d+\s*(?:kg|kgs?))", re.I)
    _RE_CLASS   = re.compile(r"(?:Class|Cabin)\s*[:\s]*(Economy|Business|First|Premium\s*Economy)", re.I)
    _RE_GSTNUM  = re.compile(r"(?:GSTIN|GST\s*No\.?)\s*[:\s]*([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z])\b", re.I)
    _RE_GSTNAM  = re.compile(r"GST\s*(?:registered\s*)?(?:company|firm|business)?\s*name[:\s]*(.+?)(?=\s*(?:\r?\n|GSTIN|GST\s*(?:No|Number|#)|$))", re.I)
    _RE_PHONE   = re.compile(r"(?:Phone|Mobile|Contact)\s*[:\s]*([+\d][\d\s\-]{7,20}\d)", re.I)
    _RE_TICKET  = re.compile(r"(?:E-?Ticket|Ticket\s*No\.?)\s*[:\s]*(\d{3}[\-\s]?\d{7,11})", re.I)
    _RE_FF      = re.compile(r"(?:Frequent\s*Fli(?:er|ght)|FFN?|Loyalty)\s*[:\s#]*([A-Z0-9]{5,15})\b", re.I)
    _RE_SEAT    = re.compile(r"Seat\s*(?:No\.?)?\s*[:\s]*(\d{1,3}[A-Z])\b", re.I)
    _RE_MEAL    = re.compile(r"(?:Meal|Food)\s*[:\s]*([A-Z]{4})\b", re.I)
    _RE_PAX     = re.compile(r"(?:Passenger|Travell?er|Name)\s*[:\s]*((?:Mr|Mrs|Ms|Miss|Dr|Prof)\.?\s+[A-Za-z][A-Za-z\s.]{2,60}?)(?=\s*\n|\s{2,}|$)", re.I)
    # Pipe: "6E 617 | VTZ → CCU | 05 Mar 26 | 20:30 → 22:00"
    _RE_SEG_PIPE= re.compile(rf"([A-Z0-9]{{2}})\s+(\d{{1,4}}[A-Z]?)\s*\|\s*([A-Z]{{3}})\s*[→\-]\s*([A-Z]{{3}})\s*\|\s*(\d{{1,2}})\s+({MONTH_PAT})\s+(\d{{2,4}})\s*\|\s*(\d{{1,2}}:\d{{2}})\s*[→\-]\s*(\d{{1,2}}:\d{{2}})", re.I)
    # Table: "6E617  VTZ  CCU  05Mar26  2030  2200"
    _RE_SEG_TAB = re.compile(rf"([A-Z0-9]{{2}})\s*(\d{{1,4}}[A-Z]?)\s+([A-Z]{{3}})\s+([A-Z]{{3}})\s+(\d{{1,2}})\s*({MONTH_PAT})\s*(\d{{2,4}})\s+(\d{{1,2}}:?\d{{2}})\s+(\d{{1,2}}:?\d{{2}})", re.I)

    def detect(self, t): return bool(self._RE_DETECT.search(t))

    def extract(self, text: str) -> Dict:
        g = lambda pat: (m := pat.search(text)) and m.group(1) or None

        m = self._RE_TBO_ID.search(text); tbo_id = m.group(1) if m else None
        m = self._RE_PNR.search(text);    pnr    = m.group(1).upper() if m else "N/A"
        m = self._RE_BKDATE.search(text)
        bk_date = _nd(m.group(1), m.group(2), m.group(3)) if m else "N/A"
        grand_t = _num(g(self._RE_TOTAL) or "")
        base_f  = _num(g(self._RE_BASE)  or "")
        othtax  = _num(g(self._RE_TAXES) or "")
        k3      = _num(g(self._RE_K3)    or "")
        bag_raw = g(self._RE_BAGCHK) or "N/A"
        baggage = re.sub(r"(\d+)\s*kg[s]?", r"\1 Kg", bag_raw, flags=re.I)
        m = self._RE_CLASS.search(text)
        class_ot= m.group(1).strip().title() if m else "N/A"
        gst_num = (g(self._RE_GSTNUM) or "N/A").upper()
        gst_co  = (g(self._RE_GSTNAM) or "N/A").strip().title()
        m = self._RE_PHONE.search(text)
        phone   = re.sub(r"[\s\-()]", "", m.group(1)) if m else "N/A"
        if phone != "N/A" and not phone.startswith("+") and len(phone) == 10: phone = f"+91{phone}"
        cur_m   = re.search(r"\b(INR|USD|EUR|GBP|AED)\b|[₹$€£]", text)
        cur     = {"₹":"INR","$":"USD","€":"EUR","£":"GBP"}.get(cur_m.group(0), cur_m.group(0)) if cur_m else "INR"
        tickets = [m.group(1) for m in self._RE_TICKET.finditer(text)]
        ff_nums = [m.group(1) for m in self._RE_FF.finditer(text)]
        seats_raw=[m.group(1) for m in self._RE_SEAT.finditer(text)]
        meal_codes=[m.group(1).upper() for m in self._RE_MEAL.finditer(text)]
        pax_names=list(dict.fromkeys(m.group(1).strip() for m in self._RE_PAX.finditer(text)))

        segments: List[Dict] = []
        for m in self._RE_SEG_PIPE.finditer(text):
            dep_ap, arr_ap = m.group(3).upper(), m.group(4).upper()
            dt = _nd(m.group(5), m.group(6), m.group(7))
            segments.append(_seg(airline=_airline_name(m.group(1)), flight_number=f"{m.group(1).upper()} {m.group(2)}",
                dep_city=_city(dep_ap), dep_airport=dep_ap, dep_date=dt, dep_time=m.group(8),
                arr_city=_city(arr_ap), arr_airport=arr_ap, arr_date=dt, arr_time=m.group(9)))
        if not segments:
            for m in self._RE_SEG_TAB.finditer(text):
                dep_ap, arr_ap = m.group(3).upper(), m.group(4).upper()
                dt = _nd(m.group(5), m.group(6), m.group(7))
                segments.append(_seg(airline=_airline_name(m.group(1)), flight_number=f"{m.group(1).upper()} {m.group(2)}",
                    dep_city=_city(dep_ap), dep_airport=dep_ap, dep_date=dt, dep_time=_hhmm(m.group(8)),
                    arr_city=_city(arr_ap), arr_airport=arr_ap, arr_date=dt, arr_time=_hhmm(m.group(9))))

        n_pax = max(len(pax_names), 1)
        passengers = []
        for pi in range(n_pax):
            p = _pax(name=pax_names[pi] if pi < len(pax_names) else "N/A",
                     ticket_number=tickets[pi] if pi < len(tickets) else "N/A",
                     frequent_flyer_number=ff_nums[pi] if pi < len(ff_nums) else "N/A",
                     baggage=baggage)
            p["fare"] = {"base_fare": base_f, "k3_gst": k3, "other_taxes": othtax, "total_fare": grand_t}
            if pi < len(seats_raw):
                p["seats"].append({"segment_index": 0, "seat_number": seats_raw[pi]})
            for si, code in enumerate(meal_codes):
                res = _resolve_code(code)
                if res["type"] == "meal":
                    p["meals"].append({"segment_index": si if si < len(segments) else 0,
                                       "code": res["code"], "name": res["name"]})
            passengers.append(p)

        return _flat_hints(("tbo","TBO (Travel Boutique Online)"),
            pnr, bk_date, phone, cur, grand_t, class_ot,
            gst_num, gst_co, baggage, base_f, k3, othtax,
            tickets, ff_nums, seats_raw, segments, passengers,
            tbo_booking_id=tbo_id)


# ══════════════════════════════════════════════════════════════════════════════
# 7. Riya Travel
# ══════════════════════════════════════════════════════════════════════════════
class RiyaExtractor:
    _RE_DETECT  = re.compile(r"Riya\s*Travel|riya\.travel|riyatravels\.com|VCH-\w+|(?:Booking\s*Ref|Reference)\s*[:\s]*RT\d", re.I)
    _RE_VOUCHER = re.compile(r"VCH-([A-Z0-9]+)\b", re.I)
    _RE_PNR     = re.compile(r"(?:Airline\s*PNR|PNR|Reference|Booking\s*Ref)\s*[:\s]*([A-Z0-9]{5,8})\b", re.I)
    _RE_BKDATE  = re.compile(rf"(?:Booking\s*Date|Date)\s*[:\s]*(\d{{1,2}})[/\-]({MONTH_PAT}|\d{{1,2}})[/\-](\d{{2,4}})", re.I)
    _RE_TOTAL   = re.compile(r"Total\s*[:\|]\s*(?:INR|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_BASE    = re.compile(r"Base\s*(?:Fare)?\s*[:\|]\s*(?:INR|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_YQ      = re.compile(r"\bYQ\b\s*[:\|]\s*(?:INR|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_K3      = re.compile(r"\bK3\b\s*[:\|]\s*(?:INR|[^0-9\s])?\s*([\d,]+(?:\.\d{1,2})?)", re.I)
    _RE_BAG     = re.compile(r"Baggage\s*[:\|]\s*(\d+\s*(?:kg|kgs?))", re.I)
    _RE_CLASS   = re.compile(r"(?:Class|Fare\s*Type|Cabin)\s*[:\|]\s*(Economy|Business|First|Premium\s*Economy|Corporate)", re.I)
    _RE_GSTNUM  = re.compile(r"GST\s*(?:No\.?|Number)\s*[:\|]\s*([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z])\b", re.I)
    _RE_GSTNAM  = re.compile(r"GST\s*Company\s*[:\|]\s*(.+?)(?=\s*(?:\n|GST\s*No|$))", re.I)
    _RE_PHONE   = re.compile(r"(?:Phone|Mobile|Contact)\s*[:\|]\s*([+\d][\d\s\-]{7,20}\d)", re.I)
    _RE_TICKET  = re.compile(r"(?:E-?Ticket|Ticket\s*No\.?)\s*[:\s]*(\d{3}[\-\s]?\d{7,11})", re.I)
    _RE_FF      = re.compile(r"(?:Frequent\s*Fli(?:er|ght)|FFN?|Loyalty)\s*[:\|#]\s*([A-Z0-9]{5,15})\b", re.I)
    _RE_SEAT    = re.compile(r"Seat\s*[:\|]\s*(\d{1,3}[A-Z])\b", re.I)
    _RE_MEAL    = re.compile(r"(?:Meal|Food)\s*[:\|]\s*([A-Z]{4})\b", re.I)
    # PAX slash: "SHARMA/RAHUL MR"
    _RE_PAX_SL  = re.compile(r"(?:Passenger|Travell?er|Name)\s*[:\|]*\s*([A-Z]{2,})/([A-Z][A-Z\s]+?)(?:\s+(MR|MRS|MS|MISS|DR|MSTR))?(?:\s*\||\s*\n|\s{2,}|$)", re.I)
    # PAX title: "Mr. Rahul Sharma"
    _RE_PAX_TI  = re.compile(r"(?:Passenger|Travell?er|Name)\s*[:\|]*\s*((?:Mr|Mrs|Ms|Miss|Dr|Prof)\.?\s+[A-Za-z][A-Za-z\s.]{2,60}?)(?=\s*\n|\s{2,}|$)", re.I)
    # "VTZ/CCU  05Mar26  6E617  20:30  22:00"
    _RE_SEG     = re.compile(rf"([A-Z]{{3}})[/\-]([A-Z]{{3}})\s+(\d{{1,2}})({MONTH_PAT})(\d{{2,4}})\s+([A-Z0-9]{{2}})\s*(\d{{1,4}}[A-Z]?)\s+(\d{{1,2}}:\d{{2}})\s+(\d{{1,2}}:\d{{2}})", re.I)

    def detect(self, t): return bool(self._RE_DETECT.search(t))

    def _cls(self, raw):
        r = raw.lower()
        if "business" in r: return "Business"
        if "first" in r:    return "First"
        if "premium" in r:  return "Premium Economy"
        return "Economy"

    def extract(self, text: str) -> Dict:
        g = lambda pat: (m := pat.search(text)) and m.group(1) or None

        m = self._RE_VOUCHER.search(text); voucher = m.group(1) if m else None
        m = self._RE_PNR.search(text);     pnr     = m.group(1).upper() if m else "N/A"
        m = self._RE_BKDATE.search(text)
        bk_date = _nd(m.group(1), m.group(2), m.group(3)) if m else "N/A"
        grand_t = _num(g(self._RE_TOTAL) or "")
        base_f  = _num(g(self._RE_BASE)  or "")
        yq      = _num(g(self._RE_YQ)    or "")
        k3      = _num(g(self._RE_K3)    or "")
        othtax  = (yq or 0) + (k3 or 0) or None
        bag_raw = g(self._RE_BAG) or "N/A"
        baggage = re.sub(r"(\d+)\s*kg[s]?", r"\1 Kg", bag_raw, flags=re.I)
        m = self._RE_CLASS.search(text)
        class_ot= self._cls(m.group(1)) if m else "N/A"
        gst_num = (g(self._RE_GSTNUM) or "N/A").upper()
        gst_co  = (g(self._RE_GSTNAM) or "N/A").strip().title()
        m = self._RE_PHONE.search(text)
        phone   = re.sub(r"[\s\-()]", "", m.group(1)) if m else "N/A"
        if phone != "N/A" and not phone.startswith("+") and len(phone) == 10: phone = f"+91{phone}"
        cur_m   = re.search(r"\b(INR|USD|EUR|GBP|AED)\b|[₹$€£]", text)
        cur     = {"₹":"INR","$":"USD","€":"EUR","£":"GBP"}.get(cur_m.group(0), cur_m.group(0)) if cur_m else "INR"
        tickets = [m.group(1) for m in self._RE_TICKET.finditer(text)]
        ff_nums = [m.group(1) for m in self._RE_FF.finditer(text)]
        seats_raw=[m.group(1) for m in self._RE_SEAT.finditer(text)]
        meal_codes=[m.group(1).upper() for m in self._RE_MEAL.finditer(text)]

        pax_names = []
        seen = set()
        for m in self._RE_PAX_SL.finditer(text):
            last, first = m.group(1).strip().title(), m.group(2).strip().title()
            title = (m.group(3) or "").strip().title()
            name = f"{title} {first} {last}".strip()
            if name.lower() not in seen: seen.add(name.lower()); pax_names.append(name)
        for m in self._RE_PAX_TI.finditer(text):
            name = m.group(1).strip()
            if name.lower() not in seen: seen.add(name.lower()); pax_names.append(name)

        segments: List[Dict] = []
        for m in self._RE_SEG.finditer(text):
            dep_ap, arr_ap = m.group(1).upper(), m.group(2).upper()
            dt = _nd(m.group(3), m.group(4), m.group(5))
            segments.append(_seg(airline=_airline_name(m.group(6)), flight_number=f"{m.group(6).upper()} {m.group(7)}",
                dep_city=_city(dep_ap), dep_airport=dep_ap, dep_date=dt, dep_time=m.group(8),
                arr_city=_city(arr_ap), arr_airport=arr_ap, arr_date=dt, arr_time=m.group(9)))

        n_pax = max(len(pax_names), 1)
        passengers = []
        for pi in range(n_pax):
            p = _pax(name=pax_names[pi] if pi < len(pax_names) else "N/A",
                     ticket_number=tickets[pi] if pi < len(tickets) else "N/A",
                     frequent_flyer_number=ff_nums[pi] if pi < len(ff_nums) else "N/A",
                     baggage=baggage)
            p["fare"] = {"base_fare": base_f, "k3_gst": k3, "other_taxes": othtax, "total_fare": grand_t}
            if pi < len(seats_raw):
                p["seats"].append({"segment_index": 0, "seat_number": seats_raw[pi]})
            for si, code in enumerate(meal_codes):
                res = _resolve_code(code)
                if res["type"] == "meal":
                    p["meals"].append({"segment_index": si if si < len(segments) else 0,
                                       "code": res["code"], "name": res["name"]})
            passengers.append(p)

        return _flat_hints(("riya","Riya Travel"),
            pnr, bk_date, phone, cur, grand_t, class_ot,
            gst_num, gst_co, baggage, base_f, k3, othtax,
            tickets, ff_nums, seats_raw, segments, passengers,
            riya_voucher=voucher)


# ══════════════════════════════════════════════════════════════════════════════
# Source registry & public API
# ══════════════════════════════════════════════════════════════════════════════
_EXTRACTORS = [
    IndiGoExtractor(), AirIndiaExpressExtractor(),
    AmadeusExtractor(), GalileoExtractor(), SabreExtractor(),
    TBOExtractor(), RiyaExtractor(),
]

SOURCE_NAMES = {
    "indigo":"IndiGo Direct", "air_india_express":"Air India Express",
    "amadeus":"Amadeus GDS",  "galileo":"Galileo/Apollo GDS",
    "sabre":"Sabre GDS",      "tbo":"TBO (Travel Boutique Online)",
    "riya":"Riya Travel",     "unknown":"Unknown / Generic",
}


def detect_source(text: str) -> str:
    for e in _EXTRACTORS:
        if e.detect(text): return e.extract(text)["source"]
    return "unknown"


def extract_by_source(text: str) -> Dict:
    for e in _EXTRACTORS:
        if e.detect(text): return e.extract(text)
    return {"source": "unknown", "source_name": SOURCE_NAMES["unknown"]}


def enrich_regex_hints(raw_text: str, base_hints: dict) -> dict:
    """
    Call AFTER regex_extract(), BEFORE llm_extract().
    Merges source-specific fields into base regex hint dict.
    Structured output fields (booking, gst_details, passengers, segments)
    always come from the source extractor when available.
    """
    src = extract_by_source(raw_text)
    if src.get("source") == "unknown":
        return base_hints

    # These always come from source extractor (more precise)
    SOURCE_WINS = {
        "source", "source_name",
        "booking", "gst_details", "passengers", "segments", "barcode",
        "parsed_segments", "passenger_names",
        "tbo_booking_id", "riya_voucher", "agency_name", "email",
        "form_of_payment", "fare_basis", "indigo_fare_type", "booking_status",
    }

    for k, v in src.items():
        if k in SOURCE_WINS:
            base_hints[k] = v
        elif base_hints.get(k) in (None, "N/A", [], {}):
            base_hints[k] = v

    return base_hints


def full_extract(raw_text: str) -> Dict:
    """
    Full extraction pipeline that produces the SAME output format as
    llm_extractor.extract() — including journey pipeline, normalization,
    booking class resolution, duration calculation, layovers, and trip type.

    Returns:
        {
            "metadata": {...},
            "booking": {...},
            "gst_details": {...},
            "passengers": [...],
            "segments": [...],
            "journey": {...},
            "barcode": ...
        }
    """
    src = extract_by_source(raw_text)
    source_name = src.get("source_name", "Unknown")
    source_key  = src.get("source", "unknown")

    # Build the standard data dict from source extractor output
    data = {
        "booking":     src.get("booking", {}),
        "gst_details": src.get("gst_details", {"gst_number": "N/A", "company_name": "N/A"}),
        "passengers":  src.get("passengers", []),
        "segments":    src.get("segments", []),
        "barcode":     src.get("barcode", None),
    }

    # Apply normalization (phone, name, baggage, booking class, meals, ancillaries, GST)
    data = normalize_data(data)

    # Apply journey pipeline (UTC conversion → clustering → layovers → trip type)
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

    jrn = data.get("journey", {})

    return {
        "metadata": {
            "version": PARSER_VERSION,
            "source": source_key,
            "source_name": source_name,
            "llm_status": "regex_only",
            "parsed_at": datetime.now(timezone.utc).isoformat() + "Z",
            "warnings": warnings,
            "errors": errors,
        },
        "booking":      data.get("booking", {}),
        "gst_details":  data.get("gst_details", {"gst_number": "N/A", "company_name": "N/A"}),
        "passengers":   data.get("passengers", []),
        "segments":     data.get("segments", []),
        "journey":      data.get("journey", {}),
        "barcode":      data.get("barcode", None),
    }


# ── Self-test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json

    INDIGO_SAMPLE = """
	
PATNI/ROMY MR 27MAY CCU DOH

------------------------------------------------------------------------------

This document is automatically generated.
Please do not respond to this mail.


                            ELECTRONIC TICKET
                       PASSENGER ITINERARY RECEIPT

 TIME TRAVELS LTD                   DATE: 17 FEB 2026
 13 CAMAC STREET                   AGENT: 1212
                                    NAME: PATNI/ROMY MR
 CALCUTTA 700 017
 IATA       : 143 27821
 TELEPHONE  : (033)40011333

 ISSUING AIRLINE                        : QATAR AIRWAYS
 TICKET NUMBER                          : ETKT 157 5013854089-90
 BOOKING REF : AMADEUS: 8QSQWK, AIRLINE: QR/8QSQWK

 FROM /TO        FLIGHT  CL DATE   DEP      FARE BASIS    NVB   NVA   BAG  ST

 KOLKATA SUBHAS  QR 541  O  27MAY  0400     OJINP1RE            27SEP 25K  OK
 CHANDRA BOSE
 DOHA HAMAD                       ARRIVAL TIME: 0630   ARRIVAL DATE: 27MAY
 INTERNATIONAL

 DOHA HAMAD      QR 341  O  27MAY  0740     OJINP1RE            27SEP 25K  OK
 INTERNATIONAL
 LISBON AIRPORT                   ARRIVAL TIME: 1330   ARRIVAL DATE: 27MAY
 TERMINAL:1



 LONDON GATWICK  QR 330  P  13JUN  0900     PJINP5ZE      31MAY 27SEP 40K  OK
 TERMINAL:N
 DOHA HAMAD                       ARRIVAL TIME: 1740   ARRIVAL DATE: 13JUN
 INTERNATIONAL

 DOHA HAMAD      QR 540  P  13JUN  1855     PJINP5ZE      31MAY 27SEP 40K  OK
 INTERNATIONAL
 KOLKATA SUBHAS                   ARRIVAL TIME: 0230   ARRIVAL DATE: 14JUN
 CHANDRA BOSE


 AT CHECK-IN, PLEASE SHOW A PICTURE IDENTIFICATION AND THE DOCUMENT YOU GAVE
 FOR REFERENCE AT RESERVATION TIME

 ENDORSEMENTS  : /C1-2 NON END/CHNG PENALTIES AS PER RULE /C4-5 NON END/CHNG
               FEE PER RULE/LOUNGE AND SEAT CHARGEABLE T AND C APPLY/C3-4 NON
 PAYMENT       : CASH

 FARE CALCULATION   :CCU QR X/DOH QR LIS185.42/-LON QR X/DOH QR CCU Q
                     LONCCU20.00 183.20NUC388.62END ROE90.061759XT
                     1552IN4735K31283P22986G4272PZ2986QA498R930184GB2771UB

 AIR FARE           : INR     35000
 TAX                : INR     1552IN    INR     4735K3    INR     1283P2
                      INR     2986G4    INR     272PZ     INR     2986QA
                      INR     498R9     INR     30184GB   INR     2771UB
 AIRLINE SURCHARGES : INR     54794YQ   INR     4892YR
 TOTAL              : INR     141953


FLIGHT(S) EMISSIONS 620.63 KG CO2 PER TOTAL NUMBER IN PARTY
SOURCE: THE DYNAMIC FIELDS COMING FROM TRAVEL IMPACT EXPLORER API RESPONSE

NOTICE
CARRIAGE AND OTHER SERVICES PROVIDED BY THE CARRIER ARE SUBJECT TO CONDITIONS
OF CARRIAGE, WHICH ARE HEREBY INCORPORATED BY REFERENCE. THESE CONDITIONS MAY
 BE OBTAINED FROM THE ISSUING CARRIER.

THE ITINERARY/RECEIPT CONSTITUTES THE PASSENGER TICKET FOR THE PURPOSES OF
ARTICLE 3 OF THE WARSAW CONVENTION, EXCEPT WHERE THE CARRIER DELIVERS TO THE
PASSENGER ANOTHER DOCUMENT COMPLYING WITH THE REQUIREMENTS OF ARTICLE 3.

PASSENGERS ON A JOURNEY INVOLVING AN ULTIMATE DESTINATION OR A STOP IN A
COUNTRY OTHER THAN THE COUNTRY OF DEPARTURE ARE ADVISED THAT INTERNATIONAL
TREATIES KNOWN AS THE MONTREAL CONVENTION, OR ITS PREDECESSOR, THE WARSAW
CONVENTION, INCLUDING ITS AMENDMENTS (THE WARSAW CONVENTION SYSTEM), MAY APPLY
TO THE ENTIRE JOURNEY, INCLUDING ANY PORTION THEREOF WITHIN A COUNTRY. FOR
SUCH PASSENGERS, THE APPLICABLE TREATY, INCLUDING SPECIAL CONTRACTS OF
CARRIAGE EMBODIED IN ANY APPLICABLE TARIFFS, GOVERNS AND MAY LIMIT THE
LIABILITY OF THE CARRIER. THESE CONVENTIONS GOVERN AND MAY LIMIT THE
LIABILITYOF AIR CARRIERS FOR DEATH OR BODILY INJURY OR LOSS OF OR DAMAGE TO
BAGGAGE, AND FOR DELAY.

THE CARRIAGE OF CERTAIN HAZARDOUS MATERIALS, LIKE AEROSOLS, FIREWORKS, AND
FLAMMABLE LIQUIDS, ABOARD THE AIRCRAFT IS FORBIDDEN. IF YOU DO NOT UNDERSTAND
THESE RESTRICTIONS, FURTHER INFORMATION MAY BE OBTAINED FROM YOUR AIRLINE.

DATA PROTECTION NOTICE: YOUR PERSONAL DATA WILL BE PROCESSED IN ACCORDANCE
WITH THE APPLICABLE CARRIER'S PRIVACY POLICY AND, IF YOUR BOOKING IS MADE VIA
A RESERVATION SYSTEM PROVIDER ( GDS ), WITH ITS PRIVACY POLICY. THESE ARE
AVAILABLE AT http://www.iatatravelcenter.com/privacy OR FROM THE CARRIER OR
GDS DIRECTLY. YOU SHOULD READ THIS DOCUMENTATION, WHICH APPLIES TO YOUR
BOOKING AND SPECIFIES, FOR EXAMPLE, HOW YOUR PERSONAL DATA IS COLLECTED,
STORED, USED, DISCLOSED AND TRANSFERRED.(APPLICABLE FOR INTERLINE CARRIAGE)
"""

    from llm_extractor import print_result
    result = full_extract(INDIGO_SAMPLE)
    print_result(result)
    print("\n--- JSON ---")
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))