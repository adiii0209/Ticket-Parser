"""
Flight Ticket Extractor — Hybrid Regex + LLM + Zero-Dependency Timezone Duration
==================================================================================
v2.2 fixes:
  - pax_type: word-boundary false positives on "efficient","sufficient","infant"
               in body text — now ONLY extracts from passenger-label context
  - class_of_travel: "Business Park" / "Business Class" false positives —
               now requires travel-context words nearby
  - frequent_flyer: suffix match on "sufficient" etc — stricter prefix anchor
  - duration_calculated: time-ordering bug for multi-segment — now per-segment
  - NEW: layover calculation between consecutive segments
  - NEW: journey_total (first dep → last arr, wall-clock)
"""

import os
import re
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

try:
    from mappings import (AIRPORT_CODES, AIRLINE_CODES, AIRPORT_TZ_MAP,
                          MEAL_CODES, ANCILLARY_CODES, resolve_booking_class)
except ImportError:
    raise RuntimeError("mappings.py not found. Place it in the same directory.")

OPENROUTER_API_KEY = "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"
OPENROUTER_URL     = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1/chat/completions")
MODEL              = os.getenv("MODEL", "openai/gpt-4o-mini")
TEMPERATURE        = 0
MAX_TOKENS         = 8100

if not OPENROUTER_API_KEY:
    raise ValueError("OPENROUTER_API_KEY not set in .env")

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("TicketExtractor")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 0 — ZERO-DEPENDENCY UTC OFFSET TABLE
# ══════════════════════════════════════════════════════════════════════════════

_IANA_OFFSETS = {
    "Asia/Kolkata":(330,330),"Asia/Kathmandu":(345,345),"Asia/Kabul":(270,270),
    "Asia/Tehran":(210,270),"Asia/Dubai":(240,240),"Asia/Qatar":(180,180),
    "Asia/Bahrain":(180,180),"Asia/Kuwait":(180,180),"Asia/Riyadh":(180,180),
    "Asia/Baghdad":(180,180),"Asia/Muscat":(240,240),"Asia/Karachi":(300,300),
    "Asia/Dhaka":(360,360),"Asia/Colombo":(330,330),"Asia/Bangkok":(420,420),
    "Asia/Ho_Chi_Minh":(420,420),"Asia/Jakarta":(420,420),"Asia/Makassar":(480,480),
    "Asia/Singapore":(480,480),"Asia/Kuala_Lumpur":(480,480),"Asia/Kuching":(480,480),
    "Asia/Manila":(480,480),"Asia/Shanghai":(480,480),"Asia/Hong_Kong":(480,480),
    "Asia/Macau":(480,480),"Asia/Taipei":(480,480),"Asia/Urumqi":(360,360),
    "Asia/Phnom_Penh":(420,420),"Asia/Vientiane":(420,420),"Asia/Yangon":(390,390),
    "Asia/Brunei":(480,480),"Asia/Tokyo":(540,540),"Asia/Seoul":(540,540),
    "Asia/Pyongyang":(540,540),"Asia/Tashkent":(300,300),"Asia/Samarkand":(300,300),
    "Asia/Almaty":(360,360),"Asia/Bishkek":(360,360),"Asia/Dushanbe":(300,300),
    "Asia/Ashgabat":(300,300),"Asia/Tbilisi":(240,240),"Asia/Yerevan":(240,240),
    "Asia/Baku":(240,240),"Asia/Jerusalem":(120,180),"Asia/Amman":(120,180),
    "Asia/Beirut":(120,180),"Asia/Nicosia":(120,180),
    "Indian/Maldives":(300,300),"Indian/Mauritius":(240,240),
    "Indian/Reunion":(240,240),"Indian/Mahe":(240,240),"Indian/Antananarivo":(180,180),
    "UTC":(0,0),
    "Europe/London":(0,60),"Europe/Dublin":(0,60),"Europe/Belfast":(0,60),
    "Europe/Paris":(60,120),"Europe/Berlin":(60,120),"Europe/Amsterdam":(60,120),
    "Europe/Brussels":(60,120),"Europe/Madrid":(60,120),"Europe/Rome":(60,120),
    "Europe/Zurich":(60,120),"Europe/Vienna":(60,120),"Europe/Copenhagen":(60,120),
    "Europe/Oslo":(60,120),"Europe/Stockholm":(60,120),"Europe/Helsinki":(120,180),
    "Europe/Athens":(120,180),"Europe/Bucharest":(120,180),
    "Europe/Istanbul":(180,180),"Europe/Lisbon":(0,60),
    "Atlantic/Madeira":(0,60),"Atlantic/Azores":(-60,0),
    "Europe/Prague":(60,120),"Europe/Budapest":(60,120),"Europe/Warsaw":(60,120),
    "Europe/Bratislava":(60,120),"Europe/Sofia":(120,180),"Europe/Belgrade":(60,120),
    "Europe/Zagreb":(60,120),"Europe/Ljubljana":(60,120),"Europe/Vilnius":(120,180),
    "Europe/Riga":(120,180),"Europe/Tallinn":(120,180),"Europe/Moscow":(180,180),
    "Europe/Minsk":(180,180),"Europe/Kiev":(120,180),"Europe/Samara":(240,240),
    "America/New_York":(-300,-240),"America/Detroit":(-300,-240),
    "America/Indiana/Indianapolis":(-300,-240),"America/Chicago":(-360,-300),
    "America/Winnipeg":(-360,-300),"America/Denver":(-420,-360),
    "America/Boise":(-420,-360),"America/Los_Angeles":(-480,-420),
    "America/Vancouver":(-480,-420),"America/Phoenix":(-420,-420),
    "America/Anchorage":(-540,-480),"Pacific/Honolulu":(-600,-600),
    "America/Toronto":(-300,-240),"America/Montreal":(-300,-240),
    "America/Halifax":(-240,-180),"America/St_Johns":(-210,-150),
    "America/Moncton":(-240,-180),"America/Edmonton":(-420,-360),
    "America/Regina":(-360,-360),"America/Yellowknife":(-420,-360),
    "America/Whitehorse":(-480,-420),"America/Iqaluit":(-300,-240),
    "America/Mexico_City":(-360,-300),"America/Monterrey":(-360,-300),
    "America/Merida":(-360,-300),"America/Chihuahua":(-420,-360),
    "America/Hermosillo":(-420,-420),"America/Mazatlan":(-420,-360),
    "America/Tijuana":(-480,-420),"America/Cancun":(-300,-300),
    "America/Bogota":(-300,-300),"America/Lima":(-300,-300),
    "America/Guayaquil":(-300,-300),"America/Panama":(-300,-300),
    "America/Costa_Rica":(-360,-360),"America/El_Salvador":(-360,-360),
    "America/Guatemala":(-360,-360),"America/Tegucigalpa":(-360,-360),
    "America/Managua":(-360,-360),"America/Belize":(-360,-360),
    "America/Havana":(-300,-240),"America/Nassau":(-300,-240),
    "America/Santo_Domingo":(-240,-240),"America/Puerto_Rico":(-240,-240),
    "America/Jamaica":(-300,-300),"America/Aruba":(-240,-240),
    "America/Curacao":(-240,-240),"America/Kralendijk":(-240,-240),
    "America/Port_of_Spain":(-240,-240),"America/Barbados":(-240,-240),
    "America/Grenada":(-240,-240),"America/St_Lucia":(-240,-240),
    "America/Lower_Princes":(-240,-240),"America/St_Kitts":(-240,-240),
    "America/Antigua":(-240,-240),"America/Cayman":(-300,-300),
    "America/Paramaribo":(-180,-180),"America/Cayenne":(-180,-180),
    "America/Guyana":(-240,-240),"America/Caracas":(-240,-240),
    "America/La_Paz":(-240,-240),"America/Asuncion":(-240,-180),
    "America/Montevideo":(-180,-120),"America/Sao_Paulo":(-180,-120),
    "America/Fortaleza":(-180,-180),"America/Recife":(-180,-180),
    "America/Bahia":(-180,-180),"America/Belem":(-180,-180),
    "America/Manaus":(-240,-240),"America/Cuiaba":(-240,-180),
    "America/Maceio":(-180,-180),"America/Santiago":(-240,-180),
    "America/Punta_Arenas":(-180,-180),
    "America/Argentina/Buenos_Aires":(-180,-180),
    "America/Argentina/Cordoba":(-180,-180),"America/Argentina/Mendoza":(-180,-180),
    "America/Argentina/Salta":(-180,-180),"America/Argentina/Jujuy":(-180,-180),
    "America/Argentina/Ushuaia":(-180,-180),
    "Africa/Johannesburg":(120,120),"Africa/Nairobi":(180,180),
    "Africa/Dar_es_Salaam":(180,180),"Africa/Kampala":(180,180),
    "Africa/Kigali":(120,120),"Africa/Bujumbura":(120,120),
    "Africa/Kinshasa":(60,60),"Africa/Windhoek":(120,120),
    "Africa/Harare":(120,120),"Africa/Lusaka":(120,120),
    "Africa/Blantyre":(120,120),"Africa/Lagos":(60,60),
    "Africa/Abidjan":(0,0),"Africa/Accra":(0,0),"Africa/Dakar":(0,0),
    "Africa/Douala":(60,60),"Africa/Libreville":(60,60),
    "Africa/Porto-Novo":(60,60),"Africa/Lome":(0,0),
    "Africa/Ouagadougou":(0,0),"Africa/Freetown":(0,0),
    "Africa/Monrovia":(0,0),"Africa/Banjul":(0,0),
    "Africa/Nouakchott":(0,0),"Africa/Asmara":(180,180),
    "Africa/Djibouti":(180,180),"Africa/Mogadishu":(180,180),
    "Africa/Addis_Ababa":(180,180),"Africa/Cairo":(120,120),
    "Africa/Casablanca":(60,60),"Africa/Algiers":(60,60),
    "Africa/Tunis":(60,60),"Africa/Tripoli":(120,120),
    "Australia/Sydney":(600,660),"Australia/Melbourne":(600,660),
    "Australia/Brisbane":(600,600),"Australia/Perth":(480,480),
    "Australia/Adelaide":(570,630),"Australia/Darwin":(570,570),
    "Australia/Hobart":(600,660),"Pacific/Auckland":(720,780),
    "Pacific/Fiji":(720,780),"Pacific/Noumea":(660,660),
    "Pacific/Tahiti":(-600,-600),"Pacific/Guam":(600,600),
    "Pacific/Saipan":(600,600),"Pacific/Tarawa":(720,720),
    "Pacific/Majuro":(720,720),"Pacific/Kwajalein":(720,720),
    "Pacific/Pohnpei":(660,660),"Pacific/Chuuk":(600,600),
    "Pacific/Palau":(540,540),"Pacific/Port_Moresby":(600,600),
    "Pacific/Efate":(660,660),"Pacific/Guadalcanal":(660,660),
    "Pacific/Apia":(780,780),
}

_SOUTHERN_DST = {
    "Australia/Sydney","Australia/Melbourne","Australia/Adelaide","Australia/Hobart",
    "Pacific/Auckland","Pacific/Fiji","America/Sao_Paulo","America/Santiago",
    "America/Asuncion","America/Cuiaba","America/Montevideo",
}

def _is_dst(iana: str, month: int) -> bool:
    if iana in _SOUTHERN_DST:
        return month >= 10 or month <= 3
    return 4 <= month <= 10

def _offset_minutes(iana: str, month: int) -> int:
    entry = _IANA_OFFSETS.get(iana)
    if not entry:
        return 0
    std, dst = entry
    return dst if _is_dst(iana, month) else std

def _to_utc(naive_dt: datetime, iana: str) -> datetime:
    off = _offset_minutes(iana, naive_dt.month)
    tz  = timezone(timedelta(minutes=off))
    return naive_dt.replace(tzinfo=tz).astimezone(timezone.utc)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — REGEX LAYER
# ══════════════════════════════════════════════════════════════════════════════

MONTH_ABBR = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
)
MONTH_MAP = {
    "jan":"Jan","feb":"Feb","mar":"Mar","apr":"Apr","may":"May","jun":"Jun",
    "jul":"Jul","aug":"Aug","sep":"Sep","oct":"Oct","nov":"Nov","dec":"Dec",
    "january":"Jan","february":"Feb","march":"Mar","april":"Apr","june":"Jun",
    "july":"Jul","august":"Aug","september":"Sep","october":"Oct",
    "november":"Nov","december":"Dec",
}
CURRENCY_SYM = {"₹":"INR","$":"USD","€":"EUR","£":"GBP","¥":"JPY"}

def _norm_date(day, mon, year):
    m = MONTH_MAP.get(mon.lower().rstrip("."), mon[:3].capitalize())
    y = year.strip()
    if len(y) == 4: y = y[2:]
    return f"{int(day):02d} {m} {y}"

def _to_24h(h, m, meridiem=None):
    hh, mm = int(h), int(m)
    if meridiem:
        mer = meridiem.upper()
        if mer == "PM" and hh != 12: hh += 12
        elif mer == "AM" and hh == 12: hh = 0
    return f"{hh:02d}:{mm:02d}"

def _iata_valid(c): return c.upper() in AIRPORT_CODES
def _city_from_iata(c): return AIRPORT_CODES.get(c.upper(), "N/A")
def _airline_from_code(c): return AIRLINE_CODES.get(c.upper(), "N/A")
def _clean_num(s):
    try: return float(re.sub(r"[,\s]","",s))
    except: return None


def _normalize_airline_name(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


def _looks_like_aircraft_designator(value: str | None) -> bool:
    token = re.sub(r"[^A-Z0-9]", "", (value or "").upper())
    if not token:
        return False
    return bool(re.fullmatch(
        r"(?:A3(?:18|19|20|21|20N|21N|30|32|39|50|59|80)"
        r"|B7(?:37|38|39|47|57|67|77|78)"
        r"|ATR?(?:42|72)"
        r"|CRJ\d{3}"
        r"|E(?:170|175|190|195)"
        r"|DH8D|Q400)",
        token,
    ))


def _normalize_flight_candidate(
    flight: str | None,
    airline: str | None = None,
    raw_candidate: str | None = None,
) -> str | None:
    """Keep only plausible airline flight numbers, never aircraft types like A321."""
    if not flight or flight == "N/A":
        return None

    cleaned = re.sub(r"\s+", " ", flight).strip().upper()
    raw_token = re.sub(r"\s+", "", (raw_candidate or flight)).strip().upper()
    if _looks_like_aircraft_designator(raw_token):
        return None

    match = re.fullmatch(r"([A-Z0-9]{2})\s*(\d{1,4}[A-Z]?)", cleaned)
    if not match:
        return None

    code = match.group(1)
    number = match.group(2)
    airline_name = AIRLINE_CODES.get(code)
    if not airline_name:
        return None

    if airline and airline != "N/A":
        if _normalize_airline_name(airline_name) != _normalize_airline_name(airline):
            return None

    return f"{code} {number}"


_TIME_BLOCKLIST = re.compile(
    r"\b(?:booking|booked|issued|issue\s+date|payment|check-?in|web\s+check-?in|"
    r"boarding|gate|counter|reporting|report\s+by|contact|phone|mobile|tel|"
    r"customer\s+care|office\s+hours|support|parsed\s+at)\b",
    re.IGNORECASE,
)
_TIME_CONTEXT_HINT = re.compile(
    r"\b(?:flight|depart|departure|arrive|arrival|from|to|sector|route|terminal|"
    r"origin|destination|std|sta|etd|eta)\b",
    re.IGNORECASE,
)
_IATA_TOKEN = re.compile(r"\b[A-Z]{3}\b")
_RE_AIRPORT_DETAIL_LINE = re.compile(r"^([A-Z]{3})\s*[-(]", re.IGNORECASE)
_RE_DAY_OFFSET = re.compile(r"(?:\+([1-3])\b|\b(next|same)\s+day\b)", re.IGNORECASE)
_RE_MONTH_DAY_TIME_LINE = re.compile(
    rf"^({MONTH_ABBR})\s+(\d{{1,2}}),\s*(\d{{1,2}}):(\d{{2}})\s*(AM|PM)?$",
    re.IGNORECASE,
)
_RE_TERMINAL_DASH_LINE = re.compile(r"^Terminal\s*-\s*(.*)$", re.IGNORECASE)
_RE_INLINE_PHONE = re.compile(r"\b([6-9]\d{9})\b")
_INLINE_PHONE_CONTEXT = re.compile(
    r"(?:\b(?:adult|child|infant|passenger|travell?er)\b|\||@)",
    re.IGNORECASE,
)


def _extract_segment_times(text: str) -> list[str]:
    """Prefer itinerary times and skip booking/check-in/contact times."""
    preferred: list[str] = []
    fallback: list[str] = []
    seen_preferred = set()
    seen_fallback = set()

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or _TIME_BLOCKLIST.search(line):
            continue

        matches = [_to_24h(m.group(1), m.group(2), m.group(3)) for m in _RE_TIME.finditer(line)]
        if not matches:
            continue

        has_flight_context = bool(
            _TIME_CONTEXT_HINT.search(line)
            or _RE_FLIGHT.search(line)
            or len(_IATA_TOKEN.findall(line)) >= 2
        )

        if has_flight_context:
            for time_value in matches:
                if time_value not in seen_preferred:
                    seen_preferred.add(time_value)
                    preferred.append(time_value)
        else:
            for time_value in matches:
                if time_value not in seen_fallback:
                    seen_fallback.add(time_value)
                    fallback.append(time_value)

    return preferred or fallback


def _extract_airport_linked_events(text: str) -> list[dict]:
    """Extract ordered airport-time events tied to nearby airport-detail lines."""
    events: list[dict] = []
    lines = [line.strip() for line in text.splitlines()]

    for idx, line in enumerate(lines):
        if not line or _TIME_BLOCKLIST.search(line):
            continue

        matches = [_to_24h(m.group(1), m.group(2), m.group(3)) for m in _RE_TIME.finditer(line)]
        if not matches:
            continue

        airport_code = None
        for look_ahead in range(1, 3):
            next_idx = idx + look_ahead
            if next_idx >= len(lines):
                break
            next_line = lines[next_idx]
            if not next_line:
                continue
            m_airport = _RE_AIRPORT_DETAIL_LINE.match(next_line)
            if m_airport:
                airport_code = m_airport.group(1).upper()
                break

        if not airport_code:
            continue

        for time_value in matches:
            event = {"airport": airport_code, "time": time_value, "date": None, "day_offset": None}
            offset_match = _RE_DAY_OFFSET.search(line)
            if offset_match:
                if offset_match.group(1):
                    event["day_offset"] = int(offset_match.group(1))
                else:
                    event["day_offset"] = 1 if (offset_match.group(2) or "").lower() == "next" else 0
            for look_back in range(1, 4):
                prev_idx = idx - look_back
                if prev_idx < 0:
                    break
                prev_line = lines[prev_idx]
                if not prev_line:
                    continue
                if event["day_offset"] is None:
                    offset_match = _RE_DAY_OFFSET.search(prev_line)
                    if offset_match:
                        if offset_match.group(1):
                            event["day_offset"] = int(offset_match.group(1))
                        else:
                            event["day_offset"] = 1 if (offset_match.group(2) or "").lower() == "next" else 0
                date_match = _RE_DATE.search(prev_line)
                if date_match:
                    event["date"] = _norm_date(date_match.group(1), date_match.group(2), date_match.group(3))
                    break
            events.append(event)

    return events


def _extract_structured_airport_events(text: str) -> list[dict]:
    """Extract explicit date/time/city/airport blocks from itinerary layouts."""
    events: list[dict] = []
    lines = [line.strip() for line in text.splitlines()]

    for idx in range(len(lines) - 2):
        line = lines[idx]
        if not line:
            continue

        match = _RE_MONTH_DAY_TIME_LINE.match(line)
        if not match:
            continue

        city = lines[idx + 1].strip() if idx + 1 < len(lines) else ""
        airport = lines[idx + 2].strip().upper() if idx + 2 < len(lines) else ""
        if not city or not re.fullmatch(r"[A-Z]{3}", airport):
            continue

        terminal = None
        if idx + 3 < len(lines):
            terminal_match = _RE_TERMINAL_DASH_LINE.match(lines[idx + 3].strip())
            if terminal_match:
                raw_terminal = terminal_match.group(1).strip()
                terminal = None if raw_terminal in ("", "-") else raw_terminal

        month = match.group(1)
        day = match.group(2)
        time_value = _to_24h(match.group(3), match.group(4), match.group(5))
        explicit_date = None

        for look_around in range(max(0, idx - 3), min(len(lines), idx + 4)):
            full_date_match = _RE_DATE.search(lines[look_around])
            if not full_date_match:
                continue
            if (
                full_date_match.group(1) == day
                and full_date_match.group(2).lower().startswith(month.lower()[:3])
            ):
                explicit_date = _norm_date(
                    full_date_match.group(1),
                    full_date_match.group(2),
                    full_date_match.group(3),
                )
                break

        events.append({
            "airport": airport,
            "city": city,
            "time": time_value,
            "date": explicit_date,
            "terminal": terminal,
            "day_offset": None,
        })

    return events


def _match_ordered_airport_events(segments: list[dict], airport_events: list[dict]) -> list[dict]:
    """Match repeated airports in sequence across multi-segment itineraries."""
    matched: list[dict] = []
    cursor = 0

    for seg in segments:
        for endpoint in ("departure", "arrival"):
            airport = (seg.get(endpoint, {}).get("airport") or "").upper()
            match = None
            if airport:
                for idx in range(cursor, len(airport_events)):
                    candidate = airport_events[idx]
                    if candidate.get("airport") == airport:
                        match = candidate
                        cursor = idx + 1
                        break
            matched.append(match or {})

    return matched


def _apply_explicit_day_offset(base_date: str | None, day_offset: int | None) -> str | None:
    if not base_date or base_date == "N/A" or day_offset is None:
        return base_date
    for fmt in _DATE_FMTS:
        try:
            dt = datetime.strptime(base_date, fmt)
            adjusted = dt + timedelta(days=day_offset)
            return adjusted.strftime("%d %b %y")
        except ValueError:
            continue
    return base_date


def _format_norm_date(value: datetime) -> str:
    return value.strftime("%d %b %y")


def _validate_segment_dates_with_timezones(seg: dict, arr_event: dict | None = None) -> None:
    """Use explicit day offsets first, then timezone ordering to repair impossible arrival dates."""
    dep = seg.get("departure", {})
    arr = seg.get("arrival", {})

    dep_date = dep.get("date", "N/A")
    arr_date = arr.get("date", "N/A")
    dep_time = dep.get("time", "N/A")
    arr_time = arr.get("time", "N/A")
    dep_airport = dep.get("airport", "")
    arr_airport = arr.get("airport", "")

    if dep_date in ("N/A", None, "") or dep_time in ("N/A", None, ""):
        return

    explicit_offset = None if not arr_event else arr_event.get("day_offset")
    if explicit_offset is not None:
        adjusted = _apply_explicit_day_offset(dep_date, explicit_offset)
        if adjusted:
            arr["date"] = adjusted
            seg["arrival"] = arr
            arr_date = adjusted

    if arr_date in ("N/A", None, "") or arr_time in ("N/A", None, ""):
        return

    dep_naive = _parse_naive(dep_date, dep_time)
    arr_naive = _parse_naive(arr_date, arr_time)
    if dep_naive is None or arr_naive is None:
        return

    dep_tz = AIRPORT_TZ_MAP.get(dep_airport.upper(), "UTC")
    arr_tz = AIRPORT_TZ_MAP.get(arr_airport.upper(), "UTC")
    dep_utc = _to_utc(dep_naive, dep_tz)
    arr_utc = _to_utc(arr_naive, arr_tz)

    if arr_utc < dep_utc:
        repaired_date = _apply_explicit_day_offset(dep_date, 1)
        if repaired_date:
            repaired_naive = _parse_naive(repaired_date, arr_time)
            if repaired_naive is not None:
                repaired_utc = _to_utc(repaired_naive, arr_tz)
                if repaired_utc >= dep_utc:
                    arr["date"] = repaired_date
                    seg["arrival"] = arr

# ── patterns ──────────────────────────────────────────────────────────────────

_RE_DATE = re.compile(
    rf"\b(\d{{1,2}})[.\s\-/]({MONTH_ABBR})[.\s\-/](\d{{2,4}})\b", re.IGNORECASE)
_RE_TIME = re.compile(r"\b(\d{1,2}):(\d{2})\s*(AM|PM)?\b", re.IGNORECASE)
_RE_PNR_AIRLINE = re.compile(r"\bAIRLINE\s*:\s*[A-Z0-9]{2}/([A-Z0-9]{5,8})\b", re.IGNORECASE)
_RE_PNR_AIRLINE_LABEL = re.compile(
    r"\bAIRLINE\s+(?:BOOKING\s+)?(?:REF(?:ERENCE)?|PNR)\s*[:#\-\s]*"
    r"(?:[A-Z0-9]{2}/)?([A-Z0-9]{5,8})\b",
    re.IGNORECASE,
)
_RE_PNR  = re.compile(
    r"(?:" 
    r"pnr\s*/\s*booking\s*ref(?:erence)?\.?"
    r"|pnr\s*(?:no\.?|number|#|:)"
    r"|booking\s*(?:ref(?:erence)?|code|number|no\.?)"
    r"|reference\s*(?:no\.?|number|code)?"
    r"|locator"
    r")[/:\s#.]*([A-Z0-9]{5,8})\b",
    re.IGNORECASE)
_RE_PNR_STRICT = re.compile(r"\b([A-Z0-9]{6})\b")
_RE_PHONE = re.compile(
    r"(?:phone|mobile|cell|contact|tel|home\s*phone)[:\s]*([+\d*][\d\s\-*().]{7,20}\d)",
    re.IGNORECASE)
_RE_TICKET = re.compile(
    r"(?:ticket\s*(?:no\.?|number|#)|e-?ticket)[:\s]*([0-9]{3}[\-\s]?[0-9]{7,11})",
    re.IGNORECASE)
_RE_FLIGHT = re.compile(r"\b([A-Z0-9]{2})\s*(\d{1,4}[A-Z]?)\b")
_RE_IATA_PAIR = re.compile(r"\b([A-Z]{3})\s*(?:to|-|→|>)\s*([A-Z]{3})\b")
_RE_IATA_PAREN = re.compile(r"(?:^|[\s,.(])([A-Za-z][A-Za-z\s]{1,30}?)\s*\(([A-Z]{3})\)")
_RE_BAGGAGE = re.compile(
    r"(?:check[\s-]?in\s*baggage|baggage\s*allowance|bag(?:gage)?)[:\s]*(\d+\s*(?:kg|kgs|lbs?|pc|pieces?))",
    re.IGNORECASE)
_RE_SEAT = re.compile(r"(?:seat\s*(?:no\.?|number)?)[:\s]*([0-9]{1,3}[A-Z])\b", re.IGNORECASE)

# ── fare patterns ─────────────────────────────────────────────────────────────
_RE_GRAND_TOTAL = re.compile(
    r"(?:grand\s*total|total\s*(?:fare|amount|price|cost)|amount\s*(?:due|payable))"
    r"[:\s]*(?:INR|USD|EUR|GBP|[₹$€£¥])?\s*([\d,]+(?:\.\d{1,2})?)", re.IGNORECASE)
_RE_BASE_FARE  = re.compile(r"(?:base\s*fare)[:\s]*(?:INR|USD|EUR|GBP|[₹$€£¥])?\s*([\d,]+(?:\.\d{1,2})?)", re.IGNORECASE)
_RE_K3_GST     = re.compile(r"(?:\bk3\b|gst)[:\s]*(?:INR|USD|EUR|GBP|[₹$€£¥])?\s*([\d,]+(?:\.\d{1,2})?)", re.IGNORECASE)
_RE_OTHER_TAX  = re.compile(r"(?:other\s*taxes?|surcharge|\byq\b|\byt\b)[:\s]*(?:INR|USD|EUR|GBP|[₹$€£¥])?\s*([\d,]+(?:\.\d{1,2})?)", re.IGNORECASE)
_RE_TOTAL_FARE = re.compile(r"(?:total\s*fare|fare\s*total|passenger\s*total)[:\s]*(?:INR|USD|EUR|GBP|[₹$€£¥])?\s*([\d,]+(?:\.\d{1,2})?)", re.IGNORECASE)
_RE_CURRENCY   = re.compile(r"\b(INR|USD|EUR|GBP|AED|SAR|SGD|JPY|CAD|AUD)\b")
_RE_TERMINAL   = re.compile(r"\bTerminal\s*:?\s*([A-Z0-9]{1,3})\b", re.IGNORECASE)
_RE_DURATION   = re.compile(r"(?:duration|flight\s*time)[:\s]*(\d{1,2})\s*h(?:rs?)?\s*(?:(\d{1,2})\s*m(?:in)?)?", re.IGNORECASE)

# ── FIX 1: class_of_travel — require travel-context words, exclude "Business Park/Center/etc." ──
_RE_CLASS = re.compile(
    r"\b(premium\s*economy|economy\s*class|business\s*class|first\s*class"
    r"|economy|business|first)"
    r"(?:\s+(?:class|fare|cabin|ticket|seat|travel|travell?er|booking))?",
    re.IGNORECASE)
# Negative context: if the word "park|center|centre|analyst|bay|lounge" follows within 3 words → skip
_RE_CLASS_NEGATIVE = re.compile(
    r"\b(?:business|economy|first)\s+(?:park|center|centre|bay|analyst|district|complex|hub|lounge|processing)\b",
    re.IGNORECASE)

# ── FIX 2: pax_type — ONLY match explicit passenger-label lines, not body text ──
# Must appear near "passenger", "pax", "Mr/Mrs/Ms/Dr", or be the standalone word on its own line
_RE_PAX_LABEL = re.compile(
    r"(?:passenger|pax|travell?er)[^\n]*?\b(adult|child|infant|adt|chd|inf)\b"
    r"|^\s*\b(adult|child|infant|adt|chd|inf)\b\s*$"
    r"|\bMr?s?\.?\s+\w|\bDr\.?\s+\w",  # title prefix = likely adult
    re.IGNORECASE | re.MULTILINE)

# ── FIX 3: frequent flyer — must be immediately after the keyword, not in body ──
_RE_FF = re.compile(
    r"(?:frequent\s*fli(?:er|ght)|ffn?\b|loyalty\s*(?:no|number|id)|mileage\s*(?:no|number)|miles?\s*id)"
    r"[:\s#]*([A-Z0-9]{5,15})\b",
    re.IGNORECASE)

_RE_BK_DATE = re.compile(
    rf"(?:booked?\s*(?:on|date)?|booking\s*date|date\s*of\s*booking)"
    rf"[:\s]*(\d{{1,2}})[.\s\-/]?({MONTH_ABBR})[.\s\-/]?(\d{{2,4}})",
    re.IGNORECASE)
_RE_BK_CLASS = re.compile(r"(?:booking\s*class|rbd|fare\s*basis|cabin\s*class)[:\s]*([A-Z])\b")

# ── GST patterns ──────────────────────────────────────────────────────────────
_RE_GST_NUMBER = re.compile(
    r"(?:GSTIN|GST\s*(?:No\.?|Number|#|IN|Identification\s*No\.?))[:\s]*"
    r"([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z])\b",
    re.IGNORECASE)
_RE_GST_COMPANY = re.compile(
    r"GST\s*(?:registered\s*)?(?:company|firm|business)?\s*name[:\s]*"
    r"([A-Z][A-Za-z0-9\s&.,\-()]+?)(?:\s*(?:\r?\n|GSTIN|GST\s*(?:No|Number|#)|$))",
    re.IGNORECASE)
_RE_EMAIL = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE)
# Fallback: GSTIN without label
_RE_GSTIN_RAW = re.compile(r"\b([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z])\b")

# ── SSR / Meal / Ancillary code patterns ──────────────────────────────────────
_ALL_SERVICE_CODES = set()
_ALL_SERVICE_CODES.update(MEAL_CODES.keys())
_ALL_SERVICE_CODES.update(ANCILLARY_CODES.keys())
_SERVICE_CODE_PAT = "|".join(sorted(_ALL_SERVICE_CODES, key=len, reverse=True))
_RE_SSR_LINE = re.compile(
    r"(?:SSR|DOCS|SEAT|MEAL|RQST|SPML)[:\s]*"
    r"(" + _SERVICE_CODE_PAT + r")"
    r"(?:\s+([A-Z]{2})\s*(\d{1,4}[A-Z]?))?"   # optional flight number
    r"(?:\s+(\d{1,2}[A-Z]{3}\d{0,4}))?"         # optional date
    r"(?:.*?(?:for|pax|passenger)?\s*([A-Z][A-Za-z\s/]+?))?",
    re.IGNORECASE)
# Also match bare 4-letter codes in service/meal context
_RE_MEAL_CODE = re.compile(
    r"(?:meal|service|ssr)[:\s]*\b(" + _SERVICE_CODE_PAT + r")\b", re.IGNORECASE)
_RE_SERVICE_KEYWORD = re.compile(
    r"\b(?:meal|service|ssr|additional\s+services?|additional\s+bag|baggage|corporate\s+travell?er)\b",
    re.IGNORECASE,
)

_PAX_MAP = {"adult":"ADT","adt":"ADT","child":"CHD","chd":"CHD","infant":"INF","inf":"INF"}
_BLOCKED_TEXT_VALUES = {
    "9831020012",
    "3340011333",
    "TIME TOURS TECH",
}
_BLOCKED_PHONE_DIGITS = {
    "9831020012",
    "9831020008",
    "3340011333",
    "913340011333",
    "919831020008",
    "919831020012",
}
_PNR_STOPWORDS = {
    "AMADEUS",
    "AIRLINE",
    "REFERENCE",
    "ERENCE",
    "REFERE",
    "REFENC",
    "THESE",
    "THOSE",
    "TOURS",
    "TECH",
    "PHONE",
    "MOBILE",
    "CONTACT",
    "TICKET",
    "BOOKING",
    "RECORD",
    "LOCATOR",
}


def _sanitize_blocked_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip(" ,:/-")
    if not cleaned:
        return None
    digits_only = re.sub(r"\D", "", cleaned)
    upper = cleaned.upper()
    if digits_only in _BLOCKED_TEXT_VALUES or upper in _BLOCKED_TEXT_VALUES:
        return None
    return cleaned


def _phone_digits(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")


def _is_blocked_phone(value: str | None) -> bool:
    digits = _phone_digits(value)
    if not digits:
        return False
    if digits in _BLOCKED_PHONE_DIGITS:
        return True
    if digits.startswith("91") and digits[2:] in _BLOCKED_PHONE_DIGITS:
        return True
    if len(digits) == 11 and digits.startswith("0") and digits[1:] in _BLOCKED_PHONE_DIGITS:
        return True
    return False


def _normalize_phone_candidate(phone: str | None) -> str:
    if not phone or phone == "N/A":
        return "N/A"

    cleaned = re.sub(r"[^\d+]", "", phone)
    if not cleaned or cleaned == "+":
        return "N/A"

    if cleaned.startswith("+"):
        digits = _phone_digits(cleaned)
        normalized = f"+{digits}" if digits else "N/A"
    else:
        digits = _phone_digits(cleaned)
        if not digits:
            normalized = "N/A"
        elif digits.startswith("0") and len(digits) == 11:
            normalized = f"+91{digits[1:]}"
        elif len(digits) == 10 and not digits.startswith("0"):
            normalized = f"+91{digits}"
        elif digits.startswith("91") and len(digits) == 12:
            normalized = f"+{digits}"
        else:
            normalized = f"+{digits}"

    return "N/A" if _is_blocked_phone(normalized) else normalized


def _normalize_ticket_candidate(ticket: str | None, phone: str | None = None) -> str | None:
    """Keep only plausible e-ticket numbers and reject phone-like values."""
    if not ticket or ticket == "N/A":
        return None

    cleaned = re.sub(r"\s+", "", ticket).strip(" ,:/")
    if not cleaned:
        return None

    digits = re.sub(r"\D", "", cleaned)
    phone_digits = _phone_digits(phone)
    if len(digits) < 13:
        return None
    if phone_digits and digits == phone_digits:
        return None
    if phone_digits.startswith("91") and digits == phone_digits[2:]:
        return None
    if _is_blocked_phone(digits):
        return None

    # Tickets are typically digits with an optional coupon/range suffix like -86.
    if not re.fullmatch(r"\d{13}(?:-\d+)?", cleaned):
        return None

    return cleaned


def _looks_like_customer_mobile(phone: str | None) -> bool:
    digits = _phone_digits(phone)
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    if digits.startswith("0") and len(digits) == 11:
        digits = digits[1:]
    return len(digits) == 10 and digits[:1] in {"6", "7", "8", "9"}


def _extract_phone(text: str) -> str | None:
    candidates = []
    seen = set()
    for match in _RE_PHONE.finditer(text):
        normalized = _normalize_phone_candidate(match.group(1))
        if normalized == "N/A" or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)

    # Fallback for passenger/profile lines like: Adult | 9831020006 | mail@...
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or not _INLINE_PHONE_CONTEXT.search(line):
            continue
        for match in _RE_INLINE_PHONE.finditer(line):
            normalized = _normalize_phone_candidate(match.group(1))
            if normalized == "N/A" or normalized in seen:
                continue
            seen.add(normalized)
            candidates.append(normalized)

    if not candidates:
        return None

    for candidate in candidates:
        if _looks_like_customer_mobile(candidate):
            return candidate
    return candidates[0]


def _is_probable_pnr(candidate: str | None) -> bool:
    if not candidate:
        return False
    token = candidate.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{6}", token):
        return False
    if token in _PNR_STOPWORDS:
        return False
    if token.isalpha() and len(set(token)) <= 2:
        return False
    return True


def _extract_pnr(text: str) -> str | None:
    for pattern in (_RE_PNR_AIRLINE, _RE_PNR_AIRLINE_LABEL):
        for match in pattern.finditer(text):
            token = match.group(1).upper()
            if _is_probable_pnr(token):
                return token

    for match in _RE_PNR.finditer(text):
        token = match.group(1).upper()
        if _is_probable_pnr(token):
            return token

    for label in re.finditer(r"(?:pnr|booking\s*(?:ref(?:erence)?|code|number|no\.?)|reference|locator)\b", text, re.IGNORECASE):
        window = text[label.end():label.end() + 120]
        if ":" in window:
            window = window.split(":", 1)[1]
        for token_match in _RE_PNR_STRICT.finditer(window.upper()):
            token = token_match.group(1)
            if _is_probable_pnr(token):
                return token

    return None


def _is_noise_name(name: str | None) -> bool:
    sanitized = _sanitize_blocked_text(name)
    if not sanitized:
        return True

    upper = sanitized.upper()
    if upper in {"N/A", "PASSENGER", "PASSENGER 1"}:
        return False

    if re.fullmatch(r"\+?\d[\d\s().-]{7,}", sanitized):
        return True

    tokens = re.findall(r"[A-Z]+", upper)
    if not tokens:
        return True

    banned_tokens = {"TIME", "TOURS", "TECH", "PHONE", "MOBILE", "CONTACT", "EMAIL", "ADDRESS"}
    if any(tok in banned_tokens for tok in tokens):
        return True

    return False


def _strip_title_prefix(name: str) -> str:
    return re.sub(r"^(?:Mr|Mrs|Ms|Miss)\.?\s+", "", name.strip(), flags=re.IGNORECASE)


def _find_title_for_name(raw_text: str, name: str) -> str | None:
    if not raw_text or not name:
        return None
    base = _strip_title_prefix(name)
    if not base:
        return None

    base_norm = re.sub(r"\s+", " ", base).strip()
    if not base_norm:
        return None

    # Try direct "Title First Last"
    tokens = [re.escape(t) for t in base_norm.split()]
    if tokens:
        name_pat = r"\s+".join(tokens)
        m = re.search(rf"\b(MR|MRS|MS|MISS)\.?\s+{name_pat}\b", raw_text, re.IGNORECASE)
        if m:
            t = m.group(1).upper()
            return {"MR": "Mr", "MRS": "Mrs", "MS": "Ms", "MISS": "Miss"}.get(t)

    # Try "LAST/FIRST" with title prefix
    if len(tokens) >= 2:
        last = tokens[-1]
        first = tokens[0]
        m = re.search(rf"\b(MR|MRS|MS|MISS)\.?\s+{last}\s*/\s*{first}\b", raw_text, re.IGNORECASE)
        if m:
            t = m.group(1).upper()
            return {"MR": "Mr", "MRS": "Mrs", "MS": "Ms", "MISS": "Miss"}.get(t)

    # Try "LAST/FIRST" with title suffix
    if len(tokens) >= 2:
        last = tokens[-1]
        first = tokens[0]
        m = re.search(rf"\b{last}\s*/\s*{first}\s+(MR|MRS|MS|MISS)\.?\b", raw_text, re.IGNORECASE)
        if m:
            t = m.group(1).upper()
            return {"MR": "Mr", "MRS": "Mrs", "MS": "Ms", "MISS": "Miss"}.get(t)

    return None


def _apply_titles_from_text(data: dict, raw_text: str) -> None:
    if not raw_text:
        return
    for pax in data.get("passengers", []):
        name = pax.get("name") or ""
        if not name or name == "N/A":
            continue
        # If name already has a title, keep it
        if re.match(r"^(?:Mr|Mrs|Ms|Miss)\.?\s+", name, re.IGNORECASE):
            continue
        title = _find_title_for_name(raw_text, name)
        if title:
            pax["name"] = f"{title} {name}"


def _detect_currency(text):
    for sym, code in CURRENCY_SYM.items():
        if sym in text: return code
    m = _RE_CURRENCY.search(text)
    return m.group(1) if m else "N/A"


def _extract_pax_type(text):
    """
    FIX 1: Only extract pax type from clear passenger-label context.
    Titles (Mr/Mrs/Ms/Dr) = ADT. Explicit child/infant labels = CHD/INF.
    Default = ADT. Never match from inside body paragraphs.
    """
    # Look for explicit child/infant label in passenger context
    child_pat = re.compile(
        r"(?:passenger|pax)[^\n]*?\b(child|chd|infant|inf)\b", re.IGNORECASE)
    m = child_pat.search(text)
    if m:
        return [_PAX_MAP[m.group(1).lower()]]

    # Count passenger lines with titles
    title_count = len(re.findall(r"\b(?:Mr|Mrs|Ms|Miss|Dr|Prof)\.?\s+[A-Z]", text))
    if title_count > 0:
        return ["ADT"] * title_count

    # Explicit "adult" in passenger context only
    adult_pat = re.compile(r"(?:passenger|pax)[^\n]*?\b(adult|adt)\b", re.IGNORECASE)
    m = adult_pat.search(text)
    if m:
        return ["ADT"]

    return ["ADT"]


def _extract_class(text):
    """Extract cabin class only from itinerary/travel-context lines."""
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if _RE_CLASS_NEGATIVE.search(line):
            continue
        if re.search(r"\b(?:gst|email|mail|phone|mobile|contact|address|policy|terms|conditions)\b", line, re.IGNORECASE):
            continue

        has_strong_label = bool(re.search(
            r"(?:fare\s*type|cabin\s*class|class\s*of\s*travel|class\s*of\s*service|travel\s*class)",
            line,
            re.IGNORECASE,
        ))
        has_itinerary_context = bool(
            _TIME_CONTEXT_HINT.search(line)
            or _RE_FLIGHT.search(line)
            or len(_IATA_TOKEN.findall(line)) >= 2
        )

        if not (has_strong_label or has_itinerary_context):
            continue

        m = _RE_CLASS.search(line)
        if not m:
            continue

        raw = m.group(1).lower()
        if "premium" in raw:
            return "Premium Economy"
        if "economy" in raw:
            return "Economy"
        if "business" in raw:
            return "Business"
        if "first" in raw:
            return "First"
    return None


def _extract_listed_service_items(text: str) -> list[dict]:
    """Extract service items from loose comma/tab-separated lists."""
    items = []
    seen = set()

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if not _RE_SERVICE_KEYWORD.search(line):
            continue

        parts = [part.strip(" .:-") for part in re.split(r"[,|\t]+", line) if part.strip(" .:-")]
        if len(parts) < 2:
            continue

        has_known_code = any(part.upper() in MEAL_CODES or part.upper() in ANCILLARY_CODES for part in parts)
        if not has_known_code:
            continue

        for part in parts:
            token = re.sub(r"\s+", " ", part).strip()
            upper = token.upper()
            item = None

            if upper in MEAL_CODES:
                item = {"code": upper, "name": MEAL_CODES[upper], "type": "meal", "flight": None, "date": None, "passenger": None}
            elif upper in ANCILLARY_CODES:
                item = {"code": upper, "name": ANCILLARY_CODES[upper], "type": "ancillary", "flight": None, "date": None, "passenger": None}
            elif re.search(r"\b(?:bag|baggage|travell?er)\b", token, re.IGNORECASE):
                item = {"code": "N/A", "name": token, "type": "ancillary", "flight": None, "date": None, "passenger": None}

            if not item:
                continue

            dedupe_key = (item["type"], item["code"], item["name"].lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            items.append(item)

    return items


def regex_extract(text: str) -> dict:
    r = {}

    r["pnr"] = _extract_pnr(text)

    m = _RE_BK_DATE.search(text)
    r["booking_date"] = _norm_date(m.group(1), m.group(2), m.group(3)) if m else None

    r["phone"] = _extract_phone(text)

    r["currency"] = _detect_currency(text)

    m = _RE_GRAND_TOTAL.search(text)
    r["grand_total"] = _clean_num(m.group(1)) if m else None

    # FIX: class of travel — exclude non-travel business context
    r["class_of_travel"] = _extract_class(text)

    ticket_numbers = []
    seen_tickets = set()
    for m in _RE_TICKET.finditer(text):
        ticket = _normalize_ticket_candidate(m.group(1), r["phone"])
        if not ticket or ticket in seen_tickets:
            continue
        seen_tickets.add(ticket)
        ticket_numbers.append(ticket)
    r["ticket_numbers"] = ticket_numbers
    r["frequent_flyer_numbers"] = [m.group(1) for m in _RE_FF.finditer(text)]

    m = _RE_BAGGAGE.search(text)
    r["baggage"] = m.group(1).strip() if m else None

    r["seats_raw"] = [m.group(1) for m in _RE_SEAT.finditer(text)]

    m = _RE_BASE_FARE.search(text);  r["base_fare"]  = _clean_num(m.group(1)) if m else None
    m = _RE_K3_GST.search(text);    r["k3_gst"]     = _clean_num(m.group(1)) if m else None
    m = _RE_OTHER_TAX.search(text);  r["other_taxes"]= _clean_num(m.group(1)) if m else None
    m = _RE_TOTAL_FARE.search(text); r["total_fare"] = _clean_num(m.group(1)) if m else None

    # FIX: pax type — context-aware extraction
    r["pax_types"] = _extract_pax_type(text)

    iata_pairs = []
    for m in _RE_IATA_PAIR.finditer(text):
        dep, arr = m.group(1).upper(), m.group(2).upper()
        if _iata_valid(dep) and _iata_valid(arr) and dep != arr:
            iata_pairs.append((dep, arr))
    r["iata_pairs"] = iata_pairs

    paren_airports = {}
    for m in _RE_IATA_PAREN.finditer(text):
        code = m.group(2).upper()
        if _iata_valid(code):
            city = re.sub(r"^(?:to|from|via|and)\s+", "", m.group(1).strip(), flags=re.IGNORECASE)
            paren_airports[city.lower()] = code
    r["paren_airports"] = paren_airports

    flight_numbers = []
    seen_flights = set()
    for m in _RE_FLIGHT.finditer(text):
        flight = _normalize_flight_candidate(
            f"{m.group(1).upper()} {m.group(2)}",
            raw_candidate=m.group(0),
        )
        if not flight or flight in seen_flights:
            continue
        seen_flights.add(flight)
        flight_numbers.append(flight)
    r["flight_numbers"] = flight_numbers

    r["all_dates"] = [_norm_date(m.group(1), m.group(2), m.group(3)) for m in _RE_DATE.finditer(text)]
    r["all_times"] = _extract_segment_times(text)
    r["airport_structured_events"] = _extract_structured_airport_events(text)
    r["airport_linked_events"] = _extract_airport_linked_events(text)
    r["terminals"] = [m.group(1).upper() for m in _RE_TERMINAL.finditer(text)]

    m = _RE_BK_CLASS.search(text)
    r["booking_class"] = m.group(1).upper() if m else None

    r["duration_extracted"] = None

    # ── GST details ───────────────────────────────────────────────────────────
    m = _RE_GST_NUMBER.search(text)
    if m:
        r["gst_number"] = m.group(1).upper()
    else:
        m = _RE_GSTIN_RAW.search(text)
        r["gst_number"] = m.group(1).upper() if m else None
    m = _RE_GST_COMPANY.search(text)
    gst_company = m.group(1).strip() if m else None
    if gst_company:
        upper_company = gst_company.upper()
        if _RE_EMAIL.search(gst_company):
            gst_company = None
        elif re.fullmatch(r"[A-Z0-9._%+\-]+", upper_company):
            gst_company = None
        elif re.search(r"\b(?:EMAIL|MAIL|PHONE|MOBILE|CONTACT)\b", upper_company):
            gst_company = None
    r["gst_company_name"] = gst_company

    # ── SSR / Meal / Ancillary service codes ──────────────────────────────────
    ssr_items = []
    for m in _RE_SSR_LINE.finditer(text):
        code = m.group(1).upper()
        is_meal = code in MEAL_CODES
        is_anc  = code in ANCILLARY_CODES
        if not (is_meal or is_anc):
            continue
        item = {
            "code": code,
            "name": MEAL_CODES.get(code) or ANCILLARY_CODES.get(code, code),
            "type": "meal" if is_meal else "ancillary",
            "flight": f"{m.group(2).upper()} {m.group(3)}" if m.group(2) else None,
            "date": m.group(4) if m.group(4) else None,
            "passenger": m.group(5).strip() if m.group(5) else None,
        }
        ssr_items.append(item)
    # Also pick up bare meal codes in meal/service context
    for m in _RE_MEAL_CODE.finditer(text):
        code = m.group(1).upper()
        if code in MEAL_CODES and not any(s["code"] == code for s in ssr_items):
            ssr_items.append({
                "code": code,
                "name": MEAL_CODES[code],
                "type": "meal",
                "flight": None, "date": None, "passenger": None,
            })
    for item in _extract_listed_service_items(text):
        if not any(
            s["type"] == item["type"]
            and s.get("code") == item.get("code")
            and s.get("name", "").lower() == item.get("name", "").lower()
            for s in ssr_items
        ):
            ssr_items.append(item)
    r["ssr_items"] = ssr_items

    r["regex_warnings"] = []
    return r


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — LLM LAYER
# ══════════════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT = """
You are a STRICT airline ticket data extraction engine.
Output ONLY valid JSON. No markdown. No explanation. No commentary.

RULES:
- NEVER guess, calculate, infer, or fabricate.
- Missing field -> "N/A" (strings) or null (numbers/arrays).
- Dates -> "DD Mon YY"  (e.g. "30 Jan 26")
- Times -> 24h HH:MM    (e.g. "14:35")
- Segment departure/arrival times must come only from itinerary/flight segments.
  Ignore booking, issued, payment, check-in, web check-in, boarding, gate,
  reporting, contact/support, and office-hours times even if they look valid.
- Never derive or back-calculate departure/arrival time from flight duration, layover duration,
  elapsed time, or journey total. Times must be explicitly shown for that segment.
- When a standalone time is followed by an airport-detail line like "CCU-..." or
  "BOM-...", treat that time as belonging to that airport segment.
- For multi-segment, layover, or return itineraries, extract segment endpoints in travel order.
  Do not reuse the same time/date for different segments unless the ticket explicitly shows that same value twice.
- When the same airport appears more than once across the itinerary, keep matching times/dates by sequence,
  not just by airport code.
- If a segment shows arrival on the next day or with a day offset like `+1`, assign the correct arrival date.
- If an arrival time is earlier than the departure time and the ticket indicates overnight travel,
  keep the arrival date on the next calendar day rather than copying the departure date.
- Validate dates using explicit day-offset markers first, then verify the segment timing order against
  the departure/arrival airport timezones. If the stated dates are impossible for that segment ordering,
  correct only the segment date that is clearly offset-driven; otherwise return "N/A" rather than guessing.
- `duration_extracted` must always be "N/A". Do not extract or infer it from ticket text.
- Departure/arrival terminals must be tied to the respective airport only.
  Never copy a terminal from the destination airport to the origin airport or vice versa.
  If a terminal is shown for only one airport, keep the other terminal as "N/A".
- Do not infer or hallucinate terminals from airline defaults, city defaults, or airport knowledge.
- Airports -> 3-letter IATA code only. departure != arrival.
- Currency -> 3-letter code. Infer from symbol: INR USD EUR GBP
- Fares -> exact numeric only (strip commas/symbols). null if absent.
- Base fare must come only from an explicit base-fare label.
  Do not copy grand total, passenger total, or total fare into base_fare.
- pax_type: extract from the actual passenger/traveller details using the ticket text.
  Use ADT/CHD/INF, but do not guess from unrelated body text.
- Seats: extract from the actual passenger/segment details using the ticket text.
  Do not rely on regex-only seat snippets when the segment/passenger mapping is explicit in the ticket.
- class_of_travel: ONLY from fare/cabin/class labels. "Business Park" is NOT Business class.
- Ticket number -> full numeric string (10-14 digits) or "N/A"
- Seats -> link to segment_index (0-based)
- Passenger names: Clean up any LASTNAME/FIRSTNAME format to "Firstname Lastname".
  Remove extra slashes, numbers, or positioning artifacts. Output clean title-case names.
- Meals: Extract per-segment. Use 4-letter SSR codes (e.g. VGML, NVML, CPML, AVML, PTSW).
  If a meal code appears with a flight number, link it to the matching segment_index.
- If a meal/service is visible but no reliable SSR code is shown, still return it with
  code = "N/A" and the best clean service name.
- Ancillaries: Extract per-segment per-passenger. Include wheelchair (WCHR/WCHS/WCHC),
  extra baggage (XBAG), fast-track (FAST), lounge (LOUG), seat selection (SEAT), etc.
  Use the 4-letter SSR/service code.
- If an ancillary/service is visible but no reliable SSR code is shown, still return it with
  code = "N/A" and the best clean service name.
- GST details: Extract ONLY from GST Information section. The GST company name is the
  company registered under GST, NOT the travel agency or booking office name.
- Barcode -> raw string if visible, else null
- PNR must NOT be null if visible.
- If both a GDS/Amadeus locator and an airline booking reference are visible,
  return the airline booking reference as `booking.pnr`.
- Example: `BOOKING REF : AMADEUS: 9P9CUS, AIRLINE: EY/9P9CUS` -> `booking.pnr = "9P9CUS"`.
- Example: `Airline Booking Reference EK/ECU362` -> `booking.pnr = "ECU362"`.
- frequent_flyer_number: ONLY from FF/loyalty label lines. Not from body text.
- Validate each segment after extraction:
  departure/arrival airport pair, departure date+time, arrival date+time, and day offset must be internally consistent.
  If a date or time is not explicitly tied to that segment, return "N/A" instead of guessing.

OUTPUT SCHEMA (return exactly this structure):
{
  "booking": {
    "pnr": "string or N/A",
    "booking_date": "DD Mon YY or N/A",
    "phone": "string or N/A",
    "currency": "INR/USD/EUR/etc or N/A",
    "grand_total": number or null,
    "class_of_travel": "Economy|Business|First|Premium Economy|N/A"
  },
  "gst_details": {
    "gst_number": "15-char GSTIN or N/A",
    "company_name": "Registered company name or N/A"
  },
  "passengers": [
    {
      "name": "Full Name (clean, title-case, no slashes)",
      "pax_type": "ADT|CHD|INF",
      "ticket_number": "string or N/A",
      "frequent_flyer_number": "string or N/A",
      "baggage": "string or N/A",
      "meals": [{"segment_index": 0, "code": "VGML", "name": "Veg Meal"}],
      "ancillaries": [{"segment_index": 0, "code": "WCHR", "name": "Wheelchair (Ramp)"}],
      "fare": {
        "base_fare": number or null,
        "k3_gst": number or null,
        "other_taxes": number or null,
        "total_fare": number or null
      },
      "seats": [{"segment_index": 0, "seat_number": "string"}]
    }
  ],
  "segments": [
    {
      "airline": "Full Airline Name or N/A",
      "flight_number": "XX 1234",
      "booking_class": "string or N/A",
      "departure": {
        "city": "City Name or N/A",
        "airport": "XXX or N/A",
        "date": "DD Mon YY or N/A",
        "time": "HH:MM or N/A",
        "terminal": "string or N/A"
      },
      "arrival": {
        "city": "City Name or N/A",
        "airport": "XXX or N/A",
        "date": "DD Mon YY or N/A",
        "time": "HH:MM or N/A",
        "terminal": "string or N/A"
      },
      "duration_extracted": "N/A"
    }
  ],
  "barcode": "string or null"
}
"""


def llm_extract(raw_text: str, regex_hints: dict) -> dict:
    # Keep the LLM focused on raw itinerary text for segment timing.
    # Passing regex-collected times/fares can bias it toward booking/check-in
    # noise, mis-labeled monetary values, over-narrow service extraction, or
    # terminal values that are not clearly tied to the correct airport.
    llm_hint_payload = {
        k: v
        for k, v in regex_hints.items()
        if k not in {"all_times", "base_fare", "k3_gst", "other_taxes", "total_fare", "ssr_items", "terminals"}
        and v not in (None, [], {}, "N/A")
    }
    hint_block = (
        "=== REGEX PRE-EXTRACTED HINTS ===\n"
        + json.dumps(llm_hint_payload, indent=2)
        + "\n=== END HINTS ===\n\n=== TICKET TEXT ===\n" + raw_text
    )
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user",   "content": hint_block},
    ]
    content = _call_llm(messages)
    return _parse_llm_json(content)


def _call_llm(messages: list[dict]) -> str:
    headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL,
        "messages": messages,
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
    }
    response = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=90)
    if response.status_code != 200:
        raise RuntimeError(f"LLM API error {response.status_code}: {response.text[:300]}")

    try:
        return response.json()["choices"][0]["message"]["content"]
    except Exception as exc:
        raise RuntimeError(f"Malformed LLM API response: {exc}; body={response.text[:500]}") from exc


def _extract_json_candidate(content: str) -> str:
    cleaned = re.sub(r"^```(?:json)?\s*", "", content, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("No JSON found in LLM response")
    return cleaned[start:end + 1]


def _lightweight_json_repair(candidate: str) -> str:
    repaired = candidate.replace("\ufeff", "")
    repaired = re.sub(r",(\s*[}\]])", r"\1", repaired)
    return repaired


def _request_json_repair(candidate: str, err: json.JSONDecodeError) -> str:
    repair_prompt = (
        "The following text is intended to be JSON for a flight-ticket schema, but it is invalid.\n"
        "Repair it into valid JSON only.\n"
        "Rules:\n"
        "- Output only valid JSON.\n"
        "- Do not add markdown fences.\n"
        "- Preserve existing values where possible.\n"
        "- If a field is incomplete or broken, use \"N/A\" for strings, null for numbers, [] for arrays.\n"
        f"- Original parse error: {err.msg} at line {err.lineno} column {err.colno}.\n\n"
        "INVALID JSON:\n"
        f"{candidate}"
    )
    repair_messages = [
        {
            "role": "system",
            "content": (
                "You repair malformed JSON. "
                "Return only valid JSON with no markdown or explanation."
            ),
        },
        {"role": "user", "content": repair_prompt},
    ]
    return _call_llm(repair_messages)


def _parse_llm_json(content: str) -> dict:
    candidate = _extract_json_candidate(content)
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as first_err:
        repaired_candidate = _lightweight_json_repair(candidate)
        if repaired_candidate != candidate:
            try:
                parsed = json.loads(repaired_candidate)
                log.warning(
                    "LLM returned invalid JSON; local repair succeeded after %s at line %s col %s",
                    first_err.msg,
                    first_err.lineno,
                    first_err.colno,
                )
                return parsed
            except json.JSONDecodeError:
                pass

        log.warning(
            "LLM returned invalid JSON; requesting repair after %s at line %s col %s",
            first_err.msg,
            first_err.lineno,
            first_err.colno,
        )
        repaired_content = _request_json_repair(candidate, first_err)
        repaired_json = _extract_json_candidate(repaired_content)
        try:
            parsed = json.loads(repaired_json)
            log.warning(
                "LLM invalid JSON was repaired successfully after %s at line %s col %s",
                first_err.msg,
                first_err.lineno,
                first_err.colno,
            )
            return parsed
        except json.JSONDecodeError as second_err:
            raise ValueError(
                "LLM JSON repair failed: "
                f"{second_err.msg} at line {second_err.lineno} column {second_err.colno}"
            ) from second_err


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — MERGE
# ══════════════════════════════════════════════════════════════════════════════

def merge(llm_data: dict, rx: dict) -> dict:
    bk = llm_data.get("booking", {})
    for field, key in [("pnr","pnr"),("booking_date","booking_date"),("phone","phone"),
                       ("currency","currency"),("grand_total","grand_total")]:
        if rx.get(key) not in (None, "N/A"):
            bk[field] = rx[key]
    # Cabin class is only trusted from strict regex travel-context extraction.
    bk["class_of_travel"] = rx.get("class_of_travel") if rx.get("class_of_travel") not in (None, "N/A") else "N/A"
    for f, d in [("pnr","N/A"),("booking_date","N/A"),("phone","N/A"),
                 ("currency","N/A"),("grand_total",None),("class_of_travel","N/A")]:
        bk.setdefault(f, d)
    llm_data["booking"] = bk

    # ── GST details ───────────────────────────────────────────────────────────
    gst = llm_data.get("gst_details", {})
    if rx.get("gst_number"):
        gst["gst_number"] = rx["gst_number"]
    if rx.get("gst_company_name"):
        gst["company_name"] = rx["gst_company_name"]
    gst.setdefault("gst_number", "N/A")
    gst.setdefault("company_name", "N/A")
    llm_data["gst_details"] = gst

    # ── Build flight-number → segment_index map for SSR linking ───────────────
    segments = llm_data.get("segments", [])
    flt_to_seg = {}
    for si, seg in enumerate(segments):
        fn = seg.get("flight_number", "")
        if fn and fn != "N/A":
            flt_to_seg[fn.replace(" ", "")] = si

    passengers = llm_data.get("passengers", [{}]) or [{}]
    assigned_ticket_numbers = set()
    for i, pax in enumerate(passengers):
        ticket_number = pax.get("ticket_number", "N/A")
        ticket_number = _normalize_ticket_candidate(ticket_number, bk.get("phone"))
        if not ticket_number and i < len(rx.get("ticket_numbers", [])):
            candidate = rx["ticket_numbers"][i]
            if candidate not in assigned_ticket_numbers:
                ticket_number = candidate
        if ticket_number and ticket_number not in assigned_ticket_numbers:
            pax["ticket_number"] = ticket_number
            assigned_ticket_numbers.add(ticket_number)
        else:
            pax["ticket_number"] = "N/A"

        # FIX: ff# — only from regex if regex actually found one from a label
        if i < len(rx.get("frequent_flyer_numbers",[])):
            pax["frequent_flyer_number"] = rx["frequent_flyer_numbers"][i]
        # If LLM found it but looks like garbage (too short, all lowercase), clear it
        ff = pax.get("frequent_flyer_number","N/A")
        if ff and ff != "N/A" and (len(ff) < 5 or ff.islower()):
            pax["frequent_flyer_number"] = "N/A"
        pax.setdefault("frequent_flyer_number","N/A")

        # pax_type is LLM-first; regex only fills missing values.
        rx_pts = rx.get("pax_types",["ADT"])
        if pax.get("pax_type") in (None, "", "N/A") and i < len(rx_pts):
            pax["pax_type"] = rx_pts[i]
        pax.setdefault("pax_type","ADT")

        if rx.get("baggage"): pax["baggage"] = rx["baggage"]
        pax.setdefault("baggage","N/A")

        # ── Meals: migrate old "meal" string → new "meals" array ──────────────
        existing_meals = pax.get("meals", [])
        if not existing_meals:
            old_meal = pax.pop("meal", None)
            if old_meal and old_meal != "N/A":
                code_upper = old_meal.strip().upper()
                resolved = MEAL_CODES.get(code_upper, old_meal.strip())
                existing_meals = [{
                    "segment_index": 0,
                    "code": code_upper if code_upper in MEAL_CODES else "N/A",
                    "name": resolved,
                }]
        else:
            pax.pop("meal", None)
        # Enrich meal entries with resolved names from MEAL_CODES
        for ml in existing_meals:
            c = ml.get("code", "").upper()
            ml["segment_index"] = _normalize_segment_index(ml.get("segment_index", 0), len(segments))
            ml["code"] = c if c in MEAL_CODES else "N/A"
            if c in MEAL_CODES and ml.get("name") in (None, "", "N/A", c):
                ml["name"] = MEAL_CODES[c]
        pax["meals"] = existing_meals

        # ── Ancillaries: ensure each has resolved name ────────────────────────
        existing_anc = pax.get("ancillaries", [])
        # Handle old flat list of strings → convert to objects
        if existing_anc and isinstance(existing_anc[0], str):
            new_anc = []
            for a in existing_anc:
                code = a.strip().upper()
                if code in MEAL_CODES:
                    existing_meals.append({
                        "segment_index": 0,
                        "code": code,
                        "name": MEAL_CODES[code],
                    })
                    continue
                new_anc.append({
                    "segment_index": 0,
                    "code": code if code in ANCILLARY_CODES else "N/A",
                    "name": ANCILLARY_CODES.get(code, a.strip())
                })
            existing_anc = new_anc
        normalized_anc = []
        for ac in existing_anc:
            c = ac.get("code", "").upper()
            ac["segment_index"] = _normalize_segment_index(ac.get("segment_index", 0), len(segments))
            if c in MEAL_CODES:
                if not any(
                    m.get("segment_index") == ac.get("segment_index", 0) and m.get("code") == c
                    for m in existing_meals
                ):
                    existing_meals.append({
                        "segment_index": ac.get("segment_index", 0),
                        "code": c,
                        "name": ac.get("name") if ac.get("name") not in (None, "", "N/A", c) else MEAL_CODES[c],
                    })
                continue
            ac["code"] = c if c in ANCILLARY_CODES else "N/A"
            if c in ANCILLARY_CODES and ac.get("name") in (None, "", "N/A", c):
                ac["name"] = ANCILLARY_CODES[c]
            normalized_anc.append(ac)
        pax["ancillaries"] = normalized_anc

        pax.setdefault("name","N/A")

        fare = pax.get("fare",{})
        for f, k in [("base_fare","base_fare"),("k3_gst","k3_gst"),
                     ("other_taxes","other_taxes"),("total_fare","total_fare")]:
            if fare.get(f) is None and rx.get(k) is not None:
                fare[f] = rx[k]
            fare.setdefault(f, None)
        pax["fare"] = fare

        # Seats are LLM-first; only use raw regex seat fallback when missing and unambiguous.
        if rx.get("seats_raw") and not pax.get("seats"):
            if len(passengers) == 1:
                pax["seats"] = [{"segment_index": j, "seat_number": s}
                                for j, s in enumerate(rx["seats_raw"])]
        pax.setdefault("seats",[])

    # ── Merge regex SSR items into passengers ─────────────────────────────────
    ssr_items = rx.get("ssr_items", [])
    for ssr in ssr_items:
        # Determine target segment_index from flight number
        seg_idx = 0
        if ssr.get("flight"):
            fn_key = ssr["flight"].replace(" ", "")
            seg_idx = flt_to_seg.get(fn_key, 0)

        # Determine target passenger (try to match by name; if ambiguous, do not broadcast on multi-pax bookings)
        target_pax_indices = [0] if len(passengers) == 1 else []
        if ssr.get("passenger"):
            pax_name_lower = ssr["passenger"].strip().lower()
            for pi, p in enumerate(passengers):
                pn = p.get("name", "").lower()
                if pax_name_lower in pn or pn in pax_name_lower:
                    target_pax_indices = [pi]
                    break

        code = ssr["code"]
        for pi in target_pax_indices:
            pax = passengers[pi]
            if ssr["type"] == "meal":
                # Don't add duplicates
                if not any(m.get("code") == code and m.get("segment_index") == seg_idx
                           for m in pax["meals"]):
                    pax["meals"].append({
                        "segment_index": seg_idx,
                        "code": code,
                        "name": ssr["name"],
                    })
            else:  # ancillary
                if not any(a.get("code") == code and a.get("segment_index") == seg_idx
                           for a in pax["ancillaries"]):
                    pax["ancillaries"].append({
                        "segment_index": seg_idx,
                        "code": code,
                        "name": ssr["name"],
                    })

    llm_data["passengers"] = passengers

    regex_built_segments = False
    if not segments:
        regex_built_segments = True
        for dep_c, arr_c in rx.get("iata_pairs",[]):
            segments.append({
                "airline":"N/A","flight_number":"N/A","booking_class":"N/A",
                "departure":{"city":_city_from_iata(dep_c),"airport":dep_c,
                             "date":"N/A","time":"N/A","terminal":"N/A"},
                "arrival":  {"city":_city_from_iata(arr_c),"airport":arr_c,
                             "date":"N/A","time":"N/A","terminal":"N/A"},
                "duration_extracted":"N/A",
            })

    flt_nums  = rx.get("flight_numbers",[])
    all_dates = rx.get("all_dates",[])
    all_times = rx.get("all_times",[])
    airport_structured_events = rx.get("airport_structured_events", [])
    airport_linked_events = rx.get("airport_linked_events", [])
    terminals = rx.get("terminals",[])
    paren_ap  = rx.get("paren_airports",{})

    ordered_structured_events = _match_ordered_airport_events(segments, airport_structured_events)
    ordered_linked_events = _match_ordered_airport_events(segments, airport_linked_events)

    for i, seg in enumerate(segments):
        current_flight = _normalize_flight_candidate(seg.get("flight_number"), seg.get("airline"))
        if current_flight:
            seg["flight_number"] = current_flight
        elif seg.get("flight_number") not in ("N/A", None, ""):
            seg["flight_number"] = "N/A"

        if i < len(flt_nums) and seg.get("flight_number") in ("N/A",None,""):
            candidate_flight = _normalize_flight_candidate(flt_nums[i], seg.get("airline"))
            if candidate_flight:
                seg["flight_number"] = candidate_flight
                ac = candidate_flight.split()[0]
            else:
                ac = None
            if seg.get("airline") in ("N/A",None,""):
                seg["airline"] = _airline_from_code(ac) if ac else "N/A"
        seg.setdefault("flight_number","N/A")
        seg.setdefault("airline","N/A")
        if rx.get("booking_class") and seg.get("booking_class") in ("N/A",None,""):
            seg["booking_class"] = rx["booking_class"]
        seg.setdefault("booking_class","N/A")
        seg["duration_extracted"] = "N/A"

        dep_ep = seg.get("departure", {})
        arr_ep = seg.get("arrival", {})
        dep_airport = (dep_ep.get("airport") or "").upper()
        arr_airport = (arr_ep.get("airport") or "").upper()
        dep_structured = ordered_structured_events[i * 2] if i * 2 < len(ordered_structured_events) else {}
        arr_structured = ordered_structured_events[i * 2 + 1] if i * 2 + 1 < len(ordered_structured_events) else {}
        dep_event = ordered_linked_events[i * 2] if i * 2 < len(ordered_linked_events) else {}
        arr_event = ordered_linked_events[i * 2 + 1] if i * 2 + 1 < len(ordered_linked_events) else {}

        # LLM remains the primary source for segment timings.
        # Explicit structured regex blocks only verify/correct obviously wrong pairings.
        if dep_structured.get("time") and dep_airport == dep_structured.get("airport"):
            if dep_ep.get("time") in ("N/A", None, "") or dep_ep.get("time") != dep_structured.get("time"):
                dep_ep["time"] = dep_structured["time"]
            if dep_structured.get("date") and (
                dep_ep.get("date") in ("N/A", None, "") or dep_ep.get("date") != dep_structured.get("date")
            ):
                dep_ep["date"] = dep_structured["date"]
            if dep_structured.get("city") and dep_ep.get("city") in ("N/A", None, ""):
                dep_ep["city"] = dep_structured["city"]
            if dep_structured.get("terminal") and dep_ep.get("terminal") in ("N/A", None, ""):
                dep_ep["terminal"] = dep_structured["terminal"]

        if arr_structured.get("time") and arr_airport == arr_structured.get("airport"):
            if arr_ep.get("time") in ("N/A", None, "") or arr_ep.get("time") != arr_structured.get("time"):
                arr_ep["time"] = arr_structured["time"]
            if arr_structured.get("date") and (
                arr_ep.get("date") in ("N/A", None, "") or arr_ep.get("date") != arr_structured.get("date")
            ):
                arr_ep["date"] = arr_structured["date"]
            if arr_structured.get("city") and arr_ep.get("city") in ("N/A", None, ""):
                arr_ep["city"] = arr_structured["city"]
            if arr_structured.get("terminal") and arr_ep.get("terminal") in ("N/A", None, ""):
                arr_ep["terminal"] = arr_structured["terminal"]

        seg["departure"] = dep_ep
        seg["arrival"] = arr_ep

        for endpoint, t_idx, d_idx in [("departure",0,0),("arrival",1,1)]:
            ep = seg.get(endpoint,{})
            city_key = ep.get("city","").lower()
            if city_key in paren_ap and ep.get("airport") in ("N/A",None,""):
                ep["airport"] = paren_ap[city_key]
            ap = ep.get("airport","N/A")
            if ap and ap != "N/A" and ep.get("city") in ("N/A",None,""):
                ep["city"] = _city_from_iata(ap)

            if ep.get("date") in ("N/A",None,""):
                idx = i * 2 + d_idx
                if idx < len(all_dates): ep["date"] = all_dates[idx]

            # Keep segment times LLM-driven whenever real segments exist.
            # Regex times are only used when we had to synthesize segments from regex.
            if regex_built_segments and ep.get("time") in ("N/A",None,""):
                idx = i * 2 + t_idx
                if idx < len(all_times): ep["time"] = all_times[idx]

            event_idx = i * 2 + (0 if endpoint == "departure" else 1)
            endpoint_event = ordered_linked_events[event_idx] if event_idx < len(ordered_linked_events) else {}
            linked_time = endpoint_event.get("time")
            if linked_time and ep.get("time") in ("N/A",None,""):
                ep["time"] = linked_time
            linked_date = endpoint_event.get("date")
            if linked_date and ep.get("date") in ("N/A",None,""):
                ep["date"] = linked_date

            tidx = i * 2 + (0 if endpoint=="departure" else 1)
            if regex_built_segments and ep.get("terminal") in ("N/A",None,"") and tidx < len(terminals):
                t = terminals[tidx]
                ep["terminal"] = t if t.startswith("T") else f"T{t}"

            for f, d in [("city","N/A"),("airport","N/A"),("date","N/A"),
                          ("time","N/A"),("terminal","N/A")]:
                ep.setdefault(f,d)
            seg[endpoint] = ep

        _validate_segment_dates_with_timezones(seg, arr_structured or arr_event)

    llm_data["segments"] = segments
    return llm_data


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — JOURNEY PIPELINE (Cluster → Durations → Layovers → Trip Type)
#
#   The pipeline processes segments in this order:
#     1. compute_segment_utc_times()  — DST-aware UTC conversion per segment
#     2. cluster_journeys()           — split into journey clusters by time gap
#                                       (>18h) or airport discontinuity
#     3. compute_leg_details()        — layovers + duration per cluster
#     4. classify_trip_type()         — one_way / round_trip / multi_city
#     5. build_journey_output()       — assemble final journey dict
#
#   Deterministic, O(n), no LLM dependency.
# ══════════════════════════════════════════════════════════════════════════════

_DATE_FMTS = ["%d %b %y","%d %B %y","%d %b %Y","%d %B %Y"]

# Connection threshold: max hours between arrival and next departure
# to be considered a layover within the same journey.
# Anything longer is a new journey (e.g., outbound vs return).
CONNECTION_THRESHOLD_HOURS = 18


def _parse_naive(date_str, time_str):
    if not date_str or date_str == "N/A" or not time_str or time_str == "N/A":
        return None
    for fmt in _DATE_FMTS:
        try: return datetime.strptime(f"{date_str} {time_str}", f"{fmt} %H:%M")
        except ValueError: continue
    return None


def _calc_single_duration(dep_airport, dep_date, dep_time, arr_airport, arr_date, arr_time):
    """Calculate single-segment flight duration using AIRPORT_TZ_MAP for DST-aware conversion."""
    dep_naive = _parse_naive(dep_date, dep_time)
    arr_naive = _parse_naive(arr_date, arr_time)
    if dep_naive is None or arr_naive is None:
        return "N/A", None, None

    dep_tz = AIRPORT_TZ_MAP.get(dep_airport.upper(), "UTC")
    arr_tz = AIRPORT_TZ_MAP.get(arr_airport.upper(), "UTC")

    dep_utc = _to_utc(dep_naive, dep_tz)
    arr_utc = _to_utc(arr_naive, arr_tz)

    # If arrival is before departure, assume next-day arrival
    if arr_utc < dep_utc:
        arr_utc += timedelta(days=1)

    diff_min = int((arr_utc - dep_utc).total_seconds() / 60)
    if diff_min < 0:
        return "N/A", None, None

    h, m = divmod(diff_min, 60)
    return f"{h}h {m}m", dep_utc, arr_utc


# ── Step 1: Compute UTC times ─────────────────────────────────────────────────

def compute_segment_utc_times(segments: list) -> list:
    """
    For each segment, compute DST-aware UTC departure/arrival times.
    Also writes `duration_calculated` onto each segment dict.
    Returns list of (dep_utc, arr_utc) tuples, parallel to segments.
    """
    utc_times = []
    for seg in segments:
        dep = seg.get("departure", {})
        arr = seg.get("arrival", {})
        dur_str, dep_utc, arr_utc = _calc_single_duration(
            dep.get("airport",""), dep.get("date","N/A"), dep.get("time","N/A"),
            arr.get("airport",""), arr.get("date","N/A"), arr.get("time","N/A"),
        )
        seg["duration_calculated"] = dur_str
        utc_times.append((dep_utc, arr_utc))
    return utc_times


# ── Step 2: Cluster into journeys ─────────────────────────────────────────────

def cluster_journeys(segments: list, utc_times: list,
                     threshold_hours: int = CONNECTION_THRESHOLD_HOURS) -> list:
    """
    Split segments into journey clusters. A new journey starts when:
      a) Airport discontinuity: prev arrival airport ≠ next departure airport
      b) Time gap exceeds threshold: gap > threshold_hours between arrival
         and next departure

    Returns list of clusters, each cluster is a list of segment indices.
    Deterministic, single-pass O(n).
    """
    if not segments:
        return []

    threshold_secs = threshold_hours * 3600
    clusters = [[0]]

    for i in range(1, len(segments)):
        prev_arr_ap = segments[i-1].get("arrival",{}).get("airport","").upper()
        curr_dep_ap = segments[i].get("departure",{}).get("airport","").upper()

        # Check 1: airport continuity
        airport_ok = (prev_arr_ap and curr_dep_ap and prev_arr_ap == curr_dep_ap)

        # Check 2: time gap within threshold
        _, prev_arr_utc = utc_times[i-1]
        curr_dep_utc, _ = utc_times[i]

        time_ok = False
        if prev_arr_utc and curr_dep_utc:
            gap_secs = (curr_dep_utc - prev_arr_utc).total_seconds()
            time_ok = (0 <= gap_secs <= threshold_secs)

        if airport_ok and time_ok:
            clusters[-1].append(i)   # same journey (layover connection)
        else:
            clusters.append([i])     # new journey

    return clusters


# ── Step 3: Compute per-leg details ───────────────────────────────────────────

def compute_leg_details(segments: list, utc_times: list, cluster: list) -> dict:
    """
    For a single journey cluster (list of segment indices), compute:
      - origin / destination airports
      - layovers (only between segments *within* this cluster)
      - total_duration (first dep → last arr within this cluster)
      - has_layovers flag
    """
    # Layovers within this cluster
    layovers = []
    for j in range(len(cluster) - 1):
        idx_curr = cluster[j]
        idx_next = cluster[j + 1]
        _, prev_arr = utc_times[idx_curr]
        next_dep, _ = utc_times[idx_next]
        if prev_arr and next_dep:
            layover_min = int((next_dep - prev_arr).total_seconds() / 60)
            if layover_min >= 0:
                lh, lm = divmod(layover_min, 60)
                layover_airport = segments[idx_curr].get("arrival",{}).get("airport","N/A")
                layovers.append({
                    "after_segment": idx_curr,
                    "at_airport": layover_airport,
                    "duration": f"{lh}h {lm}m"
                })

    # Total duration for this leg
    first_dep = utc_times[cluster[0]][0]
    last_arr  = utc_times[cluster[-1]][1]
    total = "N/A"
    if first_dep and last_arr:
        total_min = int((last_arr - first_dep).total_seconds() / 60)
        if total_min >= 0:
            th, tm = divmod(total_min, 60)
            total = f"{th}h {tm}m"

    origin = segments[cluster[0]].get("departure",{}).get("airport","N/A").upper()
    dest   = segments[cluster[-1]].get("arrival",{}).get("airport","N/A").upper()

    return {
        "segments": cluster,
        "from": origin,
        "to": dest,
        "total_duration": total,
        "layovers": layovers,
        "has_layovers": len(cluster) > 1,
    }


# ── Step 4: Classify trip type ────────────────────────────────────────────────

def classify_trip_type(legs: list) -> tuple:
    """
    Classify based on journey clusters (legs):
      - 1 leg                                       → one_way
      - 2 legs where leg2.dest == leg1.origin
                  AND leg2.origin == leg1.dest        → round_trip
      - 2 legs where leg1.origin == leg2.dest        → round_trip
      - Otherwise                                    → multi_city

    Returns (trip_type: str, trip_type_display: str).
    """
    if not legs:
        return "unknown", "Unknown"

    n = len(legs)
    any_layovers = any(l["has_layovers"] for l in legs)
    suffix = " with layovers" if any_layovers else ""

    if n == 1:
        return "one_way", f"One Way{suffix}"

    if n == 2:
        l1, l2 = legs[0], legs[1]
        # Classic round-trip: A→B then B→A
        if l1["from"] == l2["to"] and l1["to"] == l2["from"]:
            return "round_trip", f"Round Trip{suffix}"
        # Relaxed: returns to origin by any route
        if l1["from"] == l2["to"]:
            return "round_trip", f"Round Trip{suffix}"

    # 3+ legs: check if origin == final destination with only 2 unique endpoints
    origin     = legs[0]["from"]
    final_dest = legs[-1]["to"]
    if origin == final_dest:
        endpoints = set()
        for l in legs:
            endpoints.add(l["from"])
            endpoints.add(l["to"])
        if len(endpoints) == 2:
            return "round_trip", f"Round Trip{suffix}"

    return "multi_city", f"Multi City{suffix}"


# ── Step 5: Build unified journey output ──────────────────────────────────────

def build_journey(data: dict) -> dict:
    """
    Unified journey pipeline. Replaces old enrich_durations + detect_trip_type.

    Flow:
      1. Compute UTC times per segment (DST-aware via AIRPORT_TZ_MAP)
      2. Cluster segments into journeys (time gap >18h or airport break)
      3. Compute layovers + duration per journey cluster
      4. Classify trip type from cluster pattern
      5. Write journey dict onto data

    All operations are deterministic, O(n), and independent of LLM output.
    """
    segments = data.get("segments", [])

    if not segments:
        data["journey"] = {
            "trip_type": "unknown",
            "trip_type_display": "Unknown",
            "has_layovers": False,
            "total_duration": "N/A",
            "layovers": [],
            "legs": [],
            "segments_count": 0,
        }
        return data

    # 1. UTC times
    utc_times = compute_segment_utc_times(segments)

    # 2. Cluster
    clusters = cluster_journeys(segments, utc_times)

    # 3. Per-leg details
    legs = [compute_leg_details(segments, utc_times, c) for c in clusters]

    # 4. Classify
    trip_type, trip_display = classify_trip_type(legs)

    # 5. Aggregate
    any_layovers = any(l["has_layovers"] for l in legs)

    # All layovers across all legs (for backward compat)
    all_layovers = []
    for l in legs:
        all_layovers.extend(l["layovers"])

    # Overall total_duration = sum of all leg durations
    # (flight time + layovers within each leg, excluding inter-leg gaps)
    _DUR_RE = re.compile(r"(\d+)h\s*(\d+)m")
    total_minutes = 0
    all_legs_have_duration = True
    for l in legs:
        m = _DUR_RE.match(l.get("total_duration", "N/A"))
        if m:
            total_minutes += int(m.group(1)) * 60 + int(m.group(2))
        else:
            all_legs_have_duration = False

    if all_legs_have_duration and legs:
        th, tm = divmod(total_minutes, 60)
        overall_total = f"{th}h {tm}m"
    else:
        overall_total = "N/A"

    data["journey"] = {
        "trip_type": trip_type,
        "trip_type_display": trip_display,
        "has_layovers": any_layovers,
        "total_duration": overall_total,
        "layovers": all_layovers,
        "legs": legs,
        "segments_count": len(segments),
    }
    data["segments"] = segments   # updated with duration_calculated
    return data


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4C — NORMALIZATION (Phone, Name, Baggage)
# ══════════════════════════════════════════════════════════════════════════════

def normalize_phone(phone: str) -> str:
    """
    Normalize phone number:
      - Strip all whitespace, dashes, parens, dots
      - If starts with +, keep as-is
      - If starts with 0 and 11 digits -> assume Indian, prepend +91
      - If exactly 10 digits -> assume Indian, prepend +91
      - Otherwise prepend + if all digits
    """
    return _normalize_phone_candidate(phone)


def normalize_name(name: str) -> str:
    """
    Normalize passenger name:
      - Strip leading/trailing whitespace
      - Remove leading passenger index digits
      - Handle slash format and move titles to front
      - Move titles (Mr, Mrs, Miss, Ms) to front
      - Remove disallowed titles like Master, Dr, Prof
      - Title Case the name (excluding the title itself)
    """
    name = _sanitize_blocked_text(name)
    if not name or name == "N/A":
        return "N/A"

    # Collapse whitespace
    name = re.sub(r"\s+", " ", name.strip())

    # Remove trailing pax type indicators like (ADT), (CHD), (INF)
    name = re.sub(r"\s*\((?:ADT|CHD|INF|ADULT|CHILD|INFANT)\)\s*$", "", name, flags=re.IGNORECASE)

    # Remove leading passenger index numbers like "1." or "1" before name
    name = re.sub(r"^\d+\.?\s*", "", name)

    # Strip leading/trailing slashes
    name = name.strip("/").strip()

    found_title_label = None
    # Allowed titles and their canonical forms
    ALLOWED = {"MR": "Mr", "MRS": "Mrs", "MISS": "Miss", "MS": "Ms"}
    # Titles we recognize to either keep or strip. \b before the optional dot.
    TITLE_PAT = r"\b(MR|MRS|MS|MISS|DR|PROF|MSTR|MASTER|CAPT|REV|COL|SR|JR)\b\.?"

    def _extract_titles(s):
        nonlocal found_title_label
        while True:
            # Title at start? (Handle optional space after title/dot)
            m_start = re.match(f"^{TITLE_PAT}\\s*(.*)$", s, re.IGNORECASE)
            if m_start:
                t = m_start.group(1).upper()
                if t in ALLOWED and not found_title_label:
                    found_title_label = ALLOWED[t]
                s = m_start.group(2).strip()
                if not s: break
                continue
            # Title at end?
            m_end = re.match(f"^(.*?)\\s*{TITLE_PAT}$", s, re.IGNORECASE)
            if m_end:
                t = m_end.group(2).upper()
                if t in ALLOWED and not found_title_label:
                    found_title_label = ALLOWED[t]
                s = m_end.group(1).strip()
                if not s: break
                continue
            break
        return s

    if "/" in name:
        parts = name.split("/")
        if len(parts) == 2:
            # Extract titles from both parts but prioritize first name part
            f_clean = _extract_titles(parts[1].strip())
            l_clean = _extract_titles(parts[0].strip())
            name = f"{f_clean} {l_clean}"
        else:
            name = _extract_titles(" ".join(p.strip() for p in parts if p.strip()))
    else:
        name = _extract_titles(name)

    # Remove isolated numbers
    name = re.sub(r"\b\d+\b", "", name).strip()
    name = re.sub(r"\s+", " ", name)

    # Title Case the name part
    name = name.title()

    # Prepend allowed title if found
    if found_title_label:
        name = f"{found_title_label} {name}"

    final_name = name.strip()
    if _is_noise_name(final_name):
        return "N/A"
    return final_name or "N/A"


def normalize_baggage(baggage: str) -> str:
    """
    Normalize baggage string:
      - Lowercase units
      - Standardize: "25K" -> "25 Kg", "1PC" -> "1 Piece"
      - Clean whitespace
    """
    if not baggage or baggage == "N/A":
        return "N/A"

    baggage = baggage.strip()

    # Standardize kg
    baggage = re.sub(r"(\d+)\s*[Kk][Gg]?[Ss]?\b", r"\1 Kg", baggage)
    # Standardize pieces
    baggage = re.sub(r"(\d+)\s*(?:pc|pcs|pieces?)\b", r"\1 Piece", baggage, flags=re.IGNORECASE)
    # Standardize lbs
    baggage = re.sub(r"(\d+)\s*(?:lbs?)\b", r"\1 Lbs", baggage, flags=re.IGNORECASE)
    # Clean double spaces
    baggage = re.sub(r"\s+", " ", baggage).strip()

    return baggage


def normalize_data(data: dict) -> dict:
    """Apply all normalization steps to the merged data."""

    # ── Phone ──
    bk = data.get("booking", {})
    if bk.get("phone"):
        bk["phone"] = normalize_phone(bk["phone"])
    data["booking"] = bk

    # ── Passengers: name, baggage, meals ──
    for pax in data.get("passengers", []):
        pax["name"] = normalize_name(pax.get("name", "N/A"))
        pax["baggage"] = normalize_baggage(pax.get("baggage", "N/A"))

        # Normalize meals array
        for ml in pax.get("meals", []):
            ml["segment_index"] = _normalize_segment_index(ml.get("segment_index", 0), len(data.get("segments", [])))
            code = ml.get("code", "").upper()
            ml["code"] = code if code in MEAL_CODES else "N/A"
            if code in MEAL_CODES and ml.get("name") in (None, "", "N/A", code):
                ml["name"] = MEAL_CODES[code]
            elif ml.get("name"):
                ml["name"] = ml["name"].strip().title()

        # Normalize ancillaries array
        for ac in pax.get("ancillaries", []):
            ac["segment_index"] = _normalize_segment_index(ac.get("segment_index", 0), len(data.get("segments", [])))
            code = ac.get("code", "").upper()
            ac["code"] = code if code in ANCILLARY_CODES else "N/A"
            if code in ANCILLARY_CODES and ac.get("name") in (None, "", "N/A", code):
                ac["name"] = ANCILLARY_CODES[code]
            elif ac.get("name"):
                ac["name"] = ac["name"].strip().title()

        # ── Fare: Recalculate other_taxes ──
        # Formula: other_taxes = total_fare - k3_gst - base_fare
        fare = pax.get("fare", {})
        tf = fare.get("total_fare")
        bf = fare.get("base_fare")
        k3 = fare.get("k3_gst") or 0.0
        if tf is not None and bf is not None:
            fare["other_taxes"] = round(float(tf) - float(bf) - float(k3), 2)
            pax["fare"] = fare

    # ── GST details ──
    gst = data.get("gst_details", {})
    cn = gst.get("company_name", "N/A")
    if cn:
        gst["company_name"] = _sanitize_gst_company_name(cn)
    data["gst_details"] = gst

    # ── Segments: city names, booking class ──
    for seg in data.get("segments", []):
        for ep_key in ("departure", "arrival"):
            ep = seg.get(ep_key, {})
            city = ep.get("city", "N/A")
            if city and city != "N/A":
                ep["city"] = city.strip().title()
            # Ensure airport code is uppercase
            ap = ep.get("airport", "N/A")
            if ap and ap != "N/A":
                ep["airport"] = ap.strip().upper()
            seg[ep_key] = ep

        # Normalize airline name
        airline = seg.get("airline", "N/A")
        if airline and airline != "N/A":
            seg["airline"] = airline.strip().title()
            airline = seg["airline"]

        # Resolve booking class to full form
        normalized_flight = _normalize_flight_candidate(seg.get("flight_number", "N/A"), airline)
        seg["flight_number"] = normalized_flight or "N/A"

        if seg["flight_number"] == "N/A" and airline not in ("N/A", None, ""):
            # If the flight number doesn't match the airline, prefer clearing it over keeping aircraft type noise.
            pass
        elif seg["flight_number"] != "N/A" and airline in ("N/A", None, ""):
            al_code = seg["flight_number"].split()[0]
            seg["airline"] = _airline_from_code(al_code) or "N/A"

        bk_cls = seg.get("booking_class", "N/A")
        if bk_cls and bk_cls != "N/A" and len(bk_cls) == 1:
            # Extract airline code from flight number to get airline-specific class
            flt = seg.get("flight_number", "")
            al_code = flt.split()[0] if flt and flt != "N/A" else None
            resolved = resolve_booking_class(bk_cls, al_code)
            seg["booking_class"] = resolved

    return data


def _sanitize_gst_company_name(value: str | None) -> str:
    cleaned = _sanitize_blocked_text(value)
    if not cleaned:
        return "N/A"
    if _RE_EMAIL.search(cleaned):
        return "N/A"
    upper = cleaned.upper()
    if re.search(r"\b(?:EMAIL|MAIL|PHONE|MOBILE|CONTACT)\b", upper):
        return "N/A"
    if re.fullmatch(r"[A-Z0-9._%+\-]+", upper):
        return "N/A"
    return cleaned.strip().title()


def _normalize_segment_index(value, segment_count: int) -> int:
    try:
        idx = int(value)
    except (TypeError, ValueError):
        return 0
    if segment_count <= 0:
        return 0
    if idx < 0:
        return 0
    if idx >= segment_count:
        return 0
    return idx


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — FINAL ASSEMBLY
# ══════════════════════════════════════════════════════════════════════════════

PARSER_VERSION = "hybrid_v3.2"


def _validate(data):
    warnings, errors = [], []
    bk = data.get("booking", {})
    if not bk.get("pnr") or bk["pnr"] == "N/A":
        warnings.append("PNR not found")
    if not bk.get("grand_total"):
        warnings.append("grand_total not found — may be in a separate invoice")
    for i, seg in enumerate(data.get("segments", [])):
        dep, arr = seg.get("departure",{}), seg.get("arrival",{})
        if dep.get("airport") == "N/A": warnings.append(f"Segment {i}: departure airport missing")
        if arr.get("airport") == "N/A": warnings.append(f"Segment {i}: arrival airport missing")
        if dep.get("airport") == arr.get("airport") != "N/A":
            errors.append(f"Segment {i}: departure == arrival ({dep['airport']})")
        if seg.get("duration_calculated") == "N/A":
            warnings.append(f"Segment {i}: duration could not be calculated")
    for i, pax in enumerate(data.get("passengers", [])):
        if not pax.get("name") or pax["name"] == "N/A":
            errors.append(f"Passenger {i}: name missing")
    return warnings, errors


def extract(raw_text: str) -> dict:
    log.info("Step 1/5 — Regex extraction ...")
    rx = regex_extract(raw_text)
    log.info(f"  PNR={rx['pnr']}  class={rx['class_of_travel']}  "
             f"pax_types={rx['pax_types']}  ff={rx['frequent_flyer_numbers']}  "
             f"flights={rx['flight_numbers']}  times={rx['all_times']}")

    log.info("Step 2/5 — LLM extraction ...")
    try:
        llm_data = llm_extract(raw_text, rx)
    except Exception as e:
        log.warning(f"  LLM Extraction FAILED ({e}). Falling back to Regex-only mode.")
        llm_data = {
            "booking": {},
            "gst_details": {},
            "passengers": [{"name": "Passenger 1"}],
            "segments": [],
            "barcode": None
        }

    log.info("Step 3/5 — Merging ...")
    merged = merge(llm_data, rx)

    log.info("Step 4/5 — Normalization (phone, name, baggage) ...")
    _apply_titles_from_text(merged, raw_text)
    merged = normalize_data(merged)

    log.info("Step 5/5 — Journey pipeline (cluster → durations → layovers → trip type) ...")
    merged = build_journey(merged)
    jrn = merged.get("journey", {})
    log.info(f"  Trip: {jrn.get('trip_type_display','?')} | "
             f"Legs: {len(jrn.get('legs',[]))} | Layovers: {jrn.get('has_layovers')}")

    warnings, errors = _validate(merged)
    if not merged.get("segments"):
        warnings.append("No segments found by LLM or Regex fallback")

    warnings += rx.get("regex_warnings", [])

    return {
        "metadata": {
            "version": PARSER_VERSION,
            "llm_status": "success" if llm_data.get("segments") else "fallback_regex",
            "parsed_at": datetime.now(timezone.utc).isoformat() + "Z",
            "warnings": warnings,
            "errors": errors,
        },
        "booking":      merged.get("booking", {}),
        "gst_details":  merged.get("gst_details", {"gst_number": "N/A", "company_name": "N/A"}),
        "passengers":   merged.get("passengers", []),
        "segments":     merged.get("segments", []),
        "journey":      merged.get("journey", {}),
        "barcode":      merged.get("barcode", None),
    }


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — TERMINAL PRINTER
# ══════════════════════════════════════════════════════════════════════════════

def _c(t,code): return f"\033[{code}m{t}\033[0m"
def _bold(t):   return _c(t,"1")
def _green(t):  return _c(t,"32")
def _cyan(t):   return _c(t,"36")
def _yellow(t): return _c(t,"33")
def _red(t):    return _c(t,"31")
def _dim(t):    return _c(t,"2")


def print_result(result: dict):
    SEP  = "-" * 64
    SEP2 = "=" * 64
    print(f"\n{_bold(_cyan(SEP2))}")
    print(_bold(_cyan("  FLIGHT TICKET EXTRACTION RESULT  v3.2")))
    print(_bold(_cyan(SEP2)))

    meta = result.get("metadata", {})
    print(f"\n{_bold('METADATA')}")
    print(f"  Parser   : {meta.get('version')}")
    print(f"  LLM      : {meta.get('llm_status')}")
    print(f"  Parsed at: {meta.get('parsed_at')}")
    for w in meta.get("warnings",[]): print(f"  {_yellow('WARN')} {w}")
    for e in meta.get("errors",   []): print(f"  {_red('ERR')}  {e}")
    if not meta.get("warnings") and not meta.get("errors"):
        print(f"  {_green('OK')} No warnings or errors")

    bk = result.get("booking",{})
    print(f"\n{_bold('BOOKING')}")
    print(f"  PNR         : {_green(str(bk.get('pnr','N/A')))}")
    print(f"  Booking Date: {bk.get('booking_date','N/A')}")
    print(f"  Phone       : {bk.get('phone','N/A')}")
    print(f"  Currency    : {bk.get('currency','N/A')}")
    print(f"  Grand Total : {bk.get('grand_total','N/A')}")
    print(f"  Class       : {bk.get('class_of_travel','N/A')}")

    gst = result.get("gst_details",{})
    if gst.get("gst_number","N/A") != "N/A" or gst.get("company_name","N/A") != "N/A":
        print(f"\n{_bold('GST DETAILS')}")
        print(f"  GSTIN     : {_green(str(gst.get('gst_number','N/A')))}")
        print(f"  Company   : {gst.get('company_name','N/A')}")

    print(f"\n{_bold('PASSENGERS')}  ({len(result.get('passengers',[]))} pax)")
    for i, pax in enumerate(result.get("passengers",[])):
        print(f"  {SEP}")
        print(f"  [{i}] {_bold(pax.get('name','N/A'))}  ({pax.get('pax_type','ADT')})")
        print(f"      Ticket# : {pax.get('ticket_number','N/A')}")
        print(f"      FF#     : {pax.get('frequent_flyer_number','N/A')}")
        print(f"      Baggage : {pax.get('baggage','N/A')}")
        meals = pax.get("meals",[])
        if meals:
            for ml in meals:
                seg_i = ml.get("segment_index", "?")
                print(f"      Meal    : {_cyan(ml.get('code',''))} {ml.get('name','')} (seg {seg_i})")
        else:
            print(f"      Meal    : None")
        anc = pax.get("ancillaries",[])
        if anc:
            for ac in anc:
                seg_i = ac.get("segment_index", "?")
                print(f"      Anc.    : {_cyan(ac.get('code',''))} {ac.get('name','')} (seg {seg_i})")
        else:
            print(f"      Anc.    : None")
        fare = pax.get("fare",{})
        print(f"      Base    : {fare.get('base_fare','N/A')}")
        print(f"      K3/GST  : {fare.get('k3_gst','N/A')}")
        print(f"      OtherTax: {fare.get('other_taxes','N/A')}")
        print(f"      Total   : {_green(str(fare.get('total_fare','N/A')))}")
        for s in pax.get("seats",[]):
            print(f"      Seat    : {s.get('seat_number')} (seg {s.get('segment_index')})")

    print(f"\n{_bold('SEGMENTS')}  ({len(result.get('segments',[]))} segment(s))")
    segs = result.get("segments",[])
    for i, seg in enumerate(segs):
        print(f"  {SEP}")
        print(f"  [{i}] {_bold(seg.get('flight_number','N/A'))}  "
              f"{seg.get('airline','N/A')}  class={seg.get('booking_class','N/A')}")
        dep = seg.get("departure",{})
        arr = seg.get("arrival",{})
        print(f"      DEP: {_cyan(dep.get('airport','N/A'))} {dep.get('city','N/A')}"
              f"  {dep.get('date','N/A')} {dep.get('time','N/A')}"
              f"  Terminal {dep.get('terminal','N/A')}")
        print(f"      ARR: {_cyan(arr.get('airport','N/A'))} {arr.get('city','N/A')}"
              f"  {arr.get('date','N/A')} {arr.get('time','N/A')}"
              f"  Terminal {arr.get('terminal','N/A')}")
        print(f"      Duration (ticket): {seg.get('duration_extracted','N/A')}")
        print(f"      Duration (calc)  : {_green(seg.get('duration_calculated','N/A'))}")

    # Journey summary
    jrn = result.get("journey",{})
    print(f"\n{_bold('JOURNEY SUMMARY')}")
    print(f"  Trip Type       : {_green(jrn.get('trip_type_display','N/A'))}")
    print(f"  Total Duration  : {_green(jrn.get('total_duration','N/A'))}")
    print(f"  Legs ({len(jrn.get('legs',[]))}):")
    for li, leg in enumerate(jrn.get("legs",[])):
        print(f"    Leg {li+1}: {_cyan(leg.get('from','?'))} -> {_cyan(leg.get('to','?'))}"
              f"  ({leg.get('total_duration','N/A')})"
              f"  segs={leg.get('segments',[])}")
        for lv in leg.get("layovers",[]):
            print(f"      Layover @ {_yellow(lv.get('at_airport','?'))}: {_yellow(lv['duration'])}")

    bc = result.get("barcode")
    print(f"\n{_bold('BARCODE')}: {bc if bc else _dim('None')}")
    print(f"\n{_bold(_cyan(SEP2))}")
    print(_bold(_cyan("  JSON OUTPUT")))
    print(_bold(_cyan(SEP2)))
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print()


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    SAMPLE = """
	
PNR/Booking Ref.: Q4HVFI
Status	Date of Booking*	Payment Status
CONFIRMED	04Mar26 07:29:39 (UTC)	Approved
*Booking Date reflects in UTC (Universal Time Coordinated), all other timings mentioned are as per Local Time.
 
IndiGo Passenger - 1/1
Check-in now	Flight Status
 
IndiGo Flight(s)
 

Mr. Sarat chandra s Yellanki
Date	From (Terminal)	Departs	Flight Number
(Aircraft type)	Check-in/Bag
drop closes	To (Terminal)	Arrives	Via
05 Mar 26	Visakhapatnam	20:30	6E 617  
(A320)	19:30	Kolkata	22:00	
 
IndiGo Flight(s)
 

Mr. Sarat chandra s Yellanki
Date	From (Terminal)	Departs	Flight Number
(Aircraft type)	Check-in/Bag
drop closes	To (Terminal)	Arrives	Via
08 Mar 26	Kolkata	18:15	6E 512  
(A320)	17:15	Visakhapatnam	19:50	
 
Seats and Additional Services
VTZCCU
CCUVTZ
Passenger name	Seat	Services Purchased	Seat	Services Purchased
Mr. Sarat chandra s Yellanki
10C	CPTR,VCSW	10C	CPTR,VCSW
 
Exclusive Rates on Kolkata Hotels!View all
Save additional up to 30%* with code HOTELDEAL
3/5(15 reviews)
The Samilton
  
₹ 5375 ₹ 3870 /night
+ ₹ 194 Taxes & fees
Book Now
4/5(274 reviews)
Kenilworth Hotel, Kolkata
   
₹ 12962 ₹ 10500 /night
+ ₹ 1890 Taxes & fees
Book Now
3/5(16 reviews)
Roland Hotel
  
₹ 4374 ₹ 3150 /night
+ ₹ 158 Taxes & fees
Book Now
4/5(84 reviews)
The Elgin Fairlawn Kolkata
   
₹ 9000 /night
+ ₹ 1620 Taxes & fees
Book Now
 
 

 
Refer Section-3, Series M Part IV of the Civil Aviation Requirements for information on facilities in cases of denied boarding, cancellations, and delays. Details at DGCA website: Home | Directorate General of Civil Aviation | Government of India (dgca.gov.in)
 
Tips for a hassle-free travel experience

Free mandatory web check-in
Check-in online for free 365 days to 60 min before flight.

120 min before departure
Reach the airport to allow yourself sufficient time for necessary procedures.

60 min before departure
Drop your bags and proceed for boarding.

25 min before departure
Boarding gate closes.
 
Travel and Baggage Information
VTZCCU
•  Fare Type: Corporate Fare
•  Airport counters close 60 minutes prior to the scheduled departure time.
•  Boarding gates close 25 minutes prior to the scheduled departure time.
•   Check-in Baggage: 15kg per person (1 piece only). Excess baggage/additional piece is subject to applicable charges.
•   Disclaimer: 15 Kg per person (One piece only). For Double/Triple or MultiSeats bookings, extra 10 kg will be applicable. Baggage in excess of 15 kg will be subject to additional charges of INR 1000 per piece in addition to the excess baggage charges of INR 700 per kg at the airport.
•   Hand Baggage: One hand bag up to 10 kgs and 115 cms (L+W+H), shall be allowed per customer. For contactless travel we recommend to place it under the seat in front, on board.
•   International passengers, please note that carrying satellite mobile phones into India is prohibited.
•   No Change Fee**
•   No Cancellation Fee**
•  All passengers must present valid photo identification in original at the time of check-in.
•   For Cards issued outside India: All our customers using cards issued outside India will be unable to perform web check-in, as card verification is necessary. Customers travelling on such bookings must present either a hard or soft copy of their signed card for verification at the time of check-in at the airport. If the transaction remains un-verified, the amount will be refunded, and you can complete the same booking using an alternate mode of payment. Please note that failing which, your booking will be cancelled and the amount will be forfeited. We strongly recommend you to check your registered email ID for all the notifications regarding your booking.
•  Carry a printed or soft copy of boarding pass and baggage tag, you can print them at the airport kiosk as well. Please note only certain airports are equipped with kiosks which print baggage tags, hence it is advised that you mention your name and PNR on a thick paper and tag it to your baggage before reaching the airport.
•  Remember to wear your mask, carrying a sanitiser is recommended.
•  Please check state guidelines https://bit.ly/3dC9zT5, before the journey..
•   All Indian and foreign citizens traveling to Nagaland (except citizen of Nagaland) are required to obtain a mandatory Inner Line Permit (ILP) to enter the state. Apply here.
CCUVTZ
•  Fare Type: Corporate Fare
•  Airport counters close 60 minutes prior to the scheduled departure time.
•  Boarding gates close 25 minutes prior to the scheduled departure time.
•   Check-in Baggage: 15kg per person (1 piece only). Excess baggage/additional piece is subject to applicable charges.
•   Disclaimer: 15 Kg per person (One piece only). For Double/Triple or MultiSeats bookings, extra 10 kg will be applicable. Baggage in excess of 15 kg will be subject to additional charges of INR 1000 per piece in addition to the excess baggage charges of INR 700 per kg at the airport.
•   Hand Baggage: One hand bag up to 10 kgs and 115 cms (L+W+H), shall be allowed per customer. For contactless travel we recommend to place it under the seat in front, on board.
•   International passengers, please note that carrying satellite mobile phones into India is prohibited.
•   No Change Fee**
•   No Cancellation Fee**
•  All passengers must present valid photo identification in original at the time of check-in.
•   For Cards issued outside India: All our customers using cards issued outside India will be unable to perform web check-in, as card verification is necessary. Customers travelling on such bookings must present either a hard or soft copy of their signed card for verification at the time of check-in at the airport. If the transaction remains un-verified, the amount will be refunded, and you can complete the same booking using an alternate mode of payment. Please note that failing which, your booking will be cancelled and the amount will be forfeited. We strongly recommend you to check your registered email ID for all the notifications regarding your booking.
•  Carry a printed or soft copy of boarding pass and baggage tag, you can print them at the airport kiosk as well. Please note only certain airports are equipped with kiosks which print baggage tags, hence it is advised that you mention your name and PNR on a thick paper and tag it to your baggage before reaching the airport.
•  Remember to wear your mask, carrying a sanitiser is recommended.
•  Please check state guidelines https://bit.ly/3dC9zT5, before the journey..
•   All Indian and foreign citizens traveling to Nagaland (except citizen of Nagaland) are required to obtain a mandatory Inner Line Permit (ILP) to enter the state. Apply here.
 
**T&C Apply
For Your Benefits
Infant.jpg	SBI.jpg
 
Terms & Conditions
• For more information on your itinerary, please click here
• To read our conditions of carriage as per Indian regulations, please click here
• To understand more about processing of personal data, please refer our Privacy Policy
• For details on the Passenger Charter’ issued by the Ministry of Civil Aviation (MoCA), please click here
For your information
 
	
Fare Summary
Total Fare		
Refund Amount		
Personal contact information
Address : 13 CAMAC STREET
Company Name : TIME TRAVELS PVT LTD
Home Phone : 91*9490876758
Email : mail@timetours.in
Update Contact details
 
GST Information
GST Company Name : VESUVIUS INDIA LTD
GST Number : 19AAACV8995Q1Z1
 
Interglobe Aviation ltd.(IndiGo), Global Business Park, Gurgaon, Haryana, India. Call 0124-4973838 or 0124-6173838
     
Book Flight | Flight Status | Edit Booking | Check-in | View GST Invoice | Partner Login | FAQs | Contact Us
Copyright 2026 IndiGo All rights reserved.
FacebookInstagramTwitterYoutubeLinkedin"""

    result = extract(SAMPLE)
    print_result(result)
