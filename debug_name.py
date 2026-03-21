import re

def normalize_name(name: str) -> str:
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
    # Titles we recognize to either keep or strip
    TITLE_PAT = r"\b(MR|MRS|MS|MISS|DR|PROF|MSTR|MASTER|CAPT|REV|COL|SR|JR)\b\.?"

    def _extract_titles(s):
        nonlocal found_title_label
        while True:
            # Title at start?
            m_start = re.match(f"^{TITLE_PAT}\\s+(.+)$", s, re.IGNORECASE)
            if m_start:
                t = m_start.group(1).upper()
                if t in ALLOWED and not found_title_label:
                    found_title_label = ALLOWED[t]
                s = m_start.group(2).strip()
                continue
            # Title at end?
            m_end = re.match(f"^(.+?)\\s+{TITLE_PAT}$", s, re.IGNORECASE)
            if m_end:
                t = m_end.group(2).upper()
                if t in ALLOWED and not found_title_label:
                    found_title_label = ALLOWED[t]
                s = m_end.group(1).strip()
                continue
            # Just a title?
            if re.match(f"^{TITLE_PAT}$", s, re.IGNORECASE):
                t = s.rstrip(".").upper()
                if t in ALLOWED and not found_title_label:
                    found_title_label = ALLOWED[t]
                return ""
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
    return final_name or "N/A"

print(f"'{normalize_name('Mr. John Doe')}'")
print(f"'{normalize_name('Mr John Doe')}'")
print(f"'{normalize_name('JOHN DOE MR')}'")
print(f"'{normalize_name('DOE/JOHN MR')}'")
print(f"'{normalize_name('DOE/MR JOHN')}'")
print(f"'{normalize_name('MRS Jane Smith')}'")
