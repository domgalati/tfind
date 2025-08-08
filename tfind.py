import sys
import re
import os
from datetime import datetime, date
from typing import Optional, Pattern, List

# ---- Optional dependency: dateutil -------------------------------------------------
try:
    from dateutil import parser as dateutil_parser
except Exception:
    dateutil_parser = None

# ------------------------------------------------------------------------------------
# ANSI Colors
COLOR_CODES = {
    "reset": "\x1b[0m",
    "cyan": "\x1b[36m",
    "yellow": "\x1b[33m",
    "green": "\x1b[32m",
    "red": "\x1b[31m",
    "magenta": "\x1b[35m",
    "blue": "\x1b[34m"
}

def ansi_enabled() -> bool:
    return sys.stdout.isatty() and not os.environ.get("NO_COLOR")

def colorize(text: str, color: str) -> str:
    return f"{COLOR_CODES.get(color, COLOR_CODES['cyan'])}{text}{COLOR_CODES['reset']}"

# ------------------------------------------------------------------------------------
EPOCH_RE = re.compile(r"^\d{10}(?:\d{3})?$")
TIME_CORE_RE = re.compile(r"\d{1,2}:\d{2}:\d{2}(?:\.\d{1,6})?")
MONTH_RE = re.compile(r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*", re.I)

def parse_epoch(value: str) -> Optional[datetime]:
    if not EPOCH_RE.match(value):
        return None
    try:
        iv = int(value)
        if len(value) == 13:
            return datetime.fromtimestamp(iv / 1000.0)
        return datetime.fromtimestamp(iv)
    except Exception:
        return None

def parse_user_datetime(value: str) -> Optional[datetime]:
    dt = parse_epoch(value)
    if dt is not None:
        return dt
    if dateutil_parser is None:
        return None
    try:
        return dateutil_parser.parse(value)
    except Exception:
        return None

def input_is_time_only(s: str) -> bool:
    has_time = bool(re.search(r"\d{1,2}:\d{2}", s))
    has_year = bool(re.search(r"\b\d{4}\b", s))
    has_month_day = bool(re.search(r"\b\d{1,2}[\-/]\d{1,2}\b", s)) or bool(MONTH_RE.search(s))
    return has_time and not (has_year or has_month_day)

SPECIALS = r".()[]{}^$|?+*\\"

def escape_lit(ch: str) -> str:
    return "\\" + ch if ch in SPECIALS else ch

def build_regex_from_example(example: str) -> Pattern:
    s = example.strip()
    if s.startswith("["):
        s = s[1:]
    if s.endswith("]"):
        s = s[:-1]
    tokens: List[str] = []
    i = 0
    while i < len(s):
        ch = s[i]
        if ch.isalpha():
            j = i
            while j < len(s) and s[j].isalpha():
                j += 1
            word = s[i:j]
            if MONTH_RE.fullmatch(word):
                tokens.append(r"[A-Za-z]{3,9}")
            else:
                tokens.append(r"[A-Za-z]+")
            i = j
        elif ch.isdigit():
            j = i
            while j < len(s) and s[j].isdigit():
                j += 1
            if (i > 0 and s[i-1] == ':') or (j < len(s) and s[j] == ':'):
                tokens.append(r"\d{1,2}")
            else:
                tokens.append(r"\d{1,4}")
            i = j
        elif ch == '.':
            tokens.append(r"\.\d{1,9}")
            i += 1
        else:
            tokens.append(escape_lit(ch))
            i += 1
    core = ''.join(tokens).replace(' ', r"\s+")
    return re.compile(rf"\[?({core})\]?")

class Extractor:
    def __init__(self, user_pattern: Optional[Pattern] = None):
        self.user_pattern = user_pattern

    def extract(self, line: str) -> Optional[str]:
        if self.user_pattern is not None:
            m = self.user_pattern.search(line)
            if m:
                return m.group(1)
        m = TIME_CORE_RE.search(line)
        if m:
            start = max(0, m.start() - 40)
            end = min(len(line), m.end() + 40)
            return line[start:end].strip()
        if dateutil_parser is not None and any(ch.isdigit() for ch in line):
            return line
        return None

def parse_line_timestamp(ts_text: str, date_anchor: Optional[date]) -> Optional[datetime]:
    dt = parse_epoch(ts_text)
    if dt is not None:
        return dt
    if dateutil_parser is None:
        return None
    try:
        dt = dateutil_parser.parse(ts_text, fuzzy=True)
    except Exception:
        return None
    lacks_date = (
        not re.search(r"\b\d{4}\b", ts_text)
        and not re.search(r"\b\d{1,2}[\-/]\d{1,2}\b", ts_text)
        and MONTH_RE.search(ts_text) is None
    )
    if lacks_date and date_anchor is not None:
        return datetime.combine(date_anchor, dt.time())
    return dt

def file_size(path: str) -> int:
    with open(path, 'rb') as f:
        f.seek(0, 2)
        return f.tell()

def find_first_date_anchor(path: str, extractor: Extractor) -> Optional[date]:
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for _ in range(2000):
                line = f.readline()
                if not line:
                    break
                ts_text = extractor.extract(line)
                if not ts_text:
                    continue
                dt = parse_line_timestamp(ts_text, None)
                if dt:
                    return dt.date()
    except Exception:
        return None
    return None

def binary_search_start(path: str, target: datetime, extractor: Extractor, date_anchor: Optional[date]) -> int:
    low = 0
    hi = file_size(path)
    with open(path, 'rb') as f:
        while low < hi:
            mid = (low + hi) // 2
            f.seek(mid)
            f.readline()
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                s = line.decode('utf-8', errors='ignore')
            except Exception:
                s = line.decode('latin1', errors='ignore')
            ts_text = extractor.extract(s)
            ts = parse_line_timestamp(ts_text, date_anchor) if ts_text else None
            if ts is None or ts < target:
                low = pos + 1
            else:
                hi = mid
    return low

def print_range(path: str, start_s: str, end_s: str, readability: bool = False, color: str = "cyan") -> None:
    start_dt = parse_user_datetime(start_s)
    end_dt = parse_user_datetime(end_s)
    if not start_dt or not end_dt:
        sys.exit("Error: could not parse <start> or <end>.")
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt
    user_pat = None
    try:
        if not input_is_time_only(start_s) or not input_is_time_only(end_s):
            exemplar = start_s if not input_is_time_only(start_s) else end_s
            user_pat = build_regex_from_example(exemplar)
        else:
            user_pat = build_regex_from_example(start_s)
    except Exception:
        user_pat = None
    extractor = Extractor(user_pat)
    need_anchor = input_is_time_only(start_s) or input_is_time_only(end_s)
    date_anchor = find_first_date_anchor(path, extractor) if need_anchor else None
    if need_anchor and date_anchor is not None:
        if input_is_time_only(start_s):
            start_dt = datetime.combine(date_anchor, start_dt.time())
        if input_is_time_only(end_s):
            end_dt = datetime.combine(date_anchor, end_dt.time())
    start_pos = binary_search_start(path, start_dt, extractor, date_anchor)
    do_color = readability and ansi_enabled()
    with open(path, 'rb') as f:
        f.seek(start_pos)
        for raw in f:
            try:
                line = raw.decode('utf-8', errors='ignore')
            except Exception:
                line = raw.decode('latin1', errors='ignore')
            ts_text = extractor.extract(line)
            if not ts_text:
                continue
            ts = parse_line_timestamp(ts_text, date_anchor)
            if ts is None:
                continue
            if ts > end_dt:
                break
            if start_dt <= ts <= end_dt:
                if do_color and ts_text:
                    line = line.replace(ts_text, colorize(ts_text, color), 1)
                sys.stdout.write(line)

def main(argv: List[str]) -> None:
    readability = False
    color = "cyan"
    args = []
    for a in argv[1:]:
        if a == "-r" or a == "--readability":
            readability = True
        elif a.startswith("--readability="):
            readability = True
            color = a.split("=", 1)[1]
        else:
            args.append(a)
    if len(args) < 3:
        print("Usage: tfind [--readability[=COLOR] | -r] <logfile> <start> <end>")
        sys.exit(1)
    path, start_s, end_s = args
    print_range(path, start_s, end_s, readability, color)

if __name__ == "__main__":
    main(sys.argv)
