import sys
import re
import os
from datetime import datetime, date, timezone, timedelta
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
    "blue": "\x1b[34m",
}

def ansi_enabled() -> bool:
    return sys.stdout.isatty() and not os.environ.get("NO_COLOR")

def colorize(text: str, color: str) -> str:
    return f"{COLOR_CODES.get(color, COLOR_CODES['cyan'])}{text}{COLOR_CODES['reset']}"

# ------------------------------------------------------------------------------------
EPOCH_RE = re.compile(r"^\d{10}(?:\d{3})?$")
TIME_CORE_RE = re.compile(r"\d{1,2}:\d{2}:\d{2}(?:\.\d{1,6})?")
MONTH_RE = re.compile(r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*", re.I)
INPUT_ORDER_VALUES = {"auto", "sorted", "unsorted"}

# ------------------------------------------------------------------------------------
# Known timestamp formats

class FormatSpec:
    __slots__ = ("name", "regex", "strptime_variants", "has_year")

    def __init__(
        self,
        name: str,
        regex: Pattern,
        strptime_variants: Optional[List[str]],
        has_year: bool = True,
    ):
        self.name = name
        self.regex = regex
        self.strptime_variants = strptime_variants  # None = epoch, handled via parse_epoch
        self.has_year = has_year


KNOWN_FORMATS: List[FormatSpec] = [
    # FIX Protocol compact: 20260428-06:00:33.450
    FormatSpec(
        "fix",
        re.compile(r"(?<!\d)(\d{8}-\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)"),
        ["%Y%m%d-%H:%M:%S.%f", "%Y%m%d-%H:%M:%S"],
    ),
    # ISO 8601 with T separator: 2025-08-08T13:23:00.000Z or ...+05:30
    FormatSpec(
        "iso8601_T",
        re.compile(
            r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:?\d{2})?)"
        ),
        [
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ],
    ),
    # ISO 8601 with space / Log4j comma: 2025-08-08 13:23:00.000 or ,123
    FormatSpec(
        "iso8601_space",
        re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:[.,]\d{1,6})?)"),
        ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S,%f", "%Y-%m-%d %H:%M:%S"],
    ),
    # Apache/NCSA: 31/Aug/1995:20:00:00 -0400
    FormatSpec(
        "apache",
        re.compile(r"\[?(\d{1,2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2}(?:\s[+-]\d{4})?)\]?"),
        ["%d/%b/%Y:%H:%M:%S %z", "%d/%b/%Y:%H:%M:%S"],
    ),
    # Syslog: Aug  8 13:23:00  (no year — needs date_anchor)
    FormatSpec(
        "syslog",
        re.compile(r"([A-Za-z]{3}\s{1,2}\d{1,2} \d{2}:\d{2}:\d{2})"),
        ["%b %d %H:%M:%S"],
        has_year=False,
    ),
    # Unix epoch milliseconds: 1234567890123
    FormatSpec(
        "epoch_ms",
        re.compile(r"(?<!\d)(\d{13})(?!\d)"),
        None,
    ),
    # Unix epoch seconds: 1234567890
    FormatSpec(
        "epoch_s",
        re.compile(r"(?<!\d)(\d{10})(?!\d)"),
        None,
    ),
]


def _try_strptime(ts_text: str, fmt: FormatSpec) -> Optional[datetime]:
    """Try each strptime variant for a known format. Returns a datetime or None."""
    if fmt.strptime_variants is None:
        return None
    # Normalize runs of whitespace (e.g. syslog "Aug  8" -> "Aug 8")
    text = re.sub(r"\s+", " ", ts_text.strip())
    for sfmt in fmt.strptime_variants:
        try:
            return datetime.strptime(text, sfmt)
        except ValueError:
            continue
    return None


def detect_format(path: str) -> Optional[FormatSpec]:
    """Sample the first 50 lines and return the first FormatSpec that parses at least one."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = []
            for _ in range(50):
                line = f.readline()
                if not line:
                    break
                lines.append(line)
    except OSError:
        return None
    for fmt in KNOWN_FORMATS:
        for line in lines:
            m = fmt.regex.search(line)
            if not m:
                continue
            if fmt.strptime_variants is None:
                if parse_epoch(m.group(1)) is not None:
                    return fmt
            else:
                if _try_strptime(m.group(1), fmt) is not None:
                    return fmt
    return None


# ------------------------------------------------------------------------------------

def parse_timezone_value(value: str) -> Optional[timezone]:
    v = value.strip()
    if v.upper() in {"UTC", "Z"}:
        return timezone.utc
    m = re.fullmatch(r"([+-])(\d{2}):?(\d{2})", v)
    if not m:
        return None
    sign = 1 if m.group(1) == "+" else -1
    hours = int(m.group(2))
    mins = int(m.group(3))
    if hours > 23 or mins > 59:
        return None
    return timezone(sign * timedelta(hours=hours, minutes=mins))


def normalize_datetime(dt: datetime, default_tz: Optional[timezone]) -> datetime:
    if dt.tzinfo is None:
        if default_tz is None:
            return dt
        dt = dt.replace(tzinfo=default_tz)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


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


def parse_user_datetime(
    value: str,
    strptime_format: Optional[str] = None,
    default_tz: Optional[timezone] = None,
) -> Optional[datetime]:
    if strptime_format is not None:
        try:
            return normalize_datetime(datetime.strptime(value, strptime_format), default_tz)
        except Exception:
            return None
    dt = parse_epoch(value)
    if dt is not None:
        return normalize_datetime(dt, default_tz)
    if dateutil_parser is None:
        return None
    try:
        return normalize_datetime(dateutil_parser.parse(value), default_tz)
    except Exception:
        return None


def input_is_time_only(s: str) -> bool:
    has_time = bool(re.search(r"\d{1,2}:\d{2}", s))
    # \b\d{4}\b misses years embedded in compact dates like 20260428 (no word boundary
    # between the year digits and the trailing month/day digits), so also check for
    # any run of 5+ consecutive digits, which can only be a date component, not a time.
    has_year = bool(re.search(r"\b\d{4}\b", s)) or bool(re.search(r"\d{5,}", s))
    has_month_day = bool(re.search(r"\b\d{1,2}[\-/]\d{1,2}\b", s)) or bool(MONTH_RE.search(s))
    return has_time and not (has_year or has_month_day)


class Extractor:
    def __init__(
        self,
        fmt: Optional[FormatSpec] = None,
        user_pattern: Optional[Pattern] = None,
    ):
        self.fmt = fmt
        self.user_pattern = user_pattern

    def extract(self, line: str) -> Optional[str]:
        if self.user_pattern is not None:
            m = self.user_pattern.search(line)
            if m:
                return m.group(1)
        if self.fmt is not None:
            m = self.fmt.regex.search(line)
            if m:
                return m.group(1)
        # Fallback: find HH:MM:SS core and grab leading context for the date prefix.
        # Trailing context is capped at 10 chars (enough for a timezone offset like
        # " +00:00") so log message body doesn't poison the parse.
        m = TIME_CORE_RE.search(line)
        if m:
            start = max(0, m.start() - 40)
            end = min(len(line), m.end() + 10)
            return line[start:end].strip()
        if dateutil_parser is not None and any(ch.isdigit() for ch in line):
            return line
        return None


def parse_line_timestamp(
    ts_text: str,
    date_anchor: Optional[date],
    fmt: Optional[FormatSpec] = None,
    strptime_format: Optional[str] = None,
    default_tz: Optional[timezone] = None,
) -> Optional[datetime]:
    if strptime_format is not None:
        try:
            return normalize_datetime(datetime.strptime(ts_text, strptime_format), default_tz)
        except Exception:
            return None
    if fmt is not None:
        dt = parse_epoch(ts_text.strip()) if fmt.strptime_variants is None else _try_strptime(ts_text, fmt)
        if dt is not None:
            if not fmt.has_year and date_anchor is not None:
                # Syslog has month+day but no year. Borrow only the year from the
                # anchor so that e.g. "Jul 27" stays Jul 27, not the anchor's date.
                dt = dt.replace(year=date_anchor.year)
            return normalize_datetime(dt, default_tz)
        return None
    # Fallback for unrecognized formats: epoch then fuzzy dateutil
    dt = parse_epoch(ts_text.strip())
    if dt is not None:
        return normalize_datetime(dt, default_tz)
    if dateutil_parser is None:
        return None
    try:
        dt = dateutil_parser.parse(ts_text, fuzzy=True)
    except Exception:
        return None
    lacks_date = (
        not re.search(r"\b\d{4}\b", ts_text)
        and not re.search(r"\d{5,}", ts_text)
        and not re.search(r"\b\d{1,2}[\-/]\d{1,2}\b", ts_text)
        and MONTH_RE.search(ts_text) is None
    )
    if lacks_date and date_anchor is not None:
        dt = datetime.combine(date_anchor, dt.time())
    return normalize_datetime(dt, default_tz)


def file_size(path: str) -> int:
    with open(path, "rb") as f:
        f.seek(0, 2)
        return f.tell()


def find_first_date_anchor(
    path: str,
    extractor: Extractor,
    fmt: Optional[FormatSpec] = None,
    strptime_format: Optional[str] = None,
    default_tz: Optional[timezone] = None,
) -> Optional[date]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for _ in range(2000):
                line = f.readline()
                if not line:
                    break
                ts_text = extractor.extract(line)
                if not ts_text:
                    continue
                # Year-less formats (syslog): use dateutil so the anchor gets the
                # current year rather than strptime's default of 1900.
                if fmt is not None and not fmt.has_year:
                    if dateutil_parser is not None:
                        try:
                            return dateutil_parser.parse(ts_text.strip(), fuzzy=True).date()
                        except Exception:
                            continue
                else:
                    dt = parse_line_timestamp(ts_text, None, fmt, strptime_format, default_tz)
                    if dt:
                        return dt.date()
    except Exception:
        return None
    return None


def binary_search_start(
    path: str,
    target: datetime,
    extractor: Extractor,
    date_anchor: Optional[date],
    strptime_format: Optional[str] = None,
    default_tz: Optional[timezone] = None,
    fmt: Optional[FormatSpec] = None,
) -> int:
    low = 0
    hi = file_size(path)
    with open(path, "rb") as f:
        while low < hi:
            mid = (low + hi) // 2
            f.seek(mid)
            f.readline()
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                s = line.decode("utf-8", errors="ignore")
            except Exception:
                s = line.decode("latin1", errors="ignore")
            ts_text = extractor.extract(s)
            ts = (
                parse_line_timestamp(ts_text, date_anchor, fmt, strptime_format, default_tz)
                if ts_text
                else None
            )
            if ts is None or ts < target:
                low = pos + 1
            else:
                hi = mid
    return low


def print_range(
    path: str,
    start_s: str,
    end_s: str,
    readability: bool = False,
    color: str = "cyan",
    timestamp_format: Optional[str] = None,
    timestamp_regex: Optional[str] = None,
    default_tz: Optional[timezone] = None,
    input_order: str = "auto",
) -> None:
    start_dt = parse_user_datetime(start_s, timestamp_format, default_tz)
    end_dt = parse_user_datetime(end_s, timestamp_format, default_tz)
    if not start_dt or not end_dt:
        sys.exit("Error: could not parse <start> or <end>.")
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    user_pat = None
    if timestamp_regex:
        try:
            user_pat = re.compile(timestamp_regex)
            if user_pat.groups < 1:
                sys.exit("Error: --timestamp-regex must include at least one capture group.")
        except re.error as exc:
            sys.exit(f"Error: invalid --timestamp-regex: {exc}")

    # Auto-detect format unless the user has supplied both extraction and parsing overrides.
    fmt = None
    if timestamp_format is None and timestamp_regex is None:
        fmt = detect_format(path)

    extractor = Extractor(fmt, user_pat)

    need_anchor = (
        input_is_time_only(start_s)
        or input_is_time_only(end_s)
        or (fmt is not None and not fmt.has_year)
    )
    date_anchor = (
        find_first_date_anchor(path, extractor, fmt, timestamp_format, default_tz)
        if need_anchor
        else None
    )
    if need_anchor and date_anchor is not None:
        if input_is_time_only(start_s):
            start_dt = normalize_datetime(datetime.combine(date_anchor, start_dt.time()), default_tz)
        if input_is_time_only(end_s):
            end_dt = normalize_datetime(datetime.combine(date_anchor, end_dt.time()), default_tz)

    if input_order not in INPUT_ORDER_VALUES:
        sys.exit("Error: --input-order must be one of: auto, sorted, unsorted.")

    start_pos = 0
    if input_order != "unsorted":
        start_pos = binary_search_start(
            path, start_dt, extractor, date_anchor, timestamp_format, default_tz, fmt
        )

    do_color = readability and ansi_enabled()
    with open(path, "rb") as f:
        f.seek(start_pos)
        for raw in f:
            try:
                line = raw.decode("utf-8", errors="ignore")
            except Exception:
                line = raw.decode("latin1", errors="ignore")
            ts_text = extractor.extract(line)
            if not ts_text:
                continue
            ts = parse_line_timestamp(ts_text, date_anchor, fmt, timestamp_format, default_tz)
            if ts is None:
                continue
            if ts > end_dt and input_order != "unsorted":
                break
            if start_dt <= ts <= end_dt:
                if do_color and ts_text:
                    line = line.replace(ts_text, colorize(ts_text, color), 1)
                sys.stdout.write(line)


def main(argv: Optional[List[str]] = None) -> None:
    if argv is None:
        argv = sys.argv
    readability = False
    color = "cyan"
    timestamp_format = None
    timestamp_regex = None
    default_tz = None
    input_order = "auto"
    args = []
    for a in argv[1:]:
        if a == "-r" or a == "--readability":
            readability = True
        elif a.startswith("--readability="):
            readability = True
            color = a.split("=", 1)[1]
        elif a.startswith("--timestamp-format="):
            timestamp_format = a.split("=", 1)[1]
        elif a.startswith("--timestamp-regex="):
            timestamp_regex = a.split("=", 1)[1]
        elif a.startswith("--timezone="):
            tz_value = a.split("=", 1)[1]
            default_tz = parse_timezone_value(tz_value)
            if default_tz is None:
                sys.exit("Error: invalid --timezone. Use UTC, Z, +HH:MM, -HH:MM, +HHMM, or -HHMM.")
        elif a.startswith("--input-order="):
            input_order = a.split("=", 1)[1].strip().lower()
        else:
            args.append(a)
    if len(args) < 3:
        print(
            "Usage: tfind [--readability[=COLOR] | -r] [--timestamp-format=FMT] "
            "[--timestamp-regex=REGEX] [--timezone=TZ] [--input-order=auto|sorted|unsorted] "
            "<logfile> <start> <end>"
        )
        sys.exit(1)
    path, start_s, end_s = args
    print_range(
        path,
        start_s,
        end_s,
        readability,
        color,
        timestamp_format,
        timestamp_regex,
        default_tz,
        input_order,
    )


if __name__ == "__main__":
    main()
