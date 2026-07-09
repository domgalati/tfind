#!/usr/bin/env python3
"""tfind: binary-search a log file for a time range.

Design notes (why this version differs from the old one):

  * Format detection votes across a sample instead of trusting the first
    matching line, and reports its guess to stderr so a wrong guess is
    visible and overridable with --timestamp-format / --timestamp-regex.
  * Lines without a parseable timestamp (stack traces, wrapped messages,
    payload dumps) are treated as continuations of the previous line and
    printed when that line was in range. They are never silently dropped.
  * The binary search scans forward past unparseable lines at each probe
    instead of blindly moving right, so a block of continuation lines near
    the boundary can no longer push the search past the real start.
  * Epoch timestamps are interpreted as UTC, not machine-local time.
  * The early-exit in the print loop only trusts timestamps parsed by the
    detected format or a user-supplied format, and requires several
    consecutive out-of-range lines, so one garbage fuzzy parse cannot
    truncate output.
  * Syslog-style year-less timestamps get a December/January wraparound
    adjustment when borrowing the year from the date anchor.
"""

import argparse
import os
import re
import sys
from datetime import datetime, date, timezone, timedelta
from typing import List, Optional, Pattern, Tuple

# ---- Optional dependency: dateutil (fuzzy fallback only) ---------------------------
try:
    from dateutil import parser as dateutil_parser
except Exception:
    dateutil_parser = None

# ------------------------------------------------------------------------------------
# ANSI colors

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
# Constants

EPOCH_RE = re.compile(r"^\d{10}(?:\d{3})?$")
TIME_CORE_RE = re.compile(r"\d{1,2}:\d{2}:\d{2}(?:\.\d{1,6})?")
MONTH_RE = re.compile(r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*", re.I)

DETECT_SAMPLE_LINES = 200      # lines sampled for format detection
MIN_DETECT_HITS = 3            # a format needs at least this many parsed hits to win
PROBE_SCAN_BYTES = 65536       # max bytes to scan forward past unparseable lines per probe
OUT_OF_RANGE_LIMIT = 5         # consecutive trusted out-of-range lines before early exit
ANCHOR_SCAN_LINES = 2000       # lines scanned when hunting for a date anchor


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
        self.strptime_variants = strptime_variants  # None means epoch, handled by parse_epoch
        self.has_year = has_year


KNOWN_FORMATS: List[FormatSpec] = [
    # FIX protocol compact: 20260428-06:00:33.450
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
    # ISO 8601 with space separator, plus Log4j comma millis: 2025-08-08 13:23:00,123
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
    # Syslog: Aug  8 13:23:00 (no year, needs the date anchor)
    FormatSpec(
        "syslog",
        re.compile(r"([A-Za-z]{3}\s{1,2}\d{1,2} \d{2}:\d{2}:\d{2})"),
        ["%b %d %H:%M:%S"],
        has_year=False,
    ),
    # Unix epoch milliseconds: 1234567890123 (kept after structured formats on purpose,
    # so an order ID cannot outrank a real timestamp during detection)
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


# ------------------------------------------------------------------------------------
# Parsing primitives

def _try_strptime(ts_text: str, fmt: FormatSpec) -> Optional[datetime]:
    """Try each strptime variant for a known format."""
    if fmt.strptime_variants is None:
        return None
    # Collapse whitespace runs so syslog "Aug  8" becomes "Aug 8"
    text = re.sub(r"\s+", " ", ts_text.strip())
    for sfmt in fmt.strptime_variants:
        try:
            return datetime.strptime(text, sfmt)
        except ValueError:
            continue
    return None


def parse_epoch(value: str) -> Optional[datetime]:
    """Parse a 10 or 13 digit epoch string. Always interpreted as UTC.

    fromtimestamp() without a tz argument converts to machine-local time,
    which shifts the whole search window by the host's UTC offset. Passing
    tz=timezone.utc keeps epoch input consistent with the rest of the
    pipeline; normalize_datetime strips it back to naive UTC.
    """
    if not EPOCH_RE.match(value):
        return None
    try:
        iv = int(value)
        if len(value) == 13:
            return datetime.fromtimestamp(iv / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(iv, tz=timezone.utc)
    except Exception:
        return None


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
    """Reduce every datetime to naive UTC so comparisons are apples to apples."""
    if dt.tzinfo is None:
        if default_tz is None:
            return dt
        dt = dt.replace(tzinfo=default_tz)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def apply_anchor_year(dt: datetime, anchor: date) -> datetime:
    """Borrow the year from the anchor, with a Dec/Jan wraparound adjustment.

    A syslog file opened in late December can contain January lines from the
    following year (and vice versa if the anchor itself is a January line).
    Without this, the borrowed year breaks monotonicity across New Year and
    with it the binary search invariant.
    """
    year = anchor.year
    if dt.month == 1 and anchor.month == 12:
        year += 1
    elif dt.month == 12 and anchor.month == 1:
        year -= 1
    try:
        return dt.replace(year=year)
    except ValueError:
        # Feb 29 borrowed into a non-leap year
        return dt.replace(year=year, day=28)


def input_is_time_only(s: str) -> bool:
    has_time = bool(re.search(r"\d{1,2}:\d{2}", s))
    # \b\d{4}\b misses years embedded in compact dates like 20260428 (no word
    # boundary between year and month digits), so also treat any run of 5+
    # digits as a date component.
    has_year = bool(re.search(r"\b\d{4}\b", s)) or bool(re.search(r"\d{5,}", s))
    has_month_day = bool(re.search(r"\b\d{1,2}[\-/]\d{1,2}\b", s)) or bool(MONTH_RE.search(s))
    return has_time and not (has_year or has_month_day)


def parse_user_datetime(
    value: str,
    strptime_format: Optional[str],
    default_tz: Optional[timezone],
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


# ------------------------------------------------------------------------------------
# Extraction and per-line parsing

class Extractor:
    def __init__(self, fmt: Optional[FormatSpec], user_pattern: Optional[Pattern]):
        self.fmt = fmt
        self.user_pattern = user_pattern

    def extract(self, line: str) -> Optional[str]:
        if self.user_pattern is not None:
            m = self.user_pattern.search(line)
            return m.group(1) if m else None
        if self.fmt is not None:
            m = self.fmt.regex.search(line)
            return m.group(1) if m else None
        # Fallback for undetected formats: find an HH:MM:SS core and grab
        # leading context for the date prefix. Trailing context is capped at
        # 10 chars (room for a timezone offset) so message body does not
        # poison the fuzzy parse.
        m = TIME_CORE_RE.search(line)
        if m:
            start = max(0, m.start() - 40)
            end = min(len(line), m.end() + 10)
            return line[start:end].strip()
        return None


def parse_line_timestamp(
    ts_text: str,
    date_anchor: Optional[date],
    fmt: Optional[FormatSpec],
    strptime_format: Optional[str],
    default_tz: Optional[timezone],
) -> Tuple[Optional[datetime], bool]:
    """Parse an extracted timestamp string.

    Returns (datetime_or_None, trusted). trusted is True when the parse came
    from a user-supplied format or the detected FormatSpec, False when it
    came from the fuzzy fallback. Only trusted parses may trigger the early
    exit in the print loop.
    """
    if strptime_format is not None:
        try:
            return normalize_datetime(datetime.strptime(ts_text, strptime_format), default_tz), True
        except Exception:
            return None, False
    if fmt is not None:
        if fmt.strptime_variants is None:
            dt = parse_epoch(ts_text.strip())
        else:
            dt = _try_strptime(ts_text, fmt)
        if dt is None:
            return None, False
        if not fmt.has_year and date_anchor is not None:
            # Syslog has month+day but no year. Borrow only the year so that
            # "Jul 27" stays Jul 27, not the anchor's full date.
            dt = apply_anchor_year(dt, date_anchor)
        return normalize_datetime(dt, default_tz), True
    # No known format: epoch, then fuzzy dateutil.
    dt = parse_epoch(ts_text.strip())
    if dt is not None:
        return normalize_datetime(dt, default_tz), False
    if dateutil_parser is None:
        return None, False
    try:
        dt = dateutil_parser.parse(ts_text, fuzzy=True)
    except Exception:
        return None, False
    lacks_date = (
        not re.search(r"\b\d{4}\b", ts_text)
        and not re.search(r"\d{5,}", ts_text)
        and not re.search(r"\b\d{1,2}[\-/]\d{1,2}\b", ts_text)
        and MONTH_RE.search(ts_text) is None
    )
    if lacks_date and date_anchor is not None:
        dt = datetime.combine(date_anchor, dt.time())
    return normalize_datetime(dt, default_tz), False


# ------------------------------------------------------------------------------------
# Format detection

def detect_format(path: str) -> Optional[FormatSpec]:
    """Sample the head of the file and let the formats vote.

    A format scores one point per sampled line where its regex matches AND
    the match actually parses. The winner needs MIN_DETECT_HITS unless the
    file is tiny, in which case any parsed hit beats nothing. The guess is
    reported on stderr so the user can see it and override it.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = []
            for _ in range(DETECT_SAMPLE_LINES):
                line = f.readline()
                if not line:
                    break
                lines.append(line)
    except OSError:
        return None
    if not lines:
        return None

    scores = {}
    for fmt in KNOWN_FORMATS:
        hits = 0
        for line in lines:
            m = fmt.regex.search(line)
            if not m:
                continue
            if fmt.strptime_variants is None:
                if parse_epoch(m.group(1)) is not None:
                    hits += 1
            elif _try_strptime(m.group(1), fmt) is not None:
                hits += 1
        if hits:
            scores[fmt.name] = (hits, fmt)

    if not scores:
        return None

    # Highest hit count wins; ties broken by KNOWN_FORMATS order, which
    # deliberately puts the structured formats ahead of bare epochs.
    best_name = None
    best_hits = 0
    for fmt in KNOWN_FORMATS:
        entry = scores.get(fmt.name)
        if entry and entry[0] > best_hits:
            best_hits, best_name = entry[0], fmt.name

    threshold = MIN_DETECT_HITS if len(lines) >= MIN_DETECT_HITS else 1
    if best_hits < threshold:
        sys.stderr.write(
            f"tfind: format detection inconclusive (best guess '{best_name}' "
            f"matched only {best_hits} of {len(lines)} sampled lines); "
            f"falling back to fuzzy parsing. Use --timestamp-format to override.\n"
        )
        return None

    winner = scores[best_name][1]
    sys.stderr.write(
        f"tfind: detected timestamp format '{winner.name}' "
        f"({best_hits}/{len(lines)} sampled lines)\n"
    )
    return winner


# ------------------------------------------------------------------------------------
# File machinery

def file_size(path: str) -> int:
    with open(path, "rb") as f:
        f.seek(0, 2)
        return f.tell()


def find_first_date_anchor(
    path: str,
    extractor: Extractor,
    fmt: Optional[FormatSpec],
    strptime_format: Optional[str],
    default_tz: Optional[timezone],
) -> Optional[date]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for _ in range(ANCHOR_SCAN_LINES):
                line = f.readline()
                if not line:
                    break
                ts_text = extractor.extract(line)
                if not ts_text:
                    continue
                if fmt is not None and not fmt.has_year:
                    # Year-less formats: dateutil supplies the current year;
                    # without dateutil, assume the current year outright.
                    if dateutil_parser is not None:
                        try:
                            return dateutil_parser.parse(ts_text.strip(), fuzzy=True).date()
                        except Exception:
                            continue
                    dt = _try_strptime(ts_text, fmt)
                    if dt is not None:
                        return dt.replace(year=datetime.now().year).date()
                else:
                    dt, _ = parse_line_timestamp(ts_text, None, fmt, strptime_format, default_tz)
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
    fmt: Optional[FormatSpec],
    strptime_format: Optional[str],
    default_tz: Optional[timezone],
) -> int:
    """Find a byte offset at or before the first line with timestamp >= target.

    At each probe, unparseable lines (continuations, stack traces) are
    skipped by reading forward until a parseable line appears, capped at
    PROBE_SCAN_BYTES. If no parseable line exists between the probe and hi,
    the probe is undecidable and we shrink hi to mid: undershooting only
    costs a little extra scanning in the print loop, while the old behavior
    (treating unparseable as "before target") could overshoot and silently
    drop matching lines.
    """
    low = 0
    hi = file_size(path)
    with open(path, "rb") as f:
        while low < hi:
            mid = (low + hi) // 2
            f.seek(mid)
            if mid > 0:
                f.readline()  # discard the partial line the seek landed in
            pos = f.tell()
            found = None
            scanned = 0
            while pos < hi and scanned < PROBE_SCAN_BYTES:
                raw = f.readline()
                if not raw:
                    break
                next_pos = f.tell()
                s = raw.decode("utf-8", errors="ignore")
                ts_text = extractor.extract(s)
                if ts_text:
                    ts, _ = parse_line_timestamp(
                        ts_text, date_anchor, fmt, strptime_format, default_tz
                    )
                    if ts is not None:
                        found = (ts, next_pos)
                        break
                scanned += len(raw)
                pos = next_pos
            if found is None:
                hi = mid
            else:
                ts, next_pos = found
                if ts < target:
                    low = next_pos
                else:
                    hi = mid
    return low


# ------------------------------------------------------------------------------------
# Main range printer

def print_range(
    path: str,
    start_s: str,
    end_s: str,
    readability: bool,
    color: str,
    timestamp_format: Optional[str],
    timestamp_regex: Optional[str],
    default_tz: Optional[timezone],
    input_order: str,
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
        except re.error as exc:
            sys.exit(f"Error: invalid --timestamp-regex: {exc}")
        if user_pat.groups < 1:
            sys.exit("Error: --timestamp-regex must include at least one capture group.")

    # Auto-detect unless the user supplied their own extraction or parsing.
    fmt = None
    if timestamp_format is None and timestamp_regex is None:
        fmt = detect_format(path)

    extractor = Extractor(fmt, user_pat)

    need_anchor = (
        input_is_time_only(start_s)
        or input_is_time_only(end_s)
        or (fmt is not None and not fmt.has_year)
    )
    date_anchor = None
    if need_anchor:
        # For year-less log formats, a year typed by the user in <start> or
        # <end> beats any guess made from the file: the file simply does not
        # know what year it is, and dateutil would assume the current one,
        # which is wrong for last year's logs.
        if fmt is not None and not fmt.has_year:
            for s_val, dt_val in ((start_s, start_dt), (end_s, end_dt)):
                if re.search(r"\b\d{4}\b", s_val) or re.search(r"\d{5,}", s_val):
                    date_anchor = dt_val.date()
                    break
        if date_anchor is None:
            date_anchor = find_first_date_anchor(
                path, extractor, fmt, timestamp_format, default_tz
            )
    if need_anchor and date_anchor is not None:
        if input_is_time_only(start_s):
            start_dt = normalize_datetime(datetime.combine(date_anchor, start_dt.time()), default_tz)
        if input_is_time_only(end_s):
            end_dt = normalize_datetime(datetime.combine(date_anchor, end_dt.time()), default_tz)
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt

    start_pos = 0
    if input_order != "unsorted":
        start_pos = binary_search_start(
            path, start_dt, extractor, date_anchor, fmt, timestamp_format, default_tz
        )

    # Early exit is only safe when timestamps come from a trusted parser and
    # several consecutive lines agree; a single fuzzy misparse must not end
    # the scan.
    allow_early_exit = input_order != "unsorted"

    do_color = readability and ansi_enabled()
    in_range = False
    consecutive_past_end = 0

    with open(path, "rb") as f:
        f.seek(start_pos)
        for raw in f:
            line = raw.decode("utf-8", errors="ignore")
            ts_text = extractor.extract(line)
            ts = None
            trusted = False
            if ts_text:
                ts, trusted = parse_line_timestamp(
                    ts_text, date_anchor, fmt, timestamp_format, default_tz
                )
            if ts is None:
                # Continuation line (stack trace, wrapped message, payload).
                # It belongs to the last timestamped line; print it when that
                # line was in range.
                if in_range:
                    sys.stdout.write(line)
                continue
            if start_dt <= ts <= end_dt:
                in_range = True
                consecutive_past_end = 0
                if do_color and ts_text:
                    line = line.replace(ts_text, colorize(ts_text, color), 1)
                sys.stdout.write(line)
            elif ts > end_dt:
                in_range = False
                if allow_early_exit and trusted:
                    consecutive_past_end += 1
                    if consecutive_past_end >= OUT_OF_RANGE_LIMIT:
                        break
            else:
                # Before the range (binary search deliberately undershoots).
                in_range = False
                consecutive_past_end = 0


# ------------------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(
        prog="tfind",
        description="Binary-search a log file and print all lines in a time range.",
        epilog=(
            "Examples:\n"
            "  tfind app.log '2026-04-28 06:00:00' '2026-04-28 06:15:00'\n"
            "  tfind -r fix.log 06:00 06:15\n"
            "  tfind --timestamp-format='%%Y%%m%%d %%H%%M%%S' odd.log '20260428 060000' '20260428 061500'\n"
            "  tfind --timezone=UTC --input-order=unsorted mixed.log 1745820000 1745820900"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("logfile")
    ap.add_argument("start")
    ap.add_argument("end")
    ap.add_argument(
        "-r", "--readability",
        nargs="?", const="cyan", default=None, metavar="COLOR",
        help="colorize matched timestamps (optionally pick a color: cyan, yellow, green, red, magenta, blue)",
    )
    ap.add_argument(
        "--timestamp-format", metavar="FMT",
        help="strptime format for log lines AND for <start>/<end>; disables auto-detection",
    )
    ap.add_argument(
        "--timestamp-regex", metavar="REGEX",
        help="regex with one capture group used to extract the timestamp from each line",
    )
    ap.add_argument(
        "--timezone", metavar="TZ",
        help="timezone applied to naive timestamps: UTC, Z, +HH:MM, -HH:MM, +HHMM, -HHMM",
    )
    ap.add_argument(
        "--input-order", choices=["auto", "sorted", "unsorted"], default="auto",
        help="unsorted disables binary search and early exit; default: auto",
    )
    args = ap.parse_args(argv)

    default_tz = None
    if args.timezone:
        default_tz = parse_timezone_value(args.timezone)
        if default_tz is None:
            sys.exit("Error: invalid --timezone. Use UTC, Z, +HH:MM, -HH:MM, +HHMM, or -HHMM.")

    readability = args.readability is not None
    color = args.readability if readability else "cyan"
    if readability and color not in COLOR_CODES:
        sys.exit(f"Error: unknown color '{color}'. Choices: {', '.join(c for c in COLOR_CODES if c != 'reset')}")

    if not os.path.isfile(args.logfile):
        sys.exit(f"Error: no such file: {args.logfile}")

    print_range(
        args.logfile,
        args.start,
        args.end,
        readability,
        color,
        args.timestamp_format,
        args.timestamp_regex,
        default_tz,
        args.input_order,
    )


if __name__ == "__main__":
    main()