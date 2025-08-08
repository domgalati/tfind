# tfind

**tfind** — *Instant, format-agnostic log slicing by time range.*

`tfind` lets you jump straight to the exact seconds you care about in massive log files without waiting for `grep` or `less` to churn through everything. It auto-detects common timestamp formats (or lets you specify one), uses a binary search to land directly at your start time, and streams only the matching lines.

---

## Features

- **Zero-setup** — `tfind logfile "13:23:00.000" "13:23:30.000"`
- **Multi-format timestamp parsing** (ISO8601, syslog, Apache, epoch, etc.)
- **Binary search** to skip straight to the right offset
- **Partial-date input**: time-only strings are auto-anchored to the first date in the file
- **Readability mode**: optional timestamp coloring for quick scanning
- Handles **multi-GB logs** in constant memory
- **Script-friendly output** for piping into other tools

---

## Installation

```bash
# Clone the repo
git clone https://github.com/yourusername/tfind.git
cd tfind

# (Optional) Install in an isolated environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

# Usage
`tfind [--readability[=COLOR] | -r] <logfile> <start> <end>`

Examples:

## Find all log lines between 13:23:00.000 and 13:23:30.000
`tfind app.log 13:23:00.000 13:23:30.000`

## Same, but color the timestamps in yellow
`tfind -r=yellow app.log 13:23:00.000 13:23:30.000`

## Using full date-time strings
`tfind app.log "2025-08-08 13:23:00.000" "2025-08-08 13:23:30.000"`

# Performance
tfind uses a binary search over the log file to jump directly to the start time, so it avoids scanning the entire file. On multi-gigabyte logs, this can reduce lookup time from minutes to seconds.
