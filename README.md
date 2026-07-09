# tfind

**tfind** — *Instant, format-agnostic log slicing by time range.*

`tfind` is built for the case where you have a very large, timestamp-sorted log file and you only care about a small interval somewhere in the middle. Instead of scanning from byte 0 like `grep`, it detects the log's timestamp format, binary-searches to the first matching region, and then streams only the lines in range.

This is where it shines:

- multi-GB logs
- monotonic timestamps
- narrow windows like a few milliseconds, seconds, or minutes
- "show me what happened around 07:18:00.200" workflows

This is not its sweet spot:

- unsorted logs
- "give me half the file"
- situations where a simple anchored `grep` already matches exactly what you want

---

## Features

- **Binary-search start position** for sorted logs
- **Multi-format timestamp detection** (FIX, ISO8601, Apache, syslog, epoch, etc.)
- **Continuation-line support** for stack traces and wrapped payloads
- **Partial-date input**: time-only strings are anchored to a date found in the file
- **Readability mode**: optional timestamp coloring for terminal output
- **Constant-memory streaming** for multi-GB files

---

## Installation

```bash
git clone https://github.com/domgalati/tfind.git
cd tfind
pip install .
```

For local development/editable installs:

```bash
pip install -e .
```

## Important dependency note

If you run `f_tfind.py` directly, make sure `python-dateutil` is installed unless you are supplying an explicit parsing path for your `<start>` and `<end>` arguments, usually `--timestamp-format` and, for nontrivial log lines, a matching `--timestamp-regex`.

```bash
pip install python-dateutil
```

or just install the project so the dependency is pulled in:

```bash
pip install -e .
```

Why this matters:

- `--timestamp-regex` only tells `tfind` how to extract timestamps from log lines
- it does **not** tell `tfind` how to parse the `<start>` and `<end>` values you typed
- without `python-dateutil`, free-form inputs like `20260709-07:18:00.200` may fail unless you also provide `--timestamp-format`
- for formats like FIX, `--timestamp-format` often pairs with `--timestamp-regex` so the parser sees only the timestamp field and not the rest of the line

---

## Usage

```bash
tfind [--readability[=COLOR] | -r] [--timestamp-format=FMT] [--timestamp-regex=REGEX] [--timezone=TZ] [--input-order=auto|sorted|unsorted] <logfile> <start> <end>
```

Options:

- `--timestamp-format=FMT` - force parsing with a Python `strptime` format
- `--timestamp-regex=REGEX` - force extraction using a regex with at least one capture group
- `--timezone=TZ` - default timezone for naive timestamps (`UTC`, `Z`, `+HH:MM`, `-HHMM`)
- `--input-order=...` - `auto`/`sorted` uses binary-search start, `unsorted` scans from beginning

---

## Examples

### Slice a normal app log

```bash
tfind app.log 13:23:00.000 13:23:30.000
```

### Same, but color timestamps in yellow

```bash
tfind -r=yellow app.log 13:23:00.000 13:23:30.000
```

### Using full date-time strings

```bash
tfind app.log "2025-08-08 13:23:00.000" "2025-08-08 13:23:30.000"
```

### Apache-style timestamps with explicit format and regex

```bash
tfind --timestamp-format="%d/%b/%Y:%H:%M:%S %z" --timestamp-regex="\\[([^\\]]+)\\]" access.log "31/Aug/1995:20:00:00 -0400" "31/Aug/1995:23:58:08 -0400"
```

### Unsorted logs

```bash
tfind --input-order=unsorted app.log "2025-08-08 13:23:00" "2025-08-08 13:23:30"
```

### FIX logs with explicit format

```bash
tfind --timestamp-format="%Y%m%d-%H:%M:%S.%f" --timestamp-regex='^(\d{8}-\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)' examples/FIX.4.4-TRADER-VENUE.messages.log "20260505-07:38:50.120" "20260505-07:38:50.280"
```

---

## Included sample FIX log

The repo includes a small FIX message log at:

```text
examples/FIX.4.4-TRADER-VENUE.messages.log
```

It is useful for verifying format detection and command syntax, but it is intentionally small. It is **not** meant to be a benchmark file.

---

## Benchmarking on your hardware

To see where `tfind` wins, benchmark against a large, timestamp-sorted log and send output to `/dev/null` so terminal rendering does not dominate the timing.

```bash
LOG=/path/to/huge/FIX.4.4-TRADER-VENUE.messages.log
```

### Narrow interval in the middle of a huge FIX log

```bash
/usr/bin/time -f 'wall=%e sec  maxrss=%MKB' \
  python3 f_tfind.py "$LOG" \
  '20260709-07:18:00.200' '20260709-07:18:00.299' > /dev/null
```

Example result from a large generated FIX log:

```text
tfind: detected timestamp format 'fix' (200/200 sampled lines)
wall=0.12 sec  maxrss=16452KB
```

### Rough `grep` comparison for the same narrow FIX slice

```bash
/usr/bin/time -f 'wall=%e sec  maxrss=%MKB' \
  grep -E '^20260709-07:18:00\.(2[0-9][0-9]):' "$LOG" > /dev/null
```

Example result from the same large log:

```text
wall=2.99 sec  maxrss=2356KB
```

### Time comparison against `grep`

Using the generated `15G` FIX log and writing output to `/dev/null`, the timings below are warm-cache medians from 3 runs per command. The `grep` side used anchored regexes that matched the same timestamp windows as the `tfind` range query.

- `500 ms` window (`07:18:00.000` through `07:18:00.499`): `tfind` `0.257 sec`, `grep` `2.785 sec`, about `10.8x` faster
- `1 s` window (`07:18:00.000` through `07:18:00.999`): `tfind` `0.434 sec`, `grep` `2.776 sec`, about `6.4x` faster
- `3 s` window (`07:18:00.000` through `07:18:02.999`): `tfind` `1.157 sec`, `grep` `3.175 sec`, about `2.7x` faster
- `5 s` window (`07:18:00.000` through `07:18:04.999`): `tfind` `1.918 sec`, `grep` `3.160 sec`, about `1.6x` faster

This is the intended workload for `tfind`: a small interval in the middle of a very large sorted file. As the matching window grows, the advantage shrinks because `tfind` still has to parse and emit every matching line once it lands near the start.

### Browse results without skewing the benchmark

If you want to inspect the output in `less`, time the extraction first and browse afterward:

```bash
python3 f_tfind.py "$LOG" \
  '20260709-07:18:00.200' '20260709-07:18:00.299' > /tmp/tfind-window.log

less /tmp/tfind-window.log
```

Piping millions of lines directly into `less` can make the shell look hung because the producer blocks on the pipe while `less` is still consuming data.
