#!/usr/bin/env python3
"""
Generate a *very large* FIX message log (e.g., 20 GB) between a counterparty and an exchange.

Design goals
------------
- Writes realistic-looking FIX 4.4 messages (headers, BodyLength(9), CheckSum(10) computed correctly).
- Mixed message types: MarketDataSnapshot (W), MarketDataIncrementalRefresh (X), NewOrderSingle (D), ExecutionReport (8).
- Monotonic timestamps and sequence numbers.
- Fast I/O: buffered writes, minimal per-message allocations, prebuilt templates.
- You choose target size via --size-gb or --size-bytes and where to write.
- Delimiter selectable: real SOH ("\x01") or human-readable pipe "|" (default: SOH).

Usage
-----
    python fix_log_generator.py out.fix --size-gb 20 \
        --sender CPTY01 --target EXCH01 --symbol BTC-USD \
        --start "2025-08-08 16:00:00" --rate 250000

Arguments
---------
- out.fix: output filepath (will be created; use a fast disk!)
- --size-gb / --size-bytes: how big to make the file. (Mutually exclusive)
- --sender / --target: 49/56 CompIDs
- --symbol: trading symbol string (default BTC-USD)
- --fix-version: default FIX.4.4
- --rate: approximate messages per second (affects timestamps; the generator will run as fast as your disk allows, not throttled)
- --delim: "SOH" (\x01) or "PIPE" (|). Default SOH.
- --md-weight / --nos-weight / --er-weight / --inc-weight: mix ratios (integers; default 70/15/10/5)
- --start: starting wall-clock (local time OK); default now.

Notes
-----
- Generating 20 GB can take a while and requires fast storage. NVMe strongly recommended.
- The generator is CPU-light and I/O-heavy. For even faster output, run with Python -O and a large OS write cache.
- If you want to simulate *one hour* of logs exactly, use --rate to control timestamp spacing and stop on time instead of size (not implemented by default because target here is size).
"""

import argparse
import os
import random
import string
import time
from datetime import datetime, timedelta
import sys
try:
    from tqdm.auto import tqdm
except Exception:
    tqdm = None

SOH = "\x01"

# -------------- Helpers --------------

def now_utc_precise() -> datetime:
    return datetime.utcnow()


def ts_utc_fix(dt: datetime) -> str:
    # FIX SendingTime format: YYYYMMDD-HH:MM:SS.sss
    return dt.strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


def checksum(msg_bytes: bytes) -> int:
    return sum(msg_bytes) % 256


def make_header(fixver: str, seq: int, sender: str, target: str, sending_time: str, delim: str) -> str:
    # 8=...|9=BODYLEN|35=... placed later; here we build generic header tail
    return delim.join([
        f"34={seq}",
        f"49={sender}",
        f"52={sending_time}",
        f"56={target}",
    ]) + delim


def wrap_fix(fixver: str, msg_type: str, body_tail: str, seq: int, sender: str, target: str, sending_time: str, delim: str) -> bytes:
    # Build body (from 35=.. through just before 10=)
    body = f"35={msg_type}{delim}" + make_header(fixver, seq, sender, target, sending_time, delim) + body_tail
    # Compute BodyLength = len from after 9=..<SOH> to just before 10=
    head = f"8={fixver}{delim}9="
    tmp = f"{head}0000{delim}{body}"
    blen = len(tmp.encode('ascii')) - len(f"{head}0000{delim}".encode('ascii'))
    msg_wo_cs = f"8={fixver}{delim}9={blen}{delim}{body}"
    cs = checksum(msg_wo_cs.encode('ascii'))
    full = f"{msg_wo_cs}10={cs:03d}{delim}"

    # prepend exchange-style timestamp and add newline
    wall_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")  # e.g., 2025-04-02 14:23:00.000100
    return f"{wall_ts} {full}\n".encode('ascii')


# -------------- Message builders --------------

def rand_str(n=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))


def rand_price(base=30000.0, spread=200.0):
    return round(base + random.uniform(-spread, spread), 2)


def rand_qty():
    return random.randint(1, 5) * 10


def build_md_snapshot(seq, symbol, sender, target, fixver, dt, delim):
    sending_time = ts_utc_fix(dt)
    entries = []
    # 6 entries: 3 bids, 3 asks
    for etype in ('0', '1'):  # 0=Bid, 1=Offer
        for lvl in range(3):
            px = rand_price(30000.0 + (100 if etype == '1' else -100), spread=400.0)
            sz = rand_qty()
            entries.append(delim.join([
                f"269={etype}",      # MDEntryType
                f"270={px}",         # MDEntryPx
                f"271={sz}",         # MDEntrySize
                f"278={rand_str(6)}",# MDEntryID
            ]))
    body_tail = delim.join([
        f"55={symbol}",
        "268=6",  # NoMDEntries
        *(e for e in entries),
    ]) + delim
    return wrap_fix(fixver, 'W', body_tail, seq, sender, target, sending_time, delim)


def build_md_incremental(seq, symbol, sender, target, fixver, dt, delim):
    sending_time = ts_utc_fix(dt)
    # 2 updates: one bid change, one ask change
    entries = []
    for etype in ('0', '1'):
        px = rand_price(30000.0 + (100 if etype == '1' else -100), spread=600.0)
        sz = rand_qty()
        entries.append(delim.join([
            "279=2",              # MDUpdateAction: 0-new,1-change,2-delete -> we use 1 or 0 typically; choose 1
            f"269={etype}",
            f"270={px}",
            f"271={sz}",
            f"278={rand_str(6)}",
        ]))
    body_tail = delim.join([
        f"55={symbol}",
        "268=2",
        *(e for e in entries),
    ]) + delim
    return wrap_fix(fixver, 'X', body_tail, seq, sender, target, sending_time, delim)


def build_new_order_single(seq, symbol, sender, target, fixver, dt, delim):
    sending_time = ts_utc_fix(dt)
    side = random.choice(['1','2'])  # 1=Buy, 2=Sell
    px = rand_price()
    qty = rand_qty()
    clid = rand_str(12)
    body_tail = delim.join([
        f"11={clid}",         # ClOrdID
        f"21=1",              # HandlInst (1=Automated execution order, private)
        f"38={qty}",          # OrderQty
        f"40=2",              # OrdType=2 (Limit)
        f"44={px}",           # Price
        f"54={side}",         # Side
        f"55={symbol}",
        f"59=0",              # TimeInForce=0 (Day)
        f"60={sending_time}", # TransactTime
    ]) + delim
    return wrap_fix(fixver, 'D', body_tail, seq, sender, target, sending_time, delim)


def build_execution_report(seq, symbol, sender, target, fixver, dt, delim):
    sending_time = ts_utc_fix(dt)
    side = random.choice(['1','2'])
    qty = rand_qty()
    last_qty = random.randint(1, qty)
    last_px = rand_price()
    body_tail = delim.join([
        f"6={last_px}",        # AvgPx (use last_px for simplicity)
        f"14={last_qty}",      # CumQty (simplified)
        f"17={rand_str(12)}",  # ExecID
        f"31={last_px}",       # LastPx
        f"32={last_qty}",      # LastQty
        f"37={rand_str(10)}",  # OrderID
        f"39=2",               # OrdStatus=2 (Filled) — simplified
        f"54={side}",
        f"55={symbol}",
        f"150=F",              # ExecType=F (Trade)
        f"151=0",              # LeavesQty=0
        f"60={sending_time}",
    ]) + delim
    return wrap_fix(fixver, '8', body_tail, seq, sender, target, sending_time, delim)


# -------------- Main generator --------------

def generate(path: str, size_target: int, sender: str, target: str, symbol: str, fixver: str,
             delim_choice: str, rate: int, weights, start_dt: datetime, pbar=None):
    delim = SOH if delim_choice.upper() == 'SOH' else '|'

    builders = [
        (build_md_snapshot, weights['md']),
        (build_md_incremental, weights['inc']),
        (build_new_order_single, weights['nos']),
        (build_execution_report, weights['er']),
    ]
    population = []
    for fn, w in builders:
        population.extend([fn]*w)

    seq = 1
    bytes_written = 0
    dt = start_dt
    tick = timedelta(microseconds=max(1, int(1_000_000 / max(1, rate))))

    # throttle tqdm updates to avoid overhead
    progress_accum = 0
    PROGRESS_CHUNK = 1 << 20  # 1 MiB

    with open(path, 'wb', buffering=8*1024*1024) as f:
        while bytes_written < size_target:
            fn = random.choice(population)
            msg = fn(seq, symbol, sender, target, fixver, dt, delim)
            f.write(msg)
            mlen = len(msg)
            bytes_written += mlen
            progress_accum += mlen
            seq += 1
            dt += tick

            if pbar is not None and progress_accum >= PROGRESS_CHUNK:
                pbar.update(progress_accum)
                progress_accum = 0

    if pbar is not None and progress_accum:
        pbar.update(progress_accum)

    return bytes_written, seq-1



def parse_args():
    p = argparse.ArgumentParser(description="Generate a huge FIX 4.4 log")
    p.add_argument('output', help='Output filepath')
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--size-gb', type=float, help='Target size in GB (decimal GB)')
    g.add_argument('--size-bytes', type=int, help='Target size in bytes')
    p.add_argument('--sender', default='CPTY01')
    p.add_argument('--target', default='EXCH01')
    p.add_argument('--symbol', default='BTC-USD')
    p.add_argument('--fix-version', default='FIX.4.4')
    p.add_argument('--rate', type=int, default=250_000, help='Messages per second for timestamp spacing (not throttling)')
    p.add_argument('--delim', choices=['SOH','PIPE'], default='SOH')
    p.add_argument('--md-weight', type=int, default=70)
    p.add_argument('--inc-weight', type=int, default=5)
    p.add_argument('--nos-weight', type=int, default=15)
    p.add_argument('--er-weight', type=int, default=10)
    p.add_argument('--start', default=None, help='Start datetime (e.g., "2025-08-08 16:00:00"). Defaults to now UTC')
    p.add_argument('--progress', action='store_true', help='Force-enable progress bar')
    p.add_argument('--no-progress', action='store_true', help='Disable progress bar')
    return p.parse_args()


def main():
    args = parse_args()
    if args.size_gb is not None:
        size_target = int(args.size_gb * (10**9))
    else:
        size_target = args.size_bytes

    start_dt = now_utc_precise() if args.start is None else datetime.fromisoformat(args.start)

    weights = {
        'md': max(0, args.md_weight),
        'inc': max(0, args.inc_weight),
        'nos': max(0, args.nos_weight),
        'er': max(0, args.er_weight),
    }
    if sum(weights.values()) == 0:
        raise SystemExit('At least one message type weight must be > 0')

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)

    # decide progress behavior
    show_progress = (args.progress or (sys.stderr.isatty() and not args.no_progress)) and (tqdm is not None)

    start_time = time.time()
    if show_progress:
        with tqdm(total=size_target, unit='B', unit_scale=True, unit_divisor=1024,
                  desc='Writing', smoothing=0.05, mininterval=0.25, leave=False) as pbar:
            bytes_written, total_msgs = generate(
                path=args.output, size_target=size_target, sender=args.sender, target=args.target,
                symbol=args.symbol, fixver=args.fix_version, delim_choice=args.delim, rate=args.rate,
                weights=weights, start_dt=start_dt, pbar=pbar,
            )
    else:
        bytes_written, total_msgs = generate(
            path=args.output, size_target=size_target, sender=args.sender, target=args.target,
            symbol=args.symbol, fixver=args.fix_version, delim_choice=args.delim, rate=args.rate,
            weights=weights, start_dt=start_dt, pbar=None,
        )

    elapsed = time.time() - start_time
    gb = bytes_written / (10**9)
    mbps = (bytes_written / (1024*1024)) / max(0.001, elapsed)
    print(f"Wrote {gb:.2f} GB, {total_msgs} messages in {elapsed:.1f}s ({mbps:.1f} MiB/s)")



if __name__ == '__main__':
    main()
