#!/usr/bin/env python3
import argparse, subprocess, sys

PY = sys.executable or "python3"

def run(cmd: list[str]):
    print("+", " ".join(cmd))
    rc = subprocess.call(cmd)
    if rc != 0:
        sys.exit(rc)

def main():
    p = argparse.ArgumentParser(prog="runner", description="Project runner")
    sub = p.add_subparsers(dest="cmd", required=True)

    # ingest
    pi = sub.add_parser("ingest", help="Download 1m bars -> data/")
    pi.add_argument("--symbols", nargs="+", required=True, help="e.g. SPY QQQ AAPL")
    pi.add_argument("--period", default="1d", help="1d/5d/7d")

    # inspect raw data/
    sub.add_parser("inspect", help="Print schema + ranges for files in data/")

    # normalize data/* -> data_norm/*
    sub.add_parser("normalize", help="Normalize raw files")

    # signals from data_norm/*
    ps = sub.add_parser("signal", help="Compute MA20/RSI14 & print LONG/FLAT")
    ps.add_argument("--symbols", nargs="*", help="limit to tickers, e.g. AAPL SPY")

    # all-in-one: ingest -> normalize -> signal
    pa = sub.add_parser("all", help="ingest -> normalize -> signal")
    pa.add_argument("--symbols", nargs="+", required=True)
    pa.add_argument("--period", default="1d")
    pa.add_argument("--signal-only", nargs="*", help="limit printed symbols")

    args = p.parse_args()

    if args.cmd == "ingest":
        run([PY, "ingest.py", "--symbols", *args.symbols, "--period", args.period])

    elif args.cmd == "inspect":
        run([PY, "inspect_data.py"])

    elif args.cmd == "normalize":
        run([PY, "normalize.py"])

    elif args.cmd == "signal":
        cmd = [PY, "gen_signal.py"]
        if args.symbols:
            cmd += ["--symbols", *args.symbols]
        run(cmd)

    elif args.cmd == "all":
        run([PY, "ingest.py", "--symbols", *args.symbols, "--period", args.period])
        run([PY, "normalize.py"])
        cmd = [PY, "gen_signal.py"]
        if args.signal_only:
            cmd += ["--symbols", *args.signal_only]
        run(cmd)

if __name__ == "__main__":
    main()
