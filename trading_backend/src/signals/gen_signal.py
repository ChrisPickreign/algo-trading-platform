import argparse, glob, os, pandas as pd
# Tells us how overbought or oversold the equity is
from ta.momentum import RSIIndicator
from zoneinfo import ZoneInfo

def load_latest_norm(path):
    df = pd.read_parquet(path)
    df["time"] = pd.to_datetime(df["time"], utc=True)
    return df.sort_values("time")

def compute_features(df):
    df = df.copy()
    # Calculate 20-period moving average and 14-period RSI, will be NaN for first few rows
    df["ma20"]  = df["close"].rolling(20).mean()
    df["rsi14"] = RSIIndicator(df["close"], 14).rsi()
    return df

def rule_long_flat(row):
    # Simple, interpretable:
    # LONG if price above MA20 and RSI>55, else FLAT
    if row["close"] > row["ma20"] and row["rsi14"] > 55:
        return "LONG"
    return "FLAT"

def log_signal(row, sym, sig, path="logs/signals.csv"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    et = row["time"].tz_convert(ZoneInfo("America/New_York"))
    header = not os.path.exists(path)
    with open(path, "a") as f:
        if header:
            f.write("time_utc,time_et,symbol,close,ma20,rsi14,signal\n")
        f.write(",".join([
            row["time"].isoformat(),
            et.isoformat(),
            sym,
            f"{row['close']:.6f}",
            f"{row['ma20']:.6f}",
            f"{row['rsi14']:.3f}",
            sig
        ]) + "\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="data_norm", help="folder with normalized parquet files")
    p.add_argument("--symbols", nargs="*", help="limit to these symbols (e.g., SPY AAPL)")
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.dir, "*.parquet")))
    if not files:
        raise SystemExit(f"No files found in {args.dir}. Run normalize.py first.")

    for f in files:
        sym = os.path.basename(f).split("_")[0].upper()
        if args.symbols and sym not in set(s.upper() for s in args.symbols):
            continue

        df = load_latest_norm(f)
        df = compute_features(df)
        last = df.iloc[-1]

        sig = rule_long_flat(last)
        print(f"{sym} | {last['time'].isoformat()} | close={last['close']:.2f} "
              f"| ma20={last['ma20']:.2f} | rsi14={last['rsi14']:.1f} | SIGNAL={sig}")
        log_signal(last, sym, sig)

if __name__ == "__main__":
    main()