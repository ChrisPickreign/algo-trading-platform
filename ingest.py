import argparse, os, sys
import pandas as pd
import yfinance as yf
from datetime import datetime

def fetch_1m(symbol: str, period: str = "1d") -> pd.DataFrame:
    df = yf.download(symbol, interval="1m", period=period, auto_adjust=True, progress=False)
    if df.empty:
        raise RuntimeError(f"No data for {symbol}")
    df = df.rename(columns=str.lower)
    df.index = pd.to_datetime(df.index)
    df.index.name = "time"
    df["symbol"] = symbol
    return df.reset_index()

def save_parquet(df: pd.DataFrame, out_dir: str = "data"):
    os.makedirs(out_dir, exist_ok=True)
    date_tag = datetime.now().strftime("%Y-%m-%d")
    sym = df["symbol"].iloc[0]
    path = os.path.join(out_dir, f"{sym}_{date_tag}.parquet")
    if os.path.exists(path):
        old = pd.read_parquet(path)
        df = pd.concat([old, df], ignore_index=True).drop_duplicates(subset=["time"]).sort_values("time")
    df.to_parquet(path, index=False)
    return path

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", default=["SPY","QQQ"])
    p.add_argument("--period", default="1d", help="yfinance period for 1m bars: 1d/5d/7d")
    p.add_argument("--out", default="data")
    args = p.parse_args()

    for s in args.symbols:
        try:
            df = fetch_1m(s, args.period)
            path = save_parquet(df, args.out)
            print(f"[ok] {s}: {len(df)} rows -> {path}")
        except Exception as e:
            print(f"[err] {s}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
