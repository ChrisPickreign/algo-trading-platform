import os, glob, pandas as pd

paths = sorted(glob.glob("data/*.parquet"))
assert paths, "No parquet files in ./data"

for p in paths:
    df = pd.read_parquet(p)
    # normalize schema
    if "time" not in df.columns and df.index.name == "time":
        df = df.reset_index()
    if "symbol" not in df.columns:
        # infer from filename like data/SPY_2025-11-03.parquet
        base = os.path.basename(p)
        sym = base.split("_")[0]
        df["symbol"] = sym

    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")

    sym = df["symbol"].iloc[0]
    print(f"\n== {sym} :: {p}")
    print("rows:", len(df), "| time range:", df["time"].min(), "→", df["time"].max())
    print(df.tail(3)[["time","open","high","low","close","volume"]])