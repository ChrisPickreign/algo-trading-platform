import os, glob
import pandas as pd

REQUIRED = ["time","open","high","low","close","volume","symbol"]

def normalize_one(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)

    # 1) Lowercase cols (handles normal & MultiIndex)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(a).lower() for (a, *_rest) in df.columns]
    else:
        df.columns = [str(c).lower() for c in df.columns]

    # 2) Ensure 'time' column exists
    if "time" in df.columns:
        pass  # already there (this is the case for your current ingest)
    elif isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index().rename(columns={df.columns[0]: "time"})
    elif any(c in df.columns for c in ("datetime","date","timestamp")):
        for cand in ("time","datetime","timestamp","date"):
            if cand in df.columns:
                df = df.rename(columns={cand: "time"})
                break
    else:
        raise ValueError(f"{path}: no 'time' column or datetime index found")

    # 3) Ensure OHLCV names exist (map common variants if needed)
    rename_map = {
        "adj close": "close",
        "volume_": "volume",  # guard against stray suffixes
    }
    df = df.rename(columns=rename_map)

    missing_core = {"open","high","low","close","volume"} - set(df.columns)
    if missing_core:
        raise ValueError(f"{path}: missing core cols {missing_core}; got {list(df.columns)}")

    # 4) Parse/clean time, sort
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")

    # 5) Ensure symbol column
    if "symbol" not in df.columns:
        df["symbol"] = os.path.basename(path).split("_")[0].upper()
    else:
        df["symbol"] = df["symbol"].astype(str).str.upper()

    # 6) Reorder/select
    return df[["time","open","high","low","close","volume","symbol"]]

def main():
    paths = sorted(glob.glob("data/*.parquet"))
    assert paths, "No parquet files in ./data"
    os.makedirs("data_norm", exist_ok=True)
    for p in paths:
        out = normalize_one(p)
        out_path = os.path.join("data_norm", os.path.basename(p))
        out.to_parquet(out_path, index=False)
        print(f"[ok] {p} -> {out_path} rows={len(out)} "
              f"range={out.time.min()} → {out.time.max()}")

if __name__ == "__main__":
    main()
