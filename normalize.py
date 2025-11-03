import glob, os, pandas as pd
from ast import literal_eval

def flatten_cols(cols):
    out = []
    for c in cols:
        if isinstance(c, tuple):
            a, b = c if len(c)==2 else (str(c), "")
        else:
            try:
                a, b = literal_eval(c)  # parse "('close','aapl')"
                if not isinstance(a, str): a = str(a)
                if not isinstance(b, str): b = str(b)
            except Exception:
                a, b = str(c), ""
        out.append((a.strip().lower(), b.strip().lower()))
    return out

def normalize_one(path):
    df = pd.read_parquet(path)

    # flatten weird columns
    cols = flatten_cols(df.columns)
    flat = {}
    for (name, ticker), series in zip(cols, df.T.values):
        # prefer the field name ('close','aapl') -> 'close'
        key = name
        flat.setdefault(key, series)
    df = pd.DataFrame(flat)

    # ensure required cols exist
    req = {"time","open","high","low","close","volume"}
    missing = req - set(df.columns)
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")

    # types & ordering
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time")
    # if no symbol column, infer from filename
    if "symbol" not in df.columns:
        sym = os.path.basename(path).split("_")[0]
        df["symbol"] = sym.upper()

    return df[["time","open","high","low","close","volume","symbol"]]

def main():
    paths = sorted(glob.glob("data/*.parquet"))
    assert paths, "No parquet files in ./data"
    os.makedirs("data_norm", exist_ok=True)
    for p in paths:
        out = normalize_one(p)
        out_path = os.path.join("data_norm", os.path.basename(p))
        out.to_parquet(out_path, index=False)
        print(f"[ok] {p} -> {out_path} rows={len(out)} range={out.time.min()} → {out.time.max()}")

if __name__ == "__main__":
    main()