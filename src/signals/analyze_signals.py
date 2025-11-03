#!/usr/bin/env python3
import pandas as pd, os, sys

LOG_PATH = "logs/signals.csv"

if not os.path.exists(LOG_PATH):
    sys.exit(f"No signal log found at {LOG_PATH}. Run gen_signal.py first.")

df = pd.read_csv(LOG_PATH, parse_dates=["time_utc", "time_et"])

# --- basic counts ---
total = len(df)
per_symbol = df["symbol"].value_counts()
print(f"Total signals logged: {total}")
print("\nSignals per symbol:\n", per_symbol)

# --- LONG vs FLAT ratio ---
ratio = df["signal"].value_counts(normalize=True) * 100
print("\nSignal distribution (%):\n", ratio.round(2))

# --- average indicators by signal ---
avg_features = df.groupby("signal")[["close","ma20","rsi14"]].mean().round(2)
print("\nAverage indicators by signal:\n", avg_features)

# --- simple sanity: % of times close>ma20 ---
df["above_ma"] = df["close"] > df["ma20"]
above_pct = df.groupby("symbol")["above_ma"].mean() * 100
print("\n% bars above MA20 per symbol:\n", above_pct.round(2))

# --- time range ---
print("\nTime range:", df["time_et"].min(), "→", df["time_et"].max())
