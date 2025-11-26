# algo-trading-platform
A platform that will automatically execute up to date trading strategies

---

## Setup

### 1) Python env
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```
---

## Example Executables

## need this first to add permissions
chmod +x runner.py

# ingest raw data -> data/
./runner.py ingest --symbols SPY QQQ AAPL --period 1d

# inspect raw files
./runner.py inspect

# normalize raw -> data_norm/
./runner.py normalize

# generate signals from normalized
./runner.py signal
./runner.py signal --symbols SPY

# full pipeline
./runner.py all --symbols SPY QQQ AAPL --period 1d
./runner.py all --symbols SPY QQQ AAPL --period 1d --signal-only SPY
