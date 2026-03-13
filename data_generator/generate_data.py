import os
import random
from faker import Faker
from datetime import datetime, timedelta
import csv

fake = Faker()
random.seed(42)
Faker.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data/raw")
os.makedirs(f"{OUTPUT_DIR}/claims",      exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/policies",    exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/trades",      exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/market_data", exist_ok=True)

# ── Config ────────────────────────────────────────────────
NUM_POLICIES     = 500
NUM_CLAIMS       = 300
NUM_TRADES       = 1000
NUM_MARKET_ROWS  = 500

POLICY_TYPES     = ["Health", "Motor", "Home", "Life", "Travel"]
CLAIM_STATUSES   = ["Open", "In Review", "Approved", "Rejected", "Closed"]
TRADE_TYPES      = ["BUY", "SELL"]
INSTRUMENTS      = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]
CURRENCIES       = ["USD", "EUR", "GBP"]

def rand_date(start_days_ago=365, end_days_ago=0):
    start = datetime.now() - timedelta(days=start_days_ago)
    end   = datetime.now() - timedelta(days=end_days_ago)
    return start + (end - start) * random.random()

def write_csv(path, rows, headers):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)
    print(f"  Written: {path}  ({len(rows)} rows)")

# ── 1. Policies ───────────────────────────────────────────
def gen_policies():
    rows = []
    for i in range(NUM_POLICIES):
        start = rand_date(730, 30)
        rows.append({
            "policy_id":        f"POL-{i+1:05d}",
            "customer_id":      f"CUST-{random.randint(1, 300):05d}",
            "policy_type":      random.choice(POLICY_TYPES),
            "start_date":       start.strftime("%Y-%m-%d"),
            "end_date":         (start + timedelta(days=365)).strftime("%Y-%m-%d"),
            "premium_amount":   round(random.uniform(200, 5000), 2),
            "currency":         random.choice(CURRENCIES),
            "status":           random.choice(["Active", "Expired", "Cancelled"]),
            "agent_id":         f"AGT-{random.randint(1, 50):03d}",
            "created_at":       start.strftime("%Y-%m-%d %H:%M:%S"),
        })
    # introduce ~5% duplicates to test deduplication
    rows += random.sample(rows, k=int(NUM_POLICIES * 0.05))
    write_csv(f"{OUTPUT_DIR}/policies/policies.csv", rows,
              ["policy_id","customer_id","policy_type","start_date","end_date",
               "premium_amount","currency","status","agent_id","created_at"])

# ── 2. Claims ─────────────────────────────────────────────
def gen_claims():
    rows = []
    for i in range(NUM_CLAIMS):
        filed = rand_date(365, 0)
        # introduce ~10% nulls in claim_amount to test null handling
        amount = round(random.uniform(500, 50000), 2) if random.random() > 0.10 else None
        rows.append({
            "claim_id":       f"CLM-{i+1:05d}",
            "policy_id":      f"POL-{random.randint(1, NUM_POLICIES):05d}",
            "claim_date":     filed.strftime("%Y-%m-%d"),
            "claim_amount":   amount,
            "currency":       random.choice(CURRENCIES),
            "status":         random.choice(CLAIM_STATUSES),
            "description":    fake.sentence(nb_words=8),
            "handler_id":     f"HND-{random.randint(1, 20):03d}",
            "created_at":     filed.strftime("%Y-%m-%d %H:%M:%S"),
        })
    write_csv(f"{OUTPUT_DIR}/claims/claims.csv", rows,
              ["claim_id","policy_id","claim_date","claim_amount","currency",
               "status","description","handler_id","created_at"])

# ── 3. Trades ─────────────────────────────────────────────
def gen_trades():
    rows = []
    for i in range(NUM_TRADES):
        ts = rand_date(30, 0)
        rows.append({
            "trade_id":       f"TRD-{i+1:06d}",
            "instrument":     random.choice(INSTRUMENTS),
            "trade_type":     random.choice(TRADE_TYPES),
            "quantity":       random.randint(1, 1000),
            "price":          round(random.uniform(10, 3000), 4),
            "currency":       random.choice(CURRENCIES),
            "trader_id":      f"TDR-{random.randint(1, 30):03d}",
            "desk_id":        f"DSK-{random.randint(1, 5):02d}",
            "trade_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "settlement_date": (ts + timedelta(days=2)).strftime("%Y-%m-%d"),
            "status":         random.choice(["Pending", "Settled", "Failed", "Cancelled"]),
        })
    write_csv(f"{OUTPUT_DIR}/trades/trades.csv", rows,
              ["trade_id","instrument","trade_type","quantity","price","currency",
               "trader_id","desk_id","trade_timestamp","settlement_date","status"])

# ── 4. Market data ────────────────────────────────────────
def gen_market_data():
    rows = []
    for _ in range(NUM_MARKET_ROWS):
        ts = rand_date(30, 0)
        base = random.uniform(50, 3000)
        rows.append({
            "instrument":  random.choice(INSTRUMENTS),
            "open":        round(base, 4),
            "high":        round(base * random.uniform(1.00, 1.05), 4),
            "low":         round(base * random.uniform(0.95, 1.00), 4),
            "close":       round(base * random.uniform(0.97, 1.03), 4),
            "volume":      random.randint(100000, 10000000),
            "timestamp":   ts.strftime("%Y-%m-%d %H:%M:%S"),
        })
    write_csv(f"{OUTPUT_DIR}/market_data/market_data.csv", rows,
              ["instrument","open","high","low","close","volume","timestamp"])

# ── Run all ───────────────────────────────────────────────
if __name__ == "__main__":
    print("\nGenerating mock financial data...\n")
    gen_policies()
    gen_claims()
    gen_trades()
    gen_market_data()
    print("\nDone! All files written to data/raw/")