import os
import pandas as pd
from DATA import get_dataframe, prioritize_filings, get_10k_year, get_10q_quarter

# -----------------------------
# CONFIG
# -----------------------------
TICKER_LIST_CSV = "sp500_yfin.csv"
DATA_FOLDER = "DATA"
CHECK_YEAR = 2026

# -----------------------------
# Check if file already exists
# -----------------------------
def file_exists(path):
    return os.path.exists(path)

# -----------------------------
# Update latest filings
# -----------------------------
def update_latest_for_ticker(ticker):

    print(f"\n🔎 Checking updates for {ticker}")

    folder_10k = os.path.join(DATA_FOLDER, ticker, "10K")
    folder_10q = os.path.join(DATA_FOLDER, ticker, "10Q")

    os.makedirs(folder_10k, exist_ok=True)
    os.makedirs(folder_10q, exist_ok=True)

    try:
        df = get_dataframe(ticker)
        df = prioritize_filings(df)
    except Exception as e:
        print(f"⚠️ Failed to fetch {ticker}: {e}")
        return

    # -------------------------
    # Check 10-K
    # -------------------------
    file_10k = os.path.join(folder_10k, f"{ticker}_10K_{CHECK_YEAR}.csv")

    if not file_exists(file_10k):

        df_10k = get_10k_year(df, CHECK_YEAR)

        if not df_10k.empty:
            df_10k.to_csv(file_10k, index=False)
            print(f"✅ New 10-K saved: {file_10k}")
        else:
            print("ℹ️ No 10-K available yet")

    else:
        print("✔ 10-K already exists")

    # -------------------------
    # Check all 10-Q
    # -------------------------
    for q in range(1,5):

        file_10q = os.path.join(folder_10q, f"{ticker}_10Q_{CHECK_YEAR}_Q{q}.csv")

        if file_exists(file_10q):
            print(f"✔ Q{q} already exists")
            continue

        df_10q = get_10q_quarter(df, CHECK_YEAR, q)

        if not df_10q.empty:
            df_10q.to_csv(file_10q, index=False)
            print(f"✅ New 10-Q saved: {file_10q}")
        else:
            print(f"ℹ️ No Q{q} filing yet")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":

    tickers_df = pd.read_csv(TICKER_LIST_CSV)
    tickers = tickers_df["Ticker"].dropna().unique()

    print(f"Tickers to check: {len(tickers)}")

    for ticker in tickers:
        update_latest_for_ticker(ticker)

    print("\n🎯 Dataset update complete")