import os
import pandas as pd
from DATA import get_dataframe, prioritize_filings, get_10k_year, get_10q_quarter  # your functions

# -----------------------------
# CONFIGURATION
# -----------------------------
TICKER_LIST_CSV = "sp500_yfin.csv"  # CSV should have a column 'Ticker'
DATA_FOLDER = "DATA"                 # Base folder for all tickers
START_YEAR = 2016
END_YEAR = 2025

# -----------------------------
# HELPER FUNCTION: Save filings for one ticker
# -----------------------------
def save_filings_for_ticker(ticker, start_year=START_YEAR, end_year=END_YEAR):
    print(f"\n📥 Downloading filings for {ticker}")

    # Create folder structure: DATA/ticker/10K and DATA/ticker/10Q
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

    for year in range(start_year, end_year + 1):
        # 10-K filings
        df_10k = get_10k_year(df, year)
        if not df_10k.empty:
            file_path = os.path.join(folder_10k, f"{ticker}_10K_{year}.csv")
            df_10k.to_csv(file_path, index=False)
            print(f"✅ Saved {file_path}")

        # 10-Q filings (all quarters)
        for q in range(1, 5):
            df_10q = get_10q_quarter(df, year, q)
            if not df_10q.empty:
                file_path = os.path.join(folder_10q, f"{ticker}_10Q_{year}_Q{q}.csv")
                df_10q.to_csv(file_path, index=False)
                print(f"✅ Saved {file_path}")

# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    tickers_df = pd.read_csv(TICKER_LIST_CSV)
    tickers = tickers_df['Ticker'].dropna().unique()
    print(f"Total tickers to download: {len(tickers)}")

    for ticker in tickers:
        save_filings_for_ticker(ticker)