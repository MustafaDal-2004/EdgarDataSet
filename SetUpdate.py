import os
import pandas as pd

# -----------------------------
# CONFIG
# -----------------------------
DATA_FOLDER = "DATA"           # Existing SEC data folder
OUTPUT_FOLDER = "DATA_Q_YR_WIDE"  # Where wide fiscal-year CSVs will be saved
FILING_10Q = "10-Q"
FILING_10K = "10-K"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -----------------------------
# HELPERS
# -----------------------------
def clean_series(df):
    """Convert tag/value pairs into a clean Series with unique tag index."""
    if df.empty or "tag" not in df.columns or "value" not in df.columns:
        return pd.Series(dtype=float)
    df = df[["tag", "value"]].drop_duplicates(subset="tag")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.set_index("tag")["value"]

def load_filings(folder):
    """Load all CSVs from folder, parse 'end', and precompute Series per end date."""
    if not os.path.exists(folder):
        return {}, pd.DataFrame()
    files = [f for f in os.listdir(folder) if f.endswith(".csv")]
    df_list = []
    series_dict = {}
    for file in files:
        df = pd.read_csv(os.path.join(folder, file))
        if df.empty or "end" not in df.columns:
            continue
        df["end"] = pd.to_datetime(df["end"], errors="coerce")
        df = df.dropna(subset=["end"])
        df_list.append(df)
        for end_date in df["end"].unique():
            s = clean_series(df[df["end"] == end_date])
            series_dict[end_date] = s
    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
    else:
        full_df = pd.DataFrame()
    return series_dict, full_df

# -----------------------------
# CORE FUNCTION
# -----------------------------
def build_fiscal_year_wide_local(ticker):
    """
    Build fiscal-year-wide CSVs for an existing ticker,
    including post-last-10K filings.
    """
    print(f"\n📥 Processing {ticker}")
    output_folder = os.path.join(OUTPUT_FOLDER, ticker)
    os.makedirs(output_folder, exist_ok=True)

    # Load local filings
    series_10q, _ = load_filings(os.path.join(DATA_FOLDER, ticker, FILING_10Q))
    series_10k, _ = load_filings(os.path.join(DATA_FOLDER, ticker, FILING_10K))

    if not series_10q and not series_10k:
        print(f"⚠️ No filings found for {ticker}")
        return

    # Sort dates
    k_dates = sorted(series_10k.keys())
    q_dates_sorted = sorted(series_10q.keys())

    last_k_year = None

    # -----------------------------
    # Build fiscal year files
    # -----------------------------
    for i, fy_end in enumerate(k_dates):
        prev_k = k_dates[i - 1] if i > 0 else pd.Timestamp.min
        last_k_year = fy_end.year

        base_tags = series_10k[fy_end].index
        df_wide = pd.DataFrame(index=base_tags)

        # Add 10-Qs between previous 10-K and current 10-K
        for q_end in q_dates_sorted:
            if q_end <= prev_k:
                continue
            if q_end > fy_end:
                continue
            if q_end == fy_end:
                continue  # skip Q matching 10-K
            s_q = series_10q[q_end].reindex(base_tags)
            col_name = f"{q_end.year}_{q_end.month:02d}"
            df_wide[col_name] = s_q

        # Add 10-K as last column
        df_wide[f"{fy_end.year}_10K"] = series_10k[fy_end].reindex(base_tags)

        # Save file if data exists
        if df_wide.dropna(how="all").shape[1] > 0:
            out_file = os.path.join(output_folder, f"{ticker}_{fy_end.year}_wide.csv")
            df_wide.to_csv(out_file)
            print(f"✅ Saved {out_file}")
        else:
            print(f"⚠️ No data to save for fiscal year {fy_end.year} for ticker {ticker}")

    # -----------------------------
    # Capture filings after last 10-K
    # -----------------------------
    if last_k_year is not None:
        post_k_dates = [q_end for q_end in q_dates_sorted if q_end.year > last_k_year]
        for q_end in post_k_dates:
            base_tags = series_10q[q_end].index
            df_post = pd.DataFrame(index=base_tags)
            df_post[f"{q_end.year}_{q_end.month:02d}"] = series_10q[q_end].reindex(base_tags)
            out_file = os.path.join(output_folder, f"{ticker}_{q_end.year}_{q_end.month:02d}_post10K.csv")
            df_post.to_csv(out_file)
            print(f"✅ Saved post-10K filing: {out_file}")

# -----------------------------
# MAIN
# -----------------------------
def main():
    tickers = [d for d in os.listdir(DATA_FOLDER) if os.path.isdir(os.path.join(DATA_FOLDER, d))]
    for ticker in tickers:
        try:
            build_fiscal_year_wide_local(ticker)
        except Exception as e:
            print(f"❌ Failed processing {ticker}: {e}")

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    main()
