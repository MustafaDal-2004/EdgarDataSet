import os
import requests
import pandas as pd
import shutil

# -----------------------------
# CONFIG
# -----------------------------
TICKER_CSV = "sp500_yfin.csv"
DATA_FOLDER = "DATA"
USER_EMAIL = "mustafadal2004@proton.me"
HEADERS = {"User-Agent": USER_EMAIL}

OUTPUT_FOLDER = "DATA_YR"
FILING_10Q = "10-Q"
FILING_10K = "10-K"

DATA_FOLDER = "DATA"  # Base folder where tickers are stored
MIN_ROWS_FULL_REPORT = 40  # Updated minimum rows
DUPLICATE_TAG_THRESHOLD = 0.95

os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -----------------------------
# HELPERS
# -----------------------------
def ensure_folder(path):
    os.makedirs(path, exist_ok=True)

def get_cik(ticker):
    """Fetch CIK from SEC ticker list."""
    url = "https://www.sec.gov/files/company_tickers.json"
    data = requests.get(url, headers=HEADERS).json()
    df = pd.DataFrame.from_dict(data, orient="index")
    df["cik_str"] = df["cik_str"].astype(str).str.zfill(10)
    ticker = ticker.upper()
    if ticker not in df["ticker"].values:
        raise ValueError(f"{ticker} not found in SEC ticker list")
    return df[df["ticker"] == ticker]["cik_str"].values[0]

def get_feature_list(cik, standard="us-gaap"):
    """Fetch all XBRL facts for a given CIK."""
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    data = requests.get(url, headers=HEADERS).json()
    return data.get("facts", {}).get(standard, {})

def get_dataframe(ticker):
    """Fetch all filings for a ticker."""
    cik = get_cik(ticker)
    tags = get_feature_list(cik)
    records = []
    for tag, tag_data in tags.items():
        for unit, values in tag_data.get("units", {}).items():
            for item in values:
                records.append({
                    "tag": tag,
                    "unit": unit,
                    "value": item.get("val"),
                    "start": item.get("start"),
                    "end": item.get("end"),
                    "form": item.get("form"),
                    "filed": item.get("filed"),
                    "frame": item.get("frame")
                })
    df = pd.DataFrame(records)
    if df.empty:
        return df
    for col in ["start", "end", "filed"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.dropna(subset=["end"])

def save_filings(df, ticker):
    """Save all filings into folders by form type."""
    base_folder = os.path.join(DATA_FOLDER, ticker)
    ensure_folder(base_folder)
    forms = df["form"].dropna().unique()
    for form_type in forms:
        form_folder = os.path.join(base_folder, form_type.replace("/", "_"))
        ensure_folder(form_folder)
        df_form = df[df["form"] == form_type]
        for end_date in df_form["end"].drop_duplicates():
            file_name = f"{ticker}_{form_type}_{end_date.date()}.csv"
            file_path = os.path.join(form_folder, file_name)
            df_end = df_form[df_form["end"] == end_date]
            df_end.to_csv(file_path, index=False)
            print(f"✅ Saved {file_path}")

def move_to_amendment(file_path, ticker, form_type):
    """Move file into amendment folder."""
    ka_folder = os.path.join(DATA_FOLDER, ticker, f"{form_type}A")
    os.makedirs(ka_folder, exist_ok=True)
    target_path = os.path.join(ka_folder, os.path.basename(file_path))
    shutil.move(file_path, target_path)
    print(f"📦 Moved {file_path} → {form_type}A folder")

def is_partial_or_amendment(df, previous_tags_set=None):
    """Determine if filing is partial or amendment."""
    if len(df) < MIN_ROWS_FULL_REPORT:
        return True
    if previous_tags_set is not None:
        tags = set(df["tag"].dropna())
        if len(tags) == 0:
            return True
        duplicate_ratio = len(tags & previous_tags_set) / len(tags)
        if duplicate_ratio >= DUPLICATE_TAG_THRESHOLD:
            return True
    return False

def process_ticker(ticker):
    print(f"\n🔹 Processing {ticker}")

    # Process 10-Qs normally
    for form_type in ["10-Q"]:
        folder = os.path.join(DATA_FOLDER, ticker, form_type)
        if not os.path.exists(folder):
            print(f"⚠️ Folder not found: {folder}")
            continue

        files = sorted([f for f in os.listdir(folder) if f.endswith(".csv")])
        if not files:
            print(f"⚠️ No CSVs in {folder}")
            continue

        end_tags_map = {}
        for file in files:
            file_path = os.path.join(folder, file)
            try:
                df = pd.read_csv(file_path)
                if "end" not in df.columns or "tag" not in df.columns:
                    continue
                df["end"] = pd.to_datetime(df["end"], errors="coerce")
                df = df.dropna(subset=["end"])
                if df.empty:
                    continue

                moved = False
                for end_date in df["end"].unique():
                    df_end = df[df["end"] == end_date]
                    prev_tags = end_tags_map.get(end_date)
                    if is_partial_or_amendment(df_end, prev_tags):
                        move_to_amendment(file_path, ticker, form_type.replace("-", ""))
                        moved = True
                        break
                    end_tags_map[end_date] = set(df_end["tag"].dropna())

                if not moved:
                    print(f"✅ Kept {file_path}")
            except Exception as e:
                print(f"⚠️ Failed to read {file_path}: {e}")

    # Process 10-Ks: only keep fiscal-year-end filings
    form_type = "10-K"
    folder = os.path.join(DATA_FOLDER, ticker, form_type)
    if not os.path.exists(folder):
        print(f"⚠️ Folder not found: {folder}")
        return

    files = sorted([f for f in os.listdir(folder) if f.endswith(".csv")])
    if not files:
        print(f"⚠️ No CSVs in {folder}")
        return

    # Map end-of-year filings
    fy_to_files = {}
    for file in files:
        file_path = os.path.join(folder, file)
        try:
            df = pd.read_csv(file_path)
            if "end" not in df.columns or "tag" not in df.columns:
                continue
            df["end"] = pd.to_datetime(df["end"], errors="coerce")
            df = df.dropna(subset=["end"])
            if df.empty or len(df) < MIN_ROWS_FULL_REPORT:
                move_to_amendment(file_path, ticker, form_type.replace("-", ""))
                continue

            fy = df["end"].dt.year.iloc[0]
            fy_to_files.setdefault(fy, []).append((df["end"].max(), file_path))
        except Exception as e:
            print(f"⚠️ Failed to read {file_path}: {e}")

    # Keep only last 10-K per fiscal year
    for fy, files_list in fy_to_files.items():
        files_list.sort()  # sort by end date
        for _, file_path in files_list[:-1]:  # move all but last
            move_to_amendment(file_path, ticker, form_type.replace("-", ""))
        print(f"✅ Kept fiscal-year-end 10-K for {ticker} {fy}")

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
        # Precompute Series for each unique end
        for end_date in df["end"].unique():
            s = clean_series(df[df["end"] == end_date])
            series_dict[end_date] = s
    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
    else:
        full_df = pd.DataFrame()
    return series_dict, full_df

def build_fiscal_year_wide(ticker):
    print(f"\n📥 Processing {ticker}")
    output_folder = os.path.join(OUTPUT_FOLDER, ticker)
    os.makedirs(output_folder, exist_ok=True)

    # Load filings
    series_10q, df_10q = load_filings(os.path.join(DATA_FOLDER, ticker, FILING_10Q))
    series_10k, df_10k = load_filings(os.path.join(DATA_FOLDER, ticker, FILING_10K))

    if not series_10q and not series_10k:
        print(f"⚠️ No filings found for {ticker}")
        return

    # Sort dates ascending
    k_dates = sorted(series_10k.keys())
    q_dates_sorted = sorted(series_10q.keys())

    last_k_year = None  # Track last 10-K year for post-10K filings

    # -----------------------------
    # Build fiscal year wide files
    # -----------------------------
    for i, fy_end in enumerate(k_dates):
        prev_k = k_dates[i - 1] if i > 0 else pd.Timestamp.min
        last_k_year = fy_end.year  # update last K year

        base_tags = series_10k[fy_end].index
        df_wide = pd.DataFrame(index=base_tags)

        # Add all 10-Qs between previous 10-K and current 10-K
        for q_end in q_dates_sorted:
            if q_end <= prev_k:
                continue
            if q_end > fy_end:
                continue
            if q_end == fy_end:
                continue  # skip Q that matches 10-K
            s_q = series_10q[q_end].reindex(base_tags)
            col_name = f"{q_end.year}_{q_end.month:02d}"
            df_wide[col_name] = s_q

        # Add 10-K as last column
        df_wide[f"{fy_end.year}_10K"] = series_10k[fy_end].reindex(base_tags)

        # Save if data exists
        if df_wide.dropna(how="all").shape[1] > 0:
            out_file = os.path.join(output_folder, f"{ticker}_{fy_end.year}_wide.csv")
            df_wide.to_csv(out_file)
            print(f"✅ Saved {out_file}")
        else:
            print(f"⚠️ No data to save for fiscal year {fy_end.year} for ticker {ticker}")

    # -----------------------------
    # Capture all filings after last 10-K
    # -----------------------------
    if last_k_year is not None:
        post_k_dates = [q_end for q_end in q_dates_sorted if q_end.year > last_k_year]

        for q_end in post_k_dates:
            base_tags = series_10q[q_end].index
            df_post = pd.DataFrame(index=base_tags)
            df_post[f"{q_end.year}_{q_end.month:02d}"] = series_10q[q_end].reindex(base_tags)

            # Save post-10K file
            out_file = os.path.join(output_folder, f"{ticker}_{q_end.year}_{q_end.month:02d}_post10K.csv")
            df_post.to_csv(out_file)
            print(f"✅ Saved post-10K filing: {out_file}")
            
# -----------------------------
# MAIN PIPELINE
# -----------------------------
def main():

    # Load tickers
    if not os.path.exists(TICKER_CSV):
        raise FileNotFoundError(f"{TICKER_CSV} not found")

    tickers = pd.read_csv(TICKER_CSV)["Ticker"].dropna().unique()

    for ticker in tickers:
        try:
            print(f"\n==============================")
            print(f"🚀 STARTING {ticker}")
            print(f"==============================")

            # -----------------------------
            # 1. DOWNLOAD FILINGS
            # -----------------------------
            df = get_dataframe(ticker)

            if df.empty:
                print(f"⚠️ No SEC data for {ticker}")
                continue

            save_filings(df, ticker)

            # -----------------------------
            # 2. ORGANIZE FILINGS
            # -----------------------------
            process_ticker(ticker)

            # -----------------------------
            # 3. BUILD YEARLY DATASET
            # -----------------------------
            build_fiscal_year_wide(ticker)

            print(f"🎉 Completed {ticker}")

        except Exception as e:
            print(f"❌ Failed processing {ticker}: {e}")


# -----------------------------
# RUN SCRIPT
# -----------------------------
if __name__ == "__main__":
    main()
