import os
import re
import requests
import pandas as pd
import shutil

# -----------------------------
# CONFIG
# -----------------------------
USER_EMAIL = "ENTER EMAIL HERE"
HEADERS = {"User-Agent": USER_EMAIL}

DATA_FOLDER = "DATA"
OUTPUT_FOLDER = "DATA_YR"
MIN_ROWS_FULL_REPORT = 40
DUPLICATE_TAG_THRESHOLD = 0.95

os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -----------------------------
# HELPERS (UNCHANGED)
# -----------------------------
def sanitize(name):
    return re.sub(r"[^A-Za-z0-9_]", "_", name)

def ensure_folder(path):
    os.makedirs(path, exist_ok=True)

def get_cik(ticker):
    url = "https://www.sec.gov/files/company_tickers.json"
    data = requests.get(url, headers=HEADERS).json()
    df = pd.DataFrame.from_dict(data, orient="index")
    df["cik_str"] = df["cik_str"].astype(str).str.zfill(10)
    ticker = ticker.upper()
    if ticker not in df["ticker"].values:
        raise ValueError(f"{ticker} not found")
    return df[df["ticker"] == ticker]["cik_str"].values[0]

def download_filings(cik):
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    data = requests.get(url, headers=HEADERS).json()
    facts = data.get("facts", {}).get("us-gaap", {})
    records = []
    for tag, tag_data in facts.items():
        for unit, values in tag_data.get("units", {}).items():
            for item in values:
                records.append({
                    "tag": tag,
                    "unit": unit,
                    "value": item.get("val"),
                    "start": item.get("start"),
                    "end": item.get("end"),
                    "form": item.get("form"),
                    "filed": item.get("filed")
                })
    df = pd.DataFrame(records)
    for col in ["start", "end", "filed"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.dropna(subset=["end"])

def is_partial(df, prev_tags=None):
    if len(df) < MIN_ROWS_FULL_REPORT:
        return True
    if prev_tags is not None:
        tags = set(df["tag"].dropna())
        if not tags:
            return True
        ratio = len(tags & prev_tags) / len(tags)
        if ratio >= DUPLICATE_TAG_THRESHOLD:
            return True
    return False

def move_file(file_path, ticker, form_type):
    folder = os.path.join(DATA_FOLDER, ticker, sanitize(form_type))
    ensure_folder(folder)
    target_path = os.path.join(folder, os.path.basename(file_path))
    shutil.move(file_path, target_path)
    print(f"📦 Moved {file_path} → {folder}")

# -----------------------------
# KA/QA PROCESSOR
# -----------------------------
def process_ticker_ka_qa(ticker):
    ticker = ticker.upper()
    print(f"\n🚀 Processing {ticker}")

    try:
        cik = get_cik(ticker)
        df = download_filings(cik)
        if df.empty:
            print("⚠️ No filings found")
            return
    except Exception as e:
        print(f"❌ Error: {e}")
        return

    base = os.path.join(DATA_FOLDER, ticker)
    ensure_folder(base)

    # Save by form
    for form_type in df["form"].dropna().unique():
        folder = os.path.join(base, sanitize(form_type))
        ensure_folder(folder)
        df_form = df[df["form"] == form_type]
        for end_date in df_form["end"].drop_duplicates():
            file_path = os.path.join(folder, f"{ticker}_{sanitize(form_type)}_{end_date.date()}.csv")
            ensure_folder(os.path.dirname(file_path))
            df_form[df_form["end"] == end_date].to_csv(file_path, index=False)
            print(f"✅ Saved {file_path}")

    # Move partial/amendments (10-K/A, 10-Q/A)
    for form in ["10-K", "10-Q"]:
        folder = os.path.join(base, sanitize(form))
        if not os.path.exists(folder):
            continue
        files = sorted(f for f in os.listdir(folder) if f.endswith(".csv"))
        end_tags_map = {}
        for file in files:
            path = os.path.join(folder, file)
            try:
                df_file = pd.read_csv(path)
                df_file["end"] = pd.to_datetime(df_file["end"], errors="coerce")
                df_file = df_file.dropna(subset=["end"])
                if df_file.empty:
                    continue
                moved = False
                for end_date in df_file["end"].unique():
                    prev_tags = end_tags_map.get(end_date)
                    if is_partial(df_file[df_file["end"] == end_date], prev_tags):
                        move_file(path, ticker, form + "_A")
                        moved = True
                        break
                    end_tags_map[end_date] = set(df_file[df_file["end"] == end_date]["tag"].dropna())
                if not moved:
                    print(f"✅ Kept {path}")
            except Exception as e:
                print(f"⚠️ Failed {path}: {e}")

    print(f"🎉 Done KA/QA processing for {ticker}")

# -----------------------------
# FISCAL YEAR WIDE BUILDER
# -----------------------------
def build_fiscal_year_wide(ticker):
    ticker = ticker.upper()
    print(f"\n📥 Building fiscal-year-wide data for {ticker}")
    output_folder = os.path.join(OUTPUT_FOLDER, ticker)
    ensure_folder(output_folder)

    # Helper to load filings
    def load_series(form):
        folder = os.path.join(DATA_FOLDER, ticker, sanitize(form))
        if not os.path.exists(folder):
            return {}
        series_dict = {}
        for file in os.listdir(folder):
            if not file.endswith(".csv"):
                continue
            path = os.path.join(folder, file)
            df = pd.read_csv(path)
            df["end"] = pd.to_datetime(df["end"], errors="coerce")
            df = df.dropna(subset=["end"])
            for end_date in df["end"].unique():
                s = df[df["end"] == end_date][["tag","value"]].drop_duplicates("tag")
                s.set_index("tag", inplace=True)
                s = pd.to_numeric(s["value"], errors="coerce")
                series_dict[end_date] = s
        return series_dict

    series_10k = load_series("10-K")
    series_10q = load_series("10-Q")

    k_dates = sorted(series_10k.keys())
    q_dates_sorted = sorted(series_10q.keys())

    last_k_date = k_dates[-1] if k_dates else pd.Timestamp.min

    # -----------------------------
    # Build fiscal-year-wide CSVs for years that have 10-K
    # -----------------------------
    for i, fy_end in enumerate(k_dates):
        prev_k = k_dates[i-1] if i > 0 else pd.Timestamp.min
        base_tags = series_10k[fy_end].index
        df_wide = pd.DataFrame(index=base_tags)

        for q_end in q_dates_sorted:
            if prev_k < q_end < fy_end:
                df_wide[f"{q_end.year}_{q_end.month:02d}"] = series_10q[q_end].reindex(base_tags)

        df_wide[f"{fy_end.year}_10K"] = series_10k[fy_end].reindex(base_tags)

        out_file = os.path.join(output_folder, f"{ticker}_{fy_end.year}_wide.csv")
        df_wide.to_csv(out_file)
        print(f"✅ Saved fiscal year file: {out_file}")

    # -----------------------------
    # Build pending 10-Qs for filings after the last 10-K
    # -----------------------------
    pending_q_dates = [q_end for q_end in q_dates_sorted if q_end > last_k_date]
    if pending_q_dates:
        base_tags = series_10q[pending_q_dates[0]].index
        df_pending = pd.DataFrame(index=base_tags)
        for q_end in sorted(pending_q_dates):
            df_pending[f"{q_end.year}_{q_end.month:02d}"] = series_10q[q_end].reindex(base_tags)

        pending_file = os.path.join(output_folder, f"{ticker}_pending_10Q_post_last_10K.csv")
        df_pending.to_csv(pending_file)
        print(f"✅ Saved pending 10-Q filings after last 10-K: {pending_file}")
    else:
        print(f"ℹ️ No new 10-Q filings after last 10-K for {ticker}")

# -----------------------------
# RUN FOR ALL SP500 TICKERS
# -----------------------------
if __name__ == "__main__":
    # Load tickers from CSV
    sp500_file = "sp500_yfin.csv"
    if not os.path.exists(sp500_file):
        raise FileNotFoundError(f"{sp500_file} not found")

    df_sp500 = pd.read_csv(sp500_file)
    if "Ticker" not in df_sp500.columns:
        raise ValueError("CSV must have a column named 'Ticker'")

    tickers = df_sp500["Ticker"].dropna().unique()

    for ticker in tickers:
        try:
            process_ticker_ka_qa(ticker)
            build_fiscal_year_wide(ticker)
        except Exception as e:
            print(f"❌ Failed processing {ticker}: {e}")
