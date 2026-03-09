import requests
import pandas as pd
from Concept_List import concept_keywords  # <-- import driver concept list

# -----------------------------
# CONFIGURATION
# -----------------------------
USER_EMAIL = "mustafadal2004@proton.me"  # SEC requires identification

# -----------------------------
# SEC DATA FUNCTIONS
# -----------------------------
def get_cik(ticker, user_email=USER_EMAIL):
    headers = {"User-Agent": user_email}
    tickers_url = "https://www.sec.gov/files/company_tickers.json"
    tickers = requests.get(tickers_url, headers=headers).json()
    tickers_df = pd.DataFrame.from_dict(tickers, orient="index")
    tickers_df["cik_str"] = tickers_df["cik_str"].astype(str).str.zfill(10)
    if ticker not in tickers_df['ticker'].values:
        raise ValueError(f"Ticker '{ticker}' not found in SEC database.")
    return tickers_df[tickers_df["ticker"] == ticker]["cik_str"].values[0]

def get_feature_list(cik, standard="us-gaap", user_email=USER_EMAIL):
    headers = {"User-Agent": user_email}
    facts_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    facts = requests.get(facts_url, headers=headers).json()
    return facts.get("facts", {}).get(standard, {})

def get_dataframe(ticker, standard="us-gaap", user_email=USER_EMAIL):
    cik = get_cik(ticker, user_email)
    print(f"CIK for {ticker}: {cik}")
    tags = get_feature_list(cik, standard, user_email)
    print(f"Total {standard} tags found: {len(tags)}")
    
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
                    "frame": item.get("frame"),
                    "filed": item.get("filed")
                })
    df = pd.DataFrame(records)
    df["end"] = pd.to_datetime(df["end"], errors="coerce")
    df["filed"] = pd.to_datetime(df["filed"], errors="coerce")
    df = df[df["filed"] != df["end"]]
    df = df.sort_values(["tag", "end"]).drop_duplicates(subset=["tag", "end"], keep="last")
    return df

# -----------------------------
# DATA CLEANING & PRIORITIZATION
# -----------------------------
def prioritize_filings(df_input):
    df_copy = df_input.copy()
    form_priority = {"10-Q":1, "10-K":2, "10-K/A":3}
    df_copy["form_priority"] = df_copy["form"].map(form_priority).fillna(0)
    df_copy = df_copy.sort_values(["end","form_priority","filed"], ascending=[True,False,False])
    df_copy = df_copy.drop_duplicates(subset=["tag","end"], keep="first")
    return df_copy.drop(columns=["form_priority"])

def get_concept_list(df_input):
    return sorted(df_input["tag"].unique())

def detect_tags(df_input, concept_keywords):
    tag_mapping = {}
    for concept, keywords in concept_keywords.items():
        matches = [tag for tag in df_input["tag"].unique()
                   if any(keyword.lower() in tag.lower() for keyword in keywords)]
        if matches:
            best_tag = df_input[df_input["tag"].isin(matches)].groupby("tag")["value"].count().idxmax()
            tag_mapping[concept] = best_tag
        else:
            tag_mapping[concept] = None
    return tag_mapping

def filter_and_pivot(df_input, tag_mapping):
    rows = []
    for concept, tag in tag_mapping.items():
        if tag is not None:
            temp = df_input[df_input["tag"] == tag][["end","value"]].copy()
            temp["concept"] = concept
            rows.append(temp)
    if not rows:
        return pd.DataFrame()
    df_filtered = pd.concat(rows, ignore_index=True)
    df_pivot = df_filtered.pivot_table(index="concept", columns="end", values="value", aggfunc="first")
    df_pivot = df_pivot.sort_index(axis=1)
    return df_pivot

def save_pivot(df_pivot, output_file):
    df_pivot.to_excel(output_file)
    print(f"✅ Saved pivoted driver concepts to {output_file}")

# -----------------------------
# HELPER FUNCTIONS TO GET SPECIFIC FILINGS
# -----------------------------
def get_10k_year(df, year):
    df_10k = df[df["form"].str.contains("10-K", na=False)].copy()
    df_10k = df_10k[df_10k["end"].dt.year == year]
    return prioritize_filings(df_10k)

def get_10q_quarter(df, year, quarter):
    df_10q = df[df["form"].str.contains("10-Q", na=False)].copy()
    quarter_months = {1:[3], 2:[6], 3:[9], 4:[12]}
    df_10q = df_10q[df_10q["end"].dt.year == year]
    df_10q = df_10q[df_10q["end"].dt.month.isin(quarter_months[quarter])]
    return prioritize_filings(df_10q)

# -----------------------------
# MAIN EXECUTION
# -----------------------------
# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    TICKER = "AMZN"
    df = get_dataframe(TICKER)
    df = prioritize_filings(df)

    # All concept list
    all_concepts = get_concept_list(df)
    print(f"Total concepts found: {len(all_concepts)}")

    # Detect driver tags
    tags_mapping = detect_tags(df, concept_keywords)

    # Pivot driver concepts
    driver_df = filter_and_pivot(df, tags_mapping)

    # Save pivoted driver concepts
    save_pivot(driver_df, f"{TICKER}_driver_concepts_pivot.xlsx")

    # -----------------------------
    # Extract specific filings
    # -----------------------------
    k2025 = get_10k_year(df, 2025)
    q1_2025 = get_10q_quarter(df, 2025, 1)

    # Save these specific filings to CSV
    if not k2025.empty:
        k2025.to_csv(f"{TICKER}_10K_2025.csv", index=False)
        print(f"✅ Saved 10-K 2025 to {TICKER}_10K_2025.csv")
    else:
        print("⚠️ No 10-K found for 2025")

    if not q1_2025.empty:
        q1_2025.to_csv(f"{TICKER}_10Q_2025_Q1.csv", index=False)
        print(f"✅ Saved 10-Q 2025 Q1 to {TICKER}_10Q_2025_Q1.csv")
    else:
        print("⚠️ No 10-Q found for 2025 Q1")