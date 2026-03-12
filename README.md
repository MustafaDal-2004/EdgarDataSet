# EdgarDataSet

Python3 scripts to build and maintain your own SEC Edgar financial dataset.

## Overview

This project allows you to download, organize, and update historical filings for a list of tickers (e.g., S&P 500) directly from the SEC Edgar database.

## Getting Started

1. Open the `SetBuild.py` script.
2. Enter your email in the **config section** (used for SEC request headers).
3. Ensure you have a CSV of tickers named `sp500_yfin.csv`.
4. Run `SetBuild.py` to download filings for the specified tickers, covering the years 2015–2025.

## Updating Your Dataset

- Use `SetUpdate.py` to scan filings for 2026 (or any new year) and update your existing dataset.
- You can run updates as frequently as you like to keep your dataset current.

## Data Storage

- By default, all data is saved as CSVs.
- If preferred, you can modify the scripts (`SetBuild.py` and `SetUpdate.py`) to save in Parquet format for faster reads/writes.

## Requirements

- Python 3
- Packages: `pandas`, `requests`, `os`

## Notes

- This project is intended for research purposes.
- The dataset reflects SEC filings but may **not be 100% accurate** due to inconsistencies in filings or tag variations.
