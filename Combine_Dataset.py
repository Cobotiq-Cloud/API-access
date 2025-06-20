#!/usr/bin/env python3
"""
combine_reports.py

Reads gausium.csv and pudu.csv, drops any tz info on Start DateTime,
filters by your cutoffs, and writes combined.csv.
"""

import pandas as pd
from datetime import datetime

def load_and_filter(path: str, cutoff: datetime) -> pd.DataFrame:
    # parse Start DateTime, then drop any timezone so comparisons work
    df = pd.read_csv(path, parse_dates=['Start DateTime'])
    # if tz-aware, remove tzinfo
    if df['Start DateTime'].dt.tz is not None:
        df['Start DateTime'] = df['Start DateTime'].dt.tz_localize(None)
    return df[df['Start DateTime'] > cutoff].copy()

def combine_datasets(
    gaussian_path: str,
    pudu_path: str,
    output_path: str
) -> None:
    # your cutoff dates (naive datetimes)
    gaussian_cutoff = datetime(2025, 4, 22)
    pudu_cutoff     = datetime(2025, 5, 27)
    
    df_g = load_and_filter(gaussian_path, gaussian_cutoff)
    df_p = load_and_filter(pudu_path, pudu_cutoff)
    
    combined = pd.concat([df_g, df_p], ignore_index=True)
    combined.to_csv(output_path, index=False)
    print(f"Saved {len(df_g)} Gaussian rows + {len(df_p)} Pudu rows → {output_path}")

if __name__ == "__main__":
    combine_datasets(
        'gausium.csv',   # your actual Gaussian-export filename
        'pudu.csv',
        'combined.csv'
    )
