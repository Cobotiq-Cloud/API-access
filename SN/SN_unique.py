#!/usr/bin/env python3
"""
Export all *unique* serial-number values from a CSV into a plain-text file.

↪  Example
    $ python export_sn.py 06122025_SN_Number.csv
    Wrote 4187 unique serial numbers to 06122025_SN_Number.txt
"""

from pathlib import Path
import pandas as pd

# ----------------------------- configuration ---------------------------------
# List the possible column names that might hold serial numbers in *your* data.
SN_COLUMNS = ("S/N", "Serial Number", "SN", "Serial_No")

# -----------------------------------------------------------------------------


def export_unique_sn(csv_path: str | Path,
                     txt_path: str | Path | None = None,
                     sn_columns: tuple[str, ...] = SN_COLUMNS) -> Path:
    """Read *csv_path*, find the first matching serial-number column,
    export unique values to *txt_path* (defaults to same name with .txt)."""

    csv_path = Path(csv_path)
    if txt_path is None:
        txt_path = csv_path.with_suffix(".txt")

    # Read the CSV (as strings so nothing gets converted to floats / dates)
    df = pd.read_csv(csv_path, dtype=str, engine="python")

    # Locate the column that contains the serial numbers
    for col in sn_columns:
        if col in df.columns:
            sn_col = col
            break
    else:
        raise ValueError(
            f"Could not find a serial-number column. "
            f"Tried {sn_columns}, found {list(df.columns)} instead."
        )

    # Build a de-duplicated, cleaned list
    unique_sn = (
        df[sn_col]
        .dropna()              # drop blank cells
        .astype(str)           # ensure string type
        .str.strip()           # remove leading/trailing whitespace
        .unique()              # keep only unique values
    )

    # Write them out, one per line
    with open(txt_path, "w", encoding="utf-8") as f:
        for sn in unique_sn:
            f.write(f"{sn}\n")

    print(f"Wrote {len(unique_sn)} unique serial numbers to {txt_path}")
    return txt_path


# Allow running as a script
if __name__ == "__main__":
    import sys
    # csv_file = "/Users/chenzaowen/Desktop/API-Access/API-access/SN/06122025.csv"

    # if len(sys.argv) < 2:
    #     sys.exit("Usage: python export_sn.py <path_to_csv> [output_txt]")
    csv_file = "/Users/chenzaowen/Desktop/API-Access/API-access/SN/2025_6.csv"
    txt_file = "/Users/chenzaowen/Desktop/API-Access/API-access/SN/SN_unique.txt"
    export_unique_sn(csv_file, txt_file)



"""
CLIENT_ID        = "tzOfAx4DomHbP8Qxj4Gyw2"
CLIENT_SECRET    = "33m8Ab2wj6GqGcQXQVO5qsPOKkA78H7NQaVbRidJIDeQfOIqhG4RCcVdwJw"
ACCESS_KEY_ID    = "f2a28756-6287-11f0-84ee-172017fd24fa"   
ACCESS_KEY_SECRET = "19638129da867bd1b709a52f4d8e13c5"    
"""
