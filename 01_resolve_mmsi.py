"""
01_resolve_mmsi.py
==================

WHAT THIS SCRIPT DOES:
    Reads an operator's Trip Report (a list of trips with vessel names),
    figures out the MMSI number for each unique vessel name, and saves
    a lookup table.

WHY WE NEED IT:
    AIS broadcasts identify ships by MMSI (a 9-digit unique number),
    not by name. A Trip Report uses names. To filter the AIS firehose
    down to "only the ships we care about", we need the MMSI numbers.

HOW MMSI LOOKUP WORKS:
    1. We read the Trip Report and pull out every unique vessel name.
    2. We check if we've already recorded any AIS data for those names
       (because AIS includes the ship's name in some messages). If yes,
       we auto-fill the MMSI for free.
    3. For any names we still don't have an MMSI for, we print URLs to
       vesselfinder.com and marinetraffic.com so you can look them up
       manually and type the numbers into the CSV.

USAGE:
    python 01_resolve_mmsi.py --operator yorklaunch
    python 01_resolve_mmsi.py --operator seacabbie

OUTPUT:
    operators/<operator_key>/vessel_mmsi.csv
    Columns: vessel_name, mmsi, flag (any text), notes (any text)
    You can open this in Excel and edit it directly.
"""

# Standard Python imports — these come with Python, no install needed.
import argparse                       # for parsing --operator from the command line
import sys                            # for exiting with error messages
from pathlib import Path              # for working with file paths cleanly
from urllib.parse import quote_plus   # for safely encoding spaces etc. in URLs

# pandas is THE library for working with tables of data in Python.
# Think of it like Excel, but driven by code.
import pandas as pd

# Our own settings file
import config


def get_operator_dir(operator_key: str) -> Path:
    """
    Return the folder where one operator's files live.
    Creates it if it doesn't already exist.

    Example: operator_key="yorklaunch" -> operators/yorklaunch/
    """
    d = config.OPERATORS_DIR / operator_key
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_trip_report(path: Path) -> pd.DataFrame:
    """
    Read a Trip Report file (Excel or CSV) and return it as a DataFrame
    (which is pandas's name for an in-memory table).

    Handles slight column name variations:
        "Vessel Name", "Vessel", or "Name" -> all become "Vessel Name"
    """
    # If the file doesn't exist, stop with a helpful error.
    if not path.exists():
        sys.exit(
            f"\nTrip Report not found at {path}\n"
            f"Place the operator's Trip Report there first.\n"
            f"It needs columns: {config.TRIP_REPORT_REQUIRED_COLUMNS}\n"
        )

    # Read the file. Pandas can handle both .xlsx and .csv.
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)

    # Try several possible column names for the vessel name.
    name_col = None
    for candidate in ["Vessel Name", "Vessel", "Name"]:
        if candidate in df.columns:
            name_col = candidate
            break

    if name_col is None:
        sys.exit(
            f"\nNo vessel name column found in Trip Report.\n"
            f"Looked for: 'Vessel Name', 'Vessel', or 'Name'.\n"
            f"Found these columns instead: {list(df.columns)}\n"
        )

    # Standardise the column name to "Vessel Name" so the rest of the
    # script can rely on it.
    df = df.rename(columns={name_col: "Vessel Name"})
    return df


def load_existing_lookup(path: Path) -> pd.DataFrame:
    """
    Load the existing vessel_mmsi.csv if it exists, so we don't lose
    work between runs (manually-typed MMSIs and notes are preserved).
    """
    if path.exists():
        # dtype={"mmsi": "Int64"} tells pandas to treat MMSI as an integer
        # (with support for missing values, which "Int64" allows).
        return pd.read_csv(path, dtype={"mmsi": "Int64"})

    # No existing file -> return an empty table with the right columns.
    return pd.DataFrame(columns=["vessel_name", "mmsi", "flag", "notes"])


def auto_match_from_archive(names_needing_mmsi: list,
                            archive_dir: Path) -> dict:
    """
    Look through every AIS archive file and try to match vessel names.

    HOW IT WORKS:
        AIS messages occasionally include the ship's name (in
        "ShipStaticData" messages). The recorder saves the name into
        the archive. So if we've ever seen "YL ALPHA" broadcast its
        name with MMSI 563012345, we can match those two together
        without anyone typing anything.

    Returns a dictionary: {vessel_name: mmsi}
    """
    if not archive_dir.exists():
        return {}

    matches = {}

    # Loop through every archive file (sorted so we get consistent results)
    for csv_path in sorted(archive_dir.glob("ais_*.csv")):
        try:
            # Only read the columns we need (saves memory).
            df = pd.read_csv(csv_path, usecols=["mmsi", "ship_name"])
        except Exception:
            # Skip files we can't read (maybe corrupt, maybe being written)
            continue

        # Drop rows with missing data
        df = df.dropna(subset=["ship_name", "mmsi"])

        # Standardise to UPPERCASE for matching (case-insensitive).
        df["ship_name_upper"] = (
            df["ship_name"].astype(str).str.upper().str.strip()
        )

        # For each name we still need, look for a match in this file.
        for wanted in names_needing_mmsi:
            if wanted in matches:
                continue   # already found it in an earlier file
            wanted_upper = wanted.upper().strip()
            hit = df[df["ship_name_upper"] == wanted_upper]
            if not hit.empty:
                matches[wanted] = int(hit["mmsi"].iloc[0])

    return matches


def suggest_lookup_urls(vessel_name: str) -> list:
    """
    Build URLs for the user to visit when an MMSI can't be auto-detected.
    They click the link, find the MMSI on the website, type it into the CSV.

    quote_plus turns spaces into "+" and encodes special characters.
    """
    q = quote_plus(vessel_name)
    return [
        f"https://www.vesselfinder.com/vessels?name={q}",
        f"https://www.marinetraffic.com/en/ais/index/search/all?keyword={q}",
    ]


def main():
    # ---------------------------------------------------------------
    # Parse the command-line argument: which operator?
    # ---------------------------------------------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument("--operator", required=True,
                        help="Operator key, e.g. yorklaunch, seacabbie")
    args = parser.parse_args()

    op_dir = get_operator_dir(args.operator)
    trip_path  = op_dir / "trip_report.xlsx"
    mmsi_path  = op_dir / "vessel_mmsi.csv"

    print(f"Operator: {args.operator}")
    print(f"Reading trip report: {trip_path}")
    trip_df = load_trip_report(trip_path)

    # Get the unique vessel names, sorted alphabetically so the output
    # is consistent every run.
    unique_names = (
        trip_df["Vessel Name"]
        .dropna().astype(str).str.strip()
        .drop_duplicates().sort_values().tolist()
    )
    print(f"Found {len(unique_names)} unique vessel(s) in trip report:")
    for n in unique_names:
        print(f"  - {n}")

    # ---------------------------------------------------------------
    # Reload any MMSIs we already know (from previous runs)
    # ---------------------------------------------------------------
    lookup = load_existing_lookup(mmsi_path)
    existing = {
        row.vessel_name: row.mmsi
        for row in lookup.itertuples()
        if pd.notna(row.mmsi)
    }

    # ---------------------------------------------------------------
    # Try auto-matching from the AIS archive
    # ---------------------------------------------------------------
    need_mmsi = [n for n in unique_names if n not in existing]
    if need_mmsi:
        print(f"\nAuto-matching {len(need_mmsi)} vessel(s) "
              f"against AIS archive...")
        auto = auto_match_from_archive(need_mmsi, config.ARCHIVE_DIR)
        if auto:
            for name, mmsi in auto.items():
                print(f"  MATCH  {name!r} -> MMSI {mmsi}")
            existing.update(auto)
        else:
            print("  No matches found in archive yet.")

    # ---------------------------------------------------------------
    # Build the output table
    # ---------------------------------------------------------------
    rows = []
    for name in unique_names:
        rows.append({
            "vessel_name": name,
            "mmsi": existing.get(name),   # None if still unknown
            "flag": None,
            "notes": None,
        })
    out_df = pd.DataFrame(rows)

    # Preserve any flag/notes the user typed manually in a previous run
    if not lookup.empty:
        prev = lookup.set_index("vessel_name")
        for name in out_df["vessel_name"]:
            if name in prev.index:
                # Carry over flag if we don't have one
                if pd.isna(out_df.loc[out_df["vessel_name"] == name, "flag"]).all():
                    out_df.loc[out_df["vessel_name"] == name, "flag"] = \
                        prev.at[name, "flag"]
                # Carry over notes if we don't have any
                if pd.isna(out_df.loc[out_df["vessel_name"] == name, "notes"]).all():
                    out_df.loc[out_df["vessel_name"] == name, "notes"] = \
                        prev.at[name, "notes"]

    # Save to CSV
    out_df.to_csv(mmsi_path, index=False)
    print(f"\nSaved lookup -> {mmsi_path}")

    # ---------------------------------------------------------------
    # Tell the user what's still missing
    # ---------------------------------------------------------------
    missing = out_df[out_df["mmsi"].isna()]
    if missing.empty:
        print(f"\nAll {len(unique_names)} vessels have MMSIs.")
        print(f"Ready to run: python 02_record_ais.py --operator {args.operator}")
    else:
        print(f"\n{len(missing)} vessel(s) still need an MMSI.")
        print(f"Look them up online and fill them into {mmsi_path.name}:\n")
        for name in missing["vessel_name"]:
            print(f"  {name}")
            for url in suggest_lookup_urls(name):
                print(f"    {url}")
            print()


# This "if __name__ == ..." trick means the script only runs when
# invoked directly, not when imported by another script.
if __name__ == "__main__":
    main()
