"""
03_build_continuous_report.py
=============================

WHAT THIS SCRIPT DOES:
    Combines two things to produce a "Continuous Report":
       (a) The operator's TRIP REPORT — when each trip happened
           (Vessel, Arrived At, Completed At)
       (b) The AIS ARCHIVE — every position the recorder captured

    For each trip in the report, it pulls all AIS pings for that vessel
    that fall inside [Arrived At, Completed At] and saves them in the
    SAME column format as SeaCabbie's Continuous Report.

WHY WE NEED IT:
    SeaCabbie supplied us with both files (Trip + Continuous). York
    Launch only gave us the Trip Report. The Continuous Report is what
    Estee's Flexsim code consumes, so we need to manufacture one for
    York Launch (and any other operator) using AIS as the data source.

USAGE:
    python 03_build_continuous_report.py --operator yorklaunch
    python 03_build_continuous_report.py --operator yorklaunch --by-vessel

OUTPUT:
    output/<operator>_continuous_report.xlsx        (combined)
    output/<operator>_<vessel>_continuous.xlsx      (one per vessel)

NOTES:
    - Times in the Trip Report are assumed to be Singapore local time
      (SGT, UTC+8). AIS timestamps are UTC. We convert AIS to SGT
      before matching, so the two line up.

    - "Ignition" is approximated from speed. AIS doesn't broadcast
      ignition state. Speed > 0.3 knots = "Ignition ON".

    - "Address" is left blank. AIS provides only lat/lon. If you want
      addresses later, we can hook in a free reverse-geocoding service.
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import config


# Threshold below which we consider the vessel "stopped" for ignition
STOP_SPEED_KNOTS = 0.3


# ---------------------------------------------------------------------------
# LOADING
# ---------------------------------------------------------------------------
def load_trip_report(path: Path) -> pd.DataFrame:
    """Read the Trip Report and standardise column names + datetime parsing."""
    if not path.exists():
        sys.exit(f"\nTrip Report not found: {path}\n")

    # Pandas can read both .xlsx and .csv. Check the extension.
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)

    # Standardise vessel name column
    for candidate in ["Vessel Name", "Vessel", "Name"]:
        if candidate in df.columns:
            df = df.rename(columns={candidate: "Vessel Name"})
            break
    else:
        sys.exit(f"No vessel name column. Found: {list(df.columns)}")

    # Make sure we have arrival/completion times
    for col in ["Arrived At", "Completed At"]:
        if col not in df.columns:
            sys.exit(f"Trip Report is missing '{col}' column.")
        # dayfirst=True handles DD/MM/YYYY (Singapore convention)
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    # Drop any rows with missing key data
    df = df.dropna(subset=["Vessel Name", "Arrived At", "Completed At"])
    return df


def load_mmsi_lookup(path: Path) -> dict:
    """
    Load the MMSI lookup created by 01_resolve_mmsi.py.
    Returns a dict: {vessel_name: mmsi}
    """
    if not path.exists():
        sys.exit(
            f"\nMMSI lookup not found: {path}\n"
            f"Run first: python 01_resolve_mmsi.py --operator <key>\n"
        )
    df = pd.read_csv(path)
    df = df.dropna(subset=["mmsi"])
    return {row.vessel_name: int(row.mmsi) for row in df.itertuples()}


def load_archive_for_window(start: pd.Timestamp,
                            end: pd.Timestamp,
                            archive_dir: Path) -> pd.DataFrame:
    """
    Load all AIS archive files whose date overlaps [start, end].
    Concatenates them into one big DataFrame.

    Each file covers one UTC day. We load one extra day on each side to
    catch trips that straddle midnight UTC.
    """
    # Range of days to load (slightly wider than the trip window)
    start_d = (start - timedelta(days=1)).date()
    end_d   = (end + timedelta(days=1)).date()

    dfs = []
    day = start_d
    while day <= end_d:
        path = archive_dir / f"ais_{day.isoformat()}.csv"
        if path.exists():
            try:
                dfs.append(pd.read_csv(path))
            except Exception as e:
                print(f"  Skipping corrupt file {path.name}: {e}")
        day += timedelta(days=1)

    if not dfs:
        return pd.DataFrame()

    # Stack the daily files into one big table
    df = pd.concat(dfs, ignore_index=True)

    # Parse the UTC timestamps. Pandas usually figures out the format.
    df["timestamp_utc"] = pd.to_datetime(
        df["timestamp_utc"], utc=True, errors="coerce"
    )
    df = df.dropna(subset=["timestamp_utc", "latitude", "longitude"])

    # Convert UTC to Singapore time. The Trip Report is in SGT, so we
    # need to compare like-with-like.
    # tz_convert turns the timezone label without changing the underlying
    # moment in time. tz_localize(None) drops the timezone label entirely
    # so pandas can compare with the trip report's tz-naive timestamps.
    df["timestamp_sgt"] = (
        df["timestamp_utc"]
        .dt.tz_convert("Asia/Singapore")
        .dt.tz_localize(None)
    )
    return df


# ---------------------------------------------------------------------------
# BUILD ONE TRIP'S WORTH OF CONTINUOUS-REPORT ROWS
# ---------------------------------------------------------------------------
def build_continuous_for_trip(trip: pd.Series,
                              mmsi: int,
                              archive: pd.DataFrame) -> pd.DataFrame:
    """
    For one trip, return the AIS rows that fall inside its time window,
    formatted to match the SeaCabbie Continuous Report layout.
    """
    s, e = trip["Arrived At"], trip["Completed At"]
    if archive.empty:
        return pd.DataFrame()

    # Filter the archive to this MMSI within the trip's time window.
    mask = (
        (archive["mmsi"] == mmsi)
        & (archive["timestamp_sgt"] >= s)
        & (archive["timestamp_sgt"] <= e)
    )
    seg = archive.loc[mask].sort_values("timestamp_sgt")
    if seg.empty:
        return pd.DataFrame()

    # Build a new DataFrame in the exact column order of the
    # Continuous Report. Each row is one AIS ping inside the trip window.
    out = pd.DataFrame({
        "Date":      seg["timestamp_sgt"].dt.strftime("%d/%m/%Y"),
        "Time":      seg["timestamp_sgt"].dt.strftime("%H:%M:%S"),
        # Speed-based ignition heuristic
        "Ignition":  seg["sog_knots"].fillna(0).apply(
                         lambda x: "Ignition ON"
                                   if x > STOP_SPEED_KNOTS
                                   else "Ignition OFF"),
        "Speed":     seg["sog_knots"].fillna(0).round(2),
        "Heading":   seg["heading_deg"].fillna(0).astype(int),
        "Latitude":  seg["latitude"].round(6),
        "Longitude": seg["longitude"].round(6),
        "Address":   "",
        "Vessel":    trip["Vessel Name"],
        "MMSI":      mmsi,
    })
    return out[config.CONTINUOUS_REPORT_COLUMNS]


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--operator", required=True,
                        help="Operator key (e.g. yorklaunch)")
    parser.add_argument("--by-vessel", action="store_true",
                        help="Also write one file per vessel "
                             "(matches SeaCabbie workflow)")
    args = parser.parse_args()

    op_dir = config.OPERATORS_DIR / args.operator
    trip_path = op_dir / "trip_report.xlsx"
    mmsi_path = op_dir / "vessel_mmsi.csv"

    print(f"Operator: {args.operator}")
    print("Loading trip report and MMSI lookup...")
    trip_df = load_trip_report(trip_path)
    mmsi_lookup = load_mmsi_lookup(mmsi_path)

    # Warn about vessels in the trip report that we can't look up
    missing = [n for n in trip_df["Vessel Name"].unique()
               if n not in mmsi_lookup]
    if missing:
        print(f"\nWARNING: {len(missing)} vessel(s) have no MMSI "
              f"and will be skipped:")
        for n in missing:
            print(f"  - {n}")

    # Load only the archive files we'll need (faster, less memory)
    span_start = trip_df["Arrived At"].min()
    span_end   = trip_df["Completed At"].max()
    print(f"\nTrip report window: {span_start} -> {span_end}")
    print("Loading AIS archive...")
    archive = load_archive_for_window(
        span_start, span_end, config.ARCHIVE_DIR
    )
    if archive.empty:
        sys.exit(
            "\nNo AIS archive data covers the trip report window.\n"
            "Run the recorder for a while first:\n"
            f"  python 02_record_ais.py --operator {args.operator}\n"
        )
    print(f"  {len(archive):,} AIS pings loaded "
          f"({archive['mmsi'].nunique()} unique vessels in archive)")

    # Loop through every trip and build its continuous-report rows.
    all_rows = []
    per_vessel = {}
    n_matched = 0

    for _, trip in trip_df.iterrows():
        name = trip["Vessel Name"]
        mmsi = mmsi_lookup.get(name)
        if mmsi is None:
            continue
        rows = build_continuous_for_trip(trip, mmsi, archive)
        if rows.empty:
            continue
        n_matched += 1
        all_rows.append(rows)
        per_vessel.setdefault(name, []).append(rows)

    if not all_rows:
        sys.exit(
            "\nNo AIS data matched any trip in the report.\n"
            "Either the archive doesn't cover the trip dates, or the\n"
            "vessels' MMSIs weren't broadcast during those windows.\n"
        )

    # Combine all the per-trip chunks into one big DataFrame
    combined = pd.concat(all_rows, ignore_index=True)

    # Remove any duplicates (same vessel + timestamp twice)
    combined = (
        combined
        .drop_duplicates(subset=["MMSI", "Date", "Time"])
        .sort_values(["Vessel", "Date", "Time"])
    )

    # Save the combined file
    combined_path = config.OUTPUT_DIR / f"{args.operator}_continuous_report.xlsx"
    combined.to_excel(combined_path, index=False)
    print(f"\nWrote combined report: {combined_path.name}")
    print(f"  {len(combined):,} rows, {n_matched} trip(s) matched.")

    # Optionally save one file per vessel (matches SeaCabbie workflow)
    if args.by_vessel:
        for name, chunks in per_vessel.items():
            vdf = pd.concat(chunks, ignore_index=True)
            vdf = (
                vdf
                .drop_duplicates(subset=["MMSI", "Date", "Time"])
                .sort_values(["Date", "Time"])
            )
            # Sanitize vessel name to a safe filename
            safe = "".join(c if c.isalnum() else "_" for c in name)
            vpath = config.OUTPUT_DIR / f"{args.operator}_{safe}_continuous.xlsx"
            vdf.to_excel(vpath, index=False)
            print(f"  {name:30s} -> {vpath.name}  ({len(vdf):,} rows)")

    print("\nNext step:")
    print(f"  Feed {combined_path.name} into Estee's "
          "flexsim_input_simulation.py")


if __name__ == "__main__":
    main()
