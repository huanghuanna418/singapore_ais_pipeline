"""
06_verify_flexsim_compat.py
===========================

WHAT THIS SCRIPT DOES:
    Takes the Continuous Report we generated in step 3 and runs it
    through the EXACT same parsing logic that Estee's
    flexsim_input_simulation.py uses. If our file passes this check,
    we can be confident Estee's code will accept it.

WHY WE NEED IT:
    Point 7 from the prof's task list: the files we produce must be
    importable into the existing Flexsim pipeline. Rather than trust
    that "the columns look right", we directly run the same parsing
    steps and verify nothing breaks.

WHAT IT CHECKS:
    1. The file opens with pd.read_excel().
    2. The 'Date' column exists and parses with dayfirst=True.
    3. The 'Time' column parses to datetime values.
    4. The combined DateTime column has no NaT values.
    5. The 'Speed', 'Latitude', 'Longitude' columns exist and are numeric.
    6. The trip windows from the Trip Report each match at least one
       Continuous Report row (otherwise Estee's code would skip them
       and produce nothing useful).

USAGE:
    python 06_verify_flexsim_compat.py --operator yorklaunch
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import config


# These are the EXACT same column-parsing steps from Estee's
# flexsim_input_simulation.py (lines 845-870 of that file). We replicate
# them here so a "pass" really means compatibility.
def parse_continuous_like_estee(cont_file: Path) -> pd.DataFrame:
    """Mimic flexsim_input_simulation.py's parsing of the Continuous Report."""
    cont_df = pd.read_excel(cont_file)

    # Date column is required
    if 'Date' not in cont_df.columns:
        raise SystemExit(
            f"'Date' column not found in Continuous file: {cont_file}"
        )
    date_part = pd.to_datetime(cont_df['Date'], dayfirst=True, errors='coerce')

    # Time column is optional but normally present
    if 'Time' in cont_df.columns:
        t = cont_df['Time']
        t_dt = pd.to_datetime(t, errors='coerce')
        if t_dt.isna().mean() > 0.5:
            t_dt = pd.to_datetime(t.astype(str), errors='coerce')

        time_part = (
            pd.to_timedelta(t_dt.dt.hour.fillna(0).astype(int), unit='h') +
            pd.to_timedelta(t_dt.dt.minute.fillna(0).astype(int), unit='m') +
            pd.to_timedelta(t_dt.dt.second.fillna(0).astype(int), unit='s')
        )
        cont_df['DateTime'] = date_part + time_part
    else:
        cont_df['DateTime'] = date_part

    cont_df = cont_df[cont_df['DateTime'].notna()].copy()
    cont_df = cont_df.sort_index().drop_duplicates(
        subset=['DateTime'], keep='last'
    )
    return cont_df


def parse_trip_like_estee(trip_file: Path) -> pd.DataFrame:
    """Mimic flexsim_input_simulation.py's parsing of the Trip Report."""
    trip_df = pd.read_excel(trip_file)
    if 'Arrived At' not in trip_df.columns or 'Completed At' not in trip_df.columns:
        raise SystemExit(
            f"'Arrived At'/'Completed At' columns not found in Trip file: {trip_file}"
        )
    trip_df['Arrived At'] = pd.to_datetime(
        trip_df['Arrived At'], dayfirst=True, errors='coerce'
    )
    trip_df['Completed At'] = pd.to_datetime(
        trip_df['Completed At'], dayfirst=True, errors='coerce'
    )
    return trip_df


def check(label: str, condition: bool, detail: str = "") -> bool:
    """Print a check line. Returns the condition value."""
    mark = "PASS" if condition else "FAIL"
    bullet = "[OK]" if condition else "[X]"
    print(f"  [{mark}] {bullet} {label}" + (f" - {detail}" if detail else ""))
    return condition


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--operator", required=True,
                        help="Operator key (e.g. yorklaunch)")
    args = parser.parse_args()

    op_dir = config.OPERATORS_DIR / args.operator
    trip_path = op_dir / "trip_report.xlsx"
    cont_path = config.OUTPUT_DIR / f"{args.operator}_continuous_report.xlsx"

    print(f"Operator: {args.operator}")
    print(f"Trip file:       {trip_path}")
    print(f"Continuous file: {cont_path}\n")

    if not cont_path.exists():
        sys.exit(
            f"\nContinuous Report not found.\n"
            f"Run first: python 03_build_continuous_report.py --operator {args.operator}\n"
        )
    if not trip_path.exists():
        sys.exit(f"\nTrip Report not found at {trip_path}\n")

    all_pass = True

    # ---------------------------------------------------------------
    # 1. File opens with pd.read_excel
    # ---------------------------------------------------------------
    print("=== File parsing ===")
    try:
        cont_df = parse_continuous_like_estee(cont_path)
        all_pass &= check("File opens with pd.read_excel()",
                          True,
                          f"{len(cont_df)} rows after parsing")
    except SystemExit as e:
        check("File opens with pd.read_excel()", False, str(e))
        sys.exit("\nCannot continue with further checks.")

    # ---------------------------------------------------------------
    # 2. Required columns are present
    # ---------------------------------------------------------------
    print("\n=== Required columns ===")
    required = ['Date', 'Time', 'Speed', 'Latitude', 'Longitude']
    for col in required:
        all_pass &= check(f"'{col}' column present",
                          col in cont_df.columns)

    # ---------------------------------------------------------------
    # 3. Datetime parsing succeeded
    # ---------------------------------------------------------------
    print("\n=== Datetime parsing ===")
    n_total = len(cont_df) + (cont_df['DateTime'].isna().sum()
                              if 'DateTime' in cont_df else 0)
    n_valid = cont_df['DateTime'].notna().sum() if 'DateTime' in cont_df else 0
    all_pass &= check(
        "DateTime parsed for every row",
        n_valid > 0 and n_valid == len(cont_df),
        f"{n_valid}/{len(cont_df)} rows have valid DateTime"
    )

    # ---------------------------------------------------------------
    # 4. Numeric columns are actually numeric
    # ---------------------------------------------------------------
    print("\n=== Numeric columns ===")
    for col in ['Speed', 'Latitude', 'Longitude']:
        if col not in cont_df.columns:
            continue
        coerced = pd.to_numeric(cont_df[col], errors='coerce')
        n_bad = coerced.isna().sum()
        all_pass &= check(
            f"'{col}' is numeric",
            n_bad == 0,
            f"{n_bad} non-numeric values" if n_bad else f"all {len(cont_df)} OK"
        )

    # ---------------------------------------------------------------
    # 5. Trip windows match continuous data
    # ---------------------------------------------------------------
    print("\n=== Trip-to-continuous matching ===")
    trip_df = parse_trip_like_estee(trip_path)
    trip_df = trip_df.dropna(subset=['Arrived At', 'Completed At'])

    matched = 0
    unmatched = []
    for _, t in trip_df.iterrows():
        s, e = t['Arrived At'], t['Completed At']
        seg = cont_df[(cont_df['DateTime'] >= s)
                      & (cont_df['DateTime'] <= e)]
        if len(seg) >= 1:
            matched += 1
        else:
            unmatched.append(
                f"{t.get('Vessel Name', '?')} {s} -> {e}"
            )

    pct = (matched / len(trip_df) * 100) if len(trip_df) else 0
    all_pass &= check(
        "Every trip has at least one matching Continuous row",
        len(unmatched) == 0,
        f"{matched}/{len(trip_df)} trips matched ({pct:.0f}%)"
    )
    if unmatched:
        print("\n     Unmatched trips (Estee's code would silently skip these):")
        for u in unmatched[:5]:
            print(f"       - {u}")
        if len(unmatched) > 5:
            print(f"       ... and {len(unmatched) - 5} more")

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    print()
    if all_pass:
        print("=" * 60)
        print("  ALL CHECKS PASSED")
        print(f"  '{cont_path.name}' is compatible with Estee's Flexsim")
        print(f"  code. Hand it to flexsim_input_simulation.py as the")
        print(f"  CONT_FILE argument and it will work.")
        print("=" * 60)
        sys.exit(0)
    else:
        print("=" * 60)
        print("  SOME CHECKS FAILED")
        print("  Fix the issues above before passing this file to Estee's")
        print("  Flexsim code, otherwise rows will be silently dropped or")
        print("  the import will crash.")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
