"""
test_pipeline.py
================

Runs the whole pipeline end-to-end with synthetic data so you can verify
everything works without needing an aisstream.io key or a real Trip
Report.

Steps it performs:
  1. Generate a fake Trip Report for "yorklaunch" (3 vessels, 5 trips each)
  2. Generate a fake AIS archive matching those trips
  3. Generate a fake vessel_mmsi.csv lookup
  4. Run 03_build_continuous_report.py
  5. Run 04_energy_consumption.py
  6. Verify the outputs

If you see "ALL TESTS PASSED" at the end, the pipeline is healthy.

USAGE:
    python test_pipeline.py
"""

import random
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import config


OPERATOR_KEY = "yorklaunch"


def cleanup():
    """Remove any previous test artifacts."""
    op_dir = config.OPERATORS_DIR / OPERATOR_KEY
    if op_dir.exists():
        for f in op_dir.glob("*"):
            f.unlink()
    for p in config.ARCHIVE_DIR.glob("ais_*.csv"):
        p.unlink()
    for p in config.OUTPUT_DIR.glob(f"{OPERATOR_KEY}_*"):
        p.unlink()


def generate_fake_trip_report():
    op_dir = config.OPERATORS_DIR / OPERATOR_KEY
    op_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    base = datetime.now() - timedelta(days=3)
    for v in ["YL ALPHA", "YL BRAVO", "YL CHARLIE"]:
        for i in range(5):
            arr = base + timedelta(hours=i * 6 + random.randint(0, 2),
                                   minutes=random.randint(0, 30))
            comp = arr + timedelta(minutes=random.randint(30, 90))
            rows.append({
                "Vessel Name": v,
                "Arrived At":  arr,
                "Completed At": comp,
                "Location":    "JET1-JET2",
            })
    df = pd.DataFrame(rows).sort_values("Arrived At")
    df.to_excel(op_dir / "trip_report.xlsx", index=False)
    print(f"  Wrote {len(df)} fake trips for {OPERATOR_KEY}")
    return df


def generate_fake_mmsi_lookup(trip_df):
    op_dir = config.OPERATORS_DIR / OPERATOR_KEY
    names = sorted(trip_df["Vessel Name"].unique())
    rows = [{"vessel_name": n, "mmsi": 563000000 + i,
             "flag": None, "notes": None}
            for i, n in enumerate(names)]
    pd.DataFrame(rows).to_csv(op_dir / "vessel_mmsi.csv", index=False)
    print(f"  Wrote MMSI lookup for {len(rows)} vessel(s)")
    return {r["vessel_name"]: r["mmsi"] for r in rows}


def generate_fake_archive(trip_df, mmsi_lookup):
    """One AIS ping per minute inside each trip window."""
    by_date = {}

    for _, trip in trip_df.iterrows():
        mmsi = mmsi_lookup[trip["Vessel Name"]]
        # Trip times are SGT; archive is UTC
        start_utc = (pd.Timestamp(trip["Arrived At"])
                     .tz_localize("Asia/Singapore")
                     .tz_convert("UTC").tz_localize(None))
        end_utc   = (pd.Timestamp(trip["Completed At"])
                     .tz_localize("Asia/Singapore")
                     .tz_convert("UTC").tz_localize(None))
        minutes = int((end_utc - start_utc).total_seconds() // 60)

        for m in range(minutes + 1):
            ts = start_utc + timedelta(minutes=m)
            progress = m / max(1, minutes)
            # Speed profile: idle - cruise - idle
            if progress < 0.1 or progress > 0.9:
                speed = random.uniform(0, 0.4)
            else:
                speed = random.uniform(6, 12)
            lat = 1.24 + 0.02 * progress + random.uniform(-0.001, 0.001)
            lon = 103.80 + 0.05 * progress + random.uniform(-0.001, 0.001)
            row = [
                ts.isoformat() + "+00:00",
                mmsi, trip["Vessel Name"], 60,
                round(lat, 6), round(lon, 6),
                round(speed, 2),
                round(random.uniform(0, 360), 1),
                random.randint(0, 359),
                1 if speed < 0.5 else 0,
            ]
            by_date.setdefault(ts.date(), []).append(row)

    header = ["timestamp_utc", "mmsi", "ship_name", "ship_type",
              "latitude", "longitude", "sog_knots", "cog_deg",
              "heading_deg", "nav_status"]
    total = 0
    for d, rows in by_date.items():
        path = config.ARCHIVE_DIR / f"ais_{d.isoformat()}.csv"
        pd.DataFrame(rows, columns=header).to_csv(path, index=False)
        total += len(rows)
    print(f"  Wrote {total} archive rows across {len(by_date)} day file(s)")


def run_script(script_name: str, *extra_args) -> bool:
    """Run one of the pipeline scripts with --operator yorklaunch."""
    script = Path(__file__).parent / script_name
    cmd = [sys.executable, str(script), "--operator", OPERATOR_KEY, *extra_args]
    print(f"\n  $ {' '.join(cmd[-3:])}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        print("  STDERR:", result.stderr)
        return False
    # Print just the last few lines of output
    for line in result.stdout.splitlines()[-8:]:
        print(f"    | {line}")
    return True


def verify_continuous_report():
    path = config.OUTPUT_DIR / f"{OPERATOR_KEY}_continuous_report.xlsx"
    if not path.exists():
        print(f"  FAIL: {path.name} not created")
        return False
    df = pd.read_excel(path)
    required = set(config.CONTINUOUS_REPORT_COLUMNS)
    missing = required - set(df.columns)
    if missing:
        print(f"  FAIL: missing columns {missing}")
        return False
    if df.empty:
        print("  FAIL: empty output")
        return False
    print(f"  OK: {len(df)} rows, all required columns present")
    return True


def verify_energy_report():
    path = config.OUTPUT_DIR / f"{OPERATOR_KEY}_energy_consumption.xlsx"
    if not path.exists():
        print(f"  FAIL: {path.name} not created")
        return False
    df = pd.read_excel(path)
    required = {"Vessel", "Trip Start", "Trip End", "Duration (min)",
                "Distance (km)", "Energy (kWh)", "% of Battery"}
    missing = required - set(df.columns)
    if missing:
        print(f"  FAIL: missing columns {missing}")
        return False
    if df.empty:
        print("  FAIL: empty output")
        return False
    if (df["Energy (kWh)"] <= 0).any():
        print("  WARNING: some trips had zero or negative energy")
    print(f"  OK: {len(df)} trips, total energy "
          f"{df['Energy (kWh)'].sum():.1f} kWh")
    return True


def main():
    random.seed(0)

    print("=== Cleaning up previous test artifacts ===")
    cleanup()

    print("\n=== Generating fake Trip Report ===")
    trip_df = generate_fake_trip_report()

    print("\n=== Generating fake MMSI lookup ===")
    mmsi_lookup = generate_fake_mmsi_lookup(trip_df)

    print("\n=== Generating fake AIS archive ===")
    generate_fake_archive(trip_df, mmsi_lookup)

    print("\n=== Running 03_build_continuous_report.py ===")
    if not run_script("03_build_continuous_report.py"):
        sys.exit("FAIL: continuous report step failed")

    print("\n=== Verifying continuous report ===")
    if not verify_continuous_report():
        sys.exit("FAIL")

    print("\n=== Running 04_energy_consumption.py ===")
    if not run_script("04_energy_consumption.py"):
        sys.exit("FAIL: energy step failed")

    print("\n=== Verifying energy report ===")
    if not verify_energy_report():
        sys.exit("FAIL")

    print("\n=== Running 06_verify_flexsim_compat.py ===")
    if not run_script("06_verify_flexsim_compat.py"):
        sys.exit("FAIL: Flexsim compatibility check failed")

    print("\n\nALL TESTS PASSED.")


if __name__ == "__main__":
    main()
