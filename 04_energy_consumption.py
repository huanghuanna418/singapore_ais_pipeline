"""
04_energy_consumption.py
========================

WHAT THIS SCRIPT DOES:
    Estimates how much energy (in kilowatt-hours, kWh) each vessel
    trip consumed, based on:
       (a) the Continuous Report from step 3 (speed every minute)
       (b) eHC (electric harbour craft) specifications

WHY WE NEED IT:
    The end goal of the project is to figure out what kind of electric
    boats can replace today's diesel boats for each operator. To answer
    that, we need to know how much energy each trip would use if it
    were run on a specific eHC model.

HOW THE ESTIMATE WORKS:
    Energy used = Power x Time
    For boats, Power scales roughly with speed cubed (drag goes up
    quickly as you go faster). So we use this simplified model:

       Power(speed) = max_power_kW * (speed / max_speed) ** 3
                    + hotel_load_kW

    where:
       max_power_kW   = the boat's maximum motor output
       max_speed_kn   = the speed at which max_power is reached
       hotel_load_kW  = constant electricity used for lights, AC, radio

    Then we sum (Power x duration) over every minute of the trip.

    NOTE: This is a rough engineering approximation. Real eHC modelling
    would account for hull shape, sea state, payload, battery
    efficiency, regenerative braking, etc. For our purpose (comparing
    operators and identifying suitable eHC models), this is good enough.

USAGE:
    python 04_energy_consumption.py --operator yorklaunch
    python 04_energy_consumption.py --operator yorklaunch --eHC small_passenger

OUTPUT:
    output/<operator>_energy_consumption.xlsx
    Columns: Vessel, Trip Start, Trip End, Duration (min), Distance (km),
             Energy (kWh), Avg Speed (kn), eHC Model
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import config


# ---------------------------------------------------------------------------
# eHC SPECIFICATIONS (PLACEHOLDERS)
# ---------------------------------------------------------------------------
# These are EXAMPLE specs — your prof should replace them with real
# numbers from the eHC reference designs (or from the boats York Launch
# is considering). Each entry describes one electric harbour craft model.
EHC_MODELS = {
    "small_passenger": {
        "name":           "Small Passenger eHC (~12 PAX)",
        "max_power_kW":   150,    # peak motor output
        "max_speed_kn":   18,     # speed at which peak power is reached
        "hotel_load_kW":  3,      # constant draw for AC/lights/electronics
        "battery_kWh":    150,    # for reference — usable battery capacity
    },
    "medium_passenger": {
        "name":           "Medium Passenger eHC (~30 PAX)",
        "max_power_kW":   400,
        "max_speed_kn":   22,
        "hotel_load_kW":  8,
        "battery_kWh":    400,
    },
    "cargo_lighter": {
        "name":           "Cargo Lighter eHC",
        "max_power_kW":   600,
        "max_speed_kn":   12,
        "hotel_load_kW":  5,
        "battery_kWh":    800,
    },
}

DEFAULT_EHC = "small_passenger"


# ---------------------------------------------------------------------------
# DISTANCE BETWEEN TWO LAT/LON POINTS (haversine formula)
# ---------------------------------------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """
    Great-circle distance between two GPS points, in kilometers.
    Standard "haversine" formula — works well for short distances on Earth.
    """
    R = 6371.0  # Earth's mean radius in km
    # Convert degrees to radians
    lat1_r = np.radians(lat1)
    lat2_r = np.radians(lat2)
    dlat   = np.radians(lat2 - lat1)
    dlon   = np.radians(lon2 - lon1)
    # The formula
    a = (np.sin(dlat / 2) ** 2
         + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2) ** 2)
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c


# ---------------------------------------------------------------------------
# POWER MODEL
# ---------------------------------------------------------------------------
def power_kW_at_speed(speed_kn: float, ehc: dict) -> float:
    """
    Estimated electrical power draw at a given speed for one eHC model.
    Uses cube-of-speed drag model + a constant "hotel load".
    """
    if speed_kn <= 0:
        # Just hotel load (lights/AC/electronics) when stopped
        return ehc["hotel_load_kW"]
    # Cube of speed model. At max_speed, this returns max_power.
    propulsion = ehc["max_power_kW"] * (speed_kn / ehc["max_speed_kn"]) ** 3
    return propulsion + ehc["hotel_load_kW"]


# ---------------------------------------------------------------------------
# PROCESS ONE TRIP
# ---------------------------------------------------------------------------
def estimate_trip_energy(trip_rows: pd.DataFrame, ehc: dict) -> dict:
    """
    Given the Continuous Report rows for one trip, estimate total energy
    consumption and return a dict of summary stats.
    """
    # Combine Date + Time into a single datetime so we can compute durations
    trip_rows = trip_rows.copy()
    trip_rows["DateTime"] = pd.to_datetime(
        trip_rows["Date"] + " " + trip_rows["Time"],
        dayfirst=True, errors="coerce"
    )
    trip_rows = trip_rows.dropna(subset=["DateTime"]).sort_values("DateTime")

    if len(trip_rows) < 2:
        return None    # not enough data to estimate

    # Time difference (in hours) to the NEXT row.
    # We use forward-difference: each ping represents the time until the next.
    trip_rows["dt_hr"] = (
        trip_rows["DateTime"].diff().shift(-1).dt.total_seconds() / 3600.0
    )
    trip_rows["dt_hr"] = trip_rows["dt_hr"].fillna(0)

    # Power at each ping
    trip_rows["power_kW"] = trip_rows["Speed"].apply(
        lambda s: power_kW_at_speed(s, ehc)
    )

    # Energy per segment = power x duration
    trip_rows["energy_kWh"] = trip_rows["power_kW"] * trip_rows["dt_hr"]

    # Distance: sum haversine between consecutive pings
    lats = trip_rows["Latitude"].values
    lons = trip_rows["Longitude"].values
    distances = haversine_km(lats[:-1], lons[:-1], lats[1:], lons[1:])
    total_distance_km = float(np.sum(distances))

    duration_min = (trip_rows["DateTime"].iloc[-1]
                    - trip_rows["DateTime"].iloc[0]).total_seconds() / 60

    return {
        "Vessel":              trip_rows["Vessel"].iloc[0],
        "Trip Start":          trip_rows["DateTime"].iloc[0],
        "Trip End":            trip_rows["DateTime"].iloc[-1],
        "Duration (min)":      round(duration_min, 1),
        "Distance (km)":       round(total_distance_km, 3),
        "Avg Speed (kn)":      round(trip_rows["Speed"].mean(), 2),
        "Max Speed (kn)":      round(trip_rows["Speed"].max(), 2),
        "Energy (kWh)":        round(trip_rows["energy_kWh"].sum(), 2),
        "eHC Model":           ehc["name"],
        "% of Battery":        round(
            trip_rows["energy_kWh"].sum() / ehc["battery_kWh"] * 100, 1
        ),
    }


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--operator", required=True,
                        help="Operator key (e.g. yorklaunch)")
    parser.add_argument("--eHC", default=DEFAULT_EHC,
                        choices=list(EHC_MODELS.keys()),
                        help=f"eHC model (default: {DEFAULT_EHC})")
    args = parser.parse_args()

    ehc = EHC_MODELS[args.eHC]
    print(f"Operator: {args.operator}")
    print(f"eHC model: {ehc['name']}")
    print(f"  Max power: {ehc['max_power_kW']} kW")
    print(f"  Max speed: {ehc['max_speed_kn']} knots")
    print(f"  Hotel load: {ehc['hotel_load_kW']} kW")
    print(f"  Battery:   {ehc['battery_kWh']} kWh\n")

    # Load the Continuous Report we built in step 3
    report_path = (
        config.OUTPUT_DIR / f"{args.operator}_continuous_report.xlsx"
    )
    if not report_path.exists():
        sys.exit(
            f"\nContinuous Report not found: {report_path.name}\n"
            f"Run: python 03_build_continuous_report.py --operator {args.operator}\n"
        )
    print(f"Loading {report_path.name}...")
    df = pd.read_excel(report_path)

    # Group rows into "trips". A trip is a continuous block of pings
    # for one vessel. We split whenever the gap between consecutive
    # pings exceeds 5 minutes.
    df["DateTime"] = pd.to_datetime(
        df["Date"] + " " + df["Time"], dayfirst=True, errors="coerce"
    )
    df = df.dropna(subset=["DateTime"]).sort_values(["Vessel", "DateTime"])
    df["gap_min"] = (
        df.groupby("Vessel")["DateTime"]
        .diff().dt.total_seconds() / 60
    ).fillna(0)
    df["trip_id"] = (df["gap_min"] > 5).cumsum()

    summaries = []
    for (vessel, trip_id), group in df.groupby(["Vessel", "trip_id"]):
        result = estimate_trip_energy(group, ehc)
        if result is not None:
            summaries.append(result)

    if not summaries:
        sys.exit("\nNo trips with enough data to estimate energy.\n")

    out_df = pd.DataFrame(summaries)
    out_path = (
        config.OUTPUT_DIR / f"{args.operator}_energy_consumption.xlsx"
    )
    out_df.to_excel(out_path, index=False)
    print(f"\nWrote: {out_path.name}  ({len(out_df)} trips analyzed)")

    # Print a quick summary
    print("\n=== Summary ===")
    print(f"  Total energy consumed:    {out_df['Energy (kWh)'].sum():,.1f} kWh")
    print(f"  Average per trip:         {out_df['Energy (kWh)'].mean():.1f} kWh")
    print(f"  Largest single-trip draw: {out_df['Energy (kWh)'].max():.1f} kWh "
          f"(= {out_df['% of Battery'].max()}% of battery)")
    print(f"  Total distance:           {out_df['Distance (km)'].sum():.1f} km")


if __name__ == "__main__":
    main()
