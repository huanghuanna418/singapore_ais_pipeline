"""
05_web_ui.py
============

WHAT THIS SCRIPT DOES:
    Starts a small website on your computer that lets you control the
    whole pipeline from a web browser. No need to remember command-line
    flags.

WHY WE NEED IT:
    The previous scripts work fine on the command line, but a web
    interface is friendlier for non-technical users (your prof, Amanda,
    anyone collaborating). It also makes it easy to:
       - See which operators are configured
       - Upload a Trip Report by dragging and dropping
       - Trigger MMSI lookup, recording, and report generation with a click
       - View the latest Continuous Report and energy summary in a table
       - See vessel routes plotted on a Singapore map

USAGE:
    pip install flask
    python 05_web_ui.py

    Then open http://localhost:5000 in your browser.

NOTES:
    This is a LOCAL website — it only runs on your computer, no one on
    the internet can access it. To share with others, you'd need to
    deploy it to a server. That's out of scope for now.
"""

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

# Flask is the most popular small web framework for Python.
try:
    from flask import (Flask, render_template, request,
                       redirect, url_for, jsonify, send_file)
except ImportError:
    sys.exit("Run: pip install flask")

import config


# Create the web app
app = Flask(__name__,
            template_folder=str(config.TEMPLATES_DIR),
            static_folder=str(Path(__file__).parent / "web" / "static"))


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def operator_status(op_key: str) -> dict:
    """Compute the status of one operator: which files exist, how many trips, etc."""
    op_dir = config.OPERATORS_DIR / op_key
    trip_path = op_dir / "trip_report.xlsx"
    mmsi_path = op_dir / "vessel_mmsi.csv"
    report_path = config.OUTPUT_DIR / f"{op_key}_continuous_report.xlsx"
    energy_path = config.OUTPUT_DIR / f"{op_key}_energy_consumption.xlsx"

    info = {
        "key": op_key,
        "has_trip_report": trip_path.exists(),
        "has_mmsi_lookup": mmsi_path.exists(),
        "has_continuous_report": report_path.exists(),
        "has_energy_report": energy_path.exists(),
        "n_vessels": 0,
        "n_mmsi_known": 0,
    }
    if mmsi_path.exists():
        df = pd.read_csv(mmsi_path)
        info["n_vessels"] = len(df)
        info["n_mmsi_known"] = int(df["mmsi"].notna().sum())
    return info


# ---------------------------------------------------------------------------
# HOME PAGE
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    """List all known operators and their pipeline status."""
    operators_info = []
    for op in config.SINGAPORE_OPERATORS:
        info = operator_status(op["key"])
        info["name"] = op["name"]
        operators_info.append(info)

    # Count archive files (so the user knows how much AIS data exists)
    archive_files = list(config.ARCHIVE_DIR.glob("ais_*.csv"))

    return render_template(
        "index.html",
        operators=operators_info,
        archive_count=len(archive_files),
    )


# ---------------------------------------------------------------------------
# UPLOAD TRIP REPORT
# ---------------------------------------------------------------------------
@app.route("/upload/<op_key>", methods=["POST"])
def upload_trip_report(op_key: str):
    """Save an uploaded Trip Report into the operator's folder."""
    if "file" not in request.files:
        return "No file uploaded", 400
    f = request.files["file"]
    if not f.filename.lower().endswith((".xlsx", ".xls", ".csv")):
        return "File must be .xlsx, .xls, or .csv", 400

    op_dir = config.OPERATORS_DIR / op_key
    op_dir.mkdir(parents=True, exist_ok=True)
    f.save(str(op_dir / "trip_report.xlsx"))
    return redirect(url_for("operator_page", op_key=op_key))


# ---------------------------------------------------------------------------
# OPERATOR DETAIL PAGE
# ---------------------------------------------------------------------------
@app.route("/operator/<op_key>")
def operator_page(op_key: str):
    """Detail page showing one operator's data + buttons to run each step."""
    info = operator_status(op_key)
    matched_op = next((o for o in config.SINGAPORE_OPERATORS
                       if o["key"] == op_key), None)
    info["name"] = matched_op["name"] if matched_op else op_key

    # Show first few rows of the MMSI table if it exists
    mmsi_preview = []
    mmsi_path = config.OPERATORS_DIR / op_key / "vessel_mmsi.csv"
    if mmsi_path.exists():
        df = pd.read_csv(mmsi_path)
        mmsi_preview = df.fillna("").to_dict(orient="records")

    # Show summary stats from energy report if it exists
    energy_summary = None
    energy_path = config.OUTPUT_DIR / f"{op_key}_energy_consumption.xlsx"
    if energy_path.exists():
        df = pd.read_excel(energy_path)
        energy_summary = {
            "n_trips":        len(df),
            "total_kWh":      round(df["Energy (kWh)"].sum(), 1),
            "avg_kWh":        round(df["Energy (kWh)"].mean(), 1),
            "max_pct_battery":round(df["% of Battery"].max(), 1),
            "total_km":       round(df["Distance (km)"].sum(), 1),
        }

    return render_template(
        "operator.html",
        op=info,
        mmsi_preview=mmsi_preview,
        energy_summary=energy_summary,
    )


# ---------------------------------------------------------------------------
# RUN A PIPELINE STEP (resolve mmsi, build report, energy)
# ---------------------------------------------------------------------------
@app.route("/run/<step>/<op_key>", methods=["POST"])
def run_step(step: str, op_key: str):
    """
    Run one of the pipeline scripts for the given operator.
    Returns the script's stdout as plain text so the user can see what
    happened.
    """
    here = Path(__file__).parent
    valid_steps = {
        "resolve_mmsi":   here / "01_resolve_mmsi.py",
        "build_report":   here / "03_build_continuous_report.py",
        "energy":         here / "04_energy_consumption.py",
        "verify_compat":  here / "06_verify_flexsim_compat.py",
    }
    if step not in valid_steps:
        return "Unknown step", 400

    cmd = [sys.executable, str(valid_steps[step]), "--operator", op_key]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=600,
        )
        return jsonify({
            "ok": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
        })
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "stderr": "timeout"})


# ---------------------------------------------------------------------------
# MAP DATA (for route visualization)
# ---------------------------------------------------------------------------
@app.route("/map_data/<op_key>")
def map_data(op_key: str):
    """
    Return the operator's vessel positions as JSON so the map can plot them.
    """
    report_path = (
        config.OUTPUT_DIR / f"{op_key}_continuous_report.xlsx"
    )
    if not report_path.exists():
        return jsonify({"vessels": []})

    df = pd.read_excel(report_path)
    # Group by vessel; each vessel becomes one polyline on the map
    vessels = []
    for vessel, group in df.groupby("Vessel"):
        coords = (
            group[["Latitude", "Longitude"]]
            .dropna()
            .values.tolist()
        )
        vessels.append({"name": vessel, "coords": coords})
    return jsonify({"vessels": vessels})


# ---------------------------------------------------------------------------
# DOWNLOAD A FILE
# ---------------------------------------------------------------------------
@app.route("/download/<op_key>/<which>")
def download(op_key: str, which: str):
    """Let the user download the generated reports."""
    files = {
        "continuous": f"{op_key}_continuous_report.xlsx",
        "energy":     f"{op_key}_energy_consumption.xlsx",
        "mmsi":       config.OPERATORS_DIR / op_key / "vessel_mmsi.csv",
    }
    if which not in files:
        return "Unknown file", 404

    if which == "mmsi":
        path = files[which]
    else:
        path = config.OUTPUT_DIR / files[which]

    if not Path(path).exists():
        return "File not generated yet", 404
    return send_file(str(path), as_attachment=True)


# ---------------------------------------------------------------------------
# START THE SERVER
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Starting web UI at http://localhost:5000")
    print("Press Ctrl+C to stop.")
    # debug=True automatically reloads when you edit the code (handy for dev)
    app.run(host="127.0.0.1", port=5000, debug=False)
