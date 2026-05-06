"""
config.py
=========

This file holds all the settings used across every script in this project.

Why one settings file?
    Instead of hard-coding values (like file paths, API keys, time zones)
    inside every script, we keep them all here. If you need to change
    something, you change it in ONE place and every script picks up the
    change automatically.

What's in here:
    - Where files live on disk (paths)
    - Your aisstream.io API key
    - The geographic area we care about (Singapore)
    - Column names for the Continuous Report (must match SeaCabbie's format)
    - The list of Singapore harbour craft operators we know about

You don't run this file directly. Other scripts read from it.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# WHERE EVERYTHING LIVES ON DISK
# ---------------------------------------------------------------------------
# Path(__file__) gives us this file's location.
# .parent climbs up one folder, so HERE is the folder containing config.py.
# Every other path is built relative to HERE so the project is portable
# (it works no matter where you put the folder on your computer).
HERE         = Path(__file__).parent

# Folder for daily AIS recordings (one CSV per day)
ARCHIVE_DIR  = HERE / "archive"

# Folder for generated Continuous Reports
OUTPUT_DIR   = HERE / "output"

# Folder where each operator has their own settings (Trip Report path, MMSI list)
OPERATORS_DIR = HERE / "operators"

# Folder for the web interface's HTML files
TEMPLATES_DIR = HERE / "web" / "templates"

# Make sure all the folders exist (creates them on first run)
for d in [ARCHIVE_DIR, OUTPUT_DIR, OPERATORS_DIR, TEMPLATES_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# AIS DATA SERVICE (aisstream.io)
# ---------------------------------------------------------------------------
# aisstream.io is a free service that streams live AIS messages from ships.
# You need to register at https://aisstream.io/ to get an API key.
#
# We try to read the key from an "environment variable" first (a setting
# stored outside the code, which is safer for secrets). If that's not set,
# we fall back to whatever string is between the quotes below.
AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
AISSTREAM_API_KEY = os.environ.get("AISSTREAM_API_KEY", "bad26dd9a9edaa1eadf1920dd37c939206293a40")


# ---------------------------------------------------------------------------
# GEOGRAPHIC AREA: SINGAPORE PORT WATERS
# ---------------------------------------------------------------------------
# This is a "bounding box" — a rectangle on the map. We only care about
# ships inside this rectangle. The format is:
#   [[south-west corner], [north-east corner]]
#   [[lat,  lon       ], [lat,  lon         ]]
#
# This rectangle covers all of Singapore's port waters: Eastern
# Anchorage, Western Anchorage, Sudong, Changi, and the main shipping lanes.
SINGAPORE_BBOX = [[1.10, 103.50], [1.50, 104.10]]


# ---------------------------------------------------------------------------
# CONTINUOUS REPORT COLUMN FORMAT
# ---------------------------------------------------------------------------
# This must match SeaCabbie's Continuous Report exactly so the existing
# Flexsim code (flexsim_input_simulation.py) can process our output
# without any changes.
#
# These column names came from inspecting the SeaCabbie CSV/Excel files
# and the existing Python code that reads them.
CONTINUOUS_REPORT_COLUMNS = [
    "Date",         # DD/MM/YYYY format (day first, like SeaCabbie uses)
    "Time",         # HH:MM:SS format
    "Ignition",     # "Ignition ON" or "Ignition OFF"
    "Speed",        # speed in knots (1 knot ~ 1.85 km/h)
    "Heading",      # direction the bow is pointing, 0-359 degrees
    "Latitude",     # north-south position
    "Longitude",    # east-west position
    "Address",      # text address (we leave this blank, see notes below)
    # Extra columns we add (not in the original SeaCabbie format).
    # The Flexsim code ignores unknown columns, so adding these is safe.
    "Vessel",       # vessel name from the Trip Report
    "MMSI",         # AIS unique vessel ID
]

# IMPORTANT NOTES on differences from SeaCabbie's data:
#
# 1. Ignition: SeaCabbie's vessels report ignition state directly.
#    AIS doesn't broadcast this. We approximate it: if the vessel is
#    moving (speed > 0.3 knots), we say "Ignition ON". Otherwise OFF.
#    This is a reasonable proxy but not a perfect match.
#
# 2. Address: SeaCabbie's data has a text address from a reverse-geocoder.
#    AIS only gives lat/lon. We leave Address blank. If needed later,
#    we can add free reverse geocoding.


# ---------------------------------------------------------------------------
# TRIP REPORT EXPECTED COLUMNS
# ---------------------------------------------------------------------------
# What we expect to find in an operator's Trip Report file (xlsx).
# The script tolerates a few common variations of the vessel name column.
TRIP_REPORT_REQUIRED_COLUMNS = ["Vessel Name", "Arrived At", "Completed At"]
TRIP_REPORT_OPTIONAL_COLUMNS = ["Location", "Date"]


# ---------------------------------------------------------------------------
# KNOWN SINGAPORE HARBOUR CRAFT OPERATORS (from MPA's safety workgroup)
# ---------------------------------------------------------------------------
# This is task 3 from the project brief: identify Singapore operators.
# Source: MPA Harbour Craft Safety WorkGroup roster (publicly listed on
# mpa.gov.sg). These are all real, currently operating companies.
#
# Each entry has a "key" (used as a folder name and command line argument)
# and a "name" (the human-readable company name).
SINGAPORE_OPERATORS = [
    {"key": "seacabbie",      "name": "SeaCabbie"},
    {"key": "yorklaunch",     "name": "York Launch Service Pte Ltd"},
    {"key": "psamarine",      "name": "PSA Marine (Pte) Ltd"},
    {"key": "tiansan",        "name": "Tian San Shipping Pte Ltd"},
    {"key": "castlaunch",     "name": "Cast Launch Services Pte Ltd"},
    {"key": "enghup",         "name": "Eng Hup Shipping Pte Ltd"},
    {"key": "kanlianferry",   "name": "Kanlian Ferry Pte Ltd"},
    {"key": "litaocean",      "name": "Lita Ocean Pte Ltd"},
    {"key": "singaporeisland","name": "Singapore Island Cruise & Ferry Service Pte Ltd"},
    {"key": "penguin",        "name": "Penguin International Limited"},
]
