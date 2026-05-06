"""
02_record_ais.py
================
WHAT THIS SCRIPT DOES:
    Listens to the live AIS data stream from aisstream.io and writes
    every relevant message to a CSV file. It runs continuously — for
    minutes, hours, or weeks — until you stop it with Ctrl+C.

WHY WE NEED IT:
    aisstream.io is a LIVE stream — it doesn't have a "give me last
    month's data" button. To collect a month of data we have to record
    going forward for a month. This script does that recording.

KEY FEATURES:
    - Filters by MMSI, so we only save data for vessels we care about.
      You can specify --operator yorklaunch (uses MMSIs from that
      operator's vessel_mmsi.csv), or --operator all (records every
      MMSI we know about across all operators), or --no-filter to
      record everything in Singapore waters.

    - Daily archive files. Data goes into archive/ais_YYYY-MM-DD.csv
      (one file per UTC day). This way, if one file gets corrupted,
      we lose at most one day instead of everything.

    - Auto-reconnects on disconnect. Internet drops happen. The
      recorder waits 15 seconds and tries again, forever.

    - Saves to disk frequently. If you press Ctrl+C, you don't lose
      the last 30 minutes of data.

USAGE:
    # Record only vessels for one operator:
    python 02_record_ais.py --operator yorklaunch

    # Record vessels across ALL operators we know about:
    python 02_record_ais.py --operator all

    # Stop after a fixed number of minutes (good for testing):
    python 02_record_ais.py --operator all --minutes 60

    # Record EVERY vessel in Singapore (no MMSI filter, files get big!):
    python 02_record_ais.py --no-filter

    On a laptop, prevent it from sleeping while recording:
        macOS:  caffeinate -i python 02_record_ais.py --operator all
        Linux:  systemd-inhibit --what=sleep python 02_record_ais.py ...

OUTPUT:
    archive/ais_2026-04-24.csv   (UTC date)
    archive/ais_2026-04-25.csv
    ...
"""

# Standard library imports
import argparse                                # command-line arguments
import asyncio                                 # for async (network) code
import csv                                     # for writing CSV files
import json                                    # for parsing JSON messages
import signal                                  # to handle Ctrl+C cleanly
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import config


# ---------------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------------
# How often to flush data to disk. Higher = faster, but more risk of
# losing recent data on a crash.
FLUSH_EVERY_N_MESSAGES = 20

# Column layout for the archive files. The order matters because
# the analyzer code reads by column position in some places.
ARCHIVE_COLUMNS = [
    "timestamp_utc",   # when the AIS message was received
    "mmsi",            # ship's unique 9-digit ID
    "ship_name",       # may be empty until we get a ShipStaticData msg
    "ship_type",       # numeric AIS code (70=Cargo, 80=Tanker, etc.)
    "latitude",
    "longitude",
    "sog_knots",       # Speed Over Ground (knots)
    "cog_deg",         # Course Over Ground (degrees)
    "heading_deg",     # where the bow is pointing
    "nav_status",      # navigation status code
]


# ---------------------------------------------------------------------------
# LOAD THE LIST OF MMSIs WE WANT TO RECORD
# ---------------------------------------------------------------------------
def load_allowed_mmsis(operator: str) -> set:
    """
    Build a set of MMSIs to keep, based on the --operator argument.

    A "set" is a Python collection that's optimized for fast membership
    checks ("is this MMSI in the set?"). Better than a list when we have
    many lookups.
    """
    # Special case: record EVERY operator's vessels
    if operator == "all":
        all_mmsis = set()
        for op in config.SINGAPORE_OPERATORS:
            mmsi_path = config.OPERATORS_DIR / op["key"] / "vessel_mmsi.csv"
            if mmsi_path.exists():
                df = pd.read_csv(mmsi_path)
                # Only keep rows where MMSI is filled in
                mmsis = df["mmsi"].dropna().astype(int).tolist()
                all_mmsis.update(mmsis)
                if mmsis:
                    print(f"  {op['key']}: {len(mmsis)} MMSI(s)")
        return all_mmsis

    # Normal case: one operator
    mmsi_path = config.OPERATORS_DIR / operator / "vessel_mmsi.csv"
    if not mmsi_path.exists():
        sys.exit(
            f"\nNo vessel_mmsi.csv for operator '{operator}'.\n"
            f"Run first: python 01_resolve_mmsi.py --operator {operator}\n"
        )
    df = pd.read_csv(mmsi_path)
    mmsis = set(df["mmsi"].dropna().astype(int).tolist())
    if not mmsis:
        sys.exit(
            f"\n{mmsi_path.name} has no MMSIs filled in.\n"
            f"Open it in Excel, type the MMSIs, save, then re-run.\n"
        )
    return mmsis


# ---------------------------------------------------------------------------
# DAILY ARCHIVE WRITER
# ---------------------------------------------------------------------------
class DailyArchiveWriter:
    """
    Writes AIS rows to a CSV file. When the UTC date rolls over, it
    automatically closes the old file and opens a new one for the new
    day.

    A "class" in Python is a way to bundle data (the file handle, the
    current date) and behavior (write a row, close) together.
    """

    def __init__(self, archive_dir: Path):
        # __init__ is the "constructor" — runs when you create the object
        self.archive_dir = archive_dir
        self._current_date = None    # what date the current file is for
        self._fh = None              # file handle (None when no file open)
        self._writer = None          # CSV writer object
        self._rows_since_flush = 0

    def _path_for(self, d) -> Path:
        """Return the file path we'd use for date d."""
        return self.archive_dir / f"ais_{d.isoformat()}.csv"

    def _rotate_if_needed(self):
        """
        Check if the UTC date has changed. If it has, close the old
        file and open a new one for the new date.
        """
        today = datetime.now(timezone.utc).date()
        if today == self._current_date:
            return  # still the same day, nothing to do

        # Close the old file (if there is one)
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()

        # Open the new day's file in "append" mode (create if missing,
        # add to end if already exists).
        path = self._path_for(today)
        is_new_file = not path.exists()
        self._fh = open(path, "a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)

        # If this is a brand-new file, write the column header row first.
        if is_new_file:
            self._writer.writerow(ARCHIVE_COLUMNS)

        self._current_date = today
        self._rows_since_flush = 0
        print(f"[{datetime.now(timezone.utc).isoformat()}] "
              f"Writing to {path.name}")

    def write(self, row: list):
        """Write one row of AIS data."""
        self._rotate_if_needed()
        self._writer.writerow(row)
        self._rows_since_flush += 1

        # Periodically force the data from memory to disk.
        # Without this, recent data sits in a buffer and would be
        # lost if the process is killed.
        if self._rows_since_flush >= FLUSH_EVERY_N_MESSAGES:
            self._fh.flush()
            self._rows_since_flush = 0

    def close(self):
        """Cleanly shut down the writer."""
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None


# ---------------------------------------------------------------------------
# THE LISTENER (this is the actual network code)
# ---------------------------------------------------------------------------
# This function is "async" because we're using a websocket — a kind of
# always-open connection. async lets us wait for messages without
# blocking the rest of the program.
async def listen_forever(api_key: str,
                         allowed_mmsis: set,
                         writer: DailyArchiveWriter,
                         stop_after_s):
    """
    Connect to aisstream.io and forward all incoming messages to the
    writer. If the connection drops, wait 15 seconds and reconnect.
    Loop forever (or until stop_after_s seconds, if specified).
    """
    import websockets   # imported here so the module is optional

    # Build the subscription message that tells aisstream what we want.
    sub_msg = {
        "APIKey": api_key,
        "BoundingBoxes": [config.SINGAPORE_BBOX],
        # We only care about position reports (where ships are) and
        # static data (ship names). Filtering server-side saves bandwidth.
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    # If we have an MMSI filter, send it to the server so they can
    # pre-filter for us (less data over the wire).
    if allowed_mmsis:
        sub_msg["FiltersShipMMSI"] = [str(m) for m in allowed_mmsis]

    # Cache for ship names. ShipStaticData messages come every ~6 minutes,
    # but PositionReport messages come every few seconds. We remember
    # the name from static data so we can attach it to position reports.
    static_cache = {}     # {mmsi: {"name": ..., "type": ...}}
    n_kept_total = 0
    loop_start_time = time.time()

    # The OUTER loop handles reconnection. Each iteration is one
    # successful connection (until it drops or we stop).
    while True:
        # Did we hit the optional time limit?
        if stop_after_s is not None:
            if (time.time() - loop_start_time) >= stop_after_s:
                print("Wall-clock limit reached. Stopping.")
                return

        try:
            # Open a websocket connection.
            # open_timeout=30 means "wait up to 30 seconds for the
            # connection to be established" (helps on slow internet).
            # ping_interval/ping_timeout keep the connection alive
            # by sending heartbeats.
            async with websockets.connect(
                config.AISSTREAM_URL,
                open_timeout=30,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=2**20,   # max message size = 1 MB
            ) as ws:
                # Send the subscription request
                await ws.send(json.dumps(sub_msg))
                print(f"[{datetime.now(timezone.utc).isoformat()}] "
                      f"Connected. Filtering to "
                      f"{len(allowed_mmsis) if allowed_mmsis else 'ALL'} MMSI(s).")

                # The INNER loop reads messages until the connection drops
                while True:
                    if stop_after_s is not None:
                        if (time.time() - loop_start_time) >= stop_after_s:
                            return

                    try:
                        # Wait up to 60s for a message
                        raw = await asyncio.wait_for(ws.recv(), timeout=60.0)
                    except asyncio.TimeoutError:
                        # No data for a minute? That's OK, keep waiting.
                        continue

                    # Try to parse the message as JSON.
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue   # malformed message, skip it

                    # Pull out the parts we care about.
                    mtype = msg.get("MessageType")
                    meta  = msg.get("MetaData", {})
                    body  = msg.get("Message", {})
                    mmsi  = meta.get("MMSI")
                    if mmsi is None:
                        continue

                    # Belt-and-braces filter (server should already filter,
                    # but check again on our side just in case).
                    if allowed_mmsis and int(mmsi) not in allowed_mmsis:
                        continue

                    # Type 1: ship name & type. Cache it for later use.
                    if mtype == "ShipStaticData":
                        s = body.get("ShipStaticData", {})
                        static_cache[int(mmsi)] = {
                            "name": (s.get("Name") or "").strip(),
                            "type": s.get("Type", 0),
                        }

                    # Type 2: position report. Write to disk.
                    elif mtype == "PositionReport":
                        p = body.get("PositionReport", {})
                        info = static_cache.get(int(mmsi), {})
                        writer.write([
                            meta.get("time_utc",
                                     datetime.now(timezone.utc).isoformat()),
                            mmsi,
                            info.get("name", meta.get("ShipName", "")),
                            info.get("type", 0),
                            p.get("Latitude"),
                            p.get("Longitude"),
                            p.get("Sog"),
                            p.get("Cog"),
                            p.get("TrueHeading"),
                            p.get("NavigationalStatus"),
                        ])
                        n_kept_total += 1
                        # Periodic progress print
                        if n_kept_total % 50 == 0:
                            print(f"  {n_kept_total} records written, "
                                  f"{len(static_cache)} vessel profiles cached")

        # Ctrl+C: re-raise so the outer cleanup runs
        except KeyboardInterrupt:
            raise
        # Any other error: wait and reconnect
        except Exception as e:
            wait = 15
            print(f"[{datetime.now(timezone.utc).isoformat()}] "
                  f"Connection lost ({type(e).__name__}: {e}). "
                  f"Reconnecting in {wait}s...")
            await asyncio.sleep(wait)


# ---------------------------------------------------------------------------
# MAIN ENTRY POINT
# ---------------------------------------------------------------------------
def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--operator", default=None,
                        help="Operator key (e.g. yorklaunch), or 'all' "
                             "for every known operator.")
    parser.add_argument("--no-filter", action="store_true",
                        help="Record every vessel in Singapore waters "
                             "(big files; only use for exploration).")
    parser.add_argument("--minutes", type=int, default=None,
                        help="Stop after N minutes (default: run forever).")
    args = parser.parse_args()

    # Sanity checks
    if config.AISSTREAM_API_KEY in ("", "PASTE_YOUR_KEY_HERE"):
        sys.exit(
            "\nNo AISSTREAM_API_KEY set.\n"
            "Either edit config.py, or run:\n"
            "  export AISSTREAM_API_KEY=your_key_here\n"
        )

    if args.no_filter:
        allowed = set()
        print("WARNING: recording every vessel in Singapore (no MMSI filter).")
    elif args.operator:
        allowed = load_allowed_mmsis(args.operator)
        if allowed:
            print(f"Will record {len(allowed)} MMSI(s).")
    else:
        sys.exit(
            "\nMust specify --operator <key> or --no-filter.\n"
            "Examples:\n"
            "  python 02_record_ais.py --operator yorklaunch\n"
            "  python 02_record_ais.py --operator all\n"
            "  python 02_record_ais.py --no-filter\n"
        )

    writer = DailyArchiveWriter(config.ARCHIVE_DIR)
    stop_after = args.minutes * 60 if args.minutes else None

    # Set up clean shutdown on Ctrl+C or SIGTERM (e.g. from `kill` command)
    def _shutdown(*_):
        print("\nShutdown requested. Flushing data to disk...")
        writer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        # asyncio.run starts the async event loop and runs our listener
        asyncio.run(listen_forever(
            config.AISSTREAM_API_KEY,
            allowed,
            writer,
            stop_after,
        ))
    finally:
        # No matter how the program ends, make sure data is saved
        writer.close()


if __name__ == "__main__":
    main()
