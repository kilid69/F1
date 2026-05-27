"""Build the per-session cached dataset for the next-race prediction model.

Pulls every race from FastF1, runs it through the cleaning helpers
(``clean_laps``, ``clean_telemetry``, ``aggregate_lap``, ``build_session_laps``),
and writes two CSVs per session:

  data/laps/<year>_<session>_<track>.csv     -- one row per (driver, lap)
  data/results/<year>_<session>_<track>.csv  -- one row per driver

Re-runs are safe and incremental:
  * sessions whose CSVs already exist are skipped, and
  * sessions whose race date is still in the future are skipped.
So during the season, just rerun this script — it will pick up any newly
completed race and ignore everything else.

About FastF1's cache (``.fastf1_cache/``):
    Enabled briefly per session, then cleared. The cache exists only while
    one race is being processed; after the two CSVs are written, the cache
    is wiped before moving on. Net result: disk holds at most one race's
    raw data at a time, never more.

Run with:  python pipeline.py
"""

import logging
import time
from datetime import datetime
from pathlib import Path

import fastf1 as ff1
import fastf1.exceptions
import pandas as pd
from tqdm import tqdm

import helpers

# silence FastF1's chatty INFO logs (keep WARNING + ERROR)
logging.getLogger("fastf1").setLevel(logging.WARNING)


# ----------------------------- config ----------------------------------------

# Default to the current season. Replace with e.g. list(range(2018, 2027))
# when you want to pull historical seasons in bulk.
YEARS = list(range(2018, datetime.now().year + 1))
SESSIONS = ["R"]            # "R" = race; "Q", "FP1", "FP2", "FP3" also valid
SLEEP_BETWEEN = 5           # seconds between sessions — be polite to data servers


# ----------------------------- folders ---------------------------------------

DATA_DIR = Path("data")
LAPS_DIR = DATA_DIR / "laps"
RESULTS_DIR = DATA_DIR / "results"
FASTF1_CACHE = Path(".fastf1_cache")  # delete this folder by hand after bulk backfill is done

# make sure output folders exist
LAPS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
FASTF1_CACHE.mkdir(exist_ok=True)
ff1.Cache.enable_cache(str(FASTF1_CACHE))


# ----------------------------- helpers ---------------------------------------

def cache_paths(year: int, session_type: str, track_name: str):
    """Return the two CSV paths for one session: (laps_path, results_path)."""
    base = f"{year}_{session_type}_{track_name}"
    return LAPS_DIR / f"{base}.csv", RESULTS_DIR / f"{base}.csv"


def safe_to_csv(df, path):
    """Write a DataFrame to CSV atomically.

    Writes to ``<path>.tmp`` first, then renames to ``<path>`` once the write
    has fully completed. The rename is atomic, so an interruption either
    leaves the final file fully written or never created — never half-written.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def process_event(year: int, session_type: str, event) -> bool:
    """Process one event row from a season schedule.

    The schedule row already gives us the track name AND the event date, so
    we can do BOTH "is it cached?" and "has it happened?" checks BEFORE
    calling FastF1 again. Only when neither short-circuit fires do we make
    the actual download API call.

    :param event: one row of the DataFrame returned by ``ff1.get_event_schedule``.
        Required columns: ``RoundNumber``, ``Location``, ``EventDate``.
    :return: True if new files were written, False if skipped.
    """
    round_number = int(event["RoundNumber"])
    track_name = str(event["Location"]).replace(" ", "_")

    # 1. skip if both files already exist on disk (NO API call needed)
    laps_path, results_path = cache_paths(year, session_type, track_name)
    if laps_path.exists() and results_path.exists():
        print(f"  -> skip (cached): {year} {session_type} {track_name}")
        return False

    # 2. skip if the race hasn't happened yet (uses schedule's EventDate)
    event_dt = pd.Timestamp(event["EventDate"])
    now = pd.Timestamp.now(tz=event_dt.tz) if event_dt.tz else pd.Timestamp.now()
    if event_dt > now:
        print(f"  -> not yet held: {year} {session_type} {track_name} ({event_dt.date()})")
        return False

    # 3. now make the actual download — this is what costs API calls
    session = ff1.get_session(year, round_number, session_type)
    session.load()

    # 4. clean
    per_lap = helpers.build_session_laps(session)
    if per_lap.empty:
        print(f"  -> empty per-lap data for {year} {session_type} {track_name}")
        ff1.Cache.clear_cache()
        return False
    results = helpers.get_session_results(session)

    # 5. write (atomic — partial writes don't leave half-formed CSVs)
    safe_to_csv(per_lap, laps_path)
    safe_to_csv(results, results_path)
    print(
        f"  -> wrote {laps_path} ({len(per_lap)} rows) "
        f"and {results_path} ({len(results)} rows)"
    )

    # 6. clear the cache so disk only ever holds ONE race's raw data
    ff1.Cache.clear_cache()
    return True


# ----------------------------- main loop -------------------------------------

def main():
    for year in YEARS:
        print(f"\n========== Year {year} ==========")

        # ONE call per year — gives us every race in that season at once.
        # No per-race metadata lookups needed.
        try:
            schedule = ff1.get_event_schedule(year, include_testing=False)
        except Exception as e:
            print(f"  -> couldn't fetch schedule for {year}: {e}")
            continue

        for session_type in SESSIONS:
            for _, event in tqdm(
                schedule.iterrows(),
                total=len(schedule),
                desc=f"{year} {session_type}",
            ):
                try:
                    did_work = process_event(year, session_type, event)
                except fastf1.exceptions.RateLimitExceededError:
                    # FastF1 hit its 500-calls/hour limit. Don't keep hammering;
                    # stop now and let the user resume after the limit window resets.
                    print(
                        "\n  ✗ FastF1 rate limit hit (500 calls/h).\n"
                        "    Wait ~1 hour, then re-run — the CSV cache will resume\n"
                        "    where this stopped without re-downloading completed sessions."
                    )
                    return
                except Exception as e:
                    # catch-all so one bad race doesn't kill the whole loop
                    print(f"  -> error on {year} {session_type} round {event.get('RoundNumber','?')}: {e}")
                    ff1.Cache.clear_cache()  # don't leave a partial download on disk
                    continue

                if did_work:
                    time.sleep(SLEEP_BETWEEN)
        print(f"========== Year {year} done ==========")


if __name__ == "__main__":
    main()
