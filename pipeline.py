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
import shutil
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

# Pre-qualifying pace signals (practice / sprint) go on the results CSV as
# context that exists BEFORE the grid is set — unlike GridPosition, which we
# keep as a baseline but which leaks the answer. We always store exactly 3
# sessions' worth. A driver who set no lap (crash / no run) or a session that
# never happened falls back to "effectively last / nowhere".
CONTEXT_SLOTS = 3           # always store 3 pre-quali sessions per weekend
# fallbacks are set CLEARLY beyond any real lap: real practice pace tops out
# around 6-7%, and real positions stop at the field size (~20). So 10% / P25
# means "didn't set a lap" can't be mistaken for "ran, just slow".
MISSING_POS = 25.0          # no fastest lap -> beyond the real field
MISSING_PACE = 10.0         # no fastest lap -> 10% off = unambiguously "nowhere"


# ----------------------------- folders ---------------------------------------

DATA_DIR = Path("data")
LAPS_DIR = DATA_DIR / "laps"
RESULTS_DIR = DATA_DIR / "results"
FASTF1_CACHE = Path(".fastf1_cache")  # delete this folder by hand after bulk backfill is done

# make sure output folders exist
LAPS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# wipe any leftovers from previous runs before enabling the cache,
# so we start each run with a clean .fastf1_cache/ folder
if FASTF1_CACHE.exists():
    shutil.rmtree(FASTF1_CACHE)
FASTF1_CACHE.mkdir()
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


def pick_context_sessions(event) -> list[str]:
    """Choose the (up to) 3 pre-qualifying sessions of a weekend.

    We want pace signals that exist BEFORE the race grid is set, so we take
    every session that isn't the grid-setting ``Qualifying`` or the ``Race``
    itself. Across every weekend format this happens to leave exactly three:

        conventional weekend   -> ["Practice 1", "Practice 2", "Practice 3"]
        sprint weekend (2024+) -> ["Practice 1", "Sprint Qualifying", "Sprint"]
        sprint weekend (2023)  -> ["Practice 1", "Sprint Shootout", "Sprint"]
        sprint weekend (21-22) -> ["Practice 1", "Practice 2", "Sprint"]

    Heads-up: in 2021-2023 the Sprint result effectively SET the race grid, so
    for those few weekends "Sprint" pace is close to a grid leak. We keep it for
    a consistent 3-slot shape (and GridPosition stays as its own baseline column
    anyway, so the leak is reproducible/removable later).

    :param event: one row of ``ff1.get_event_schedule`` — a pandas Series with
        ``Session1`` .. ``Session5`` columns naming each session.
    """
    exclude = {"Qualifying", "Race"}
    names: list[str] = []
    for i in range(1, 6):
        name = event.get(f"Session{i}")
        if pd.notna(name) and str(name) not in exclude:
            names.append(str(name))
    return names[:CONTEXT_SLOTS]


def session_pace_and_position(year: int, round_number: int, session_name: str) -> dict:
    """Rank drivers by their single fastest lap in one early session.

    Loads the session WITHOUT telemetry (much cheaper — we only need lap times,
    not the 100-Hz car data), then turns each driver's best lap into two numbers:

        position : rank by fastest lap, 1 = quickest
        pace     : how far behind the session's fastest lap, in PERCENT
                   (percent, not seconds, so short Monaco and long Spa compare
                   fairly — 0.2s means more at Monaco than at Spa)

    Returns a dict keyed by the 3-letter driver code, e.g. FP3 at Suzuka:
        {
          "VER": (1, 0.00),   # set the session's fastest lap -> P1, 0% off
          "NOR": (2, 0.21),   # 0.21% slower than VER's best
          "LEC": (3, 0.48),
        }
    A driver who set no lap is simply absent (the caller fills the fallback).
    """
    sess = ff1.get_session(year, round_number, session_name)
    sess.load(telemetry=False, weather=False, messages=False)

    laps = sess.laps
    # drop laps whose time was deleted (track limits) so a bogus-fast lap can't
    # steal pole; guard in case the column is absent on some older sessions
    if "Deleted" in laps.columns:
        laps = laps[laps["Deleted"] != True]

    # each driver's single fastest lap; .min() skips the NaT in/out laps
    best = laps.groupby("Driver")["LapTime"].min().dropna()
    if best.empty:
        return {}

    fastest = best.min()                            # the session's overall best
    # Timedelta / Timedelta -> plain float ratio, *100 -> percent
    pace = (best - fastest) / fastest * 100.0       # 0.189s / 89.512s -> 0.21
    position = best.rank(method="min").astype(int)  # 1 = fastest, ties share rank

    return {drv: (int(position[drv]), float(pace[drv])) for drv in best.index}


def add_context_features(results: pd.DataFrame, year: int, round_number: int, event) -> pd.DataFrame:
    """Attach pre-qualifying pace/position columns onto the race results frame.

    For each of the 3 chosen sessions we add two columns, so every driver row
    gains six new fields:

        Practice1Pos, Practice1Pace,
        Practice2Pos, Practice2Pace,
        Practice3Pos, Practice3Pace

    Concretely a McLaren row might end up:
        Driver=NOR  Practice1Pos=2  Practice1Pace=0.21  Practice2Pos=1 ...

    A session that doesn't exist, or a driver with no lap, gets the MISSING_*
    fallback. ``RateLimitExceededError`` is re-raised so ``main`` can stop
    cleanly; any other per-session error just fills that slot with fallbacks.
    """
    session_names = pick_context_sessions(event)

    for i in range(CONTEXT_SLOTS):
        pos_col = f"Practice{i + 1}Pos"
        pace_col = f"Practice{i + 1}Pace"

        per_driver: dict = {}
        if i < len(session_names):
            try:
                per_driver = session_pace_and_position(year, round_number, session_names[i])
            except fastf1.exceptions.RateLimitExceededError:
                raise  # let main() catch this and stop the whole run cleanly
            except Exception as e:
                print(f"     (skipping {session_names[i]} context: {e})")

        # split the (pos, pace) tuples into two driver->value lookups
        # per_driver = {"VER": (1, 0.00), "NOR": (2, 0.21)} ->
        #   pos_map  = {"VER": 1,   "NOR": 2}
        #   pace_map = {"VER": 0.0, "NOR": 0.21}
        pos_map = {drv: v[0] for drv, v in per_driver.items()}
        pace_map = {drv: v[1] for drv, v in per_driver.items()}

        # map onto the race's Driver column; drivers not in the map -> fallback
        results[pos_col] = results["Driver"].map(pos_map).fillna(MISSING_POS)
        results[pace_col] = results["Driver"].map(pace_map).fillna(MISSING_PACE)

    return results


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
        ff1.Cache.clear_cache(deep=True)
        return False
    results = helpers.get_session_results(session)

    # 4b. attach pre-qualifying pace signals (practice / sprint) as context
    #     columns. these load 3 extra sessions with telemetry OFF, so they're
    #     cheap; they share this weekend's cache and get wiped with it in step 6.
    results = add_context_features(results, year, round_number, event)

    # stamp RoundNumber on both frames so we can order races chronologically later
    per_lap["RoundNumber"] = round_number
    results["RoundNumber"] = round_number

    # 5. write (atomic — partial writes don't leave half-formed CSVs)
    safe_to_csv(per_lap, laps_path)
    safe_to_csv(results, results_path)
    print(
        f"  -> wrote {laps_path} ({len(per_lap)} rows) "
        f"and {results_path} ({len(results)} rows)"
    )

    # 6. clear the cache so disk only ever holds ONE race's raw data
    ff1.Cache.clear_cache(deep=True)
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
                    ff1.Cache.clear_cache(deep=True)  # don't leave a partial download on disk
                    continue

                if did_work:
                    time.sleep(SLEEP_BETWEEN)
        print(f"========== Year {year} done ==========")


if __name__ == "__main__":
    main()
