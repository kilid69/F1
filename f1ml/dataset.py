"""Build training samples for the hierarchical next-race prediction model.

Each training sample is:

    INPUT  → one driver's last NUM_PAST_RACES races
             (each race is its own lap sequence)
    CONTEXT → upcoming race's grid position + track
    TARGET → that driver's actual FinalPosition in the upcoming race

The work is split across three layers:

    load_all_*()      : read every CSV from data/laps/ and data/results/
                        into combined DataFrames
    apply_mappings()  : turn string categoricals (Driver, Team, ...) into
                        integer IDs using mappings.py
    F1Dataset         : the PyTorch Dataset. Iterates over every
                        (driver, race) pair with enough history and
                        produces one training sample per __getitem__.
    collate_fn        : stacks a batch of samples into batched tensors.

The hierarchical model needs shapes like:

    past_laps_num     (B, R, L, F)     B=batch, R=10 past races,
                                        L=MAX_LAPS_PER_RACE, F=#numerical feats
    past_laps_cat     dict of (B, R, L) per categorical column
    past_laps_lens    (B, R)            actual lap count per past race
    upcoming_num      (B, n_up_num)
    upcoming_cat      dict of (B,) per categorical column
    target            (B,)              actual FinalPosition (int 0..NUM_DRIVERS-1)

Padding to MAX_LAPS_PER_RACE happens in __getitem__ so the collate_fn can
just stack — no ragged batches.
"""

import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import StandardScaler
from torch.utils.data import Dataset

import mappings
from . import config


# =============================================================================
# 1. Loading from disk
# =============================================================================

def load_all_laps(laps_dir=config.LAPS_DIR) -> pd.DataFrame:
    """Read every per-session laps CSV under ``laps_dir`` into one DataFrame.

    Filenames look like ``2024_R_Imola.csv`` or ``2024_R_Mexico_City.csv``.
    Year and Location aren't inside the CSVs themselves, only in the names,
    so we parse them out here and add them as columns.
    """
    laps_dir = Path(laps_dir)
    frames: list[pd.DataFrame] = []

    # Path.glob walks every matching file in the folder
    for csv_path in sorted(laps_dir.glob("*.csv")):
        # csv_path.stem is the filename without ".csv"
        # split with maxsplit=2 so "Mexico_City" stays as one piece
        year_str, session_type, track = csv_path.stem.split("_", 2)

        df = pd.read_csv(csv_path)

        # tag every row in this CSV with where it came from
        df["Year"] = int(year_str)
        df["SessionType"] = session_type
        # NFC normalize so accented names (Montréal) match mappings.py;
        # macOS stores filenames in a different (decomposed) unicode form
        df["Location"] = unicodedata.normalize("NFC", track.replace("_", " "))

        frames.append(df)

    # one big DataFrame: every lap from every race, identifiable by (Year, Location)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_all_results(results_dir=config.RESULTS_DIR) -> pd.DataFrame:
    """Read every per-session results CSV under ``results_dir`` into one frame.

    Same pattern as ``load_all_laps``. Each row is one driver's outcome at
    one race. Columns include ``Driver``, ``FinalPosition``, ``GridPosition``,
    ``Track``, ``Year``, ``Location``.
    """
    results_dir = Path(results_dir)
    frames: list[pd.DataFrame] = []

    for csv_path in sorted(results_dir.glob("*.csv")):

        # check load_all_laps() for comments
        year_str, session_type, track = csv_path.stem.split("_", 2)

        df = pd.read_csv(csv_path)

        df["Year"] = int(year_str)
        df["SessionType"] = session_type
        df["Location"] = unicodedata.normalize("NFC", track.replace("_", " "))

        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# =============================================================================
# 2. String → integer encoding
# =============================================================================

def apply_mappings(df: pd.DataFrame) -> pd.DataFrame:
    """Convert categorical string columns into integer IDs using mappings.py.

    Columns to map (only those present in ``df`` get mapped):
        Driver       -> mappings.drivers
        Team         -> mappings.teams
        Compound     -> mappings.compounds
        TrackStatus  -> mappings.track_status
        Track        -> mappings.tracks    (derived from Location)

    Why we do this here and not in the pipeline: keeping mappings at load
    time lets us update mappings.py without regenerating every CSV.
    """
    df = df.copy()

    # pair each categorical column with its lookup dict from mappings.py
    column_maps = {
        "Driver": mappings.drivers,
        "Team": mappings.teams,
        "Compound": mappings.compounds,
        "TrackStatus": mappings.track_status,
    }
    # .map() replaces every value with its dict lookup: "VER" -> 1, etc.
    # values not found in the dict become NaN (e.g. phantom/reserve drivers)
    for col, lookup in column_maps.items():
        if col in df.columns:
            df[col] = df[col].map(lookup)

    # Always derive Track from Location so it reflects the CURRENT mappings.py.
    # (a baked-in Track in older results CSVs can be stale if a track name was
    # added to mappings.py after that CSV was generated.)
    if "Location" in df.columns:
        df["Track"] = df["Location"].map(mappings.tracks)

    return df


# =============================================================================
# 3. The Dataset
# =============================================================================

class F1Dataset(Dataset):
    """Hierarchical dataset for predicting the next race's finishing position.

    One sample = one (driver, target_race) pair, where target_race's year is in
    ``target_years``. A sample exists only if the driver has at least
    ``NUM_PAST_RACES`` earlier races with lap data. The same race can appear in
    many samples — as the target for one driver and as history for many others.

    Expects ``laps`` and ``results`` to be ALREADY loaded, mapped, and (for the
    numerical lap features) scaled — see ``build_datasets`` which does that once.
    """

    def __init__(self, laps: pd.DataFrame, results: pd.DataFrame, target_years):
        """Group laps per (driver, race) and build the list of training samples.

        :param laps: loaded + mapped + scaled lap rows (one row per driver-lap).
        :param results: loaded + mapped result rows (one row per driver-race).
        :param target_years: iterable of years whose races are prediction targets.
        """
        target_years = set(target_years)

        num_cols = config.NUMERICAL_FEATURE_COLS
        cat_cols = config.CATEGORICAL_COLS

        # ---- 1. group every (driver, race) into its own lap array ----
        # lap_groups[(driver, year, round)] = (num_array, {cat: array}, n_laps)
        # Stored once per race, so a race shared across many samples isn't duplicated.
        self.lap_groups: dict = {}
        for (driver, year, rnd), g in laps.groupby(
            [config.DRIVER_ID_COL, "Year", "RoundNumber"]
        ):
            g = g.sort_values("LapNumber")
            # convert pd.Dataframe to numpy array -> shape: (number_of_laps, number_of_numerical_columns)
            num = np.nan_to_num(g[num_cols].to_numpy(dtype=np.float32))
            # make the categorical columns dict 
            # {"Driver": [1, 1, 1, ..., 1] (lap length, all 1 = VER), 
            #   "Team": same as driver, 
            #   "Compound": [2, 2, 2, 3, 3, ..] (lap length, tyre per lap), 
            #   "TrackStatus": same as Compound}
            cat = {c: g[c].fillna(0).astype(np.int64).to_numpy() for c in cat_cols}
            # add to the lookup dict
            self.lap_groups[(int(driver), int(year), int(rnd))] = (num, cat, len(g))

        # ---- 2. for each driver, the chronological list of races they have laps for ----
        driver_lap_races: dict = {}
        # we want something like this:
        # driver_lap_races = {
        #     1: [(2024, 6), (2024, 7), (2023, 22)],   # all of VER's races
        #     2: [(2024, 6), (2024, 7)],               # all of HAM's races
        # }
        for (driver, year, rnd) in self.lap_groups:
            driver_lap_races.setdefault(driver, []).append((year, rnd))
        for driver in driver_lap_races:
            driver_lap_races[driver].sort()  # (year, round) sorts chronologically

        # ---- 3. build one sample per (driver, target race) with enough history ----
        # Each row of `results` = one driver's outcome in one race. We turn each
        # eligible row into a training sample meaning: "for THIS driver, using
        # their last 10 races, predict their finish in THIS target race."
        #
        # one sample we want to produce (VER, target = 2024 round 8):
        # {
        #   "past":   [(1,2023,22),(1,2024,1), ... ,(1,2024,7)]  # 10 keys into lap_groups
        #   "grid":   2.0    # where VER qualified for round 8
        #   "track":  17     # round 8's track id
        #   "target": 0      # VER finished 1st -> class index 0
        # }
        self.samples: list[dict] = []
        for _, row in results.iterrows():
            # only races in the target years count (this is the train/val/test split)
            if row["Year"] not in target_years:
                continue

            driver = int(row[config.DRIVER_ID_COL])                   # e.g. 1 (VER)
            target_key = (int(row["Year"]), int(row["RoundNumber"]))  # e.g. (2024, 8)

            # keep only this driver's races that happened BEFORE the target race.
            # (year, round) tuples compare in time order: (2023,22) < (2024,1) < (2024,8)
            history = [k for k in driver_lap_races.get(driver, []) if k < target_key]

            # need a full window of 10 prior races, otherwise skip this sample
            if len(history) < config.NUM_PAST_RACES:
                continue
            past = history[-config.NUM_PAST_RACES:]  # the 10 most recent before the target

            # --- upcoming-race context (things known BEFORE the race starts) ---
            up_num = [
                config.UPCOMING_CONTEXT_DEFAULTS.get(col, 0.0) if pd.isna(row[col]) else float(row[col])
                for col in config.UPCOMING_CONTEXT_NUMERICAL
            ]
            track = int(row["Track"]) if not pd.isna(row["Track"]) else 0  # e.g. 17

            # --- target: the answer the model learns ---
            # Real finishes 1..22 -> 0-based classes 0..21 (CrossEntropy wants 0-based).
            # A DNF is stored as DNF_SENTINEL (99) or is non-numeric ('W'); it goes to
            # the dedicated last class (NUM_DRIVERS - 1 = 22), NOT a real position.
            # The value 99 is only a label here — it never enters the network as a
            # number, so its size doesn't matter (the target isn't a feature).
            pos = pd.to_numeric(row[config.TARGET_COL], errors="coerce")
            if pd.isna(pos) or int(pos) >= config.DNF_SENTINEL:
                target = config.NUM_DRIVERS - 1      # dedicated DNF class (= 22)
            else:
                target = int(pos) - 1                # P1 -> 0, P22 -> 21
            target = max(0, min(target, config.NUM_DRIVERS - 1))  # safety clamp

            # store only the KEYS for history (not the lap arrays themselves);
            # __getitem__ looks the arrays up in lap_groups and pads them on demand
            self.samples.append({
                "past": [(driver, y, r) for (y, r) in past],
                "upcoming_num": up_num,
                "track": track,
                "target": target,
            })

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx) -> dict:
        """Expand recipe #idx into ONE fixed-shape, fully-padded training example.

        PyTorch's DataLoader calls this with idx = 0, 1, 2, ... to fetch samples
        one at a time, then stacks a batch of them (see collate_fn). Stacking only
        works if every sample has the SAME shape, so we pad each race out to L laps
        with zeros right here.
        """
        s = self.samples[idx]               # the lightweight recipe built in __init__
        num_cols = config.NUMERICAL_FEATURE_COLS
        cat_cols = config.CATEGORICAL_COLS
        R = config.NUM_PAST_RACES           # 10 -> past races per sample
        L = config.MAX_LAPS_PER_RACE        # 78 -> force every race to this many laps
        F = len(num_cols)                   # 37 -> numerical features per lap

        # ---- 1. empty zero-filled shelves; the zeros ARE the padding ----
        # num is a 3D box: 10 races x 78 laps x 37 features. Lap slots we never
        # fill stay 0. lens will record how many laps were REAL per race, so the
        # model can later ignore the zero-padding.
        num = np.zeros((R, L, F), dtype=np.float32)                   # (10, 78, 37)
        cat = {c: np.zeros((R, L), dtype=np.int64) for c in cat_cols}  # each (10, 78)
        lens = np.zeros(R, dtype=np.int64)          # placeholder:  (10,) -> [0,0,0,0,0,0,0,0,0,0]

        # ---- 2. drop each of the 10 past races into its row, leaving the rest 0 ----
        # s["past"] = [(1,2023,22), (1,2024,1), ... ] -> 10 keys into lap_groups.
        # Example: race i=0 (say Monza, 53 laps) ->
        #   num[0, :53, :] = its real 53 laps, num[0, 53:78, :] stays 0, lens[0]=53
        for i, key in enumerate(s["past"]):
            g_num, g_cat, n_laps = self.lap_groups[key]   # that race's real arrays
            take = min(n_laps, L)           # if a race had >78 laps (rare), keep 78
            num[i, :take, :] = g_num[:take, :]  # g_num -> first for number_of_laps, second for column (features)
            for c in cat_cols:
                cat[c][i, :take] = g_cat[c][:take]
            lens[i] = take                  # real lap counts, e.g. [53, 78, 44, ...]

        # ---- 3. package one example as tensors: the inputs + the answer ----
        return {
            "past_laps_num": torch.from_numpy(num),       # (10, 78, 37) lap features
            "past_laps_cat": {c: torch.from_numpy(cat[c]) for c in cat_cols},  # each (10,78) ids
            "past_laps_lens": torch.from_numpy(lens),     # (10,) real lap count per race
            # upcoming numerical context (practice pos/pace), already scaled in
            # build_datasets; shape (len(UPCOMING_CONTEXT_NUMERICAL),) e.g. (6,)
            "upcoming_num": torch.tensor(s["upcoming_num"], dtype=torch.float32),
            "upcoming_cat": {"Track": torch.tensor(s["track"], dtype=torch.long)},  # () track id
            "target": torch.tensor(s["target"], dtype=torch.long),  # () the answer, class 0..21
        }


# =============================================================================
# 4. Batch collate
# =============================================================================

def collate_fn(batch: list[dict]) -> dict:
    """Stack a list of per-sample dicts into one batched dict.

    Every sample already has identical shapes (``__getitem__`` padded them),
    so this is just ``torch.stack`` with a new leading batch dimension.
    """
    cat_cols = config.CATEGORICAL_COLS
    up_cat_cols = config.UPCOMING_CONTEXT_CATEGORICAL
    return {
        "past_laps_num": torch.stack([s["past_laps_num"] for s in batch]),
        "past_laps_cat": {
            c: torch.stack([s["past_laps_cat"][c] for s in batch]) for c in cat_cols
        },
        "past_laps_lens": torch.stack([s["past_laps_lens"] for s in batch]),
        "upcoming_num": torch.stack([s["upcoming_num"] for s in batch]),
        "upcoming_cat": {
            c: torch.stack([s["upcoming_cat"][c] for s in batch]) for c in up_cat_cols
        },
        "target": torch.stack([s["target"] for s in batch]),
    }


# =============================================================================
# 5. Factory — load + map + scale ONCE, then build the temporal splits
# =============================================================================

def build_datasets(train_years, val_years, test_years,
                   laps_dir=config.LAPS_DIR, results_dir=config.RESULTS_DIR):
    """Build train / val / test datasets from the cached CSVs.

    Reads and encodes the CSVs a single time, fits TWO scalers on the training
    years only (then applies them to all data), and returns the three datasets
    plus both fitted scalers (keep them for inference):
      * lap_scaler      -> the per-lap NUMERICAL_FEATURE_COLS
      * upcoming_scaler -> the UPCOMING_CONTEXT_NUMERICAL cols (practice pos/pace)

    :return: (train_ds, val_ds, test_ds, lap_scaler, upcoming_scaler)
    """
    laps = apply_mappings(load_all_laps(laps_dir))
    results = apply_mappings(load_all_results(results_dir))

    if "RoundNumber" not in laps.columns or "RoundNumber" not in results.columns:
        raise ValueError(
            "CSVs are missing 'RoundNumber' — run add_round_numbers.py first."
        )

    num_cols = config.NUMERICAL_FEATURE_COLS
    train_mask = laps["Year"].isin(set(train_years))

    # fill NaN numerical features with TRAINING-set column average (no leakage),
    # then fit the lap scaler on training laps and apply it to ALL laps
    fill_values = laps.loc[train_mask, num_cols].mean()
    laps = laps.copy()
    laps[num_cols] = laps[num_cols].fillna(fill_values)

    lap_scaler = StandardScaler()
    lap_scaler.fit(laps.loc[train_mask, num_cols])
    laps[num_cols] = lap_scaler.transform(laps[num_cols])

    # ---- scale the upcoming-race context the same way (fit on train only) ----
    # These columns live in `results` (one row per driver-race) and sit on very
    # different scales from the lap features (positions ~1-25, pace ~0-10). We
    # standardize them too, otherwise a raw "25" would dwarf the LSTM's form_vec
    # (~ -1..1) when the two are concatenated before the MLP head.
    up_num_cols = config.UPCOMING_CONTEXT_NUMERICAL
    results = results.copy()

    # a race not yet patched with practice data won't have these columns at all;
    # create them as NaN so the fill + scale below always has something to act on
    for col in up_num_cols:
        if col not in results.columns:
            results[col] = np.nan

    # missing means "nowhere", not "average" -> fill with the per-column defaults
    # (25 / 10) BEFORE scaling, so the fallback lands in the same scaled space.
    results[up_num_cols] = results[up_num_cols].fillna(config.UPCOMING_CONTEXT_DEFAULTS)

    results_train_mask = results["Year"].isin(set(train_years))
    upcoming_scaler = StandardScaler()
    upcoming_scaler.fit(results.loc[results_train_mask, up_num_cols])
    results[up_num_cols] = upcoming_scaler.transform(results[up_num_cols])

    train_ds = F1Dataset(laps, results, train_years)
    val_ds = F1Dataset(laps, results, val_years)
    test_ds = F1Dataset(laps, results, test_years)
    return train_ds, val_ds, test_ds, lap_scaler, upcoming_scaler
