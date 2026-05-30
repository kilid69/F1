"""Single source of truth for column names, shapes, and hyperparameters.

Everything else in the package (dataset.py, model.py, train.py) imports
from here. Tune the model by editing this file, not by sprinkling numbers
across the codebase.

The model is hierarchical:
    laps  ─► INNER LSTM ─► race_vec (per past race)
    10 race_vecs ─► OUTER LSTM ─► form_vec
    form_vec + upcoming_context ─► MLP ─► predicted position

So this config has TWO "sequence" settings (inner / outer) and TWO hidden
sizes, not one.
"""


# =============================================================================
# 1. COLUMN GROUPS
# =============================================================================
# These names must match the columns in your CSVs (data/laps/, data/results/).

# --- Per-lap features (one step of the INNER LSTM) --------------------------
# Built by helpers.build_session_laps -> saved in data/laps/<year>_R_<track>.csv
NUMERICAL_FEATURE_COLS: list[str] = [
    # pace
    "LapTime", "Sector1Time", "Sector2Time", "Sector3Time",
    # speed checkpoints
    "SpeedI1", "SpeedI2", "SpeedFL", "SpeedST",
    # stint / tyre / progress
    "LapNumber", "Stint", "TyreLife", "Position", "IsPersonalBest", "IsPitLap",
    # telemetry aggregates
    "RpmAvg", "RpmMin", "RpmMax",
    "SpeedAvg", "SpeedMedian", "SpeedMin", "SpeedMax",
    "ThrottleAvg", "ThrottleMin", "ThrottleMax",
    "nGearAvg", "nGearMin", "nGearMax", "nGearMode",
    "BrakeCount", "DrsCount",
    # weather
    "AirTemp", "Humidity", "Pressure", "Rainfall",
    "TrackTemp", "WindDirection", "WindSpeed",
]

# Categorical columns inside the per-lap row. Each one gets an nn.Embedding.
CATEGORICAL_COLS: list[str] = [
    "Driver", "Team", "Compound", "TrackStatus",
]


# --- Upcoming-race context (concatenated with form_vec before the MLP) ------
# Built by helpers.get_session_results -> saved in data/results/<...>.csv
UPCOMING_CONTEXT_NUMERICAL: list[str] = [
    "Practice1Pos", "Practice1Pace",
    "Practice2Pos", "Practice2Pace",
    "Practice3Pos", "Practice3Pace",
    # "GridPosition", This is for comparison later.
]
UPCOMING_CONTEXT_CATEGORICAL: list[str] = [
    "Track",
]

# Fallback for a missing upcoming-context value. Only kicks in for a race not
# yet patched with practice data — a patched race already carries the pipeline's
# 25/10 fallbacks. 0.0 would be wrong: position 0 = "ahead of pole", pace 0 =
# "fastest lap", i.e. the OPPOSITE of "nowhere". Mirrors pipeline's MISSING_*.
UPCOMING_CONTEXT_DEFAULTS: dict[str, float] = {
    "GridPosition": 20.0,
    "Practice1Pos": 25.0, "Practice1Pace": 10.0,
    "Practice2Pos": 25.0, "Practice2Pace": 10.0,
    "Practice3Pos": 25.0, "Practice3Pace": 10.0,
}


# --- Identity / target ------------------------------------------------------
DRIVER_ID_COL: str = "Driver"
RACE_ID_COLS: list[str] = ["Year", "Location"]
TARGET_COL: str = "FinalPosition"

# In the results CSVs, a driver who did not finish (retired / disqualified /
# withdrew / not classified) is stored as this sentinel instead of a real
# finishing position. dataset.py maps it to the dedicated DNF class.
# (helpers.get_session_results writes it; keep the two values in sync.)
DNF_SENTINEL: int = 99


# =============================================================================
# 2. EMBEDDING SIZES
# =============================================================================
# For each categorical column: (num_categories, embedding_dim).
# num_categories should be >= the largest ID you'll ever see for that column.
# embedding_dim is a tuning knob — bigger = more capacity, more parameters.
# Rule of thumb: dim ~ min(50, (num_categories + 1) // 2).
EMBEDDING_SIZES: dict[str, tuple[int, int]] = {
    "Driver":      (50,  8),   # ~45 unique drivers since 2018
    "Team":        (20,  4),   # ~15 unique teams across recent seasons
    "Compound":    (6,   2),   # SOFT/MEDIUM/HARD/INTERMEDIATE/WET (+ unknown)
    "TrackStatus": (10,  3),   # FastF1 codes 1..9
    "Track":       (40,  6),   # ~30 unique circuits in mappings.py
}


# =============================================================================
# 3. SHAPES
# =============================================================================

# --- Inner LSTM: reads laps within a race ----------
MAX_LAPS_PER_RACE: int = 78       # pad / truncate each race to this many laps

# --- Outer LSTM: reads a driver's recent races ----------
NUM_PAST_RACES: int = 10          # how many past races to feed per training sample

# --- Output ----------
NUM_DRIVERS: int = 23             # output classes: positions 1..22 -> 0..21, + 1 dedicated DNF class (22)


# =============================================================================
# 4. MODEL HYPERPARAMETERS
# =============================================================================

INNER_LSTM_HIDDEN: int = 64       # size of each race_vec
OUTER_LSTM_HIDDEN: int = 128      # size of form_vec
HEAD_HIDDEN: int = 64             # hidden width in the MLP head
DROPOUT: float = 0.3              # dropout rate inside the head (raised 0.2->0.3 to fight overfitting)


# =============================================================================
# 5. TRAINING HYPERPARAMETERS
# =============================================================================

BATCH_SIZE: int = 32
LEARNING_RATE: float = 1e-3
NUM_EPOCHS: int = 50              # an UPPER bound; early stopping usually stops us sooner
WEIGHT_DECAY: float = 1e-4        # L2 regularization (raised 1e-5->1e-4 to fight overfitting)

# Early stopping: if val_loss does not beat its best for this many epochs IN A
# ROW, stop training. Your val_loss bottomed at epoch 3 last run, so patience=5
# would have let it try a few more, confirmed no improvement, then stopped ~epoch 8
# instead of grinding uselessly to epoch 50.
PATIENCE: int = 5


# =============================================================================
# 6. PATHS
# =============================================================================

LAPS_DIR: str = "data/laps"
RESULTS_DIR: str = "data/results"

# These are just STAGING files in the working dir — each run overwrites them,
# then logs a COPY into MLflow, which keeps its own versioned copy per run
# (under mlruns/). So the overwrite here is harmless; MLflow is the archive.
CHECKPOINT_PATH: str = "f1_model.pt"  # PyTorch convention (.pt or .pth)
SCALERS_PATH: str = "scalers.pkl"     # the fitted StandardScalers — needed at inference

# MLflow groups runs under a named experiment so you can compare them.
MLFLOW_EXPERIMENT: str = "f1-position"


# =============================================================================
# 7. TRAIN / VALIDATION / TEST SPLIT
# =============================================================================

TRAIN_SET: list = range(2018, 2024)
VAL_SET: list = [2024]
TEST_SET: list = [2025]