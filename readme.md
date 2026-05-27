The model I trained "f1_model1" can forecast the final result of a race by feeding in couple of laps of a race.
For instance, you can feed first 10 laps of Suzuka track into the model and check the prediction of the final result.
Of course this is just a ML model that is trained on a very limited data and it is by no means 100% accurate. 
No previous result garantee futrure results. Race conditions, new drivers, etc. can completely affect the model's results. 

## Setup

Requires Python 3.13 and [uv] (a fast Python package manager).

```bash
# 1. Install all dependencies into a local virtual environment
uv sync

# 2. Activate the environment
source .venv/bin/activate
```

That's it — you can now run any script or notebook in the project.

## Usage

```bash
# Build the per-race CSVs under data/laps/ and data/results/
# (safe to interrupt; safe to re-run — it skips races already saved
#  and stops cleanly if it hits FastF1's 500-calls/hour limit)
python pipeline.py
```

Re-run `python pipeline.py` whenever a new race is finished — it will only fetch newly completed races.

This repository will be updated depending on the free time I have.