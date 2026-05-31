"""Inference: load the trained model and predict finishing positions.

Training LEARNS the weights and saves the best ones to config.CHECKPOINT_PATH.
This file is only the prediction.

It does two things:
    1. SCORE the model on the 2025 test set — races held out of BOTH training
       and validation, so this is the most honest number we have.
    2. SHOW a handful of individual predictions in human terms: which driver,
       the model's top-3 guessed positions (with confidence), and the actual.

A note on the labels. The network speaks in 0-based class ids; humans speak in
positions. The map is:
    class 0  -> "P1"        (finished 1st)
    class 21 -> "P22"       (finished 22nd)
    class 22 -> "DNF"       (the dedicated did-not-finish slot, NUM_DRIVERS-1)

Run with:  python -m f1ml.predict
"""

import torch
from torch.utils.data import DataLoader

import mappings

from . import config
from . import dataset
from .model import F1Net
# reuse the EXACT scoring + device-moving helpers training used, so test numbers
# are measured identically to the val numbers we saw during training.
from .train import evaluate, to_device


# id -> driver code, e.g. {1: "VER", 2: "HAM", ...}. mappings.drivers is the
# other way round (name -> id), so we flip it once here.
ID_TO_DRIVER = {driver_id: name for name, driver_id in mappings.drivers.items()}


def class_to_label(c: int) -> str:
    """Turn a 0-based class id into a human label ("P1", "P22", or "DNF")."""
    if c == config.NUM_DRIVERS - 1:
        return "DNF"
    return f"P{c + 1}"          # class 0 -> P1, class 5 -> P6


def load_model(device):
    """Rebuild F1Net and pour the saved weights into it.

    ``state_dict`` is just a dict {layer name -> learned numbers}. We make a
    fresh model with the SAME architecture, then load those numbers into it.
    ``model.eval()`` turns dropout OFF — at prediction time we want the full,
    deterministic network, not the randomly-thinned training version.
    """
    model = F1Net().to(device)
    state = torch.load(config.CHECKPOINT_PATH, map_location=device)
    model.load_state_dict(state)
    model.eval()
    return model


@torch.no_grad()   # no gradients anywhere in here — we are only predicting
def predict():
    device = ("cuda" if torch.cuda.is_available()
              else "mps" if torch.backends.mps.is_available() else "cpu")

    # Build datasets exactly as training did and take the 2025 TEST split.
    # build_datasets re-fits the scalers on the SAME train data, so test_ds is
    # scaled identically to what the saved model expects — no mismatch.
    _train, _val, test_ds, _lap_sc, _up_sc = dataset.build_datasets(
        config.TRAIN_SET, config.VAL_SET, config.TEST_SET)

    model = load_model(device)
    criterion = torch.nn.CrossEntropyLoss()

    # ---- 1. score the whole 2025 test set ----
    test_loader = DataLoader(test_ds, batch_size=config.BATCH_SIZE,
                             shuffle=False, collate_fn=dataset.collate_fn)
    m = evaluate(model, test_loader, criterion, device)
    print(f"{", ".join([str(y) for y in config.TEST_SET])} TEST ({len(test_ds)} samples):  "
          f"loss={m['loss']:.4f}  acc={m['acc']:.3f}  top3={m['top3']:.3f}")

    # ---- 2. show a few individual predictions ----
    print("\nindividual predictions  (driver | model's top-3 guesses | actual):")
    for i in range(min(10, len(test_ds))):
        sample = test_ds[i]                         # the padded tensors for sample i
        # the driver id is the same across all of this sample's history races,
        # so grab it from the recipe's first (driver, year, round) key.
        driver_id = test_ds.samples[i]["past"][0][0]
        driver = ID_TO_DRIVER.get(driver_id, f"id{driver_id}")

        batch = dataset.collate_fn([sample])        # wrap as a batch of size 1
        batch = to_device(batch, device)
        logits = model(batch)                       # (1, 23) raw scores

        # softmax turns raw scores into probabilities that sum to 1, so we can
        # report a confidence %. topk grabs the 3 highest-probability classes.
        probs = logits.softmax(dim=-1)[0]           # (23,)
        top_p, top_c = probs.topk(3)                # 3 probs, 3 class ids
        guesses = "  ".join(
            f"{class_to_label(int(c))} ({p * 100:.0f}%)"
            for p, c in zip(top_p.tolist(), top_c.tolist())
        )
        actual = class_to_label(int(sample["target"].item()))
        print(f"  {driver:>4}  |  {guesses:<34}  |  actual {actual}")


@torch.no_grad()
def predict_race(year: int, round_number: int):
    """Predict EVERY driver's finishing position for one specific race.

    A race is many samples — one per driver who lined up with at least 10 prior
    races of history. We find those samples (by the year/round we stamped on each
    recipe), run the model on each, and print the driver, the model's top-3
    guessed positions with confidence, and the actual result.

    Note: We have to add a season into the config.TEST_SET in order to build
    Dataset for them. otherwise we can't call them here.

    :param year: e.g. 2025.
    :param round_number: the round within that season, e.g. 5 (the 5th race).

    Run with:  python -m f1ml.predict 2025 5
    """
    device = ("cuda" if torch.cuda.is_available()
              else "mps" if torch.backends.mps.is_available() else "cpu")

    # Build the splits (gives train-fit scaling) and load the trained weights.
    train_ds, val_ds, test_ds, _lap_sc, _up_sc = dataset.build_datasets(
        config.TRAIN_SET, config.VAL_SET, config.TEST_SET)
    model = load_model(device)

    # A given year lives in exactly ONE split (train/val/test), but we don't need
    # to care which — just scan all three for samples whose TARGET race matches.
    matches = []                                  # (dataset, index) pairs
    for ds in (train_ds, val_ds, test_ds):
        for i in range(len(ds)):
            rec = ds.samples[i]
            if rec["year"] == year and rec["round"] == round_number:
                matches.append((ds, i))

    if not matches:
        print(f"No samples for {year} round {round_number}. Either that race "
              f"isn't in the CSVs, or no driver there had 10 prior races yet.")
        return

    print(f"Prediction for {year} round {round_number}  "
          f"({len(matches)} drivers with enough history):")
    print(f"  {'driver':>6}  {'model top-3 (confidence)':<34}  actual")
    acc = []
    acc_top_10 = []
    for ds, i in matches:
        rec = ds.samples[i]
        driver = ID_TO_DRIVER.get(rec["driver"], f"id{rec['driver']}")

        batch = to_device(dataset.collate_fn([ds[i]]), device)
        probs = model(batch).softmax(dim=-1)[0]    # (23,) probabilities
        top_p, top_c = probs.topk(3)
        guesses = "  ".join(
            f"{class_to_label(int(c))} ({p * 100:.0f}%)"
            for p, c in zip(top_p.tolist(), top_c.tolist())
        )
        actual = class_to_label(int(rec["target"]))  # recipe stores the int target
        print(f"  {driver:>6}  {guesses:<34}  {actual}")
        
        # Calculating the accuracy
        acc.append(1 if actual in [class_to_label(int(c)) for c in top_c] else 0)
        # calculating the accuracy for top 10 Drivers. more useful measurement.
        if int(rec["target"]) <= 9:
            acc_top_10.append(1 if actual in [class_to_label(int(c)) for c in top_c] else 0)
    print(f"\nAccuracy of prediction: {round(sum(acc) / len(acc), 2)}")
    print(f"\nAccuracy of prediction (top 10 drivers): {round(sum(acc_top_10) / len(acc_top_10), 2)}")


if __name__ == "__main__":
    import sys

    # No args  -> score the whole 2025 test set + a few sample predictions.
    # Two args -> predict ONE race, e.g.:  python -m f1ml.predict 2025 5
    if len(sys.argv) == 3:
        predict_race(int(sys.argv[1]), int(sys.argv[2]))
    else:
        predict()
