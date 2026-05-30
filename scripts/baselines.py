"""Dumb baselines for the next-race position model — a yardstick for "is it good?"

WHY this exists:
    My last model scores roughly acc~0.15, top3~0.33 on the 2024 (validation) races.
    A number alone means nothing. Is 0.33 impressive or trivial? The only way to
    know is to ask: what would a model that learned NOTHING score on the EXACT
    same 2024 races? That's a baseline. The model is only worth its complexity
    if it clearly beats the best baseline.

Three "learned nothing" strategies:
    1. RANDOM   - guess a uniformly random finishing slot (0..22).
    2. MAJORITY - ALWAYS guess the single most common slot seen in TRAINING.
    3. DNF      - ALWAYS guess the DNF class (slot 22, "did not finish").
                  F1 has lots of DNFs, so this lazy guess can score surprisingly
                  high — which is exactly why we must check it.

We score each baseline the same way train.py scores the model:
    acc  (top-1) = fraction of races where the guess is the TRUE slot.
    top3        = fraction where the true slot is among the 3 guessed slots.
"""

from collections import Counter

import torch

from f1ml import config, dataset


def collect_targets(ds) -> torch.Tensor:
    """Pull the integer target (true finishing slot) out of every sample.

    ``ds[i]`` is the dict from ``F1Dataset.__getitem__``; ``ds[i]["target"]`` is
    a 0-dim tensor like ``tensor(7)``. ``.item()`` turns it into a plain int 7.

    :return: a 1-D LongTensor of every sample's true slot, e.g.
        ``tensor([7, 22, 3, 0, ...])`` (one entry per sample in ``ds``).
    """
    return torch.tensor([int(ds[i]["target"].item()) for i in range(len(ds))])


def main():
    # Build the SAME datasets train.py builds, so the baseline is judged on the
    # EXACT 2024 samples the model's val numbers came from — an apples-to-apples
    # comparison. (We ignore the scalers; baselines don't look at features.)
    train_ds, val_ds, _test_ds, _lap_sc, _up_sc = dataset.build_datasets(
        config.TRAIN_SET, config.VAL_SET, config.TEST_SET)

    train_targets = collect_targets(train_ds)
    val_targets = collect_targets(val_ds)
    n_val = len(val_targets)

    # --- class frequencies in the TRAINING data ---
    # Counter tallies how often each slot appears, e.g. {22: 380, 0: 95, 1: 90, ...}.
    counts = Counter(train_targets.tolist())
    # most_common(3) -> the 3 slots that appear most, e.g. [(22, 380), (0, 95), (1, 90)].
    top3_slots = [slot for slot, _ in counts.most_common(3)]
    majority_slot = top3_slots[0]              # the single most frequent slot
    dnf_slot = config.NUM_DRIVERS - 1          # last class index (= 22) is the DNF bucket

    # --- baseline 1: RANDOM (computed from theory, no luck involved) ---
    # One uniform guess among NUM_DRIVERS slots hits the true slot with prob 1/N;
    # allowing 3 guesses triples that to 3/N.
    random_acc = 1 / config.NUM_DRIVERS
    random_top3 = 3 / config.NUM_DRIVERS

    # --- baseline 2: MAJORITY (always guess the most common training slot) ---
    # (val_targets == majority_slot) is a True/False tensor; .float().mean() is the
    # fraction of True, i.e. how often that fixed guess is correct on 2024.
    majority_acc = (val_targets == majority_slot).float().mean().item()
    # "top-3" for fixed guesses = is the true slot any of our 3 most common slots?
    in_top3 = sum((val_targets == s).sum().item() for s in top3_slots)
    majority_top3 = in_top3 / n_val

    # --- baseline 3: ALWAYS DNF ---
    dnf_acc = (val_targets == dnf_slot).float().mean().item()

    # --- report ---
    print(f"val samples (2024): {n_val}")
    print(f"most common training slot: {majority_slot}   (DNF slot = {dnf_slot})")
    print(f"top-3 most common training slots: {top3_slots}")
    print(f"DNF share of 2024 results: {dnf_acc:.3f}")
    print()
    print(f"{'baseline':<18}{'acc (top1)':>12}{'top3':>10}")
    print(f"{'random':<18}{random_acc:>12.3f}{random_top3:>10.3f}")
    print(f"{'always majority':<18}{majority_acc:>12.3f}{majority_top3:>10.3f}")
    print(f"{'always DNF':<18}{dnf_acc:>12.3f}{'-':>10}")
    print()
    print("My model (latest run): acc ~0.15   top3 ~0.33")
    print("The model earns its complexity only if it CLEARLY")
    print("beats the best number in each column above. top1 near 'always DNF' = weak;")
    print("top3 well above 'always majority' = the model has real ranking skill.")


if __name__ == "__main__":
    main()
