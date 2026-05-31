"""Quick smoke test for F1Net: feed one fake batch, check the output shape,
and confirm a backward pass works. No data files needed.

Run from the repo root:  python -m tests.test_model
(the `-m ... from root` form is required so `from f1ml import ...` resolves)
"""
import torch
import torch.nn.functional as F_

from f1ml import config
from f1ml.model import F1Net

B = 4                                   # any batch size works (model is batch-agnostic)
R = config.NUM_PAST_RACES               # 10 past races
L = config.MAX_LAPS_PER_RACE            # 78 laps
F = len(config.NUMERICAL_FEATURE_COLS)  # 37 numerical features

# --- a fake batch shaped EXACTLY like dataset.collate_fn's output ---
batch = {
    # numerical lap features: random floats
    "past_laps_num": torch.randn(B, R, L, F),
    # categorical ids: random ints within each embedding's valid range (0..num_categories-1)
    "past_laps_cat": {
        col: torch.randint(0, config.EMBEDDING_SIZES[col][0], (B, R, L))
        for col in config.CATEGORICAL_COLS
    },
    # real lap count per race: 1..L (packing needs >=1)
    "past_laps_lens": torch.randint(1, L + 1, (B, R)),
    # upcoming numerical context (practice pos/pace)
    "upcoming_num": torch.randn(B, len(config.UPCOMING_CONTEXT_NUMERICAL)),
    # upcoming categorical context (Track id)
    "upcoming_cat": {
        col: torch.randint(0, config.EMBEDDING_SIZES[col][0], (B,))
        for col in config.UPCOMING_CONTEXT_CATEGORICAL
    },
}

model = F1Net()

# 1) forward pass -> shape check
model.eval()
with torch.no_grad():
    logits = model(batch)
expected = (B, config.NUM_DRIVERS)
print("forward output shape:", tuple(logits.shape), "expected:", expected)
assert logits.shape == expected, f"shape mismatch: {tuple(logits.shape)} != {expected}"

# 2) backward pass -> confirms the whole net is differentiable & loss accepts the shapes
model.train()
logits = model(batch)
target = torch.randint(0, config.NUM_DRIVERS, (B,))   # fake answers, class indices 0..22
loss = F_.cross_entropy(logits, target)
loss.backward()

# a learnable weight should now have a gradient
grad_ok = model.head[0].weight.grad is not None
print(f"backward pass OK, loss = {loss.item():.4f}, head weights got gradients: {grad_ok}")
assert grad_ok, "no gradient reached the head — something is detached"

print("\nPASS ✅  F1Net forward + backward both work; logits are (batch, NUM_DRIVERS).")
