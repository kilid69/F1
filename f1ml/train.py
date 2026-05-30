"""The training loop

For every batch:

    optimizer.zero_grad()      # clear last step's gradients
    logits = model(batch)      # forward pass
    loss = criterion(logits, target)   # how wrong were we
    loss.backward()            # backprop: compute gradients
    optimizer.step()           # nudge the weights

Run this file directly to train:  python -m f1ml.train
"""

import pickle

import mlflow
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from . import config
from . import dataset
from .model import F1Net


def to_device(obj, device):
    """Move a (possibly nested) batch onto ``device``.

    Our batch is a dict whose values are tensors OR sub-dicts of tensors
    (``past_laps_cat`` and ``upcoming_cat``). A plain ``batch.to(device)`` does
    not exist for dicts, so we walk the structure and move each tensor.

    :param obj: a tensor, a dict of tensors, or a dict of dicts of tensors.
    :param device: e.g. ``"cuda"``, ``"mps"``, or ``"cpu"``.
    :return: the same structure with every tensor moved to ``device``.
    """
    # A tensor's .to(device) is NOT in-place: it RETURNS a new tensor on the
    # device and leaves the original untouched. So we must capture & return it —
    # calling obj.to(device) without using the result does nothing.
    if torch.is_tensor(obj):
        return obj.to(device)
    # a dict (the whole batch, or a sub-dict like past_laps_cat): rebuild it with
    # every value moved. Recurse with to_device() because a value can ITSELF be a
    # sub-dict (so value.to(device) would fail — dicts have no .to()).
    if isinstance(obj, dict):
        return {key: to_device(value, device) for key, value in obj.items()}
    # anything else (e.g. a plain int) — nothing to move
    return obj


def top_k_accuracy(logits, targets, k: int = 3) -> float:
    """Fraction of samples whose TRUE class is within the model's top-k guesses.

    With k=1 this is plain accuracy; with k=3 it's your old "top-3" metric.

    :param logits: model output, shape (B, NUM_DRIVERS) — raw scores.
    :param targets: the answers, shape (B,), each an int class index 0..NUM_DRIVERS-1
        (NOT one-hot — CrossEntropy/our dataset use plain class indices).
    :param k: how many top guesses to allow.
    :return: a float in [0, 1].
    """
    # 1) the k highest-scoring class ids per sample (no softmax needed — ranking
    #    by raw logits is the same as ranking by probability)
    topk = logits.topk(k, dim=-1).indices             # (B, k)
    
    # 2) was the TRUE class among those k? targets (B,) -> (B,1) so it lines up
    # Example:
    # topk     = [[4, 1, 9],     # sample 0: model's top-3 guessed class ids
    #             [2, 7, 0]]      # sample 1
    # targets  =  [1, 5]          # the TRUE answer for each sample
    # unsqueeze(1) inserts a new axis, turning the flat list into a column:
    # [1, 5]   →   [[1],
    #               [5]]      shape (2, 1)
    # Now comparing (2,3) with (2,1) is possible.
    # sample 0:  [4==1, 1==1, 9==1]  →  [False, True,  False]
    # sample 1:  [2==5, 7==5, 0==5]  →  [False, False, False]

    hit = (topk == targets.unsqueeze(1)).any(dim=1)   # (B,) of True/False
    # 3) fraction of True over the batch
    return hit.float().mean().item()


def train_one_epoch(model, loader, criterion, optimizer, device) -> float:
    """Run ONE full pass over the training data; return the average loss.

    :param model: the F1Net.
    :param loader: a DataLoader yielding batched dicts (see dataset.collate_fn).
    :param criterion: the loss function, e.g. nn.CrossEntropyLoss().
    :param optimizer: e.g. torch.optim.Adam(model.parameters(), ...).
    :param device: where the tensors/model live.
    :return: mean loss over all samples this epoch.
    """

    # Dropout ON during training
    model.train()
    running = 0.0          # running SUM of per-sample losses this epoch
    total_samples = 0      # running COUNT of samples seen

    for batch in loader:
        batch = to_device(batch, device)
        optimizer.zero_grad()
        logits = model(batch)                       # (B, NUM_DRIVERS)
        loss = criterion(logits, batch["target"])   # target: (B,) class indices
        loss.backward()
        optimizer.step()
        # weight by batch size so the last (smaller) batch is counted fairly:
        running += loss.item() * batch["target"].size(0)    # loss.item() pulls the plain Python float out of the loss tensor (it's a 0-dim tensor; .item() → a number)
        total_samples += batch["target"].size(0)

    # calculating avg_loss
    return running / total_samples

@torch.no_grad()
def evaluate(model, loader, criterion, device) -> dict:
    """Run over val/test data WITHOUT updating weights; return metrics.

    The ``@torch.no_grad()`` decorator turns off gradient tracking for the whole
    function (faster, less memory) — so there is NO backward()/step() here.

    :return: a dict like ``{"loss": float, "acc": float, "top3": float}``.
    """
    model.eval()             # dropout OFF for evaluation
    running = 0.0            # SUM of per-sample losses
    acc_sum = 0.0            # SUM of per-sample top-1 hits
    top3_sum = 0.0           # SUM of per-sample top-3 hits
    total_samples = 0

    # same loop as train_one_epoch, but NO zero_grad / backward / step — we only measure
    for batch in loader:
        batch = to_device(batch, device)
        logits = model(batch)                      # (B, NUM_DRIVERS)
        targets = batch["target"]                  # (B,) class indices
        loss = criterion(logits, targets)

        n = targets.size(0)                        # samples in this batch
        running += loss.item() * n
        acc_sum += top_k_accuracy(logits, targets, k=1) * n   # weight by batch size,
        top3_sum += top_k_accuracy(logits, targets, k=3) * n  # same fair-average trick
        total_samples += n

    return {
        "loss": running / total_samples,
        "acc":  acc_sum / total_samples,
        "top3": top3_sum / total_samples,
    }


def train():
    """Train Pipeline."""
    # ---- 1. device ----
    device = ("cuda" if torch.cuda.is_available() 
              else "mps" if torch.backends.mps.is_available() else "cpu")

    # ---- 2. data ----
    # TEMPORAL split (NOT random — avoids look-ahead bias / data leakage):

    # I should give collate_fn because the default collate can't stack our nested dict:
    # build_datasets returns FIVE things; I keep the scalers for inference later
    train_ds, val_ds, test_ds, lap_scaler, upcoming_scaler = dataset.build_datasets(
        config.TRAIN_SET, config.VAL_SET, config.TEST_SET)

    # persist the fitted scalers ONCE so inference can scale a new race exactly
    # like training did (they don't change during training).
    with open(config.SCALERS_PATH, "wb") as f:
        pickle.dump({"lap": lap_scaler, "upcoming": upcoming_scaler}, f)

    train_loader = DataLoader(train_ds, batch_size=config.BATCH_SIZE,
                              shuffle=True, collate_fn=dataset.collate_fn) 
    
    # shuffle won't change the order inside one sample. 
    # it let us get random samples inside a batch. Avoid overfitting for later years.
    
    val_loader = DataLoader(val_ds, batch_size=config.BATCH_SIZE,
                                shuffle=False, collate_fn=dataset.collate_fn)

    # ---- 3. model, loss, optimizer ----
    model = F1Net().to(device)
    criterion = nn.CrossEntropyLoss()       # expects raw logits + class-index targets
    optimizer = torch.optim.Adam(model.parameters(),
                                 lr=config.LEARNING_RATE,
                                 weight_decay=config.WEIGHT_DECAY)
    
    # ---- 4. epoch loop, tracked by MLflow ----
    # MLflow groups runs under an experiment; start_run() opens ONE run that
    # records this training's settings, its per-epoch numbers, and its best model.
    mlflow.set_experiment(config.MLFLOW_EXPERIMENT)
    with mlflow.start_run():
        # log the knobs that DEFINE this run, so later you can see which settings
        # produced which result and compare runs side by side in the MLflow UI.
        mlflow.log_params({
            "batch_size": config.BATCH_SIZE,
            "learning_rate": config.LEARNING_RATE,
            "weight_decay": config.WEIGHT_DECAY,
            "dropout": config.DROPOUT,
            "patience": config.PATIENCE,
            "num_epochs": config.NUM_EPOCHS,
            "inner_lstm_hidden": config.INNER_LSTM_HIDDEN,
            "outer_lstm_hidden": config.OUTER_LSTM_HIDDEN,
        })

        best_val = float("inf")     # lowest val_loss we've seen so far
        epochs_without_improve = 0  # how many epochs IN A ROW since that best

        for epoch in range(config.NUM_EPOCHS):
            train_loss = train_one_epoch(model, train_loader, criterion, optimizer, device)
            val = evaluate(model, val_loader, criterion, device)
            print(f"epoch {epoch}: train_loss={train_loss:.4f}  "
                  f"val_loss={val['loss']:.4f}  acc={val['acc']:.3f}  top3={val['top3']:.3f}")

            # record this epoch's numbers; step=epoch makes MLflow draw a curve
            # over epochs (so you can SEE train_loss vs val_loss diverge = overfit).
            mlflow.log_metrics({
                "train_loss": train_loss,
                "val_loss": val["loss"],
                "acc": val["acc"],
                "top3": val["top3"],
            }, step=epoch)

            if val["loss"] < best_val:
                # NEW best: save the weights and reset the patience counter
                best_val = val["loss"]
                epochs_without_improve = 0
                torch.save(model.state_dict(), config.CHECKPOINT_PATH)
                print(f"  ↳ saved (best val_loss so far: {best_val:.4f})")
            else:
                # no improvement this epoch — burn one unit of patience
                epochs_without_improve += 1
                if epochs_without_improve >= config.PATIENCE:
                    # val_loss has stalled for PATIENCE epochs in a row -> stop.
                    # The best weights are already safe on disk (we only ever save
                    # on improvement), so stopping here loses nothing.
                    print(f"early stopping at epoch {epoch}: "
                          f"no val improvement for {config.PATIENCE} epochs "
                          f"(best val_loss={best_val:.4f})")
                    break

        # ---- 5. archive this run's best model + its scalers in MLflow ----
        # f1_model.pt currently holds the BEST epoch's weights (we only saved on
        # improvement). Log it + the scalers as artifacts so MLflow keeps a
        # versioned copy of THIS run, safe even after the next run overwrites the
        # working-dir files. best_val becomes the run's headline metric.
        mlflow.log_metric("best_val_loss", best_val)
        mlflow.log_artifact(config.CHECKPOINT_PATH)
        mlflow.log_artifact(config.SCALERS_PATH)


if __name__ == "__main__":
    train()
