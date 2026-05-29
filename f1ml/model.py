"""The hierarchical network: laps -> race form -> next-race position.

Two LSTMs stacked, plus embeddings and a small MLP head:

    laps of ONE race      ─► INNER LSTM ─► race_vec   (one vector per race)
    10 race_vecs          ─► OUTER LSTM ─► form_vec    (the driver's "form")
    form_vec + upcoming   ─► MLP head   ─► logits over driver slots

The trick that makes the inner LSTM run on all 10 races at once is a reshape:
the batch arrives as (B, R, L, F) and we fold the race axis into the batch axis
-> (B*R, L, F). One LSTM call then processes every race of every sample, and we
unfold back to (B, R, hidden) for the outer LSTM.
"""

import torch.nn as nn
import torch

from . import config


class F1Net(nn.Module):
    """Predict a finishing-position score for a driver's upcoming race.

    Input is one driver's last ``NUM_PAST_RACES`` races (each a padded lap
    sequence) plus the upcoming race's context (track + practice info).
    Output is ``NUM_DRIVERS`` raw logits — one score per finishing slot 0..20.
    """

    def __init__(self):
        """Creating the Layers.
        Pieces we'll need, and how they connect:
        """
        super().__init__()

        # ---- 1. embeddings for the per-lap categoricals ----
        # One nn.Embedding per column in config.CATEGORICAL_COLS
        # (Driver, Team, Compound, TrackStatus). Sizes live in
        # config.EMBEDDING_SIZES as (num_categories, embedding_dim), e.g.
        # "Driver" -> (50, 8): IDs 0..49 each become a length-8 vector.
        # Note: I store them in an nn.ModuleDict keyed by column name so forward()
        # can loop and look each one up. (A plain dict won't register the
        # parameters with PyTorch — they wouldn't train.)
        self.lap_embeddings = nn.ModuleDict()
        for col in config.CATEGORICAL_COLS:
            # unpack using *config.
            self.lap_embeddings[col] = nn.Embedding(*config.EMBEDDING_SIZES[col])
        
        # ---- 2. embedding for the upcoming-race Track ----
        # config.UPCOMING_CONTEXT_CATEGORICAL is just ["Track"];
        # config.EMBEDDING_SIZES["Track"] = (40, 6).
        self.track_embeddings= nn.ModuleDict()
        for col in config.UPCOMING_CONTEXT_CATEGORICAL:
            self.track_embeddings[col] = nn.Embedding(*config.EMBEDDING_SIZES[col])
        
        # ---- 3. INNER LSTM: laps -> race_vec ----
        # input_size  = len(NUMERICAL_FEATURE_COLS) + sum of the lap embedding
        #               dims, i.e. numerical + embeddings.
        # hidden_size = config.INNER_LSTM_HIDDEN (64). Use batch_first=True so it
        # expects (batch, seq_len, features). The final hidden state per sequence
        # is the race_vec, shape (B*R, 64).
        feat_size = (len(config.NUMERICAL_FEATURE_COLS) + 
                     sum([config.EMBEDDING_SIZES[cat][1] for cat in config.CATEGORICAL_COLS]))
        # input size of all features = 54 in our case
        self.inner_lstm = nn.LSTM(input_size=feat_size, hidden_size=config.INNER_LSTM_HIDDEN, batch_first=True)
        
        # ---- 4. OUTER LSTM: 10 race_vecs -> form_vec ----
        # input_size  = config.INNER_LSTM_HIDDEN (64, the race_vec size).
        # hidden_size = config.OUTER_LSTM_HIDDEN (128). Every sample always has
        # exactly R=10 race_vecs (we required 10 races of history), so this one
        # needs NO padding/packing. Final hidden state = form_vec, shape (B, 128).
        self.outer_lstm = nn.LSTM(input_size=config.INNER_LSTM_HIDDEN, 
                                  hidden_size=config.OUTER_LSTM_HIDDEN, 
                                  batch_first=True)
        
        # ---- 5. MLP head: (form_vec + upcoming context) -> logits ----
        # The vector you feed the head is the concat of:
        #   form_vec                (128)
        #   upcoming_num            len(UPCOMING_CONTEXT_NUMERICAL) = 6
        #   track embedding         EMBEDDING_SIZES["Track"][1] = 6
        # => in_features = 128 + 6 + 6 = 140.
        # A simple head: Linear(140 -> HEAD_HIDDEN=64) -> ReLU -> Dropout(DROPOUT)
        # -> Linear(64 -> NUM_DRIVERS=23). No softmax at the end.
        head_input_size = (config.OUTER_LSTM_HIDDEN +
                      len(config.UPCOMING_CONTEXT_NUMERICAL) +
                      sum(config.EMBEDDING_SIZES[cat][1] for cat in config.UPCOMING_CONTEXT_CATEGORICAL))
        self.head = nn.Sequential(
            nn.Linear(head_input_size, config.HEAD_HIDDEN),          # 140 -> 64
            nn.ReLU(),
            nn.Dropout(config.DROPOUT),                         # 0.2
            nn.Linear(config.HEAD_HIDDEN, config.NUM_DRIVERS),  # 64 -> 23 (raw logits)
        )

    def forward(self, batch:dict[str, torch.Tensor]):
        """Wire the data through the layers and return logits.

        :param batch: the dict produced by ``dataset.collate_fn`` (the DataLoader
            yields it each step). Keys and shapes (B=batch, R=10, L=78, F=37):
                past_laps_num   (B, R, L, F)
                past_laps_cat   dict col -> (B, R, L)
                past_laps_lens  (B, R)
                upcoming_num    (B, 6)
                upcoming_cat    {"Track": (B,)}
        :return: logits, shape (B, NUM_DRIVERS).
        """
        # ---- 1. embed + concat the per-lap features ----
        # For each col in CATEGORICAL_COLS: emb = self.lap_embeddings[col](batch["past_laps_cat"][col])
        #   ID tensor (B, R, L) -> embedded (B, R, L, emb_dim).
        # Then torch.cat([numerical, *embeddings], dim=-1) along the feature axis
        # -> (B, R, L, 54).
        embs = []
        for col in config.CATEGORICAL_COLS:
            embs.append(self.lap_embeddings[col](batch["past_laps_cat"][col]))
        
        x = torch.cat([batch["past_laps_num"], *embs], dim=-1)


        # ---- 2. fold races into the batch axis, run the INNER LSTM ----
        # reshape (B, R, L, 54) -> (B*R, L, 54) with .reshape(B*R, L, -1).
        # (Recommended) nn.utils.rnn.pack_padded_sequence with past_laps_lens
        # flattened to (B*R,) so the LSTM skips zero-padding. Use
        # enforce_sorted=False and put lengths on the CPU. Every race has >=1
        # real lap, so no zero-length sequence to worry about.
        # Take the final hidden state -> (B*R, 64), then reshape -> (B, R, 64).
        B, R, L, F = x.shape
        x = torch.reshape(x, shape=(B*R, L, F))
        lengths=batch["past_laps_lens"].reshape(B*R,)
        packed = nn.utils.rnn.pack_padded_sequence(x, 
                                          lengths=lengths.cpu(), 
                                          batch_first=True,
                                          enforce_sorted=False)
        _, (h_n, _) = self.inner_lstm(packed) # hidden state -> (1, 320, 64)
        race_vecs = h_n[-1].reshape(B, R, -1)  # h_n[-1] -> (320, 64) then unfold to -> (32, 10, 64)
        

