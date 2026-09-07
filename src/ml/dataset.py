"""Gold shard dataset and dataloader construction for training.

Depends on `streaming`/`torch`/`PIL`, no Spark -- unit-tested locally against real tiny MDS
shards (see tests/test_ml_dataset.py), same fixture-based convention as tests/test_shard_export.py.
"""

from __future__ import annotations

import io

from PIL import Image
from streaming import StreamingDataset
from torch.utils.data import DataLoader


def label_to_index_map(label_values: list[str]) -> dict[str, int]:
    """Map each label in a training-run config's pinned label_values to its class index.
    Order is significant -- this is what index 0/1/2 means for a given run's model, fixed by
    the config rather than derived from a shard scan (see TrainingRunSpec.label_values)."""
    return {label: index for index, label in enumerate(label_values)}


class GoldShardDataset(StreamingDataset):
    """A `streaming.StreamingDataset` over one split of a Gold shard export
    (`notebooks/31_export_gold_shards.ipynb`'s output), decoding each sample's raw image bytes
    and mapping its label through a pinned vocabulary at read time.

    `local` is either a Databricks Volume path (mounted directly, no copy needed) or a
    locally-`databricks fs cp -r`'d directory (docs/data_contract.md's Gold shard export
    section) -- this class doesn't care which, `streaming.StreamingDataset` handles both
    identically once `local` points at a real shard directory.

    Raises KeyError at __getitem__ time if a sample's label isn't in `label_to_index` -- a real
    mismatch between the training-run config's pinned label_values and this dataset_version's
    actual data, worth surfacing immediately rather than silently coercing.

    Windows note: `StreamingDataset` shares the same drive-letter/`urlparse` local-path quirk
    already documented for `MDSWriter` in tests/test_shard_export.py -- a bare `C:\\...` path can
    be misread as a URL scheme. Not a production issue (real usage always passes a `/Volumes/...`
    or already-relative local path); tests work around it the same way (`monkeypatch.chdir` +
    a relative `local` path).
    """

    def __init__(self, local: str, transform, label_to_index: dict[str, int], **streaming_kwargs):
        super().__init__(local=local, remote=None, **streaming_kwargs)
        self.transform = transform
        self.label_to_index = label_to_index

    def __getitem__(self, idx: int):
        sample = super().__getitem__(idx)
        image = Image.open(io.BytesIO(sample["image"])).convert("RGB")
        image = self.transform(image)
        label_index = self.label_to_index[sample["label"]]
        return image, label_index


def build_dataloader(
    shard_dir: str,
    transform,
    label_to_index: dict[str, int],
    batch_size: int,
    shuffle: bool,
    num_workers: int = 0,
) -> DataLoader:
    """Build a DataLoader over one split's Gold shards. `num_workers` defaults to 0 (main
    process, no multiprocessing) -- avoids Windows multiprocessing-spawn friction for local
    runs; pass a higher value on a Databricks GPU cluster where parallel loading helps.
    `pin_memory=True` lets the host->device copy in `ml.train._forward_batch` (which passes
    `non_blocking=True`) overlap with compute on a CUDA device; harmless on CPU-only runs."""
    dataset = GoldShardDataset(
        local=shard_dir, transform=transform, label_to_index=label_to_index, shuffle=shuffle, batch_size=batch_size
    )
    return DataLoader(dataset, batch_size=batch_size, num_workers=num_workers, pin_memory=True)
