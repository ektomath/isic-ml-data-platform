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
    """A StreamingDataset over one split of a shard export, decoding each image and mapping its
    label to an index.

    `local` is a Volume path or a local copy of the shards; both work the same. A label missing from
    `label_to_index` raises KeyError, since it means the training run's label_values don't match the
    data.

    On Windows, pass a relative path: an absolute drive-letter path can be misread as a URL (the
    tests chdir for this).
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
    shuffle_seed: int | None = None,
) -> DataLoader:
    """Build a DataLoader over one split's Gold shards. `num_workers` defaults to 0 (main
    process, no multiprocessing) -- avoids Windows multiprocessing-spawn friction for local
    runs; pass a higher value on a Databricks GPU cluster where parallel loading helps.
    `shuffle_seed` sets the shard reader's shuffle order; without it the library's own default
    seed is used, so pass the recipe's random_seed to make one seed govern the whole run."""
    streaming_kwargs = {"shuffle_seed": shuffle_seed} if shuffle_seed is not None else {}
    dataset = GoldShardDataset(
        local=shard_dir,
        transform=transform,
        label_to_index=label_to_index,
        shuffle=shuffle,
        batch_size=batch_size,
        **streaming_kwargs,
    )
    return DataLoader(dataset, batch_size=batch_size, num_workers=num_workers)
