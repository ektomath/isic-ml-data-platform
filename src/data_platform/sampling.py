"""Picks a Gold release's sample and assigns its splits. Pure Python, tested without Spark
(tests/test_sampling.py).

Works on one small summary per leakage group (built in Spark by
spark_io.build_gold_candidate_groups), not on image rows, so a group is always included or excluded
whole.
"""

from __future__ import annotations

import random
from collections import defaultdict
from typing import TypedDict


class GroupSummary(TypedDict):
    group_id: str
    image_count: int
    label: str


def _fill_whole_groups_by_quota(
    groups_by_label: dict[str, list[GroupSummary]],
    quotas: dict[str, int],
    seed_prefix: str,
) -> list[GroupSummary]:
    """For each label, shuffle its groups deterministically and add whole groups until the label's
    quota is reached. The last group may overshoot the quota; a group is never split.
    """
    selected: list[GroupSummary] = []
    for label in sorted(groups_by_label):
        label_groups = sorted(groups_by_label[label], key=lambda g: g["group_id"])
        random.Random(f"{seed_prefix}:{label}").shuffle(label_groups)

        quota = quotas.get(label, 0)
        running_count = 0
        for group in label_groups:
            if running_count >= quota:
                break
            selected.append(group)
            running_count += group["image_count"]

    return selected


def select_sample_and_splits(
    groups: list[GroupSummary],
    sample_size: int,
    sample_seed: int,
    split_seed: int,
    split_ratios: tuple[float, float, float] = (0.7, 0.15, 0.15),
    split_names: tuple[str, str, str] = ("train", "validation", "test"),
) -> dict[str, str]:
    """Select whole groups up to about `sample_size` images, then assign each selected group to a
    split. Both steps are stratified by label, and a group never spans two splits.

    sample_seed and split_seed are separate so one can stay fixed while the other changes: the same
    images with a different split, or exactly the same images under a new preprocessing_version.

    The result is deterministic for the same inputs: groups are sorted by group_id before each
    seeded shuffle, and the seeds are strings passed to random.Random, which (unlike hash()) is
    stable across processes.

    Sizes and ratios are targets; whole groups mean the counts land close, not exact. Every label
    gets at least one group in the sample, and a label with enough groups gets at least one in every
    split, so a rare class isn't missing from validation or test.

    Returns {group_id: split_name} for the selected groups only.
    """
    groups_by_label: dict[str, list[GroupSummary]] = defaultdict(list)
    for group in groups:
        groups_by_label[group["label"]].append(group)

    total_images = sum(group["image_count"] for group in groups)
    sample_quotas = {
        label: max(1, round(sample_size * sum(g["image_count"] for g in label_groups) / total_images))
        for label, label_groups in groups_by_label.items()
    }
    selected_groups = _fill_whole_groups_by_quota(groups_by_label, sample_quotas, seed_prefix=f"{sample_seed}:sample")

    selected_groups_by_label: dict[str, list[GroupSummary]] = defaultdict(list)
    for group in selected_groups:
        selected_groups_by_label[group["label"]].append(group)

    split_by_group: dict[str, str] = {}
    for label, label_groups in selected_groups_by_label.items():
        label_total = sum(g["image_count"] for g in label_groups)
        split_quotas = {name: round(label_total * ratio) for name, ratio in zip(split_names, split_ratios, strict=True)}

        sorted_groups = sorted(label_groups, key=lambda g: g["group_id"])
        random.Random(f"{split_seed}:split:{label}").shuffle(sorted_groups)

        groups_by_split: dict[str, list[GroupSummary]] = {name: [] for name in split_names}
        next_index = 0
        for split_name in split_names[:-1]:
            quota = split_quotas[split_name]
            running_count = 0
            while next_index < len(sorted_groups) and running_count < quota:
                group = sorted_groups[next_index]
                next_index += 1
                groups_by_split[split_name].append(group)
                running_count += group["image_count"]
        # Whatever's left goes to the last split — avoids under-filling it due
        # to rounding, and keeps every selected group assigned to exactly one split.
        groups_by_split[split_names[-1]].extend(sorted_groups[next_index:])

        _give_every_split_a_group(groups_by_split, split_names)
        for split_name, split_groups in groups_by_split.items():
            for group in split_groups:
                split_by_group[group["group_id"]] = split_name

    return split_by_group


def _give_every_split_a_group(groups_by_split: dict[str, list[GroupSummary]], split_names: tuple[str, ...]) -> None:
    """If a label has at least one group per split, make sure no split is left without one, so a
    rare label still appears in validation and test. Moves the smallest group from the split
    holding the most groups; a label with fewer groups than splits is left as it is."""
    if sum(len(split_groups) for split_groups in groups_by_split.values()) < len(split_names):
        return
    for split_name in split_names:
        if groups_by_split[split_name]:
            continue
        donor_name = max(split_names, key=lambda name: len(groups_by_split[name]))
        donor_groups = groups_by_split[donor_name]
        smallest = min(donor_groups, key=lambda group: (group["image_count"], group["group_id"]))
        donor_groups.remove(smallest)
        groups_by_split[split_name].append(smallest)
