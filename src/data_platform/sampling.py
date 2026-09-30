"""Pure-Python sampling and split-assignment logic for Gold manifest releases
(renamed from gold.py -- named after what this module does, matching
files.py/validate.py/labels.py, rather than which medallion layer calls it;
it's the only thing in here, not a general home for "Gold stuff"). No Spark
dependency, unit-tested locally like data_platform.labels/validate (see
tests/test_sampling.py).

Operates on small, driver-side per-leakage-group summaries — already aggregated
in Spark and collected back by data_platform.spark_io.build_gold_candidate_groups
— rather than per-image rows, so a group is only ever included/excluded as a
whole, by construction, and this stays fast/simple to test without Spark.
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
    """Greedy whole-group fill used by the sample-selection stage below: for each
    label (sorted, for determinism), sort its groups by group_id, shuffle
    deterministically, then add whole groups until quotas[label] is reached
    (accepting the last group's overshoot rather than splitting it). The
    split-assignment stage further down needs slightly different quota-overshoot
    handling (remainder goes to the last split, not dropped) so it doesn't call
    this — not actually shared across both stages despite the similar shape."""
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
    """Down-select whole groups to ~sample_size images (stratified by label,
    proportional to each label's share of `groups`), then assign each selected
    group to a split (also stratified by label, proportional to split_ratios).
    Both stages fill whole groups only — a group is never split across splits,
    and never partially included in the sample.

    sample_seed and split_seed are deliberately separate, not one shared seed:
    they answer different questions ("which images are in this manifest at
    all" vs. "how are they split") and a caller may want to hold one fixed
    while varying the other — e.g. rebuilding the same manifest with a
    different preprocessing_version should select the exact same images
    (same sample_seed), or testing result sensitivity to the split should hold
    the image pool fixed while trying several split_seeds.

    Deterministic for a given (groups, sample_size, sample_seed, split_seed):
    groups are sorted by group_id before each shuffle so the result never
    depends on collection order. Uses random.Random(str) rather than hash(str)
    for the per-label sub-seeds — CPython's random module hashes str seeds
    deterministically, unlike Python's builtin hash(), which varies run to run
    unless PYTHONHASHSEED is fixed.

    Not exact: sample_size and split_ratios are targets, not guarantees — group
    sizes mean the actual counts land close but rarely exact, and that's fine
    for a small sample. Every label present in `groups` gets at least one group
    in the sample (a `max(1, ...)` floor), so a rare label isn't silently
    dropped just because its proportional share rounds to zero. A label with at least one
    selected group per split also gets at least one group in every split, so validation and
    test don't silently miss a rare class.

    Returns {group_id: split_name} for only the *selected* groups — callers
    filter their image rows to this dict's keys.
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
        split_quotas = {name: round(label_total * ratio) for name, ratio in zip(split_names, split_ratios)}

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
