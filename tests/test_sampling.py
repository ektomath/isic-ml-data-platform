from data_platform.sampling import select_sample_and_splits


def _make_groups():
    # A handful of groups per label, varying image_count, so quotas/rounding
    # actually exercise whole-group selection rather than trivially matching.
    groups = []
    for i in range(20):
        groups.append({"group_id": f"benign-{i}", "image_count": 3, "label": "benign"})
    for i in range(15):
        groups.append({"group_id": f"malignant-{i}", "image_count": 2, "label": "malignant"})
    # A single, small-population label to exercise the rare-class floor.
    groups.append({"group_id": "indeterminate-0", "image_count": 1, "label": "indeterminate"})
    return groups


def test_select_sample_and_splits_is_deterministic_for_same_inputs():
    groups = _make_groups()
    result_a = select_sample_and_splits(groups, sample_size=50, sample_seed=42, split_seed=7)
    result_b = select_sample_and_splits(groups, sample_size=50, sample_seed=42, split_seed=7)
    assert result_a == result_b


def test_select_sample_and_splits_changing_sample_seed_changes_output():
    groups = _make_groups()
    result_a = select_sample_and_splits(groups, sample_size=50, sample_seed=1, split_seed=7)
    result_b = select_sample_and_splits(groups, sample_size=50, sample_seed=2, split_seed=7)
    assert result_a != result_b


def test_select_sample_and_splits_same_sample_seed_selects_the_same_images_regardless_of_split_seed():
    # The whole point of separating the two seeds: rebuilding a manifest with a
    # different preprocessing_version (or just testing split sensitivity) should
    # keep the exact same image pool as long as sample_seed is unchanged, even
    # if split_seed differs.
    groups = _make_groups()
    result_a = select_sample_and_splits(groups, sample_size=50, sample_seed=42, split_seed=1)
    result_b = select_sample_and_splits(groups, sample_size=50, sample_seed=42, split_seed=2)
    assert set(result_a) == set(result_b)
    assert result_a != result_b  # split assignment itself should still differ


def test_select_sample_and_splits_every_group_id_traces_back_to_input():
    groups = _make_groups()
    valid_group_ids = {g["group_id"] for g in groups}
    result = select_sample_and_splits(groups, sample_size=50, sample_seed=42, split_seed=7)
    assert set(result) <= valid_group_ids


def test_select_sample_and_splits_image_count_is_close_to_sample_size():
    groups = _make_groups()
    groups_by_id = {g["group_id"]: g for g in groups}
    result = select_sample_and_splits(groups, sample_size=50, sample_seed=42, split_seed=7)
    selected_image_count = sum(groups_by_id[group_id]["image_count"] for group_id in result)
    assert 35 <= selected_image_count <= 65


def test_select_sample_and_splits_every_input_label_appears_at_least_once():
    groups = _make_groups()
    groups_by_id = {g["group_id"]: g for g in groups}
    result = select_sample_and_splits(groups, sample_size=50, sample_seed=42, split_seed=7)
    selected_labels = {groups_by_id[group_id]["label"] for group_id in result}
    input_labels = {g["label"] for g in groups}
    assert selected_labels == input_labels


def test_select_sample_and_splits_every_selected_group_gets_exactly_one_split():
    groups = _make_groups()
    result = select_sample_and_splits(groups, sample_size=50, sample_seed=42, split_seed=7)
    assert set(result.values()) <= {"train", "validation", "test"}


def test_select_sample_and_splits_split_distribution_is_roughly_proportional():
    groups = _make_groups()
    groups_by_id = {g["group_id"]: g for g in groups}
    result = select_sample_and_splits(
        groups, sample_size=200, sample_seed=42, split_seed=7, split_ratios=(0.7, 0.15, 0.15)
    )

    image_counts_by_split = {"train": 0, "validation": 0, "test": 0}
    for group_id, split in result.items():
        image_counts_by_split[split] += groups_by_id[group_id]["image_count"]

    total = sum(image_counts_by_split.values())
    assert total > 0
    train_fraction = image_counts_by_split["train"] / total
    assert 0.5 <= train_fraction <= 0.9


def test_select_sample_and_splits_puts_a_rare_label_in_every_split():
    # Three small groups of a rare label: proportional quotas alone would put all of them in
    # train, leaving validation and test without the class.
    groups = [{"group_id": f"benign-{i}", "image_count": 10, "label": "benign"} for i in range(30)]
    groups += [{"group_id": f"rare-{i}", "image_count": 1, "label": "rare"} for i in range(3)]

    result = select_sample_and_splits(groups, sample_size=400, sample_seed=1, split_seed=1)

    rare_splits = {split for group_id, split in result.items() if group_id.startswith("rare-")}
    assert rare_splits == {"train", "validation", "test"}
