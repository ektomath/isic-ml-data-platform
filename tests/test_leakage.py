from data_platform.leakage import assign_groups


def _image(image_id, checksum=None, lesion_id=None, patient_id=None, dataset_key="isic_2019"):
    return {
        "dataset_key": dataset_key,
        "image_id": image_id,
        "source_checksum": checksum or f"bytes-of-{image_id}",
        "lesion_id": lesion_id,
        "patient_id": patient_id,
    }


def _group_of(groups, image_id, dataset_key="isic_2019"):
    return groups[(dataset_key, image_id)]


def test_two_lesions_of_one_patient_share_a_group():
    groups = assign_groups(
        [
            _image("a", lesion_id="L1", patient_id="P1"),
            _image("b", lesion_id="L1", patient_id="P1"),
            _image("c", lesion_id="L2", patient_id="P1"),
        ]
    )

    assert _group_of(groups, "a") == _group_of(groups, "b") == _group_of(groups, "c") == (
        "isic_2019:a",
        "lesion+patient",
    )


def test_links_chain_across_kinds():
    # "b" duplicates "a" byte for byte, and "b" shares a lesion with "c": all three belong together.
    groups = assign_groups(
        [
            _image("a", checksum="same"),
            _image("b", checksum="same", lesion_id="L1"),
            _image("c", lesion_id="L1"),
        ]
    )

    assert {_group_of(groups, image_id) for image_id in ("a", "b", "c")} == {("isic_2019:a", "duplicate+lesion")}


def test_unlinked_images_are_singletons():
    groups = assign_groups([_image("a", lesion_id="L1"), _image("b", patient_id="P1")])

    assert _group_of(groups, "a") == ("isic_2019:a", "singleton")
    assert _group_of(groups, "b") == ("isic_2019:b", "singleton")


def test_matching_ids_in_different_datasets_are_not_linked():
    groups = assign_groups([_image("a", lesion_id="L1"), _image("b", lesion_id="L1", dataset_key="milk10k")])

    assert _group_of(groups, "a") == ("isic_2019:a", "singleton")
    assert _group_of(groups, "b", "milk10k") == ("milk10k:b", "singleton")


def test_group_ids_do_not_depend_on_input_order():
    images = [_image("b", lesion_id="L1"), _image("a", lesion_id="L1"), _image("c")]

    assert assign_groups(images) == assign_groups(list(reversed(images)))


def test_no_images_gives_no_groups():
    assert assign_groups([]) == {}
