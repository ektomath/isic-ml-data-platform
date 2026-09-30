"""Tests for the Spark transformations in data_platform.spark_io, on a local SparkSession.

They cover the logic the platform's guarantees rest on (leakage grouping, rejections, label
checks, which rows a Gold release gets), not the Delta table writes, which need Databricks.
Skipped when PySpark or Java isn't available; CI installs both.
"""

import shutil

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is needed to start a local SparkSession", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from data_platform.labels import MALIGNANCY_VALUES, normalize_diagnosis_labels  # noqa: E402
from data_platform.spark_io import (  # noqa: E402
    Rejections,
    apply_label_normalization,
    assert_controlled_vocabularies,
    assign_leakage_groups,
    build_accepted_rows,
    build_manifest_rows,
    sql_string,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("spark_io_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


ACCEPTED_SCHEMA = (
    "dataset_key STRING, image_id STRING, bronze_uri STRING, source_checksum STRING, "
    "image_width INT, image_height INT, image_format STRING, malignancy STRING, "
    "patient_id STRING, lesion_id STRING"
)


def _accepted_row(image_id, checksum, lesion_id=None, patient_id=None, label="benign"):
    return ("isic_2019", image_id, f"archive:a.zip#{image_id}.jpg", checksum, 100, 100, "JPEG", label, patient_id, lesion_id)


def _groups_by_image(image_inventory_df):
    return {row["image_id"]: row["group_id"] for row in image_inventory_df.select("image_id", "group_id").collect()}


def test_assign_leakage_groups_uses_duplicate_then_lesion_then_patient_then_singleton(spark):
    accepted_df = spark.createDataFrame(
        [
            _accepted_row("dup1", "same-bytes", lesion_id="L9"),
            _accepted_row("dup2", "same-bytes"),
            _accepted_row("les1", "c1", lesion_id="L1", patient_id="P1"),
            _accepted_row("les2", "c2", lesion_id="L1"),
            _accepted_row("pat1", "c3", patient_id="P2"),
            _accepted_row("pat2", "c4", patient_id="P2"),
            _accepted_row("alone", "c5"),
        ],
        ACCEPTED_SCHEMA,
    )

    image_inventory_df, leakage_groups_df = assign_leakage_groups(accepted_df, ["malignancy"])
    group_by_image = _groups_by_image(image_inventory_df)

    # Identical bytes win over a lesion id, and share a group.
    assert group_by_image["dup1"] == group_by_image["dup2"] == "isic_2019:duplicate:same-bytes"
    # Lesion wins over patient.
    assert group_by_image["les1"] == group_by_image["les2"] == "isic_2019:lesion:L1"
    assert group_by_image["pat1"] == group_by_image["pat2"] == "isic_2019:patient:P2"
    assert group_by_image["alone"] == "isic_2019:singleton:alone"

    groups = {row["group_id"]: row for row in leakage_groups_df.collect()}
    assert groups["isic_2019:lesion:L1"]["image_count"] == 2
    assert groups["isic_2019:lesion:L1"]["group_source"] == "lesion_id"
    assert groups["isic_2019:singleton:alone"]["group_type"] == "singleton"


def test_assign_leakage_groups_marks_every_row_accepted(spark):
    accepted_df = spark.createDataFrame([_accepted_row("a", "c1")], ACCEPTED_SCHEMA)

    image_inventory_df, _ = assign_leakage_groups(accepted_df, ["malignancy"])
    row = image_inventory_df.collect()[0]

    assert row["validation_status"] == "accepted"
    assert row["validation_reason"] is None
    assert row["malignancy"] == "benign"


def test_build_accepted_rows_fills_a_missing_patient_id_column(spark):
    # MILK10k has no patient_id column at all.
    label_valid_df = spark.createDataFrame(
        [("m1", "all", "c1", "L1", "benign")],
        "image_id STRING, source_split STRING, source_checksum STRING, lesion_id STRING, malignancy STRING",
    )
    image_valid_df = spark.createDataFrame(
        [("m1", "all", "archive:a.zip#m1.jpg", 10, 20, "JPEG")],
        "image_id STRING, source_split STRING, bronze_uri STRING, image_width INT, image_height INT, image_format STRING",
    )

    row = build_accepted_rows(label_valid_df, image_valid_df, ["malignancy"], "milk10k").collect()[0]

    assert row["dataset_key"] == "milk10k"
    assert row["patient_id"] is None
    assert row["lesion_id"] == "L1"


def test_rejections_collect_rows_with_their_reason(spark):
    rejections = Rejections("isic_2019")
    rejected_df = spark.createDataFrame([("a", "archive:a.zip#a.jpg")], "image_id STRING, bronze_uri STRING")

    rejections.add(rejected_df, F.lit("unreadable image"))
    row = rejections.to_dataframe(spark).collect()[0]

    assert row["dataset_key"] == "isic_2019"
    assert row["image_id"] == "a"
    assert row["rejection_reason"] == "unreadable image"


def test_rejections_with_nothing_rejected_is_an_empty_frame_with_the_table_columns(spark):
    empty_df = Rejections("isic_2019").to_dataframe(spark)

    assert empty_df.count() == 0
    assert empty_df.columns == ["dataset_key", "image_id", "bronze_uri", "rejection_reason", "rejected_at"]


def test_apply_label_normalization_rejects_rows_without_any_label(spark):
    source_metadata_df = spark.createDataFrame(
        [
            ("a", "archive:a.zip#a.jpg", "Malignant", "Malignant melanocytic proliferations", None),
            ("b", "archive:a.zip#b.jpg", None, None, None),
        ],
        "image_id STRING, source_uri STRING, diagnosis_1 STRING, diagnosis_2 STRING, diagnosis_3 STRING",
    )
    rejections = Rejections("isic_2019")

    label_valid_df = apply_label_normalization(
        source_metadata_df,
        normalize_diagnosis_labels,
        ["diagnosis_1", "diagnosis_2", "diagnosis_3"],
        ["malignancy", "specific_diagnosis"],
        rejections,
        controlled_vocabularies={"malignancy": MALIGNANCY_VALUES},
    )

    assert [row["image_id"] for row in label_valid_df.collect()] == ["a"]
    assert label_valid_df.collect()[0]["malignancy"] == "malignant"
    assert [row["image_id"] for row in rejections.to_dataframe(spark).collect()] == ["b"]


def test_assert_controlled_vocabularies_raises_on_an_unknown_value(spark):
    df = spark.createDataFrame([("benign",), ("maybe",), (None,)], "malignancy STRING")

    with pytest.raises(ValueError, match="maybe"):
        assert_controlled_vocabularies(df, {"malignancy": MALIGNANCY_VALUES})


INVENTORY_SCHEMA = (
    "dataset_key STRING, image_id STRING, bronze_uri STRING, source_checksum STRING, "
    "malignancy STRING, group_id STRING"
)


def _inventory(spark):
    return spark.createDataFrame(
        [
            ("isic_2019", "a", "archive:a.zip#a.jpg", "c1", "benign", "g1"),
            ("isic_2019", "b", "archive:a.zip#b.jpg", "c2", "malignant", "g2"),
            ("isic_2019", "c", "archive:a.zip#c.jpg", "c3", None, "g1"),
            ("milk10k", "d", "archive:m.zip#d.jpg", "c4", "benign", "g1"),
        ],
        INVENTORY_SCHEMA,
    )


def _manifest(spark, git_commit="abc123"):
    split_df = spark.createDataFrame([("g1", "train")], "group_id STRING, split STRING")
    return build_manifest_rows(_inventory(spark), split_df, "isic_2019", "malignancy", "v1", 1, 2, "p1", git_commit)


def test_build_manifest_rows_keeps_only_the_datasets_labelled_images_in_selected_groups(spark):
    rows = _manifest(spark).collect()

    # b's group wasn't selected, c has no label, d belongs to another dataset.
    assert [row["image_id"] for row in rows] == ["a"]
    row = rows[0]
    assert (row["dataset_version"], row["split"], row["label"], row["git_commit"]) == ("v1", "train", "benign", "abc123")
    assert (row["sample_seed"], row["split_seed"], row["preprocessing_version"]) == (1, 2, "p1")


def test_build_manifest_rows_hash_is_stable(spark):
    first_hash = _manifest(spark).collect()[0]["manifest_row_hash"]

    assert first_hash == _manifest(spark).collect()[0]["manifest_row_hash"]
    assert len(first_hash) == 64


def test_build_manifest_rows_requires_a_git_commit(spark):
    with pytest.raises(ValueError, match="git_commit"):
        _manifest(spark, git_commit="")


def test_sql_string_escapes_quotes():
    assert sql_string("it's") == "'it''s'"
