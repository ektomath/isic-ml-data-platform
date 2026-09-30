"""Spark steps for the Bronze, Silver, Gold and shard-export notebooks, in that order in this file.
Everything is dataset-agnostic: a dataset's notebook passes in its tables and paths (from
build_layout), its dataset_key and its label function. Tested on a local SparkSession in
tests/test_spark_io.py.

No .cache() or .persist(): serverless compute doesn't support them. A DataFrame reused by several
actions is written to a scratch table and read back instead.
"""

from __future__ import annotations

import functools
import logging
from collections import defaultdict

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from data_platform.files import (
    IMAGE_SUFFIXES,
    ArchiveMatches,
    count_zip_members_by_suffix,
    format_bronze_uri,
    iter_archive_image_rows,
    parse_bronze_uri,
    raise_on_checksum_mismatches,
)
from data_platform.validate import MAX_DIMENSION, MIN_DIMENSION, decode_image

logger = logging.getLogger(__name__)

# ============================================================================
# Shared helpers
# ============================================================================


def sql_string(value: str) -> str:
    """Quote value as a SQL string literal, for the replaceWhere predicates below."""
    return "'" + value.replace("'", "''") + "'"


def overwrite_where(spark, df, target_table: str, predicate: str) -> None:
    """Atomically replace the rows of target_table matching predicate with df (a Delta
    replaceWhere overwrite). Rows outside the predicate are untouched, and rows matching it
    that df no longer contains are removed, so a rerun leaves exactly what this run produced.

    df is aligned to the table's columns by name first: a column the table has but df lacks
    (for example a label column only another dataset fills) is written as null.
    """
    target_fields = spark.table(target_table).schema.fields
    aligned_df = df.select(
        *[
            F.col(field.name) if field.name in df.columns else F.lit(None).cast(field.dataType).alias(field.name)
            for field in target_fields
        ]
    )
    aligned_df.write.mode("overwrite").option("replaceWhere", predicate).saveAsTable(target_table)


def bronze_image_uri(source_archive_uri, archive_member_path):
    """The same `archive:<source_archive_uri>#<archive_member_path>` string as
    files.format_bronze_uri, built as a Spark Column. No layer stores image bytes, so this URI is
    the only way back to them (ADR 004).
    """
    return F.concat(F.lit("archive:"), source_archive_uri, F.lit("#"), archive_member_path)


def _image_ids_df(spark, image_ids: list[str]):
    """Build a single-column (image_id STRING) DataFrame from a plain Python list, for an inner
    join against a table -- shared by every caller that needs to filter a table down to a known
    set of image_ids (validate_images' missing-candidate rejection, export_source_metadata_csv),
    instead of each re-typing the same createDataFrame call."""
    return spark.createDataFrame([{"image_id": image_id} for image_id in image_ids], "image_id STRING")


def scratch_table_name(silver_tables: dict, dataset_key: str, name: str) -> str:
    """A scratch table in the Silver schema, overwritten on every run. Named per dataset, so two
    datasets' Silver runs can run at the same time."""
    schema = silver_tables["image_inventory"].rsplit(".", 1)[0]
    return f"{schema}._scratch_{dataset_key}_{name}"


REJECTED_RECORDS_SCHEMA = (
    "dataset_key STRING, image_id STRING, bronze_uri STRING, rejection_reason STRING, rejected_at TIMESTAMP"
)


class Rejections:
    """Collects one dataset's rejected rows during a Silver run.

    Nothing is written as rows are rejected: write_silver_dataset writes them all at the end,
    replacing the dataset's previous rejections, so a rerun never leaves stale rejections
    behind or an image in both the inventory and the rejections.
    """

    def __init__(self, dataset_key: str):
        self.dataset_key = dataset_key
        self._frames = []

    def add(self, df, reason, bronze_uri_col: str = "bronze_uri", description: str | None = None) -> None:
        """Record df's rows as rejected. `reason` is a Column: F.lit(...) for a fixed reason, or
        F.col(...) for a per-row reason already on df. Prints the count if `description` is given."""
        rejected_df = df.select(
            F.lit(self.dataset_key).alias("dataset_key"),
            F.col("image_id"),
            F.col(bronze_uri_col).alias("bronze_uri"),
            reason.alias("rejection_reason"),
            F.current_timestamp().alias("rejected_at"),
        )
        self._frames.append(rejected_df)
        if description:
            logger.info(f"{description}: {rejected_df.count()}")

    def to_dataframe(self, spark):
        """Every rejection added so far, as one DataFrame in the rejected_records shape."""
        if not self._frames:
            return spark.createDataFrame([], REJECTED_RECORDS_SCHEMA)
        return functools.reduce(lambda left, right: left.unionByName(right), self._frames)


def assert_controlled_vocabularies(df, controlled_vocabularies: dict[str, tuple[str, ...]], context: str = "") -> None:
    """Raise if any column in `controlled_vocabularies` holds a non-null value outside its allowed
    set. All columns are checked in one aggregation. A bad value means the code producing the column
    is wrong, not the row, so the run fails instead of rejecting rows. `context` is added to the
    error message.
    """
    if not controlled_vocabularies:
        return

    unknown_columns = set(controlled_vocabularies) - set(df.columns)
    if unknown_columns:
        raise ValueError(
            f"controlled_vocabularies references column(s) not present on the DataFrame: {sorted(unknown_columns)}"
        )

    agg_exprs = [
        F.collect_set(
            F.when(F.col(column).isNotNull() & ~F.col(column).isin(*allowed_values), F.col(column))
        ).alias(column)
        for column, allowed_values in controlled_vocabularies.items()
    ]
    result = df.agg(*agg_exprs).collect()[0]

    for column, allowed_values in controlled_vocabularies.items():
        invalid_values = list(result[column] or [])[:5]
        if invalid_values:
            suffix = f" for {context}" if context else ""
            raise ValueError(
                f"Column {column!r} has value(s) outside the canonical vocabulary {allowed_values}{suffix}: "
                f"{invalid_values}. Fix the code producing this column to map onto the canonical set "
                f"instead of inventing new values."
            )


# ============================================================================
# Bronze: index the archives (checksums and locations, never bytes), load the source
# metadata and record ingestion runs.
# ============================================================================

IMAGE_INDEX_SCHEMA = StructType(
    [
        StructField("image_id", StringType(), False),
        StructField("source_split", StringType(), False),
        StructField("archive_member_path", StringType(), False),
        StructField("source_archive_uri", StringType(), True),
        StructField("byte_length", LongType(), False),
        StructField("source_checksum", StringType(), False),
    ]
)


def write_image_index_table(spark, archives: list[dict], target_table: str):
    """Stream the staged archives to compute each image's checksum and size, then write the index
    rows (no bytes) to `target_table` in one overwrite. A second image with the same (image_id,
    source_split) is skipped, with a warning. Returns image_manifest_df (image_id, source_split,
    source_uri, source_checksum).

    Each archive dict needs `staged_archive_path`, `source_split`, `archive_dbfs_path` and
    `archive_filename`, as set by files.stage_archives_and_extract_metadata.
    """
    seen_keys = set()
    index_rows = []

    for archive in archives:
        archive_row_count = 0
        duplicate_count = 0
        for row in iter_archive_image_rows(
            archive_path=archive["staged_archive_path"],
            source_split=archive["source_split"],
            source_archive_uri=archive["archive_dbfs_path"],
        ):
            key = (row["image_id"], row["source_split"])
            if key in seen_keys:
                duplicate_count += 1
                continue
            seen_keys.add(key)
            row.pop("image_bytes")
            index_rows.append(row)
            archive_row_count += 1

        if archive_row_count == 0:
            # Diagnose locally (staged archive, member metadata only) before failing loudly.
            suffix_counts = count_zip_members_by_suffix(archive["staged_archive_path"])
            if not suffix_counts:
                raise RuntimeError(f"Archive is empty, no members found: {archive['archive_filename']}")
            raise RuntimeError(
                f"No files matched known image suffixes {sorted(IMAGE_SUFFIXES)} in "
                f"{archive['archive_filename']}; archive contains file types: {suffix_counts}"
            )
        logger.info(f"Indexed {archive_row_count} images for {archive['source_split']}")
        if duplicate_count:
            logger.warning(
                f"Skipped {duplicate_count} image(s) in {archive['archive_filename']} whose image_id "
                f"(the file name without its extension) was already indexed for {archive['source_split']}"
            )

    (
        spark.createDataFrame(index_rows, schema=IMAGE_INDEX_SCHEMA)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    logger.info(f"Images indexed: {len(index_rows)}")

    return spark.table(target_table).select(
        F.col("image_id"),
        F.col("source_split"),
        bronze_image_uri(F.col("source_archive_uri"), F.col("archive_member_path")).alias("source_uri"),
        F.col("source_checksum"),
    )


def load_combined_metadata(spark, archives: list[dict]):
    """Read each archive's extracted metadata CSV (archive["metadata_target_path"]),
    tag it with source_split, and union them into one raw metadata DataFrame.

    Assumes metadata arrives as one CSV per archive/split — a dataset whose metadata
    comes from an API or a different file format needs its own loader.
    """
    metadata_frames = [
        spark.read.option("header", True)
        .csv(archive["metadata_target_path"])
        .withColumn("source_split", F.lit(archive["source_split"]))
        for archive in archives
    ]
    raw_metadata_df = metadata_frames[0]
    for metadata_frame in metadata_frames[1:]:
        raw_metadata_df = raw_metadata_df.unionByName(metadata_frame, allowMissingColumns=True)
    return raw_metadata_df


def write_source_metadata(spark, source_metadata_df, target_table: str) -> int:
    """Replace target_table with this ingestion's source metadata and return the row count, like
    write_image_index_table does for the index. Raises first if (image_id, source_split) isn't
    unique, since every later join relies on that key.
    """
    duplicate_keys = (
        source_metadata_df.groupBy("image_id", "source_split").count().where(F.col("count") > 1).limit(5).collect()
    )
    if duplicate_keys:
        examples = [(row["image_id"], row["source_split"], row["count"]) for row in duplicate_keys]
        raise ValueError(
            f"Source metadata has more than one row per (image_id, source_split), first few "
            f"(image_id, source_split, rows): {examples}"
        )
    source_metadata_df.write.mode("overwrite").saveAsTable(target_table)
    row_count = spark.table(target_table).count()
    logger.info(f"Source metadata rows written: {row_count}")
    return row_count


def write_ingestion_run(
    spark,
    bronze_tables: dict,
    ingestion_run_id: str,
    dataset_key: str,
    source_version: str,
    started_at,
    finished_at,
    records_seen: int,
    records_written: int,
    status: str = "success",
) -> None:
    """Append one row to the shared bronze.ingestion_runs table."""
    run_row = [
        {
            "ingestion_run_id": ingestion_run_id,
            "dataset_key": dataset_key,
            "source_version": source_version,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "records_seen": records_seen,
            "records_written": records_written,
        }
    ]
    spark.createDataFrame(run_row).write.mode("append").saveAsTable(bronze_tables["ingestion_runs"])


def print_bronze_outputs(
    spark, dbutils, display, landing_paths: dict, bronze_paths: dict, bronze_tables: dict, dataset_key: str
) -> None:
    """Show what Bronze ingestion produced: the landed archives, the metadata folder, index counts
    by split, this dataset's recent ingestion runs, and sample rows.
    """
    print("Landing archive contents:")
    display(dbutils.fs.ls(landing_paths["dataset_archive"]))

    print("Bronze metadata path contents:")
    display(dbutils.fs.ls(bronze_paths["metadata"]))

    print("Bronze image index rows by split:")
    display(spark.table(bronze_tables["image_index"]).groupBy("source_split").count())

    print("Recent ingestion runs:")
    display(
        spark.table(bronze_tables["ingestion_runs"])
        .where(F.col("dataset_key") == dataset_key)
        .orderBy("started_at", ascending=False)
    )

    print("Source metadata sample:")
    display(spark.table(bronze_tables["source_metadata"]))

    print("Sample index row:")
    display(spark.table(bronze_tables["image_index"]).limit(1))


# ============================================================================
# Silver: reconcile_bronze_records, apply_label_normalization, validate_images,
# build_accepted_rows, assign_leakage_groups, write_silver_dataset and print_silver_outputs,
# in the order a dataset's Silver notebook calls them. Nothing is written until
# write_silver_dataset, which replaces the dataset's rows in all three Silver tables.
# ============================================================================

DECODE_OUTPUT_SCHEMA = StructType(
    [
        StructField("image_id", StringType(), False),
        StructField("source_split", StringType(), False),
        StructField("bronze_uri", StringType(), False),
        StructField("valid", BooleanType(), False),
        StructField("image_width", IntegerType(), True),
        StructField("image_height", IntegerType(), True),
        StructField("image_format", StringType(), True),
        StructField("validation_reason", StringType(), True),
    ]
)

CHECKSUM_DUP_MIN_COUNT = 2

# group_source only ever depends on group_type, so it's a static lookup rather than
# re-testing the checksum/lesion/patient predicates a second time. Every key and value
# here is a universal Silver column name, identical for any dataset.
GROUP_SOURCE_BY_TYPE = {
    "duplicate": "source_checksum",
    "lesion": "lesion_id",
    "patient": "patient_id",
    "singleton": "image_id",
}


def reconcile_bronze_records(spark, bronze_tables: dict, rejections: Rejections):
    """Reject any Bronze image index row with no matching source metadata row.

    An orphan index row is otherwise invisible to every downstream check, since the
    rest of the pipeline only ever looks from metadata to the index, never the other
    way. Returns the loaded Bronze source metadata DataFrame.
    """
    source_metadata_df = spark.table(bronze_tables["source_metadata"])

    image_index_keys_df = spark.table(bronze_tables["image_index"]).select(
        "image_id", "source_split", "source_archive_uri", "archive_member_path"
    )
    metadata_keys_df = source_metadata_df.select("image_id", "source_split")

    orphan_index_rows_df = (
        image_index_keys_df
        .join(metadata_keys_df, on=["image_id", "source_split"], how="left_anti")
        .withColumn(
            "bronze_uri",
            bronze_image_uri(F.col("source_archive_uri"), F.col("archive_member_path")),
        )
    )

    rejections.add(
        orphan_index_rows_df,
        F.lit("no matching Bronze source metadata"),
        description="Bronze image index rows with no matching source metadata (rejected)",
    )

    return source_metadata_df


def apply_label_normalization(
    source_metadata_df,
    normalize_fn,
    input_columns: list[str],
    label_columns: list[str],
    rejections: Rejections,
    bronze_uri_col: str = "source_uri",
    controlled_vocabularies: dict[str, tuple[str, ...]] | None = None,
    scratch_table: str | None = None,
):
    """Run a dataset's label function over the metadata and return the rows with a resolved label.

    `normalize_fn(*input_columns)` returns a dict of label values; it runs as a UDF and its output
    becomes one column per `label_columns` entry. Rows where no label resolved are rejected.

    `controlled_vocabularies` maps label columns to their allowed values (e.g. {"malignancy":
    MALIGNANCY_VALUES}); free-text columns such as specific_diagnosis are left out (ADR 002). A
    value outside the vocabulary fails the run, since it means normalize_fn is wrong. The check
    reads metadata only, before any image bytes.

    `scratch_table`, when given, is where the labelled rows are written once, so the label function
    isn't recomputed by every later step (use `scratch_table_name`).
    """
    normalize_fn_udf = F.udf(normalize_fn, MapType(StringType(), StringType()))

    labeled_df = source_metadata_df.withColumn("labels", normalize_fn_udf(*[F.col(c) for c in input_columns]))
    for label_column in label_columns:
        labeled_df = labeled_df.withColumn(label_column, F.col("labels")[label_column])
    labeled_df = labeled_df.drop("labels")
    if scratch_table is not None:
        # Written once and read back, so the Python UDF above runs once instead of on every
        # later action (serverless compute doesn't allow .cache()).
        labeled_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(scratch_table)
        labeled_df = labeled_df.sparkSession.table(scratch_table)

    assert_controlled_vocabularies(
        labeled_df, controlled_vocabularies or {}, context=f"dataset_key={rejections.dataset_key!r}"
    )

    is_label_valid = F.lit(False)
    for label_column in label_columns:
        is_label_valid = is_label_valid | F.col(label_column).isNotNull()

    label_valid_df = labeled_df.where(is_label_valid)
    unlabeled_df = labeled_df.where(~is_label_valid)

    rejections.add(
        unlabeled_df,
        F.lit("unmapped or missing label"),
        bronze_uri_col=bronze_uri_col,
        description="Rows with no resolvable label (rejected)",
    )

    logger.info(f"Rows with at least one resolvable label: {label_valid_df.count()}")

    return label_valid_df


def validate_images(
    spark,
    label_valid_df,
    archives: list[dict],
    rejections: Rejections,
    bronze_uri_col: str = "source_uri",
    min_dimension: int = MIN_DIMENSION,
    max_dimension: int = MAX_DIMENSION,
):
    """Decode and validate the images for `label_valid_df`'s rows and return the valid ones. Images
    missing from their archive or failing decode or dimension checks are rejected.

    The archives must already be staged locally; each is streamed once on the driver, as in Bronze
    ingestion. Each image is decoded as it's read and its bytes dropped straight away, so only the
    small result rows are kept, never the bytes (ADR 004).

    A checksum that no longer matches Bronze's fails the run instead of rejecting the row: the
    source archive changed after ingestion (ADR 005).

    `min_dimension`/`max_dimension` default to data_platform.validate's limits.
    """
    candidate_rows = label_valid_df.select(
        "image_id", "source_split", "source_checksum", F.col(bronze_uri_col).alias("bronze_uri")
    ).collect()

    candidates_by_archive_uri: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in candidate_rows:
        source_archive_uri, archive_member_path = parse_bronze_uri(row["bronze_uri"])
        candidates_by_archive_uri[source_archive_uri][archive_member_path] = {
            "image_id": row["image_id"],
            "source_split": row["source_split"],
            "source_checksum": row["source_checksum"],
        }

    staged_path_by_archive_uri = {archive["archive_dbfs_path"]: archive["staged_archive_path"] for archive in archives}
    matches = ArchiveMatches(candidates_by_archive_uri, staged_path_by_archive_uri)

    decoded_rows = []
    for candidate_row, archive_row in matches:
        outcome = decode_image(archive_row["image_bytes"], min_dimension=min_dimension, max_dimension=max_dimension)
        decoded_rows.append(
            {
                "image_id": candidate_row["image_id"],
                "source_split": candidate_row["source_split"],
                "bronze_uri": format_bronze_uri(archive_row["source_archive_uri"], archive_row["archive_member_path"]),
                "valid": outcome["valid"],
                "image_width": outcome.get("width"),
                "image_height": outcome.get("height"),
                "image_format": outcome.get("format"),
                "validation_reason": outcome.get("reason"),
            }
        )

    raise_on_checksum_mismatches(matches.checksum_mismatches, "the checksum Bronze recorded at ingestion")

    if matches.missing:
        missing_ids_df = _image_ids_df(spark, [candidate["image_id"] for candidate in matches.missing])
        rejections.add(
            label_valid_df.join(missing_ids_df, on="image_id", how="inner"),
            F.lit("missing from source archive"),
            bronze_uri_col=bronze_uri_col,
            description="Label-valid rows missing from their source archive (rejected)",
        )

    decoded_df = spark.createDataFrame(decoded_rows, schema=DECODE_OUTPUT_SCHEMA)
    rejections.add(
        decoded_df.where(~F.col("valid")),
        F.col("validation_reason"),
        description="Images failing decode/dimension validation (rejected)",
    )

    valid_count = sum(row["valid"] for row in decoded_rows)
    logger.info(f"Candidate images decoded: {len(decoded_rows)}, passing validation: {valid_count}")

    return decoded_df.where(F.col("valid"))


def build_accepted_rows(label_valid_df, image_valid_df, label_columns: list[str], dataset_key: str):
    """Join label-valid metadata to the valid decode results: one row per accepted image, with
    dataset_key, the Silver columns and the label columns. patient_id and lesion_id are NULL for a
    dataset that doesn't have them.
    """
    joined_df = label_valid_df.join(image_valid_df, on=["image_id", "source_split"])
    identity_columns = [
        F.col(name) if name in joined_df.columns else F.lit(None).cast("string").alias(name)
        for name in ("patient_id", "lesion_id")
    ]
    return (
        joined_df
        .select(
            F.lit(dataset_key).alias("dataset_key"),
            F.col("image_id"),
            F.col("bronze_uri"),
            F.col("source_checksum"),
            F.col("image_width"),
            F.col("image_height"),
            F.col("image_format"),
            *[F.col(c) for c in label_columns],
            *identity_columns,
        )
    )


def assign_leakage_groups(accepted_df, label_columns: list[str]):
    """Assign every accepted image a leakage-control group and build the Silver rows.

    Group priority: exact duplicates (same source_checksum within the dataset), then same
    lesion_id, then same patient_id, otherwise a group of its own. Grouping is scoped per
    dataset, and group_id embeds dataset_key so it stays unique across datasets even if two
    reuse the same IDs. accepted_df must carry dataset_key (build_accepted_rows adds it).

    Pure transformation, nothing written: returns (image_inventory_df, leakage_groups_df)
    for write_silver_dataset.
    """
    checksum_counts_df = accepted_df.groupBy("dataset_key", "source_checksum").agg(
        F.count("*").alias("checksum_count")
    )
    group_source_map = F.create_map(*[F.lit(item) for pair in GROUP_SOURCE_BY_TYPE.items() for item in pair])

    grouped_df = (
        accepted_df.join(checksum_counts_df, on=["dataset_key", "source_checksum"])
        .withColumn(
            "group_type",
            F.when(F.col("checksum_count") >= CHECKSUM_DUP_MIN_COUNT, F.lit("duplicate"))
            .when(F.col("lesion_id").isNotNull(), F.lit("lesion"))
            .when(F.col("patient_id").isNotNull(), F.lit("patient"))
            .otherwise(F.lit("singleton")),
        )
        .withColumn("group_source", group_source_map[F.col("group_type")])
        .withColumn(
            # The grouping key's value still branches on group_type: Spark can't pick a
            # column by another column's value without a UDF.
            "group_key_value",
            F.when(F.col("group_type") == "duplicate", F.col("source_checksum"))
            .when(F.col("group_type") == "lesion", F.col("lesion_id"))
            .when(F.col("group_type") == "patient", F.col("patient_id"))
            .otherwise(F.col("image_id")),
        )
        .withColumn(
            "group_id",
            F.concat(F.col("dataset_key"), F.lit(":"), F.col("group_type"), F.lit(":"), F.col("group_key_value")),
        )
        .drop("group_key_value")
    )

    leakage_groups_df = grouped_df.groupBy("dataset_key", "group_id", "group_type", "group_source").agg(
        F.count("*").alias("image_count")
    )

    image_inventory_df = grouped_df.select(
        F.col("dataset_key"),
        F.col("image_id"),
        F.col("bronze_uri"),
        F.col("source_checksum"),
        F.col("image_width"),
        F.col("image_height"),
        F.col("image_format"),
        F.lit("accepted").alias("validation_status"),
        F.lit(None).cast("string").alias("validation_reason"),
        *[F.col(c) for c in label_columns],
        F.col("patient_id"),
        F.col("lesion_id"),
        F.col("group_id"),
        F.current_timestamp().alias("validated_at"),
    )

    return image_inventory_df, leakage_groups_df


def write_silver_dataset(
    spark, silver_tables: dict, image_inventory_df, leakage_groups_df, rejections: Rejections
) -> None:
    """Replace this dataset's rows in all three shared Silver tables with this run's results.

    Each table is replaced atomically for `dataset_key = <this dataset>` (Delta replaceWhere),
    so rows from an earlier run can't linger: an image accepted last time and rejected now
    leaves the inventory, and vice versa. Other datasets' rows are untouched.

    The three writes aren't one transaction. The inventory, which Gold reads, is written last, so
    a failure part-way leaves it as it was; a rerun then brings all three in line.
    """
    predicate = f"dataset_key = {sql_string(rejections.dataset_key)}"
    overwrite_where(spark, rejections.to_dataframe(spark), silver_tables["rejected_records"], predicate)
    overwrite_where(spark, leakage_groups_df, silver_tables["leakage_groups"], predicate)
    overwrite_where(spark, image_inventory_df, silver_tables["image_inventory"], predicate)

    for name in ("image_inventory", "leakage_groups", "rejected_records"):
        row_count = _dataset_rows(spark, silver_tables[name], rejections.dataset_key).count()
        logger.info(f"{name}: {row_count} rows for {rejections.dataset_key}")


def _dataset_rows(spark, table: str, dataset_key: str):
    """One dataset's rows of a shared Silver table."""
    return spark.table(table).where(F.col("dataset_key") == dataset_key)


def print_silver_outputs(spark, display, silver_tables: dict, label_columns: list[str], dataset_key: str) -> None:
    """Show one dataset's Silver results, read back from the written tables: rejection reasons,
    validation status, label distributions and leakage-group sizes.
    """
    image_inventory_df = _dataset_rows(spark, silver_tables["image_inventory"], dataset_key)
    leakage_groups_df = _dataset_rows(spark, silver_tables["leakage_groups"], dataset_key)

    print("Rejection reasons:")
    display(
        _dataset_rows(spark, silver_tables["rejected_records"], dataset_key).groupBy("rejection_reason").count()
    )

    print("Validation status:")
    display(image_inventory_df.groupBy("validation_status").count())

    for label_column in label_columns:
        print(f"{label_column} distribution:")
        display(image_inventory_df.groupBy(label_column).count().orderBy(F.desc("count")))

    print("Leakage-control group summary:")
    display(
        leakage_groups_df
        .groupBy("group_type")
        .agg(F.count("*").alias("num_groups"), F.sum("image_count").alias("total_images"))
    )


# ============================================================================
# Gold: build_gold_candidate_groups, assert_no_cross_dataset_duplicate_checksums,
# build_manifest_rows, write_gold_release and print_gold_outputs, called from
# 30_create_gold_manifest.ipynb. gold.manifest_rows holds every release, keyed by
# dataset_version.
# ============================================================================


def build_gold_candidate_groups(spark, silver_tables: dict, dataset_key: str, label_column: str) -> list[dict]:
    """Summarize one dataset's accepted, labelled images as one row per leakage group: group_id,
    image_count and label. Returned as a list of dicts, so sampling runs without Spark.

    A group's label is the one on its first image_id. Groups nearly always share one label, so this
    simplification is accepted.
    """
    inventory_df = (
        spark.table(silver_tables["image_inventory"])
        .where((F.col("dataset_key") == dataset_key) & F.col(label_column).isNotNull())
        .select("group_id", "image_id", F.col(label_column).alias("label"))
    )

    representative_label_df = (
        inventory_df
        .withColumn(
            "row_number",
            F.row_number().over(Window.partitionBy("group_id").orderBy("image_id")),
        )
        .where(F.col("row_number") == 1)
        .select("group_id", "label")
    )

    group_sizes_df = inventory_df.groupBy("group_id").agg(F.count("*").alias("image_count"))

    groups_df = group_sizes_df.join(representative_label_df, on="group_id")

    return [row.asDict() for row in groups_df.collect()]


def assert_no_cross_dataset_duplicate_checksums(spark, silver_tables: dict, dataset_keys: list[str]) -> None:
    """Raise if the same image (the same source_checksum) was accepted in more than one of
    `dataset_keys`. Run it before splits are assigned, since two copies of one image could otherwise
    land in train and test. Patient and lesion IDs aren't compared, because each dataset issues its
    own (ADR 003). Does nothing for a single dataset.
    """
    if len(dataset_keys) < 2:
        return

    duplicate_rows = (
        spark.table(silver_tables["image_inventory"])
        .where(F.col("dataset_key").isin(dataset_keys))
        .groupBy("source_checksum")
        .agg(
            F.collect_set("dataset_key").alias("duplicate_dataset_keys"),
            F.collect_list("image_id").alias("duplicate_image_ids"),
        )
        .where(F.size("duplicate_dataset_keys") > 1)
        .collect()
    )

    if duplicate_rows:
        examples = [
            {
                "source_checksum": row["source_checksum"],
                "dataset_keys": row["duplicate_dataset_keys"],
                "image_ids": row["duplicate_image_ids"],
            }
            for row in duplicate_rows[:5]
        ]
        raise ValueError(
            f"{len(duplicate_rows)} image(s) (by source_checksum) appear in more than one of "
            f"{dataset_keys} -- real cross-dataset duplicates. Left unresolved, the same image "
            f"could land in different splits across its two dataset_key copies -- real "
            f"train/test leakage. First few: {examples}. See "
            f"docs/decisions/003-cross-dataset-leakage-not-checked.md."
        )


def build_manifest_rows(
    inventory_df,
    split_df,
    dataset_key: str,
    label_column: str,
    dataset_version: str,
    sample_seed: int,
    split_seed: int,
    preprocessing_version: str,
    git_commit: str,
):
    """One dataset's rows for a Gold release: its accepted images (non-null label_column) in the
    selected groups, each with its split, seeds, preprocessing version, row hash and the git
    commit of the code writing the release.

    `inventory_df` is silver.image_inventory (or rows shaped like it); `split_df` has one row per
    selected group (group_id, split). Pure transformation, nothing written.

    sample_seed and split_seed are separate columns so gold.manifest_registry can show that two
    releases drew from the same image pool (same sample_seed) even if their splits differ.
    """
    if not git_commit:
        raise ValueError("git_commit is required -- resolve it with data_platform.provenance.resolve_git_commit")

    return (
        inventory_df
        .where((F.col("dataset_key") == dataset_key) & F.col(label_column).isNotNull())
        .join(split_df, on="group_id", how="inner")
        .select(
            F.lit(dataset_version).alias("dataset_version"),
            F.col("dataset_key"),
            F.col("image_id"),
            F.col("bronze_uri"),
            F.col("source_checksum"),
            F.col(label_column).alias("label"),
            F.col("group_id"),
            F.col("split"),
            F.lit(sample_seed).alias("sample_seed"),
            F.lit(split_seed).alias("split_seed"),
            F.lit(preprocessing_version).alias("preprocessing_version"),
        )
        .withColumn(
            "manifest_row_hash",
            F.sha2(F.concat_ws("|", "dataset_key", "image_id", "label", "split", "source_checksum"), 256),
        )
        .withColumn("created_at", F.current_timestamp())
        .withColumn("git_commit", F.lit(git_commit))
    )


def write_gold_release(
    spark,
    silver_tables: dict,
    gold_tables: dict,
    split_by_group_by_dataset: dict[str, dict[str, str]],
    label_column: str,
    dataset_version: str,
    sample_seed: int,
    split_seed: int,
    preprocessing_version: str,
    git_commit: str,
    overwrite_existing: bool = False,
) -> None:
    """Publish one Gold release: every included dataset's rows, written to gold.manifest_rows in
    a single atomic replace of `dataset_version = <this release>`.

    Releases are immutable once published: if dataset_version already has rows, this raises
    unless `overwrite_existing` is set, because models may already be trained on it. Give a
    changed selection a new dataset_version instead. With `overwrite_existing`, the old release
    is replaced entirely, never merged with the new one.

    `split_by_group_by_dataset` maps each dataset_key to select_sample_and_splits' output
    ({group_id: split}).
    """
    if not split_by_group_by_dataset:
        raise ValueError("No datasets to write.")

    manifest_table = gold_tables["manifest_rows"]
    already_published = (
        spark.table(manifest_table).where(F.col("dataset_version") == dataset_version).limit(1).count() > 0
    )
    if already_published and not overwrite_existing:
        raise ValueError(
            f"dataset_version={dataset_version!r} is already published in {manifest_table}. Releases "
            f"are immutable: give this selection a new dataset_version in its manifest config, or set "
            f"OVERWRITE_EXISTING_RELEASE = True if you really mean to replace it."
        )

    inventory_df = spark.table(silver_tables["image_inventory"])
    release_frames = []
    for dataset_key, split_by_group in split_by_group_by_dataset.items():
        split_rows = [{"group_id": group_id, "split": split} for group_id, split in split_by_group.items()]
        split_df = spark.createDataFrame(split_rows, schema="group_id STRING, split STRING")
        release_frames.append(
            build_manifest_rows(
                inventory_df, split_df, dataset_key, label_column, dataset_version,
                sample_seed, split_seed, preprocessing_version, git_commit,
            )
        )
    release_df = functools.reduce(lambda left, right: left.unionByName(right), release_frames)

    overwrite_where(spark, release_df, manifest_table, f"dataset_version = {sql_string(dataset_version)}")

    written = spark.table(manifest_table).where(F.col("dataset_version") == dataset_version)
    for row in written.groupBy("dataset_key").count().collect():
        logger.info(f"{row['dataset_key']}: {row['count']} manifest rows in dataset_version={dataset_version!r}")


def print_gold_outputs(
    spark, display, gold_tables: dict, dataset_version: str, dataset_keys: list[str], label_column: str
) -> None:
    """Print/display gold.manifest_rows summary for one dataset_version: rows per
    dataset_key, split distribution, label distribution by split, and a real
    leakage-control invariant check — raises if any group_id spans more than one split
    within this dataset_version, rather than only describing output like
    print_silver_outputs does.
    """
    manifest_df = (
        spark.table(gold_tables["manifest_rows"])
        .where((F.col("dataset_version") == dataset_version) & F.col("dataset_key").isin(dataset_keys))
    )

    print("Rows per dataset_key:")
    display(manifest_df.groupBy("dataset_key").count())

    print("Split distribution:")
    display(manifest_df.groupBy("split").count())

    print(f"{label_column} distribution by split:")
    display(manifest_df.groupBy("split", "label").count().orderBy("split", F.desc("count")))

    leaking_groups_df = (
        manifest_df.groupBy("group_id").agg(F.countDistinct("split").alias("num_splits")).where(F.col("num_splits") > 1)
    )
    leaking_group_count = leaking_groups_df.count()
    if leaking_group_count:
        display(leaking_groups_df)
        raise ValueError(
            f"{leaking_group_count} group_id(s) span more than one split within "
            f"dataset_version={dataset_version!r} — leakage-control invariant violated."
        )


# ============================================================================
# Gold export: load_manifest_rows_for_export, export_source_metadata_csv and
# print_export_outputs, called from 31_export_gold_shards.ipynb. Writing the shards needs no
# Spark and lives in data_platform.shard_export.
# ============================================================================


def load_manifest_rows_for_export(spark, gold_tables: dict, dataset_version: str) -> dict[str, list[dict]]:
    """Collect one release's gold.manifest_rows to the driver, grouped by split: {split: [row,
    ...]}. Raises if the release has no rows. Metadata only; source_checksum is included so the shard
    export can verify each image (ADR 005).
    """
    manifest_df = (
        spark.table(gold_tables["manifest_rows"])
        .where(F.col("dataset_version") == dataset_version)
        .select("image_id", "dataset_key", "bronze_uri", "source_checksum", "label", "group_id", "split")
    )

    rows_by_split: dict[str, list[dict]] = defaultdict(list)
    for row in manifest_df.collect():
        rows_by_split[row["split"]].append(row.asDict())
    if not rows_by_split:
        raise ValueError(
            f"No gold.manifest_rows for dataset_version={dataset_version!r}; run the Gold manifest "
            "notebook (30_create_gold_manifest) for this release first."
        )
    return dict(rows_by_split)


def export_source_metadata_csv(
    spark, source_metadata_table: str, dataset_key: str, image_ids: list[str], destination_path: str
) -> int:
    """Write one dataset's Bronze source metadata (age, sex, anatomic site, diagnosis levels and the
    rest) for this release's image_ids to a single CSV at `destination_path`. Returns the row count.

    This metadata otherwise stays in Bronze. It's exported beside the shards for subgroup analysis,
    or for a model that also takes tabular input, joined on image_id. There's one CSV per dataset
    because their metadata columns differ; each row gets a dataset_key column so rows from several
    datasets can be told apart (ADR 009).

    Written with pandas so the output is one file rather than a folder of Spark part files; the rows
    are small.
    """
    image_ids_df = _image_ids_df(spark, image_ids)
    metadata_pdf = (
        spark.table(source_metadata_table)
        .join(image_ids_df, on="image_id", how="inner")
        .withColumn("dataset_key", F.lit(dataset_key))
        .toPandas()
    )
    metadata_pdf.to_csv(destination_path, index=False)
    return len(metadata_pdf)


def print_export_outputs(spark, gold_tables: dict, dataset_version: str, per_split_summaries: dict) -> None:
    """Print each split's written sample count next to the release's row count for that split in
    gold.manifest_rows, and raise if any differ: a shard export must contain exactly the
    release's rows."""
    expected_by_split = {
        row["split"]: row["count"]
        for row in spark.table(gold_tables["manifest_rows"])
        .where(F.col("dataset_version") == dataset_version)
        .groupBy("split")
        .count()
        .collect()
    }
    if not expected_by_split:
        raise ValueError(
            f"No gold.manifest_rows for dataset_version={dataset_version!r}; run the Gold manifest "
            "notebook (30_create_gold_manifest) first."
        )

    print(f"Shard export summary for dataset_version={dataset_version!r}:")
    mismatches = []
    for split in sorted(set(expected_by_split) | set(per_split_summaries)):
        expected_count = expected_by_split.get(split, 0)
        written_count = per_split_summaries.get(split, {}).get("sample_count", 0)
        print(f"  {split}: {written_count} samples written (manifest has {expected_count})")
        if written_count != expected_count:
            mismatches.append((split, expected_count, written_count))

    if mismatches:
        raise ValueError(
            f"Shard sample counts don't match the manifest for dataset_version={dataset_version!r} "
            f"(split, expected, written): {mismatches}"
        )
