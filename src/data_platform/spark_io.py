"""Shared Spark helpers for Bronze, Silver, Gold, and Gold-export notebooks.
Requires a live Spark session, not unit-testable locally.

Every function here is dataset-agnostic: none references an ISIC-2019-specific
column, table, or value. A dataset's notebook supplies the dataset-specific bits
(table/path dicts from `build_layout`, its `dataset_key`, which raw columns feed
label normalization, the resulting label column names, its own
`normalize_labels`-shaped function) and calls these in sequence.

Organized in five sections, marked below: shared low-level plumbing used by
every layer, the Bronze pipeline, the Silver pipeline, the Gold pipeline, and
the Gold export pipeline. This was briefly split into a `spark_io/` package
(one submodule per section) and then merged back into this single file —
not every pure-Python module in this project splits cleanly by medallion
layer (`files.py`/`dataset_layout.py` are genuinely used across Bronze,
Silver, *and* Gold export, not one layer each), and a partial, inconsistent
application of layer-based splitting was judged not worth keeping. The one
piece of that reorganization kept: `write_gold_shards_for_splits` (Gold shard
packing) lives in `data_platform.shard_export` instead of here, since it
needs no Spark session at all — an orthogonal "does this need Spark"
distinction, the same one that already separates this module from
`files.py`/`validate.py`/`labels.py`/`sampling.py`/`dataset_layout.py`, not a
medallion-layer split.

Never call `.cache()`/`.persist()`/`.unpersist()` anywhere in this module (or in
any notebook cell). Databricks serverless compute does not support them —
`DataFrame.cache()` triggers `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is
not supported on serverless compute`, so it fails at runtime rather than just
being a missed optimization. If a DataFrame is genuinely expensive and feeds
more than one downstream action, write it to a scratch Delta table and read it
back instead, which works everywhere including serverless.
"""

from __future__ import annotations

import logging
import functools
import time
from collections import defaultdict

import pandas as pd
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
    ArchiveMatches,
    IMAGE_SUFFIXES,
    count_zip_members_by_suffix,
    format_bronze_uri,
    iter_archive_image_rows,
    raise_on_checksum_mismatches,
    parse_bronze_uri,
)
from data_platform.validate import MAX_DIMENSION, MIN_DIMENSION, decode_batch

logger = logging.getLogger(__name__)

# ============================================================================
# Shared (Bronze + Silver) — generic plumbing, not tied to either layer
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
    """Build the archive:<source_archive_uri>#<archive_member_path> URI used to locate an
    image's bytes, as a lazy Spark Column expression.

    Unlike the old table-row pointer this replaces, this URI is real and resolvable:
    `data_platform.files.parse_bronze_uri` recovers (source_archive_uri, archive_member_path)
    from it, and that pair alone is enough to find the image's bytes in its source archive —
    no Bronze table lookup needed. This has to be true now, since no layer stores image bytes
    at all (see docs/decisions/004-stream-archives-no-blob-storage.md) — the pointer is the
    only way back to the bytes.

    This builds the same string as `data_platform.files.format_bronze_uri`, just as a Spark
    Column instead of a plain Python string — Spark Columns are evaluated per-row inside
    Spark, so this can't just call that function directly. Driver-side Python code
    reconstructing this string should call `format_bronze_uri` instead of retyping the format.
    """
    return F.concat(F.lit("archive:"), source_archive_uri, F.lit("#"), archive_member_path)


def _image_ids_df(spark, image_ids: list[str]):
    """Build a single-column (image_id STRING) DataFrame from a plain Python list, for an inner
    join against a table -- shared by every caller that needs to filter a table down to a known
    set of image_ids (validate_images' missing-candidate rejection, export_source_metadata_csv),
    instead of each re-typing the same createDataFrame call."""
    return spark.createDataFrame([{"image_id": image_id} for image_id in image_ids], "image_id STRING")


def scratch_table_name(silver_tables: dict, name: str) -> str:
    """Build a scratch table name in the same catalog.schema as the Silver tables.
    Overwritten on every run, so nothing here is meant to persist between runs."""
    schema = silver_tables["image_inventory"].rsplit(".", 1)[0]
    return f"{schema}._scratch_{name}"


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
    """Raise if any column in `controlled_vocabularies` holds a non-null value outside its allowed set.

    Checks every column in one aggregation (a single Spark action) rather than one
    `.collect()` per column. A violation means whatever produced that column's
    values doesn't map onto the shared vocabulary — a systematic bug in that code,
    not per-row data variance — so this raises and fails the run rather than
    rejecting individual rows the way `Rejections.add` does. `context` is an optional
    string (e.g. `f"dataset_key={dataset_key!r}"`) included in the error message to
    help locate the caller.
    """
    if not controlled_vocabularies:
        return

    unknown_columns = set(controlled_vocabularies) - set(df.columns)
    if unknown_columns:
        raise ValueError(f"controlled_vocabularies references column(s) not present on the DataFrame: {sorted(unknown_columns)}")

    agg_exprs = [
        F.collect_set(F.when(F.col(column).isNotNull() & ~F.col(column).isin(*allowed_values), F.col(column))).alias(column)
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
# Bronze pipeline — write_image_index_table/load_combined_metadata assume
# archive-based ingestion (say so in their own docstrings); write_ingestion_run
# and print_bronze_outputs are ingestion-source-agnostic.
#
# No image bytes are ever written here — write_image_index_table streams each
# archive only to compute checksum/byte_length, and records where the image
# lives (source_archive_uri + archive_member_path), never the bytes themselves.
# See docs/decisions/004-stream-archives-no-blob-storage.md.
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
    """Stream locally staged zip archives to compute each image's checksum/byte_length and
    write the index rows (no image bytes) to target_table in one overwrite, deduping
    (image_id, source_split) first. Index rows are small, so they're collected in memory and
    written once: the table is either fully replaced or left as it was. Returns
    image_manifest_df (image_id, source_split, source_uri, source_checksum).

    Each archive dict needs `staged_archive_path`, `source_split`, `archive_dbfs_path`, and
    `archive_filename` already set (e.g. by
    `data_platform.files.stage_archives_and_extract_metadata`). Assumes archive-based
    ingestion; a dataset ingesting images another way needs its own writer.
    """
    seen_keys = set()
    index_rows = []

    for archive in archives:
        archive_row_count = 0
        for row in iter_archive_image_rows(
            archive_path=archive["staged_archive_path"],
            source_split=archive["source_split"],
            source_archive_uri=archive["archive_dbfs_path"],
        ):
            key = (row["image_id"], row["source_split"])
            if key in seen_keys:
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


def write_ingestion_run(
    spark,
    bronze_tables: dict,
    ingestion_run_id: str,
    dataset_name: str,
    source_version: str,
    started_at,
    finished_at,
    records_seen: int,
    records_written: int,
    status: str = "success",
) -> None:
    """Write one row to the shared bronze.ingestion_runs table.

    Ingestion-source-agnostic — works the same whether records came from archives,
    an API, or anything else; the only thing every ingestion approach still shares.
    """
    run_row = [
        {
            "ingestion_run_id": ingestion_run_id,
            "dataset_name": dataset_name,
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
    spark, dbutils, display, landing_paths: dict, bronze_paths: dict, bronze_tables: dict, dataset_name: str
) -> None:
    """Print/display Bronze ingestion outputs: landed archive contents, Bronze metadata
    path contents, image index row counts by split, recent ingestion runs for this
    dataset, a metadata sample, and a sample index row. `image_index`/`source_metadata`
    are already dataset-specific tables (no filter needed); `ingestion_runs` is shared
    across datasets, so it's filtered by `dataset_name`.

    `spark`, `dbutils`, and `display` are passed in explicitly rather than assumed to
    be notebook globals, same reasoning as everywhere else in this module.
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
        .where(F.col("dataset_name") == dataset_name)
        .orderBy("started_at", ascending=False)
    )

    print("Source metadata sample:")
    display(spark.table(bronze_tables["source_metadata"]))

    print("Sample index row:")
    display(spark.table(bronze_tables["image_index"]).limit(1))


# ============================================================================
# Silver pipeline — reconcile_bronze_records, apply_label_normalization,
# validate_images, build_accepted_rows, assign_leakage_groups, write_silver_dataset,
# print_silver_outputs, called in that order from a dataset's Silver notebook.
# Nothing is written until write_silver_dataset, which replaces this dataset's rows
# in all three Silver tables, so a rerun always leaves exactly one run's results.
# Silver table DDL itself is not here — it's inline in
# 00_setup_storage_and_shared_tables.ipynb, alongside bronze.ingestion_runs and
# gold.manifest_rows, so that notebook shows every shared table it creates
# directly rather than through a wrapper function.
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
    """Apply a dataset-specific label-normalization function as a Spark UDF.

    `normalize_fn(*input_columns) -> dict[str, str]` supplies the mapping logic;
    this handles wrapping it as a UDF, exploding its dict output into real columns
    (one per `label_columns` entry), splitting rows into label-valid vs unlabeled
    (a row is valid if any label_columns value resolved), and rejecting the
    unlabeled ones. Returns the label-valid DataFrame.

    `controlled_vocabularies` optionally maps a subset of `label_columns` to the
    fixed set of values that column may hold (e.g. `{"malignancy":
    data_platform.labels.MALIGNANCY_VALUES}`). A `label_columns` entry absent
    from this dict (e.g. `specific_diagnosis`) is open-ended free text and is
    never checked — this guards only the label axes meant to be cross-dataset
    comparable (see docs/decisions/002-silver-label-columns-not-map.md).

    `scratch_table`, when given, is where the labelled rows are written once, so the label
    function isn't recomputed by every later step (use `scratch_table_name`).

    Checked on labeled_df (metadata only, before any image bytes are read), so
    it's cheap. A value outside the vocabulary means
    normalize_fn itself has a bug — every row with that underlying source value
    would fail identically, it isn't per-row data variance — so this raises and
    fails the run rather than rejecting individual rows the way the
    unresolved-label path below does.
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
    silver_tables: dict,
    rejections: Rejections,
    bronze_uri_col: str = "source_uri",
    min_dimension: int = MIN_DIMENSION,
    max_dimension: int = MAX_DIMENSION,
    decode_flush_rows: int = 2000,
):
    """Stream each archive in `archives` (already locally staged, e.g. by
    `data_platform.files.stage_archives_and_extract_metadata`) looking only for
    `label_valid_df`'s candidate images, decode+validate each one found via
    `data_platform.validate.decode_batch`, reject anything missing from its archive
    or failing decode/dimension validation, and return the DataFrame of valid,
    decoded images. Decode results go to a scratch table in the Silver schema
    (`silver_tables` is only used to name it), overwritten on every run.

    No image bytes are ever joined from a Bronze table or persisted anywhere — each
    candidate's bytes are read from its source archive, decoded, and discarded within
    this function (see docs/decisions/004-stream-archives-no-blob-storage.md). Archive
    streaming happens driver-side, the same already-proven access pattern Bronze
    ingestion uses (`write_image_index_table`), rather than distributing byte reads
    across executors.

    Each freshly-streamed image's checksum is compared against `source_checksum` already
    recorded on `label_valid_df` (Bronze's, computed at ingestion time) — raises
    immediately (does not just reject the affected rows) if any mismatch, since that
    means the source archive changed after ingestion, violating the immutability every
    downstream `bronze_uri` reference depends on, not routine per-row data variance. See
    docs/decisions/005-immutable-source-archives-checksum-verified.md.

    `archives` must be the same list shape `resolve_archive_paths`/
    `stage_archives_and_extract_metadata` already produce elsewhere in this project,
    staged locally before this call. `min_dimension`/`max_dimension` default to
    `data_platform.validate`'s defaults; override them for a dataset whose images are
    a structurally different size range (see docs/datasets/).
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

    scratch_table = scratch_table_name(silver_tables, "decoded_images")
    total_decoded = 0

    def flush(pending_rows: list[dict]) -> None:
        nonlocal total_decoded
        is_first = total_decoded == 0
        decode_fn = functools.partial(decode_batch, min_dimension=min_dimension, max_dimension=max_dimension)
        for output_df in decode_fn(iter([pd.DataFrame(pending_rows)])):
            writer = spark.createDataFrame(output_df, schema=DECODE_OUTPUT_SCHEMA).write.mode(
                "overwrite" if is_first else "append"
            )
            if is_first:
                writer = writer.option("overwriteSchema", "true")
            writer.saveAsTable(scratch_table)
        total_decoded += len(pending_rows)

    matches = ArchiveMatches(candidates_by_archive_uri, staged_path_by_archive_uri)
    pending_rows: list[dict] = []

    # Not a per-row data-quality issue when a mismatch turns up below -- the source
    # archive itself changed since Bronze ingestion computed source_checksum, which
    # breaks the immutability invariant every downstream reference (bronze_uri, and
    # eventually a Gold manifest) depends on. See
    # docs/decisions/005-immutable-source-archives-checksum-verified.md.
    for candidate_row, archive_row in matches:
        pending_rows.append(
            {
                "image_id": candidate_row["image_id"],
                "source_split": candidate_row["source_split"],
                "bronze_uri": format_bronze_uri(archive_row["source_archive_uri"], archive_row["archive_member_path"]),
                "image_bytes": archive_row["image_bytes"],
            }
        )
        if len(pending_rows) >= decode_flush_rows:
            flush(pending_rows)
            pending_rows = []

    if pending_rows:
        flush(pending_rows)

    raise_on_checksum_mismatches(matches.checksum_mismatches, "the checksum Bronze recorded at ingestion")

    decoded_df = spark.table(scratch_table) if total_decoded else spark.createDataFrame([], schema=DECODE_OUTPUT_SCHEMA)

    if matches.missing:
        unmatched_image_ids = [candidate["image_id"] for candidate in matches.missing]
        missing_ids_df = _image_ids_df(spark, unmatched_image_ids)
        missing_df = label_valid_df.join(missing_ids_df, on="image_id", how="inner")
        rejections.add(
            missing_df,
            F.lit("missing from source archive"),
            bronze_uri_col=bronze_uri_col,
            description="Label-valid rows missing from their source archive (rejected)",
        )

    image_invalid_df = decoded_df.where(~F.col("valid"))
    image_valid_df = decoded_df.where(F.col("valid"))

    rejections.add(
        image_invalid_df,
        F.col("validation_reason"),
        description="Images failing decode/dimension validation (rejected)",
    )

    logger.info(f"Candidate images decoded: {decoded_df.count()}")
    logger.info(f"Images passing validation: {image_valid_df.count()}")

    return image_valid_df


def build_accepted_rows(label_valid_df, image_valid_df, label_columns: list[str], dataset_key: str):
    """Join label-valid metadata rows to image-valid decode results into one
    accepted-row-shaped DataFrame: dataset_key, universal Silver columns, and
    label_columns.

    patient_id/lesion_id are optional per dataset (see docs/data_contract.md) — a
    dataset whose Bronze source metadata has no such column gets NULL there instead
    of this failing outright.
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
    """
    predicate = f"dataset_key = {sql_string(rejections.dataset_key)}"
    overwrite_where(spark, leakage_groups_df, silver_tables["leakage_groups"], predicate)
    overwrite_where(spark, image_inventory_df, silver_tables["image_inventory"], predicate)
    overwrite_where(spark, rejections.to_dataframe(spark), silver_tables["rejected_records"], predicate)

    logger.info(f"Accepted images written to image_inventory: {image_inventory_df.count()}")
    logger.info(f"Leakage-control groups: {leakage_groups_df.count()}")


def print_silver_outputs(
    spark, display, silver_tables: dict, label_columns: list[str], dataset_key: str, image_inventory_df, leakage_groups_df
) -> None:
    """Print/display accepted-vs-rejected summary stats for one dataset's Silver
    validation run: rejection reasons, validation status, per-label-column
    distribution, and leakage-group sizes.

    `image_inventory_df`/`leakage_groups_df` are the DataFrames returned by
    `assign_leakage_groups` for this same run — passed in rather
    than re-read from storage, since they're already the tables' current state (see
    that function's docstring). `rejected_records` has no such single in-memory
    DataFrame (rejections are written separately across three earlier pipeline
    stages), so it's read back from storage here and filtered by `dataset_key`,
    since the underlying table is shared across every dataset.

    `display` is passed in explicitly rather than assumed to be a notebook global,
    same reasoning as `spark`: it's injected into the calling notebook's namespace
    by the Databricks runtime, not into arbitrary imported modules.
    """
    print("Rejection reasons:")
    display(
        spark.table(silver_tables["rejected_records"])
        .where(F.col("dataset_key") == dataset_key)
        .groupBy("rejection_reason")
        .count()
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
# Gold pipeline — build_gold_candidate_groups, write_gold_manifest_rows,
# print_gold_outputs, called in that order from a Gold manifest notebook (e.g.
# 30_create_gold_manifest.ipynb). gold.manifest_rows's DDL is not here — it's inline
# in 00_setup_storage_and_shared_tables.ipynb, same reasoning as Silver's
# tables (that notebook also creates gold.manifest_registry, a view
# summarizing gold.manifest_rows one row per dataset_version). manifest_rows
# is one shared, cross-dataset table like Silver's, but versioned by
# dataset_version rather than partitioned per dataset — more than one release
# (e.g. "sample-v1" and a later "v1") can coexist in it.
# ============================================================================


def build_gold_candidate_groups(spark, silver_tables: dict, dataset_key: str, label_column: str) -> list[dict]:
    """Aggregate silver.image_inventory (dataset_key, non-null label_column) joined to
    silver.leakage_groups into one row per leakage-control group: group_id, image_count,
    and a representative label.

    The representative label is label_column's value on the lexicographically-first
    image_id in the group — a deliberate simplification, not a bug: most groups share
    one label across every image in them, and a small sample manifest doesn't need
    exact per-image precision here (see data_platform.sampling's own docstring for the
    same discipline applied to sampling/splitting).

    Metadata-only aggregation over the full accepted table for dataset_key (no image
    bytes — a cheap groupBy), then collected to the driver: one small row per *group*, not
    per image, so this stays cheap even at tens of thousands of source rows. Returns a
    plain list of dicts so data_platform.sampling's logic never needs Spark.
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
    """Raise if the same source_checksum appears under more than one of dataset_keys' accepted
    silver.image_inventory rows -- the sound signal for real cross-dataset image duplication.
    patient_id/lesion_id are dataset-issued and deliberately NOT compared here, since they're
    not guaranteed globally unique/consistent across datasets, unlike source_checksum (a
    SHA-256 of the raw bytes). See docs/decisions/003-cross-dataset-leakage-not-checked.md.

    A no-op when dataset_keys has fewer than 2 entries -- cross-dataset duplication is only
    possible once a manifest actually spans more than one dataset_key, matching
    build_gold_candidate_groups being called once per dataset_key rather than this check being
    folded into it. Call this once, before select_sample_and_splits assigns splits: a real
    duplicate that goes undetected could otherwise land in different splits across its two
    dataset_key copies -- real train/test leakage -- which is exactly what this check exists to
    catch before sampling happens, not report after the fact.

    Metadata-only aggregation (no image bytes), same posture as build_gold_candidate_groups.
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
        raise ValueError("No datasets to write -- every dataset in the manifest config was skipped.")

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
# Gold export pipeline — load_manifest_rows_for_export, export_source_metadata_csv,
# print_export_outputs, remove_expired_exports, called in that order from
# notebooks/31_export_gold_shards.ipynb, after a dataset_version's manifest rows
# already exist (written by write_gold_manifest_rows above). The actual
# shard-packing step between these two, `write_gold_shards_for_splits`, needs no
# Spark session at all and lives in `data_platform.shard_export` instead — see
# that module's docstring for why.
#
# Shards are a fully rebuildable, derived cache — never a second source of
# truth for image bytes, same as every other layer in this project — see
# docs/decisions/004-stream-archives-no-blob-storage.md. Retention itself is
# an open question, not decided — see
# docs/decisions/006-gold-shard-retention-undecided.md.
# ============================================================================


def load_manifest_rows_for_export(spark, gold_tables: dict, dataset_version: str) -> dict[str, list[dict]]:
    """Driver-collect gold.manifest_rows for one release, grouped by split.

    Metadata-only (image_id, dataset_key, bronze_uri, source_checksum, label, group_id)
    — no image bytes touched here. source_checksum is carried through so
    `data_platform.shard_export.write_gold_shards_for_splits` can verify each image is
    still what the manifest recorded
    (docs/decisions/005-immutable-source-archives-checksum-verified.md).
    Returns {split: [row_dict, ...]}, ready for write_gold_shards_for_splits to stream
    bytes for, one split at a time.
    """
    manifest_df = (
        spark.table(gold_tables["manifest_rows"])
        .where(F.col("dataset_version") == dataset_version)
        .select("image_id", "dataset_key", "bronze_uri", "source_checksum", "label", "group_id", "split")
    )

    rows_by_split: dict[str, list[dict]] = defaultdict(list)
    for row in manifest_df.collect():
        rows_by_split[row["split"]].append(row.asDict())

    return dict(rows_by_split)


def export_source_metadata_csv(
    spark, source_metadata_table: str, dataset_key: str, image_ids: list[str], destination_path: str
) -> int:
    """Write source_metadata_table's rows for exactly this release's image_ids to one CSV file
    at destination_path -- age/sex/anatomic-site/diagnosis-hierarchy and every other raw Bronze
    metadata column (see docs/data_contract.md's Bronze contracts) that otherwise never travels
    any further down the pipeline than Bronze, unreachable once a shard export is downloaded
    locally. Exported alongside the shards for local subgroup analysis, or for a multimodal
    (image + tabular/text) model to join in by image_id -- the baseline ResNet-18 classifier
    doesn't read it, but that's this project's current model, not a constraint this export
    enforces.

    One dataset_key at a time (source_metadata_table is already one dataset_key's own Bronze
    table, e.g. bronze.isic_2019_source_metadata) rather than a combined export across a
    multi-dataset_key release -- different datasets' source metadata schemas genuinely differ
    (docs/data_contract.md's two Bronze contracts don't share every column), so one CSV per
    dataset_key avoids forcing a lossy common schema. The output still gets a `dataset_key`
    column stamped on (source_metadata_table itself has none -- it's already scoped to one
    dataset), so a multimodal model combining more than one dataset_key's CSV can tell which
    dataset each row came from -- ml.metadata_preprocessing.build_metadata_transform's per-field
    `source_columns`/`value_map` reconciliation keys off exactly this column (see
    docs/decisions/009-pin-metadata-feature-config-per-training-run.md).

    Collected to the driver via toPandas() and written with pandas rather than Spark's own CSV
    writer, so destination_path is one real file, not a directory of part-files to reassemble --
    the same "collect small metadata to the driver" posture as build_gold_candidate_groups and
    load_manifest_rows_for_export, since this is release-scoped metadata rows, never image
    bytes. Returns the row count written.
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


def remove_expired_exports(dbutils, gold_root: str, max_age_days: int) -> list[str]:
    """Delete <dataset_version> shard-export subdirectories of gold_root whose most
    recent modification is older than max_age_days. Returns the list of paths removed.

    General-purpose utility, not wired into any default flow and not scheduled —
    shard retention is an open question, not a decided policy (see
    docs/decisions/006-gold-shard-retention-undecided.md). Deleting a stale export
    costs nothing but a future rerun of 31_export_gold_shards.ipynb to rebuild it, since
    shards are always a derived, fully rebuildable cache
    (docs/decisions/004-stream-archives-no-blob-storage.md) — but nothing in this
    project currently calls this on any automatic basis. Call it directly, with an
    explicit max_age_days, whenever/if a retention decision actually gets made.

    Takes `dbutils`, not `spark` — a Databricks-runtime-injected object like `spark`,
    but not "Spark" itself; this lives here rather than in a pure-Python module for
    that reason, even though it needs no Spark session.
    """
    cutoff_seconds = max_age_days * 86400
    now = time.time()
    removed = []

    for entry in dbutils.fs.ls(gold_root):
        if not entry.isDir():
            continue
        age_seconds = now - (entry.modificationTime / 1000)
        if age_seconds >= cutoff_seconds:
            dbutils.fs.rm(entry.path, recurse=True)
            removed.append(entry.path)

    return removed
