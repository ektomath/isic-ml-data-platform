"""Shared Spark helpers for Bronze and Silver notebooks. Requires a live Spark session, not unit-testable locally.

Every function here is dataset-agnostic: none references an ISIC-2019-specific
column, table, or value. A dataset's notebook supplies the dataset-specific bits
(table/path dicts from `build_layout`, its `dataset_key`, which raw columns feed
label normalization, the resulting label column names, its own
`normalize_labels`-shaped function) and calls these in sequence.

Organized in three sections, marked below: shared low-level plumbing used by both
layers, the Bronze pipeline, and the Silver pipeline. If this file grows much
further (e.g. once Gold adds its own functions), split it into a `spark_io/`
package along the same three-way line instead of letting one file keep growing.

Never call `.cache()`/`.persist()`/`.unpersist()` anywhere in this module (or in
any notebook cell). Databricks serverless compute does not support them —
`DataFrame.cache()` triggers `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is
not supported on serverless compute`, so it fails at runtime rather than just
being a missed optimization. Where a DataFrame is genuinely expensive (image
bytes, a `mapInPandas` decode) and feeds more than one downstream action, use
`materialize()` below instead — a real write-then-read round trip through a
scratch Delta table, which is supported everywhere including serverless.
"""

from __future__ import annotations

import functools

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from data_platform.files import IMAGE_SUFFIXES, count_zip_members_by_suffix, iter_image_blob_rows
from data_platform.validate import MAX_DIMENSION, MIN_DIMENSION, decode_batch

# ============================================================================
# Shared (Bronze + Silver) — generic plumbing, not tied to either layer
# ============================================================================


def merge_into(spark, df, target_table: str, key_columns: list[str], view_name: str = "staged_merge_source") -> None:
    """Upsert df into target_table via MERGE INTO, matched on key_columns."""
    df.createOrReplaceTempView(view_name)
    on_clause = " AND ".join(f"target.{column} = source.{column}" for column in key_columns)
    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING {view_name} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def bronze_image_uri(table_name: str, source_split, image_id):
    """Build the table:<table>/<source_split>/<image_id> URI used to address a Bronze image blob row."""
    return F.concat(F.lit(f"table:{table_name}/"), source_split, F.lit("/"), image_id)


def _scratch_table(silver_tables: dict, name: str) -> str:
    """Build a scratch table name in the same catalog.schema as the Silver tables.
    Overwritten on every run, so nothing here is meant to persist between runs."""
    schema = silver_tables["image_inventory"].rsplit(".", 1)[0]
    return f"{schema}._scratch_{name}"


def materialize(spark, df, table_name: str):
    """Write df to table_name and read it back — the serverless-safe substitute for
    .cache()/.persist() (see this module's docstring). Use this only for a
    DataFrame that's both expensive to compute and consumed by more than one
    downstream action; a cheap or single-consumer DataFrame doesn't need it.
    """
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    return spark.table(table_name)


def reject_rows(
    spark,
    df,
    target_table: str,
    dataset_key: str,
    reason,
    bronze_uri_col: str = "bronze_uri",
    description: str | None = None,
):
    """Build one rejected_records-shaped row per row in df and MERGE it into target_table.

    `reason` is a Column expression (F.lit(...) for a fixed reason, F.col(...) to
    carry through a per-row reason already present in df). `dataset_key` identifies
    which dataset these rejections belong to on the shared, cross-dataset
    rejected_records table (matched together with image_id, since image_id alone
    isn't guaranteed unique across datasets). Returns the already-written rejection
    DataFrame; prints its row count if `description` is given.
    """
    rejected_df = df.select(
        F.lit(dataset_key).alias("dataset_key"),
        F.col("image_id"),
        F.col(bronze_uri_col).alias("bronze_uri"),
        reason.alias("rejection_reason"),
        F.current_timestamp().alias("rejected_at"),
    )
    merge_into(spark, rejected_df, target_table, ["dataset_key", "image_id"])
    if description:
        print(f"{description}: {rejected_df.count()}")
    return rejected_df


# ============================================================================
# Bronze pipeline — write_image_blob_table/load_combined_metadata assume
# archive-based ingestion (say so in their own docstrings); write_ingestion_run
# and print_bronze_outputs are ingestion-source-agnostic.
# ============================================================================

IMAGE_BLOB_SCHEMA = StructType(
    [
        StructField("image_id", StringType(), False),
        StructField("source_split", StringType(), False),
        StructField("archive_member_path", StringType(), False),
        StructField("source_archive_uri", StringType(), True),
        StructField("image_bytes", BinaryType(), False),
        StructField("byte_length", LongType(), False),
        StructField("source_checksum", StringType(), False),
    ]
)
IMAGE_BLOB_WRITE_BATCH_MAX_BYTES = 1024 * 1024 * 1024
IMAGE_BLOB_WRITE_BATCH_MAX_ROWS = 10_000


def write_image_blob_table(
    spark,
    archives: list[dict],
    target_table: str,
    batch_max_bytes: int = IMAGE_BLOB_WRITE_BATCH_MAX_BYTES,
    batch_max_rows: int = IMAGE_BLOB_WRITE_BATCH_MAX_ROWS,
):
    """Read image bytes from locally staged zip archives and write them to
    target_table in bounded batches, deduping (image_id, source_split) in memory
    before any bytes hit storage. Returns image_manifest_df (image_id, source_split,
    source_uri, source_checksum) read back from the written table.

    Each archive dict needs `staged_archive_path`, `source_split`, `archive_dbfs_path`,
    and `archive_filename` already set (e.g. by
    `data_platform.files.stage_archives_and_extract_metadata`). Assumes archive-based
    ingestion — a dataset ingesting images from an API/other source needs its own writer.
    """
    seen_keys = set()
    total_images_written = 0

    def flush(batch_rows):
        is_first = total_images_written == 0
        writer = spark.createDataFrame(batch_rows, schema=IMAGE_BLOB_SCHEMA).write.mode(
            "overwrite" if is_first else "append"
        )
        if is_first:
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(target_table)

    for archive in archives:
        batch = []
        batch_bytes = 0
        images_written_before_archive = total_images_written

        for row in iter_image_blob_rows(
            archive_path=archive["staged_archive_path"],
            source_split=archive["source_split"],
            source_archive_uri=archive["archive_dbfs_path"],
        ):
            key = (row["image_id"], row["source_split"])
            if key in seen_keys:
                continue
            seen_keys.add(key)

            batch.append(row)
            batch_bytes += row["byte_length"]
            if batch_bytes >= batch_max_bytes or len(batch) >= batch_max_rows:
                flush(batch)
                total_images_written += len(batch)
                batch = []
                batch_bytes = 0

        if batch:
            flush(batch)
            total_images_written += len(batch)

        archive_images_written = total_images_written - images_written_before_archive
        if archive_images_written == 0:
            # Diagnose locally (staged archive, member metadata only) before failing loudly —
            # no Volume or Spark cost either way.
            suffix_counts = count_zip_members_by_suffix(archive["staged_archive_path"])
            if not suffix_counts:
                raise RuntimeError(f"Archive is empty, no members found: {archive['archive_filename']}")
            raise RuntimeError(
                f"No files matched known image suffixes {sorted(IMAGE_SUFFIXES)} in "
                f"{archive['archive_filename']}; archive contains file types: {suffix_counts}"
            )

        print(f"Wrote {archive_images_written} image rows for {archive['source_split']}")

    image_blob_df = spark.table(target_table)
    image_manifest_df = image_blob_df.select(
        F.col("image_id"),
        F.col("source_split"),
        bronze_image_uri(target_table, F.col("source_split"), F.col("image_id")).alias("source_uri"),
        F.col("source_checksum"),
    )

    print(f"Images written to blob table: {total_images_written}")

    return image_manifest_df


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
    path contents, image blob row counts by split, recent ingestion runs for this
    dataset, a metadata sample, and a sample image row. `image_blobs`/`source_metadata`
    are already dataset-specific tables (no filter needed); `ingestion_runs` is shared
    across datasets, so it's filtered by `dataset_name`.

    `spark`, `dbutils`, and `display` are passed in explicitly rather than assumed to
    be notebook globals, same reasoning as everywhere else in this module.
    """
    print("Landing archive contents:")
    display(dbutils.fs.ls(landing_paths["dataset_archive"]))

    print("Bronze metadata path contents:")
    display(dbutils.fs.ls(bronze_paths["metadata"]))

    print("Bronze image blob rows by split:")
    display(spark.table(bronze_tables["image_blobs"]).groupBy("source_split").count())

    print("Recent ingestion runs:")
    display(
        spark.table(bronze_tables["ingestion_runs"])
        .where(F.col("dataset_name") == dataset_name)
        .orderBy("started_at", ascending=False)
    )

    print("Source metadata sample:")
    display(spark.table(bronze_tables["source_metadata"]))

    print("Sample image row:")
    display(spark.table(bronze_tables["image_blobs"]).limit(1))


# ============================================================================
# Silver pipeline — reconcile_bronze_records, apply_label_normalization,
# validate_images, build_accepted_rows, assign_leakage_groups_and_write_inventory,
# print_silver_outputs, called in that order from a dataset's Silver notebook.
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
GROUP_SOURCE_MAP = F.create_map(*[F.lit(item) for pair in GROUP_SOURCE_BY_TYPE.items() for item in pair])


def reconcile_bronze_records(spark, bronze_tables: dict, silver_tables: dict, dataset_key: str):
    """Reject any Bronze image blob with no matching source metadata row.

    An orphan blob is otherwise invisible to every downstream check, since the
    rest of the pipeline only ever looks from metadata to blobs, never the other
    way. Returns the loaded Bronze source metadata DataFrame.
    """
    source_metadata_df = spark.table(bronze_tables["source_metadata"])

    image_blob_keys_df = spark.table(bronze_tables["image_blobs"]).select("image_id", "source_split")
    metadata_keys_df = source_metadata_df.select("image_id", "source_split")

    orphan_blobs_df = (
        image_blob_keys_df
        .join(metadata_keys_df, on=["image_id", "source_split"], how="left_anti")
        .withColumn(
            "bronze_uri",
            bronze_image_uri(bronze_tables["image_blobs"], F.col("source_split"), F.col("image_id")),
        )
    )

    reject_rows(
        spark,
        orphan_blobs_df,
        silver_tables["rejected_records"],
        dataset_key,
        F.lit("no matching Bronze source metadata"),
        description="Bronze image blobs with no matching source metadata (rejected)",
    )

    return source_metadata_df


def apply_label_normalization(
    spark,
    source_metadata_df,
    normalize_fn,
    input_columns: list[str],
    label_columns: list[str],
    silver_tables: dict,
    dataset_key: str,
    bronze_uri_col: str = "source_uri",
):
    """Apply a dataset-specific label-normalization function as a Spark UDF.

    `normalize_fn(*input_columns) -> dict[str, str]` supplies the mapping logic;
    this handles wrapping it as a UDF, exploding its dict output into real columns
    (one per `label_columns` entry), splitting rows into label-valid vs unlabeled
    (a row is valid if any label_columns value resolved), and rejecting the
    unlabeled ones. Returns the label-valid DataFrame.
    """
    normalize_fn_udf = F.udf(normalize_fn, MapType(StringType(), StringType()))

    labeled_df = source_metadata_df.withColumn("labels", normalize_fn_udf(*[F.col(c) for c in input_columns]))
    for label_column in label_columns:
        labeled_df = labeled_df.withColumn(label_column, F.col("labels")[label_column])
    labeled_df = labeled_df.drop("labels")

    is_label_valid = F.lit(False)
    for label_column in label_columns:
        is_label_valid = is_label_valid | F.col(label_column).isNotNull()

    label_valid_df = labeled_df.where(is_label_valid)
    unlabeled_df = labeled_df.where(~is_label_valid)

    reject_rows(
        spark,
        unlabeled_df,
        silver_tables["rejected_records"],
        dataset_key,
        F.lit("unmapped or missing label"),
        bronze_uri_col=bronze_uri_col,
        description="Rows with no resolvable label (rejected)",
    )

    print(f"Rows with at least one resolvable label: {label_valid_df.count()}")

    return label_valid_df


def validate_images(
    spark,
    label_valid_df,
    bronze_tables: dict,
    silver_tables: dict,
    dataset_key: str,
    bronze_uri_col: str = "source_uri",
    min_dimension: int = MIN_DIMENSION,
    max_dimension: int = MAX_DIMENSION,
):
    """Left-join label-valid rows to the Bronze image blob table, reject rows with no
    matching blob, decode+validate the rest via `decode_batch` (batched via
    `mapInPandas`), reject decode/dimension failures, and return the DataFrame of
    valid, decoded images.

    `min_dimension`/`max_dimension` default to `data_platform.validate`'s defaults;
    override them for a dataset whose images are a structurally different size range
    (see docs/silver_validation_rules.md).
    """
    image_blobs_df = spark.table(bronze_tables["image_blobs"]).select("image_id", "source_split", "image_bytes")
    label_valid_with_bytes_df = materialize(
        spark,
        label_valid_df.join(image_blobs_df, on=["image_id", "source_split"], how="left"),
        _scratch_table(silver_tables, "label_valid_with_bytes"),
    )

    missing_blob_df = label_valid_with_bytes_df.where(F.col("image_bytes").isNull())
    matched_with_bytes_df = label_valid_with_bytes_df.where(F.col("image_bytes").isNotNull())

    reject_rows(
        spark,
        missing_blob_df,
        silver_tables["rejected_records"],
        dataset_key,
        F.lit("missing Bronze image blob"),
        bronze_uri_col=bronze_uri_col,
        description="Label-valid rows missing a Bronze image blob (rejected)",
    )

    decode_fn = functools.partial(decode_batch, min_dimension=min_dimension, max_dimension=max_dimension)
    decoded_df = materialize(
        spark,
        matched_with_bytes_df
        .select("image_id", "source_split", F.col(bronze_uri_col).alias("bronze_uri"), "image_bytes")
        .mapInPandas(decode_fn, schema=DECODE_OUTPUT_SCHEMA),
        _scratch_table(silver_tables, "decoded_images"),
    )

    image_invalid_df = decoded_df.where(~F.col("valid"))
    image_valid_df = decoded_df.where(F.col("valid"))

    reject_rows(
        spark,
        image_invalid_df,
        silver_tables["rejected_records"],
        dataset_key,
        F.col("validation_reason"),
        description="Images failing decode/dimension validation (rejected)",
    )

    print(f"Candidate images decoded: {decoded_df.count()}")
    print(f"Images passing validation: {image_valid_df.count()}")

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


def assign_leakage_groups_and_write_inventory(
    spark,
    accepted_df,
    label_columns: list[str],
    silver_tables: dict,
):
    """Assign leakage-control groups to accepted_df (exact-duplicate by
    source_checksum, then lesion_id, then patient_id, otherwise a singleton),
    write silver.leakage_groups, then write the accepted rows (with their
    group_id) to silver.image_inventory. Returns (image_inventory_df, leakage_groups_df)
    as written — pass these straight into print_silver_outputs rather than re-reading
    the tables, since a Silver run currently always reprocesses the full dataset
    (reconcile_bronze_records reads bronze.source_metadata unfiltered), so the
    freshly written rows already are the tables' current state. If Silver ever
    becomes incremental, this stops being equivalent to reading the table back.

    accepted_df must already carry a dataset_key column (build_accepted_rows adds
    it) — grouping and the checksum-duplicate check are scoped per dataset, and
    group_id embeds dataset_key so it stays globally unique across datasets even
    if two datasets happen to reuse the same lesion_id/patient_id/image_id values.
    """
    checksum_counts_df = accepted_df.groupBy("dataset_key", "source_checksum").agg(
        F.count("*").alias("checksum_count")
    )

    grouped_df = (
        accepted_df.join(checksum_counts_df, on=["dataset_key", "source_checksum"])
        .withColumn(
            "group_type",
            F.when(F.col("checksum_count") >= CHECKSUM_DUP_MIN_COUNT, F.lit("duplicate"))
            .when(F.col("lesion_id").isNotNull(), F.lit("lesion"))
            .when(F.col("patient_id").isNotNull(), F.lit("patient"))
            .otherwise(F.lit("singleton")),
        )
        .withColumn("group_source", GROUP_SOURCE_MAP[F.col("group_type")])
        .withColumn(
            # The grouping key's *value* (as opposed to its column name, already resolved
            # above) still needs to branch on group_type, since Spark can't select a
            # column by another column's name without a UDF.
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
    merge_into(spark, leakage_groups_df, silver_tables["leakage_groups"], ["dataset_key", "group_id"])

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
    merge_into(spark, image_inventory_df, silver_tables["image_inventory"], ["dataset_key", "image_id"])

    print(f"Accepted images written to image_inventory: {image_inventory_df.count()}")
    print(f"Leakage-control groups: {leakage_groups_df.count()}")

    return image_inventory_df, leakage_groups_df


def print_silver_outputs(
    spark, display, silver_tables: dict, label_columns: list[str], dataset_key: str, image_inventory_df, leakage_groups_df
) -> None:
    """Print/display accepted-vs-rejected summary stats for one dataset's Silver
    validation run: rejection reasons, validation status, per-label-column
    distribution, and leakage-group sizes.

    `image_inventory_df`/`leakage_groups_df` are the DataFrames returned by
    `assign_leakage_groups_and_write_inventory` for this same run — passed in rather
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
