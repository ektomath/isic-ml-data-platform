"""Shared Spark I/O helpers for notebooks. Requires a live Spark session, not unit-testable locally."""

from __future__ import annotations

from pyspark.sql import functions as F


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
