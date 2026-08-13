import datetime as dt
import os

import dateutil.relativedelta as du
import fsspec
import polars as pl
from airflow.sdk import task
from aws.rds import db
from aws.s3 import parquet_storage_options


historical_data_schema = {
    "report_date": pl.Date,
    "symbol": pl.String,
    "mark_price": pl.Float64,
    "daily_return": pl.Float64,
    "currency": pl.String,
}


@task(task_id="historical_data_upload")
def historical_data_upload(
    start_date: dt.date | None = None, end_date: dt.date | None = None
):
    """
    This task takes Barra stock price history data from S3 and populates the RDS historical_data table
    for all symbols ever held by any fund on or after their first reported position date.
    """
    db.execute_sql_file("dags/sql/historical_data_create.sql")

    # Determine date range to process
    if start_date is not None and end_date is not None:
        dates_to_process = []
        curr = start_date
        while curr <= end_date:
            dates_to_process.append(curr)
            curr += dt.timedelta(days=1)
    else:
        yesterday = dt.date.today() - du.relativedelta(days=1)
        dates_to_process = [yesterday]

    fs = fsspec.filesystem(
        "s3",
        key=os.getenv("USER_ACCESS_KEY_ID"),
        secret=os.getenv("USER_SECRET_ACCESS_KEY"),
        client_kwargs={"region_name": "us-west-2"},
    )

    for target_date in dates_to_process:
        date_str = target_date.strftime("%Y-%m-%d")

        # Query RDS to get symbols of all positions reported on or before target_date
        query = f"""
            SELECT DISTINCT symbol
            FROM positions
            WHERE report_date <= '{date_str}'
        """
        positions_symbols_df = db.execute_to_df(query)

        if positions_symbols_df.is_empty():
            continue

        symbols = positions_symbols_df["symbol"].unique().to_list()

        s3_path = f"s3://barra-stock-history/{target_date.strftime('%Y/%m/%d')}.parquet"

        if not fs.exists(s3_path):
            continue

        barra_df = pl.read_parquet(
            f"{s3_path}", storage_options=parquet_storage_options
        )

        # Filter Barra price data to only symbols ever held up to target_date
        filtered_df = barra_df.filter(pl.col("ticker").is_in(symbols))

        if filtered_df.is_empty():
            continue

        res_df = (
            filtered_df.select(
                pl.col("date")
                .cast(pl.String)
                .str.strptime(pl.Date, "%Y%m%d")
                .alias("report_date"),
                pl.col("ticker").alias("symbol"),
                pl.col("price").cast(pl.Float64).alias("mark_price"),
                pl.col("daily_return").cast(pl.Float64),
                pl.col("currency"),
            )
            .cast(historical_data_schema)
            .unique(subset=["report_date", "symbol"])
        )

        stage_table = f"HIST_DATA_{target_date.strftime('%Y%m%d')}"
        db.stage_dataframe(res_df, stage_table)

        db.execute_sql_template_file(
            "dags/sql/historical_data_merge.sql", params={"stage_table": stage_table}
        )

        db.execute(f'DROP TABLE "{stage_table}";')
