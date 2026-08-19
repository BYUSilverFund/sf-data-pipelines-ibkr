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
    This task takes Barra stock price history data from S3 and populates the RDS historical_data table.
    For every target date:
      1. Checks existing mappings in symbol_barra_mapping.
      2. For unmapped symbols active on target_date, resolves their barrid JIT using target_date prices
         (requiring price variance <= 5% or <= $0.01 for sub-cent), saving new mappings to RDS immediately.
      3. Ingests validated target_date prices into historical_data.
    """
    db.execute_sql_file("dags/sql/historical_data_create.sql")
    db.execute_sql_file("dags/sql/symbol_barra_mapping_create.sql")

    fs = fsspec.filesystem(
        "s3",
        key=os.getenv("USER_ACCESS_KEY_ID"),
        secret=os.getenv("USER_SECRET_ACCESS_KEY"),
        client_kwargs={"region_name": "us-west-2"},
    )

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

    bucket = "barra-stock-history"

    for target_date in dates_to_process:
        target_date_str = target_date.strftime("%Y-%m-%d")
        s3_path = f"s3://{bucket}/{target_date.strftime('%Y/%m/%d')}.parquet"

        if not fs.exists(s3_path):
            continue

        barra_df = pl.read_parquet(s3_path, storage_options=parquet_storage_options)

        # 1. Fetch current registered mappings from symbol_barra_mapping
        existing_mappings = db.execute_to_df(
            "SELECT symbol, barrid FROM symbol_barra_mapping"
        )
        mapped_symbols = (
            existing_mappings["symbol"].to_list()
            if not existing_mappings.is_empty()
            else []
        )

        # 2. Query positions active on target_date
        pos_on_date = db.execute_to_df(f"""
            SELECT symbol, AVG(mark_price::numeric) AS pos_price
            FROM positions
            WHERE report_date = '{target_date_str}'
              AND client_account_id != 'DU8843649'
            GROUP BY symbol
        """)

        # 3. Resolve unmapped symbols dynamically on target_date
        if not pos_on_date.is_empty():
            pos_polars = pos_on_date.with_columns(pl.col("pos_price").cast(pl.Float64))

            unmapped_pos = pos_polars.filter(~pl.col("symbol").is_in(mapped_symbols))

            if not unmapped_pos.is_empty():
                barrid_counts = (
                    barra_df.group_by("barrid")
                    .len()
                    .rename({"len": "barrid_ticker_count"})
                )

                new_resolved = (
                    barra_df.join(
                        unmapped_pos, left_on="ticker", right_on="symbol", how="inner"
                    )
                    .join(barrid_counts, on="barrid", how="left")
                    .with_columns(
                        pl.when(pl.col("pos_price") > 0.01)
                        .then(
                            (pl.col("price") - pl.col("pos_price")).abs()
                            / pl.col("pos_price")
                        )
                        .otherwise((pl.col("price") - pl.col("pos_price")).abs())
                        .alias("price_diff")
                    )
                    .filter(
                        pl.when(pl.col("pos_price") > 0.01)
                        .then(pl.col("price_diff") <= 0.05)
                        .otherwise(pl.col("price_diff") <= 0.01)
                    )
                    .sort(["ticker", "price_diff", "barrid_ticker_count"])
                    .unique(subset=["ticker"], keep="first")
                    .select(
                        pl.col("ticker").alias("symbol"),
                        pl.col("barrid"),
                        pl.lit(target_date).alias("first_detected_date"),
                    )
                )

                if not new_resolved.is_empty():
                    stage_table = "STAGE_NEW_SYMBOL_BARRA_MAPPING"
                    db.stage_dataframe(new_resolved, stage_table)
                    db.execute(f"""
                        INSERT INTO symbol_barra_mapping (symbol, barrid, first_detected_date)
                        SELECT symbol, barrid, first_detected_date
                        FROM "{stage_table}"
                        ON CONFLICT (symbol) DO NOTHING;
                    """)
                    db.execute(f'DROP TABLE "{stage_table}";')

                    # Refresh mappings for today's ingestion
                    existing_mappings = db.execute_to_df(
                        "SELECT symbol, barrid FROM symbol_barra_mapping"
                    )

        if existing_mappings.is_empty():
            continue

        # 4. Join Barra prices for target_date using registered symbol_barra_mapping
        joined_df = barra_df.join(
            existing_mappings,
            left_on=["ticker", "barrid"],
            right_on=["symbol", "barrid"],
            how="inner",
        )

        if joined_df.is_empty():
            continue

        # 5. Daily price verification against target_date position price (if position exists)
        if not pos_on_date.is_empty():
            pos_polars = pos_on_date.with_columns(pl.col("pos_price").cast(pl.Float64))
            joined_df = (
                joined_df.join(
                    pos_polars, left_on="ticker", right_on="symbol", how="left"
                )
                .with_columns(
                    pl.when(
                        pl.col("pos_price").is_not_null() & (pl.col("pos_price") > 0.01)
                    )
                    .then(
                        (pl.col("price") - pl.col("pos_price")).abs()
                        / pl.col("pos_price")
                    )
                    .when(
                        pl.col("pos_price").is_not_null()
                        & (pl.col("pos_price") <= 0.01)
                    )
                    .then((pl.col("price") - pl.col("pos_price")).abs())
                    .otherwise(pl.lit(0.0))
                    .alias("daily_price_diff")
                )
                .filter(
                    pl.when(
                        pl.col("pos_price").is_not_null() & (pl.col("pos_price") > 0.01)
                    )
                    .then(pl.col("daily_price_diff") <= 0.05)
                    .when(
                        pl.col("pos_price").is_not_null()
                        & (pl.col("pos_price") <= 0.01)
                    )
                    .then(pl.col("daily_price_diff") <= 0.01)
                    .otherwise(pl.lit(True))
                )
            )

        if joined_df.is_empty():
            continue

        res_df = (
            joined_df.select(
                pl.col("date")
                .cast(pl.String)
                .str.strptime(pl.Date, "%Y%m%d")
                .alias("report_date"),
                pl.col("ticker").alias("symbol"),
                pl.col("price").cast(pl.Float64).alias("mark_price"),
                # divide by 100 to convert from percent to decimal.
                (pl.col("daily_return").cast(pl.Float64) / 100).alias("daily_return"),
                pl.col("currency"),
            )
            .cast(historical_data_schema)
            .unique(subset=["report_date", "symbol"], keep="first")
        )

        stage_table = f"HIST_DATA_{target_date.strftime('%Y%m%d')}"
        db.stage_dataframe(res_df, stage_table)

        db.execute_sql_template_file(
            "dags/sql/historical_data_merge.sql", params={"stage_table": stage_table}
        )

        db.execute(f'DROP TABLE "{stage_table}";')
