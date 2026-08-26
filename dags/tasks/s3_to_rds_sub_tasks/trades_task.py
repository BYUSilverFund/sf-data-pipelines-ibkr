import datetime as dt

import dateutil.relativedelta as du
import fsspec
import polars as pl
import tools
from airflow.sdk import task
from aws.rds import db
from aws.s3 import storage_options
from tasks.benchmark_tasks import (
    get_intraday_benchmark_bars,
)


def clean_trades_data(df: pl.DataFrame) -> pl.DataFrame:
    trades_column_mapping = {
        "ReportDate": "report_date",
        "ClientAccountID": "client_account_id",
        "AssetClass": "asset_class",
        "SubCategory": "sub_category",
        "Description": "description",
        "CUSIP": "cusip",
        "ISIN": "isin",
        "Symbol": "symbol",
        "TradeID": "trade_id",
        "Quantity": "quantity",
        "TradePrice": "trade_price",
        "IBCommission": "ib_commission",
        "Buy/Sell": "buy_sell",
        "DateTime": "trade_datetime",
    }

    trades_schema = {
        "report_date": pl.Date,
        "client_account_id": pl.String,
        "asset_class": pl.String,
        "sub_category": pl.String,
        "description": pl.String,
        "cusip": pl.String,
        "isin": pl.String,
        "symbol": pl.String,
        "trade_id": pl.String,
        "quantity": pl.Float64,
        "trade_price": pl.Float64,
        "ib_commission": pl.Float64,
        "buy_sell": pl.String,
        "trade_datetime": pl.Datetime("us"),
    }

    # Normalize alternate case names for DateTime if present
    for col in ["dateTime", "tradeDateTime", "trade_datetime", "TradeDateTime"]:
        if col in df.columns and "DateTime" not in df.columns:
            df = df.rename({col: "DateTime"})

    available_cols = [c for c in trades_column_mapping.keys() if c in df.columns]
    current_mapping = {c: trades_column_mapping[c] for c in available_cols}

    cleaned = (
        df.filter(pl.col("ClientAccountID").ne("ClientAccountID"))
        .select(available_cols)
        .rename(current_mapping)
        .filter(pl.col("buy_sell").is_in(["BUY", "SELL"]))
        .with_columns(
            pl.col("report_date").cast(pl.String).str.strptime(pl.Date, "%Y%m%d"),
        )
    )

    if "trade_datetime" in cleaned.columns:
        cleaned = cleaned.with_columns(
            pl.col("trade_datetime")
            .cast(pl.String)
            .str.strptime(pl.Datetime("us"), "%Y%m%d;%H%M%S", strict=False)
            .alias("trade_datetime")
        )
    else:
        cleaned = cleaned.with_columns(
            pl.lit(None).cast(pl.Datetime("us")).alias("trade_datetime")
        )

    return cleaned.cast(trades_schema)


def attach_benchmark_price(
    trades_df: pl.DataFrame, start_date: dt.date, end_date: dt.date
) -> pl.DataFrame:
    """Attaches benchmark_price to trades at their trade_datetime using 1-minute benchmark bars.

    If trade_datetime is null, benchmark_price is set to null.
    """
    if trades_df.is_empty():
        return trades_df.with_columns(
            pl.lit(None).cast(pl.Float64).alias("benchmark_price")
        )

    valid_trades = trades_df.filter(pl.col("trade_datetime").is_not_null())
    null_trades = trades_df.filter(pl.col("trade_datetime").is_null()).with_columns(
        pl.lit(None).cast(pl.Float64).alias("benchmark_price")
    )

    if valid_trades.is_empty():
        return null_trades.cast({"benchmark_price": pl.Float64})

    bm_bars = get_intraday_benchmark_bars(start_date, end_date)

    if not bm_bars.is_empty():
        valid_trades = (
            valid_trades.sort("trade_datetime")
            .join_asof(
                bm_bars.sort("bm_timestamp"),
                left_on="trade_datetime",
                right_on="bm_timestamp",
                strategy="nearest",
            )
            .drop("bm_timestamp")
        )
    else:
        valid_trades = valid_trades.with_columns(
            pl.lit(None).cast(pl.Float64).alias("benchmark_price")
        )

    return pl.concat([valid_trades, null_trades]).cast({"benchmark_price": pl.Float64})


@task(task_id="trades_transform_and_load")
def trades_transform_and_load_daily():
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)

    # 1. Process raw positions data
    source_pattern = (
        f"s3://ibkr-flex-query-files/daily-files/{last_market_date}/*/*-trades.csv"
    )

    fs = fsspec.filesystem("s3", **storage_options)
    file_list = fs.glob(source_pattern)

    dfs = []
    for file in file_list:
        df = pl.read_csv(
            f"s3://{file}", storage_options=storage_options, infer_schema_length=10000
        )
        df_clean = clean_trades_data(df)
        dfs.append(df_clean)

    if not dfs:
        return

    df = pl.concat(dfs).unique(
        subset=["report_date", "client_account_id", "symbol", "trade_id"]
    )

    # 2. Attach intraday benchmark price at execution time
    df = attach_benchmark_price(df, last_market_date, last_market_date)

    # 3. Create core table if not exists
    db.execute_sql_file("dags/sql/trades_create.sql")

    # 4. Load into stage table
    stage_table = f"{last_market_date}_TRADES"
    db.stage_dataframe(df, stage_table)

    # 5. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/trades_merge.sql", params={"stage_table": stage_table}
    )

    # 6. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')


@task(task_id="trades_transform_and_load")
def trades_transform_and_load_backfill(from_date: dt.date, to_date: dt.date):
    # 1. Process raw trades data from all S3 sources
    fs = fsspec.filesystem("s3", **storage_options)

    backfill_files = fs.glob(
        f"s3://ibkr-flex-query-files/backfill-files/{from_date}_{to_date}/*/*-trades.csv"
    )
    history_files = fs.glob("s3://ibkr-flex-query-files/history-files/*/*trades*.csv")
    daily_files = fs.glob("s3://ibkr-flex-query-files/daily-files/*/*/*trades.csv")

    file_list = list(set(backfill_files + history_files + daily_files))

    dfs = []
    for file in file_list:
        try:
            df = pl.read_csv(
                f"s3://{file}",
                storage_options=storage_options,
                infer_schema_length=10000,
            )
            df_clean = clean_trades_data(df)
            # Filter to requested date range early
            df_filtered = df_clean.filter(
                pl.col("report_date").is_between(from_date, to_date)
            )
            if not df_filtered.is_empty():
                dfs.append(df_filtered)
        except Exception:
            continue

    if not dfs:
        return

    df = pl.concat(dfs).unique(
        subset=["report_date", "client_account_id", "symbol", "trade_id"]
    )

    if df.is_empty():
        return

    # 2. Attach intraday benchmark price at execution time
    df = attach_benchmark_price(df, from_date, to_date)

    # 3. Create core table if not exists
    db.execute_sql_file("dags/sql/trades_create.sql")

    # 4. Load into stage table
    stage_table = f"{from_date}_{to_date}_TRADES"
    db.stage_dataframe(df, stage_table)

    # 5. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/trades_merge.sql", params={"stage_table": stage_table}
    )

    # 6. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')


@task(task_id="trades_transform_and_load")
def trades_transform_and_load_reload():
    # 1. Get all files in S3

    def get_file_list(source_pattern: str) -> list[str]:
        fs = fsspec.filesystem("s3", **storage_options)
        return fs.glob(source_pattern)

    history_pattern = "s3://ibkr-flex-query-files/history-files/*/*trades*.csv"
    history_files = get_file_list(history_pattern)

    backfill_pattern = "s3://ibkr-flex-query-files/backfill-files/*/*/*trades.csv"
    backfill_files = get_file_list(backfill_pattern)

    daily_pattern = "s3://ibkr-flex-query-files/daily-files/*/*/*trades.csv"
    daily_files = get_file_list(daily_pattern)

    file_list = history_files + backfill_files + daily_files

    # 2. Read, clean, and concatenate files
    dfs = []
    for file in file_list:
        df = pl.read_csv(
            f"s3://{file}", storage_options=storage_options, infer_schema_length=10000
        )
        df_clean = clean_trades_data(df)
        dfs.append(df_clean)

    if not dfs:
        return

    df = pl.concat(dfs).unique(
        subset=["report_date", "client_account_id", "symbol", "trade_id"]
    )

    # 3. Attach intraday benchmark price at execution time
    min_date = df["report_date"].min() if not df.is_empty() else dt.date.today()
    max_date = df["report_date"].max() if not df.is_empty() else dt.date.today()
    df = attach_benchmark_price(df, min_date, max_date)

    # 4. Create core table if not exists
    db.execute_sql_file("dags/sql/trades_create.sql")

    # 5. Load into stage table
    stage_table = "RELOAD_TRADES"
    db.stage_dataframe(df, stage_table)

    # 6. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/trades_merge.sql", params={"stage_table": stage_table}
    )

    # 7. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')
