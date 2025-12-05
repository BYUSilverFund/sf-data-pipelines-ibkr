import datetime as dt
import os

import aws
import dotenv
import polars as pl
import yfinance as yf
from airflow.sdk import task

import config


dotenv.load_dotenv(override=True)


def get_benchmark_data(start_date: dt.date, end_date: dt.date) -> pl.DataFrame:
    column_mapping = {
        "Date": "date",
        "Ticker": "ticker",
        "Close": "adjusted_close",
        "Dividends": "dividends_per_share",
    }

    data = yf.download(tickers=["IWV"], start=start_date, end=end_date, actions=True)
    if data.empty:
        df = pl.DataFrame(
            {"Date": start_date, "Ticker": "IWV", "Close": 0, "Dividends": 0}
        )
    else:
        df = pl.from_pandas(data.stack(future_stack=True).reset_index())
    return (
        df.select(column_mapping.keys())
        .rename(column_mapping)
        .sort("date")
        .with_columns(
            pl.col("date").dt.date(),
            pl.col("adjusted_close").pct_change().alias("return"),
        )
        .with_columns(pl.col("return").fill_null(0.0))
        .drop_nulls("return")
        .sort("date")
    )


@task(task_id="benchmark_etl")
def benchmark_etl_daily() -> None:
    from_date = config.min_date
    to_date = dt.date.today()

    # 1. Pull calendar data
    df = get_benchmark_data(from_date, to_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file("dags/sql/benchmark_create.sql")

    # 3. Load into stage table
    stage_table = f"{to_date}_BENCHMARK"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/benchmark_merge.sql", params={"stage_table": stage_table}
    )

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')


@task(task_id="benchmark_etl")
def benchmark_etl_backfill(from_date: dt.date, to_date: dt.date) -> None:
    # 1. Pull calendar data
    df = get_benchmark_data(from_date, to_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file("dags/sql/benchmark_create.sql")

    # 3. Load into stage table
    stage_table = f"{from_date}_{to_date}_BENCHMARK"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/benchmark_merge.sql", params={"stage_table": stage_table}
    )

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')


@task(task_id="benchmark_etl")
def benchmark_etl_reload() -> None:
    from_date = config.min_date
    to_date = dt.date.today()

    # 1. Pull calendar data
    df = get_benchmark_data(from_date, to_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file("dags/sql/benchmark_create.sql")

    # 3. Load into stage table
    stage_table = f"{from_date}_{to_date}_BENCHMARK"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/benchmark_merge.sql", params={"stage_table": stage_table}
    )

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')
