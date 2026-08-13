import datetime as dt
import os

import dotenv
import fredapi
import polars as pl
from airflow.sdk import task
from aws.rds import db


import config


dotenv.load_dotenv(override=True)


def get_risk_free_rate(start_date: dt.date, end_date: dt.date) -> pl.DataFrame:
    series_id = "DGS10"
    api_key = os.getenv("FRED_API_KEY")

    fred = fredapi.Fred(api_key=api_key)
    df = (
        fred.get_series(series_id)
        .rename("yield")
        .to_frame()
        .reset_index(names=["date"])
    )

    return (
        pl.from_pandas(df)
        .with_columns(pl.col("date").dt.date(), pl.col("yield") / 100)
        .sort("date")
        .with_columns(pl.col("yield").forward_fill())
        .with_columns(pl.col("yield").shift(1).alias("yield_lag"))
        .with_columns(
            (100 / (1 + pl.col("yield_lag") * 30 / 360)).alias("price_lag"),
            (100 / (1 + pl.col("yield") * 29 / 360)).alias("price"),
        )
        .with_columns((pl.col("price") / pl.col("price_lag") - 1).alias("return"))
        .filter(pl.col("date").is_between(start_date, end_date))
        .sort("date")
        .select("date", "return")
    )


@task(task_id="risk_free_rate_etl")
def risk_free_rate_etl_daily() -> None:
    week_ago = dt.date.today() - dt.timedelta(days=7)
    # 1. Get risk free rate data
    df = get_risk_free_rate(week_ago, dt.date.today())

    # 2. Create core table if not exists
    db.execute_sql_file("dags/sql/risk_free_rate_create.sql")

    # 3. Load into stage table
    stage_table = f"{dt.date.today()}_RISK_FREE_RATE"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/risk_free_rate_merge.sql", params={"stage_table": stage_table}
    )

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')


@task(task_id="risk_free_rate_etl")
def risk_free_rate_etl_backfill(from_date: dt.date, to_date: dt.date) -> None:
    # 1. Get risk free rate data
    df = get_risk_free_rate(from_date, to_date)

    # 2. Create core table if not exists
    db.execute_sql_file("dags/sql/risk_free_rate_create.sql")

    # 3. Load into stage table
    stage_table = f"{from_date}_{to_date}_RISK_FREE_RATE"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/risk_free_rate_merge.sql", params={"stage_table": stage_table}
    )

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')


@task(task_id="risk_free_rate_etl")
def risk_free_rate_etl_reload() -> None:
    from_date = config.min_date
    to_date = dt.date.today()

    # 1. Get risk free rate data
    df = get_risk_free_rate(from_date, to_date)

    # 2. Create core table if not exists
    db.execute_sql_file("dags/sql/risk_free_rate_create.sql")

    # 3. Load into stage table
    stage_table = f"{from_date}_{to_date}_RISK_FREE_RATE"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file(
        "dags/sql/risk_free_rate_merge.sql", params={"stage_table": stage_table}
    )

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')
