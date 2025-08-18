import polars as pl
import os
import fredapi
import dotenv
import datetime as dt
import tools
import aws
import dateutil.relativedelta as du
from airflow.sdk import task

dotenv.load_dotenv(override=True)

def get_risk_free_rate(start_date: dt.date, end_date: dt.date) -> pl.DataFrame:
    series_id = 'DGS10'
    api_key = os.getenv("FRED_API_KEY")

    fred = fredapi.Fred(api_key=api_key)
    df = fred.get_series(series_id).rename("yield").to_frame().reset_index(names=['date'])

    return (
        pl.from_pandas(df)
        .with_columns(
            pl.col('date').dt.date(),
            pl.col('yield').truediv(100)
        )
        .sort('date')
        .with_columns(
            pl.col('yield').shift(1).alias('yield_lag')
        )
        .with_columns(
            pl.lit(100).truediv(
                1 + pl.col('yield_lag') * 30 / 360
            ).alias('price_lag'),
            pl.lit(100).truediv(
                1 + pl.col('yield') * 29 / 360
            ).alias('price'),
        )
        .with_columns(
            pl.col('price').truediv('price_lag').sub(1).alias('return'),
            pl.col('yield').truediv(360).alias('daily_yield')
        )
        .sort('date')
        .with_columns(
            pl.col('return', 'daily_yield').fill_null(strategy='backward')
        )
        .filter(pl.col('date').is_between(start_date, end_date))
        .sort('date')
        .select('date', 'return')
    )

@task(task_id="risk_free_rate_etl")
def risk_free_rate_etl_daily() -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)

    # 1. Get risk free rate data
    df = get_risk_free_rate(last_market_date, last_market_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/risk_free_rate_create.sql')

    # 3. Load into stage table
    stage_table = f"{last_market_date}_RISK_FREE_RATE"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('pipelines/ibkr/sql/risk_free_rate_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')

@task(task_id="risk_free_rate_etl")
def risk_free_rate_etl_backfill(start_date: dt.date, end_date: dt.date) -> None:
    # 1. Get risk free rate data
    df = get_risk_free_rate(start_date, end_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/risk_free_rate_create.sql')

    # 3. Load into stage table
    stage_table = f"{start_date}_{end_date}_RISK_FREE_RATE"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('pipelines/ibkr/sql/risk_free_rate_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')