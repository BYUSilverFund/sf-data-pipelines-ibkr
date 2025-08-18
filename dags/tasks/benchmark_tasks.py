import yfinance as yf
import datetime as dt
import polars as pl
import tools
import aws
import dateutil.relativedelta as du
import os
import dotenv
from airflow.sdk import task

dotenv.load_dotenv(override=True)

def get_benchmark_data(start_date: dt.date, end_date: dt.date) -> pl.DataFrame:
    column_mapping = {
        'Date': 'date',
        'Ticker': 'ticker',
        'Close': 'adjusted_close', # yfinance returns the adjusted close value as "Close"
        'Dividends': 'dividends_per_share'
    }

    df = yf.download(tickers=['IWV'], start=start_date, end=end_date, actions=True).stack(future_stack=True).reset_index()

    return (
        pl.from_pandas(df)
        .select(column_mapping.keys())
        .rename(column_mapping)
        .sort('date')
        .with_columns(
            pl.col('date').dt.date(),
            pl.col('adjusted_close').pct_change().alias('return')
        )
        .drop_nulls('return')
        .sort('date')
    )

@task(task_id='benchmark_etl')
def benchmark_etl_daily() -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday) 
    day_after_last_market_date = last_market_date + du.relativedelta(days=1)
    day_before_last_market_date = last_market_date - du.relativedelta(days=1)
    previous_market_date = tools.get_last_market_date(reference_date=day_before_last_market_date)

    # 1. Pull calendar data
    df = get_benchmark_data(previous_market_date, day_after_last_market_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('dags/sql/benchmark_create.sql')

    # 3. Load into stage table
    stage_table = f"{last_market_date}_BENCHMARK"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('dags/sql/benchmark_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')

@task(task_id="benchmark_etl")
def benchmark_etl_backfill(start_date: dt.date, end_date: dt.date) -> None:
    # 1. Pull calendar data
    df = get_benchmark_data(start_date, end_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('dags/sql/benchmark_create.sql')

    # 3. Load into stage table
    stage_table = f"{start_date}_{end_date}_BENCHMARK"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('dags/sql/benchmark_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')