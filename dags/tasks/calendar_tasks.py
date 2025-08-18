import tools
import aws
import datetime as dt
import dateutil.relativedelta as du
import os
import pandas_market_calendars as mcal
import polars as pl
from airflow.sdk import task

def get_market_calendar(start_date: dt.date, end_date: dt.date, exchange: str = 'NYSE') -> pl.DataFrame:
    # Load market calendar
    calendar = mcal.get_calendar(exchange)

    # Get schedule
    schedule = calendar.schedule(start_date=start_date.isoformat(), end_date=end_date.isoformat())

    # Extract only the trading dates (from the market open times)
    trading_days = schedule.index.date

    # Convert to Polars DataFrame
    return pl.DataFrame({"date": trading_days})

@task(task_id="calendar_etl")
def calendar_etl_daily() -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)

    # 1. Pull calendar data
    df = get_market_calendar(last_market_date, last_market_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/calendar_create.sql')

    # 3. Load into stage table
    stage_table = f"{last_market_date}_CALENDAR"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('pipelines/ibkr/sql/calendar_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')

@task(task_id="calendar_etl")
def calendar_etl_backfill(start_date: dt.date, end_date: dt.date) -> None:
    # 1. Pull calendar data
    df = get_market_calendar(start_date, end_date)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/calendar_create.sql')

    # 3. Load into stage table
    stage_table = f"{start_date}_{end_date}_CALENDAR"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('pipelines/ibkr/sql/calendar_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')