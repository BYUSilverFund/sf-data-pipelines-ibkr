import tools
import datetime as dt
import dateutil.relativedelta as du
import aws
import os
import polars as pl
import fsspec
import dotenv
from airflow.sdk import task

dotenv.load_dotenv(override=True)

def clean_delta_nav_data(df: pl.DataFrame) -> pl.DataFrame:
    delta_nav_column_mapping = {
        'FromDate': 'from_date',
        'ToDate': 'to_date',
        'ClientAccountID': 'client_account_id',
        'StartingValue': 'starting_value',
        'EndingValue': 'ending_value',
        'DepositsWithdrawals': 'deposits_withdrawals',
        'Dividends': 'dividends'
    }

    delta_nav_schema = {
        'from_date': pl.Date,
        'to_date': pl.Date,
        'client_account_id': pl.String,
        'starting_value': pl.Float64,
        'ending_value': pl.Float64,
        'deposits_withdrawals': pl.Float64,
        'dividends': pl.Float64
    }

    return (
        df
        .filter(pl.col('ClientAccountID').ne('ClientAccountID'))
        .select(delta_nav_column_mapping.keys())
        .rename(delta_nav_column_mapping)
        .with_columns(
            pl.col('from_date', 'to_date').cast(pl.String).str.strptime(pl.Date, "%Y%m%d"),
        )
        .filter(pl.col('from_date').eq(pl.col('to_date')))
        .cast(delta_nav_schema)
        .select(
            pl.col('to_date').alias('date'),
            'client_account_id',
            'starting_value',
            'ending_value',
            'deposits_withdrawals',
            'dividends'
        )
    )

@task(task_id="delta_nav_transform_and_load")
def delta_nav_transform_and_load_daily():
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)

    # 1. Process raw positions data
    source_pattern = f"s3://ibkr-flex-query-files/daily-files/{last_market_date}/*/*-delta_nav.csv"

    storage_options = {
        "key": os.getenv('COGNITO_ACCESS_KEY_ID'),
        "secret": os.getenv('COGNITO_SECRET_ACCESS_KEY'),
    }

    fs = fsspec.filesystem("s3", **storage_options)
    file_list = fs.glob(source_pattern)

    dfs = []
    for file in file_list:
        df = pl.read_csv(f"s3://{file}", storage_options=storage_options, infer_schema_length=10000)
        df_clean = clean_delta_nav_data(df)
        dfs.append(df_clean)

    df = pl.concat(dfs)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/delta_nav_create.sql')

    # 3. Load into stage table
    stage_table = f"{last_market_date}_DELTA_NAV"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('pipelines/ibkr/sql/delta_nav_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')

@task(task_id="delta_nav_transform_and_load")
def delta_nav_transform_and_load_backfill(start_date: dt.date, end_date: dt.date):
    # 1. Process raw positions data
    source_pattern = f"s3://ibkr-flex-query-files/backfill-files/{start_date}_{end_date}/*/*-delta_nav.csv"

    storage_options = {
        "key": os.getenv('COGNITO_ACCESS_KEY_ID'),
        "secret": os.getenv('COGNITO_SECRET_ACCESS_KEY'),
    }

    fs = fsspec.filesystem("s3", **storage_options)
    file_list = fs.glob(source_pattern)

    dfs = []
    for file in file_list:
        df = pl.read_csv(f"s3://{file}", storage_options=storage_options, infer_schema_length=10000)
        df_clean = clean_delta_nav_data(df)
        dfs.append(df_clean)

    df = pl.concat(dfs)

    # 2. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('pipelines/ibkr/sql/delta_nav_create.sql')

    # 3. Load into stage table
    stage_table = f"{start_date}_{end_date}_DELTA_NAV"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('pipelines/ibkr/sql/delta_nav_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')