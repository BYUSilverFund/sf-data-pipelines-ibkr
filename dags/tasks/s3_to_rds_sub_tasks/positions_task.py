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

def clean_positions_data(df: pl.DataFrame) -> pl.DataFrame:
    positions_column_mapping = {
        'ReportDate': 'report_date',
        'ClientAccountID': 'client_account_id',
        'AssetClass': 'asset_class',
        'SubCategory': 'sub_category',
        'Description': 'description',
        'CUSIP': 'cusip',
        'ISIN': 'isin',
        'Symbol': 'symbol',
        'MarkPrice': 'mark_price',
        'Quantity': 'quantity',
        'FXRateToBase': 'fx_rate_to_base'
    }

    positions_schema = {
        'report_date': pl.Date,
        'client_account_id': pl.String,
        'asset_class': pl.String,
        'sub_category': pl.String,
        'description': pl.String,
        'cusip': pl.String,
        'isin': pl.String,
        'symbol': pl.String,
        'mark_price': pl.Float64,
        'quantity': pl.Float64,
        'fx_rate_to_base': pl.Float64
    }

    return (
        df
        .filter(pl.col('ClientAccountID').ne('ClientAccountID'))
        .select(positions_column_mapping.keys())
        .rename(positions_column_mapping)
        .with_columns(
            pl.col('report_date').cast(pl.String).str.strptime(pl.Date, "%Y%m%d"),
        )
        .cast(positions_schema)
    )

@task(task_id="positions_transform_and_load")
def positions_transform_and_load_daily():
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)

    # 1. Process raw positions data
    source_pattern = f"s3://ibkr-flex-query-files/daily-files/{last_market_date}/*/*-positions.csv"

    storage_options = {
        "key": os.getenv('COGNITO_ACCESS_KEY_ID'),
        "secret": os.getenv('COGNITO_SECRET_ACCESS_KEY'),
    }

    fs = fsspec.filesystem("s3", **storage_options)
    file_list = fs.glob(source_pattern)

    dfs = []
    for file in file_list:
        df = pl.read_csv(f"s3://{file}", storage_options=storage_options, infer_schema_length=10000)
        df_clean = clean_positions_data(df)
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
    db.execute_sql_file('dags/sql/positions_create.sql')

    # 3. Load into stage table
    stage_table = f"{last_market_date}_POSITIONS"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('dags/sql/positions_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')

@task(task_id="positions_transform_and_load")
def positions_transform_and_load_backfill(start_date: dt.date, end_date: dt.date):
    # 1. Process raw positions data
    source_pattern = f"s3://ibkr-flex-query-files/backfill-files/{start_date}_{end_date}/*/*-positions.csv"

    storage_options = {
        "key": os.getenv('COGNITO_ACCESS_KEY_ID'),
        "secret": os.getenv('COGNITO_SECRET_ACCESS_KEY'),
    }

    fs = fsspec.filesystem("s3", **storage_options)
    file_list = fs.glob(source_pattern)

    dfs = []
    for file in file_list:
        df = pl.read_csv(f"s3://{file}", storage_options=storage_options, infer_schema_length=10000)
        df_clean = clean_positions_data(df)
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
    db.execute_sql_file('dags/sql/positions_create.sql')

    # 3. Load into stage table
    stage_table = f"{start_date}_{end_date}_POSITIONS"
    db.stage_dataframe(df, stage_table)

    # 4. Merge into core table
    db.execute_sql_template_file('dags/sql/positions_merge.sql', params={'stage_table': stage_table})

    # 5. Drop stage table
    db.execute(f'DROP TABLE "{stage_table}";')