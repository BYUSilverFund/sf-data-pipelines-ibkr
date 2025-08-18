import tools
import datetime as dt
import dateutil.relativedelta as du
import aws
import os
import dotenv
from airflow.sdk import task, task_group
from config import configs

dotenv.load_dotenv(override=True)

@task
def extract_and_store_task_daily(config: dict, query: str) -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)
    
    # 1. Pull data from IBKR
    df = tools.ibkr_query(
        token=config['token'],
        query_id=config['queries'][query],
        from_date=last_market_date,
        to_date=last_market_date
    )

    # 2. Save to S3
    s3 = aws.S3(
        aws_access_key_id=os.getenv('COGNITO_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('COGNITO_SECRET_ACCESS_KEY'),
        region_name=os.getenv('COGNITO_REGION'),
    )

    file_name = f"daily-files/{last_market_date}/{config['fund']}/{last_market_date}-{config['fund']}-{query}.csv"
    s3.drop_file(file_name=file_name, bucket_name='ibkr-flex-query-files', file_data=df)

@task
def extract_and_store_task_backfill(config: dict, query: str, start_date: dt.date, end_date: dt.date):
    # 1. Pull data from IBKR
    df = tools.ibkr_query_batches(
        token=config['token'],
        query_id=config['queries'][query],
        start_date=start_date,
        end_date=end_date
    )

    # 2. Save to S3
    s3 = aws.S3(
        aws_access_key_id=os.getenv('COGNITO_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('COGNITO_SECRET_ACCESS_KEY'),
        region_name=os.getenv('COGNITO_REGION'),
    )

    file_name = f"backfill-files/{start_date}_{end_date}/{config['fund']}/{start_date}_{end_date}-{config['fund']}-{query}.csv"
    s3.drop_file(file_name=file_name, bucket_name='ibkr-flex-query-files', file_data=df)

@task_group
def exctract_and_store_group_daily(query: str):
    for config in configs:
        extract_and_store_task_daily.override(task_id=f"{config['fund']}_{query}_extract_and_store")(config, query)

@task_group
def exctract_and_store_group_backfill(query: str, start_date: dt.date, end_date: dt.date) -> None:
    for config in configs:
        extract_and_store_task_backfill.override(task_id=f"{config['fund']}_{query}_extract_and_store")(config, query, start_date, end_date)

@task_group
def ibkr_to_s3_daily():
    queries = ['delta_nav', 'dividends', 'positions', 'trades']
    for query in queries:
        exctract_and_store_group_daily.override(group_id=f"{query}_extract_and_store")(query)

@task_group
def ibkr_to_s3_backfill(start_date: dt.date, end_date: dt.date):
    queries = ['delta_nav', 'dividends', 'positions', 'trades']
    for query in queries:
        exctract_and_store_group_backfill.override(group_id=f"{query}_extract_and_store")(query, start_date, end_date)