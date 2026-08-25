import datetime as dt
import logging
import os
import time

import aws
import dateutil.relativedelta as du
import polars as pl
import tools
from airflow.sdk import task, task_group
from config import configs

# this file contains the tasks that pull from the ibkr api and store the results in S3.
# the tasks are not async / concurrent, they could be but there is potential for api rate limits so we have not tried to make them async. (this is something that could be done in the future)


logger = logging.getLogger(__name__)


# this function recalls the ibkr api if the initial call fails.
def _retry_ibkr_call(
    func, max_attempts: int = 5, backoff_secs: int = 2, *args, **kwargs
):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            logger.warning(
                "IBKR request attempt %d/%d failed: %s",
                attempt,
                max_attempts,
                e,
            )
            if attempt < max_attempts:
                time.sleep(backoff_secs * attempt)

    logger.error("All %d IBKR attempts failed: %s", max_attempts, last_exc)
    return None


@task
def extract_and_store_task_daily(config: dict, query: str) -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday)

    # 1. Pull data from IBKR with retries
    df = _retry_ibkr_call(
        tools.ibkr_query,
        token=config["token"],
        query_id=config["queries"][query],
        from_date=last_market_date,
        to_date=last_market_date,
    )

    if df is None:
        logger.info(
            "No data returned after retries for %s %s; uploading empty file.",
            config.get("fund"),
            query,
        )
        df = pl.DataFrame()

    # 2. Save to S3
    s3 = aws.S3(
        aws_access_key_id=os.getenv("USER_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("USER_SECRET_ACCESS_KEY"),
        region_name=os.getenv("REGION"),
    )

    file_name = f"daily-files/{last_market_date}/{config['fund']}/{last_market_date}-{config['fund']}-{query}.csv"
    s3.drop_file(file_name=file_name, bucket_name="ibkr-flex-query-files", file_data=df)


@task
def extract_and_store_task_backfill(
    config: dict, query: str, from_date: dt.date, to_date: dt.date
):
    # 1. Pull data from IBKR with retries
    df = _retry_ibkr_call(
        tools.ibkr_query_batches,
        token=config["token"],
        query_id=config["queries"][query],
        from_date=from_date,
        to_date=to_date,
    )

    if df is None:
        logger.info(
            "No data returned after retries for %s %s; uploading empty file.",
            config.get("fund"),
            query,
        )
        df = pl.DataFrame()

    # 2. Save to S3
    s3 = aws.S3(
        aws_access_key_id=os.getenv("USER_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("USER_SECRET_ACCESS_KEY"),
        region_name=os.getenv("REGION"),
    )

    file_name = f"backfill-files/{from_date}_{to_date}/{config['fund']}/{from_date}_{to_date}-{config['fund']}-{query}.csv"
    s3.drop_file(file_name=file_name, bucket_name="ibkr-flex-query-files", file_data=df)


@task_group
def exctract_and_store_group_daily(query: str):
    for config in configs:
        extract_and_store_task_daily.override(
            task_id=f"{config['fund']}_{query}_extract_and_store"
        )(config, query)


@task_group
def exctract_and_store_group_backfill(
    query: str, from_date: dt.date, to_date: dt.date
) -> None:
    # pass
    for config in configs:
        extract_and_store_task_backfill.override(
            task_id=f"{config['fund']}_{query}_extract_and_store"
        )(config, query, from_date, to_date)


@task_group
def ibkr_to_s3_daily():
    queries = ["delta_nav", "dividends", "positions", "trades"]
    for query in queries:
        exctract_and_store_group_daily.override(group_id=f"{query}_extract_and_store")(
            query
        )


@task_group
def ibkr_to_s3_backfill(from_date: dt.date, to_date: dt.date):
    queries = ["delta_nav", "dividends", "positions", "trades"]
    for query in queries:
        exctract_and_store_group_backfill.override(
            group_id=f"{query}_extract_and_store"
        )(query, from_date, to_date)
