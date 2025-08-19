from tasks.s3_to_rds_sub_tasks.positions_task import (
    positions_transform_and_load_daily,
    positions_transform_and_load_backfill,
    positions_transform_and_load_reload,
)
from tasks.s3_to_rds_sub_tasks.trades_task import (
    trades_transform_and_load_daily,
    trades_transform_and_load_backfill,
    trades_transform_and_load_reload
)
from tasks.s3_to_rds_sub_tasks.dividends_task import (
    dividends_transform_and_load_daily,
    dividends_transform_and_load_backfill,
    dividends_transform_and_load_reload
)
from tasks.s3_to_rds_sub_tasks.delta_nav_task import (
    delta_nav_transform_and_load_daily,
    delta_nav_transform_and_load_backfill,
    delta_nav_transform_and_load_reload
)
import datetime as dt
from airflow.sdk import task_group


@task_group
def s3_to_rds_daily():
    positions_transform_and_load_daily()
    trades_transform_and_load_daily()
    dividends_transform_and_load_daily()
    delta_nav_transform_and_load_daily()


@task_group
def s3_to_rds_backfill(start_date: dt.date, end_date: dt.date):
    positions_transform_and_load_backfill(start_date, end_date)
    trades_transform_and_load_backfill(start_date, end_date)
    dividends_transform_and_load_backfill(start_date, end_date)
    delta_nav_transform_and_load_backfill(start_date, end_date)


@task_group
def s3_to_rds_reload():
    positions_transform_and_load_reload()
    trades_transform_and_load_reload()
    dividends_transform_and_load_reload()
    delta_nav_transform_and_load_reload()