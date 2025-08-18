from tasks.s3_to_rds_sub_tasks.positions_flow import positions_transform_and_load_daily, positions_transform_and_load_backfill
from tasks.s3_to_rds_sub_tasks.trades_flow import trades_transform_and_load_daily, trades_transform_and_load_backfill
from tasks.s3_to_rds_sub_tasks.dividends_flow import dividends_transform_and_load_daily, dividends_transform_and_load_backfill
from tasks.s3_to_rds_sub_tasks.delta_nav_flow import delta_nav_transform_and_load_daily, delta_nav_transform_and_load_backfill
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