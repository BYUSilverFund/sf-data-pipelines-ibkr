import datetime as dt
from airflow.sdk import task_group
from tasks.return_materialization_sub_tasks.holding_returns_task import (
    holding_return_materializations_daily,
    holding_return_materializations_backfill,
    holding_return_materializations_reload,
)
from tasks.return_materialization_sub_tasks.fund_returns_task import (
    fund_return_materializations_daily,
    fund_return_materializations_backfill,
    fund_return_materializations_reload,
)
from tasks.return_materialization_sub_tasks.all_fund_returns_task import (
    all_fund_return_materializations_daily,
    all_fund_return_materializations_backfill,
    all_fund_return_materializations_reload,
)


@task_group(group_id="return_materializations")
def return_materializations_daily():
    holding_return_materializations_daily()
    fund_return_materializations_daily()
    all_fund_return_materializations_daily()


@task_group(group_id="return_materializations")
def return_materializations_backfill(from_date: dt.date, to_date: dt.date):
    holding_return_materializations_backfill(from_date, to_date)
    fund_return_materializations_backfill(from_date, to_date)
    all_fund_return_materializations_backfill(from_date, to_date)


@task_group(group_id="return_materializations")
def return_materializations_reload():
    holding_return_materializations_reload()
    fund_return_materializations_reload()
    all_fund_return_materializations_reload()
