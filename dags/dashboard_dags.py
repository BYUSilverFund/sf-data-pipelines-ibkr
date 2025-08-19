from airflow.sdk import dag, task, get_current_context
import datetime as dt
import dateutil.relativedelta as du
from tasks.ibkr_to_s3_tasks import ibkr_to_s3_daily, ibkr_to_s3_backfill
from tasks.s3_to_rds_tasks import s3_to_rds_daily, s3_to_rds_backfill, s3_to_rds_reload
from tasks.benchmark_tasks import (
    benchmark_etl_daily,
    benchmark_etl_backfill,
    benchmark_etl_reload,
)
from tasks.risk_free_rate_tasks import (
    risk_free_rate_etl_daily,
    risk_free_rate_etl_backfill,
    risk_free_rate_etl_reload,
)
from tasks.calendar_tasks import (
    calendar_etl_daily,
    calendar_etl_backfill,
    calendar_etl_reload,
)
from tasks.return_materialization_tasks import (
    return_materializations_daily,
    return_materializations_backfill,
    return_materializations_reload,
)


@dag(schedule="@daily", max_active_tasks=1)
def dashboard_dag_daily():
    [
        [ibkr_to_s3_daily() >> s3_to_rds_daily(), calendar_etl_daily()]
        >> return_materializations_daily(),
        risk_free_rate_etl_daily(),
        benchmark_etl_daily(),
    ]


@dag(schedule=None, start_date=None)
def dashboard_dag_backfill():
    @task
    def get_dates() -> dict[str, str]:
        context = get_current_context()
        from_date = context["dag_run"].conf.get("from_date")
        to_date = context["dag_run"].conf.get("to_date")

        min_start_date = dt.date.today() - du.relativedelta(years=1)
        min_start_date = min_start_date.replace(day=1) + du.relativedelta(months=1)

        max_end_date = dt.date.today() - du.relativedelta(days=1)

        if from_date is None and to_date is None:
            return {"from_date": min_start_date, "to_date": max_end_date}

        else:
            from_date = dt.datetime.strptime(from_date, "%Y-%m-%d").date()
            to_date = dt.datetime.strptime(to_date, "%Y-%m-%d").date()
            return {"from_date": from_date, "to_date": to_date}

    dates = get_dates()
    from_date = dates["from_date"]
    to_date = dates["to_date"]

    [
        [
            ibkr_to_s3_backfill(from_date, to_date)
            >> s3_to_rds_backfill(from_date, to_date),
            calendar_etl_backfill(from_date, to_date),
        ]
        >> return_materializations_backfill(from_date, to_date),
        risk_free_rate_etl_backfill(from_date, to_date),
        benchmark_etl_backfill(from_date, to_date),
    ]


@dag(schedule=None, max_active_tasks=1)
def dashboard_dag_reload():
    [
        [s3_to_rds_reload(), calendar_etl_reload()] >> return_materializations_reload(),
        risk_free_rate_etl_reload(),
        benchmark_etl_reload(),
    ]


dashboard_dag_daily()
dashboard_dag_backfill()
dashboard_dag_reload()
