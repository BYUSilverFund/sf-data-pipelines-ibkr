from airflow.sdk import dag
from tasks.ibkr_to_s3_tasks import ibkr_to_s3_daily
from tasks.s3_to_rds_tasks import s3_to_rds_daily
from tasks.benchmark_tasks import benchmark_etl_daily
from tasks.risk_free_rate_tasks import risk_free_rate_etl_daily
from tasks.calendar_tasks import calendar_etl_daily
from tasks.return_materialization_tasks import return_materializations_daily

@dag(
    schedule="@daily",
    start_date=None,
    max_active_tasks=1
)
def dashboard_dag_daily():
    [
        [ibkr_to_s3_daily() >> s3_to_rds_daily(), calendar_etl_daily()]
        >> return_materializations_daily(),
        risk_free_rate_etl_daily(),
        benchmark_etl_daily(),
    ]

dashboard_dag_daily()
