from airflow.sdk import dag, task, task_group
from tasks.ibkr_to_s3_tasks import ibkr_to_s3_daily
from tasks.s3_to_rds_tasks import s3_to_rds_daily

@dag(
    schedule="@daily",
    start_date=None,
)
def dashboard_dag_daily():
    [
        [ibkr_to_s3_daily() >> s3_to_rds_daily(), calendar_etl()]
        >> return_materializations_daily(),
        risk_free_rate_etl(),
        benchmark_etl(),
    ]


@task_group
def return_materializations_daily():
    holding_return_materializations()
    fund_return_materializations()
    all_funds_return_materializations()


@task
def holding_return_materializations():
    pass


@task
def fund_return_materializations():
    pass


@task
def all_funds_return_materializations():
    pass


@task
def calendar_etl():
    pass


@task
def benchmark_etl():
    pass


@task
def risk_free_rate_etl():
    pass


dashboard_dag_daily()
