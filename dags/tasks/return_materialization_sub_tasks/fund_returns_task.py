import tools
import aws
import datetime as dt
import dateutil.relativedelta as du
import os
from airflow.sdk import task

@task(task_id="fund_return_materializations")
def fund_return_materializations_daily() -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    last_market_date = tools.get_last_market_date(reference_date=yesterday) 
    
    # 1. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('dags/sql/fund_returns_create.sql')

    # 2. Materialize table
    db.execute_sql_template_file(
        file_name='dags/sql/fund_returns_materialize.sql',
        params={'start_date': last_market_date, 'end_date': last_market_date}
    )

@task(task_id="fund_return_materializations")
def fund_return_materializations_backfill(from_date: dt.date, to_date: dt.date) -> None:
    # 1. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file('dags/sql/fund_returns_create.sql')

    # 2. Materialize table
    db.execute_sql_template_file(
        file_name='dags/sql/fund_returns_materialize.sql',
        params={'start_date': from_date, 'end_date': to_date}
    )