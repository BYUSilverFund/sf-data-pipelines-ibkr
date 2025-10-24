import datetime as dt
import os

import aws
import dateutil.relativedelta as du
import tools
from airflow.sdk import task

import config


@task(task_id="holding_return_materializations")
def holding_return_materializations_daily() -> None:
    yesterday = dt.date.today() - du.relativedelta(days=1)
    end_date = tools.get_last_market_date(reference_date=yesterday)
    start_date = end_date - du.relativedelta(days=7)

    # 1. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file("dags/sql/holding_returns_create.sql")

    # 2. Materialize table
    db.execute_sql_template_file(
        file_name="dags/sql/holding_returns_materialize.sql",
        params={"start_date": start_date, "end_date": end_date},
    )


@task(task_id="holding_return_materializations")
def holding_return_materializations_backfill(
    from_date: dt.date, to_date: dt.date
) -> None:
    # 1. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file("dags/sql/holding_returns_create.sql")

    # 2. Materialize table
    db.execute_sql_template_file(
        file_name="dags/sql/holding_returns_materialize.sql",
        params={"start_date": from_date, "end_date": to_date},
    )


@task(task_id="holding_return_materializations")
def holding_return_materializations_reload() -> None:
    from_date = config.min_date
    to_date = dt.date.today()

    # 1. Create core table if not exists
    db = aws.RDS(
        db_endpoint=os.getenv("DB_ENDPOINT"),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
        db_port=os.getenv("DB_PORT"),
    )
    db.execute_sql_file("dags/sql/holding_returns_create.sql")

    # 2. Materialize table
    db.execute_sql_template_file(
        file_name="dags/sql/holding_returns_materialize.sql",
        params={"start_date": from_date, "end_date": to_date},
    )
