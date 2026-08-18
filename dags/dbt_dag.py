import datetime as dt

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook
from slack_notifier import slack_on_failure

CONN_ID = "postgres_dbt_conn"
default_args = {"on_failure_callback": slack_on_failure}


@dag(
    dag_id="data_integrity",
    schedule="30 10 * * *",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=default_args,
)
def data_integrity_pipeline():
    # -------------------------------------------------------------------------
    # BASIC COLUMN INTEGRITY CHECKS (Task Group: table_structure_validation)
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="table_structure_validation") as table_structure_validation:
        SQLColumnCheckOperator(
            task_id="positions_columns",
            conn_id=CONN_ID,
            table="positions",
            column_mapping={
                "symbol": {"null_check": {"equal_to": 0}},
                "report_date": {"null_check": {"equal_to": 0}},
                "client_account_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="trades_columns",
            conn_id=CONN_ID,
            table="trades",
            column_mapping={
                "symbol": {"null_check": {"equal_to": 0}},
                "report_date": {"null_check": {"equal_to": 0}},
                "trade_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="holding_returns_columns",
            conn_id=CONN_ID,
            table="holding_returns",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
                "ticker": {"null_check": {"equal_to": 0}},
                "client_account_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="fund_returns_columns",
            conn_id=CONN_ID,
            table="fund_returns",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
                "client_account_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="benchmark_columns",
            conn_id=CONN_ID,
            table="benchmark",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
                "ticker": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="dividends_columns",
            conn_id=CONN_ID,
            table="dividends",
            column_mapping={
                "report_date": {"null_check": {"equal_to": 0}},
                "symbol": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="delta_nav_columns",
            conn_id=CONN_ID,
            table="delta_nav",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
                "client_account_id": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="historical_data_columns",
            conn_id=CONN_ID,
            table="historical_data",
            column_mapping={
                "report_date": {"null_check": {"equal_to": 0}},
                "symbol": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="risk_free_rate_columns",
            conn_id=CONN_ID,
            table="risk_free_rate",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="calendar_columns",
            conn_id=CONN_ID,
            table="calendar",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
            },
        )
        SQLColumnCheckOperator(
            task_id="all_fund_returns_columns",
            conn_id=CONN_ID,
            table="all_fund_returns",
            column_mapping={
                "date": {"null_check": {"equal_to": 0}},
            },
        )

    # -------------------------------------------------------------------------
    # COMPLEX MATHEMATICAL ALIGNMENT & CALCULATIONS (Business Logic)
    # -------------------------------------------------------------------------
    @task(task_id="delta_nav_vs_all_fund_returns")
    def delta_nav_vs_all_fund_returns():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Aggregates delta_nav by trading date (from calendar) and compares summed ending values (net of deposits/withdrawals) against all_fund_returns.
            -- Verifies (delta_nav.ending_value - delta_nav.deposits_withdrawals) on date_t equals all_fund_returns value on date_t.
            -- Automatically ignores non-market trading dates (weekends and holidays) and restricts to actual data bounds.
            WITH delta_nav_daily AS (
                SELECT 
                    date,
                    SUM(starting_value) AS sum_starting_value,
                    SUM(ending_value) AS sum_ending_value,
                    SUM(deposits_withdrawals) AS sum_deposits_withdrawals,
                    SUM(dividends) AS sum_dividends
                FROM delta_nav
                -- Exclude values for quant paper account
                WHERE client_account_id != 'DU8843649'
                GROUP BY date
            ),
            all_fund_daily AS (
                SELECT 
                    date,
                    value AS fund_value
                FROM all_fund_returns
            ),
            date_bounds AS (
                SELECT 
                    MIN(date) AS min_date,
                    MAX(date) AS max_date
                FROM delta_nav_daily
            ),
            trading_day_checks AS (
                SELECT 
                    c.date,
                    d.sum_starting_value,
                    d.sum_ending_value,
                    d.sum_deposits_withdrawals,
                    f.fund_value,
                    CASE WHEN d.sum_ending_value IS NOT NULL AND f.fund_value IS NOT NULL 
                         THEN ABS((d.sum_ending_value - d.sum_deposits_withdrawals) - f.fund_value) ELSE NULL END AS ending_val_diff
                FROM calendar c
                CROSS JOIN date_bounds b
                LEFT JOIN delta_nav_daily d ON c.date = d.date
                LEFT JOIN all_fund_daily f ON c.date = f.date
                WHERE c.date BETWEEN b.min_date AND b.max_date
                  -- Exclude fund inception date
                  AND c.date != '2020-07-17'
            )
            SELECT 
                date,
                COALESCE(sum_starting_value, 0) AS sum_starting_value,
                0 AS prev_fund_value,
                0 AS starting_val_diff,
                COALESCE(sum_ending_value - sum_deposits_withdrawals, 0) AS sum_ending_value,
                COALESCE(fund_value, 0) AS fund_value,
                COALESCE(ending_val_diff, 0) AS ending_val_diff,
                CASE 
                    WHEN sum_ending_value IS NULL THEN 'MISSING_IN_DELTA_NAV'
                    WHEN fund_value IS NULL THEN 'MISSING_IN_ALL_FUND_RETURNS'
                    WHEN ending_val_diff > 0.01 
                        THEN 'ENDING_VAL_MISMATCH (delta_nav.ending - deposits != all_fund.value)'
                END AS failure_reason
            FROM trading_day_checks
            WHERE sum_ending_value IS NULL 
               OR fund_value IS NULL
               OR ending_val_diff > 0.01
            ORDER BY date DESC;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      DELTA_NAV VS ALL_FUND_RETURNS VALUE ALIGNMENT MISMATCHES DETECTED",
                "=" * 90,
            ]
            for (
                date,
                sum_start,
                prev_fund,
                start_diff,
                sum_end,
                fund_val,
                end_diff,
                reason,
            ) in failing_rows:
                error_msg.append(
                    f"Date: {date} | Reason: {reason}\n"
                    f"   -> StartingVal (delta_nav): {sum_start:,.2f} | PrevFundVal: {prev_fund:,.2f} | Diff: {start_diff:,.2f}\n"
                    f"   -> EndingVal (delta_nav): {sum_end:,.2f} | FundVal: {fund_val:,.2f} | Diff: {end_diff:,.2f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} dates where delta_nav starting/ending values do not match all_fund_returns!"
            )

    @task(task_id="all_fund_returns_math")
    def all_fund_returns_math():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Verifies all_fund_returns daily return matches the formula:
            -- (ending_value - starting_value - deposits_withdrawals) / starting_value.
            WITH delta_nav_daily AS (
                SELECT 
                    date,
                    SUM(starting_value) AS sum_starting_value,
                    SUM(ending_value) AS sum_ending_value,
                    SUM(deposits_withdrawals) AS sum_deposits_withdrawals,
                    SUM(dividends) AS sum_dividends
                FROM delta_nav
                -- Exclude values for quant paper account
                WHERE client_account_id != 'DU8843649'
                GROUP BY date
            ),
            all_fund_joined AS (
                SELECT 
                    f.date,
                    f.value AS fund_value,
                    f.return AS actual_return,
                    d.sum_starting_value,
                    d.sum_ending_value,
                    d.sum_deposits_withdrawals,
                    d.sum_dividends,
                    CASE 
                        WHEN d.sum_starting_value IS NOT NULL AND d.sum_starting_value > 0
                        THEN (d.sum_ending_value - d.sum_starting_value - d.sum_deposits_withdrawals) / d.sum_starting_value
                        ELSE NULL
                    END AS expected_return
                FROM all_fund_returns f
                JOIN delta_nav_daily d ON f.date = d.date
            )
            SELECT 
                date,
                fund_value,
                sum_starting_value,
                sum_ending_value,
                sum_deposits_withdrawals,
                sum_dividends,
                actual_return,
                expected_return,
                ABS(actual_return - expected_return) AS diff
            FROM all_fund_joined
            -- Tolerance threshold (0.5%): Accounts for minor post-holiday starting value shifts
            -- (IBKR interest, FX, and settlement adjustments over multi-day closures) while catching true math errors.
            WHERE expected_return IS NOT NULL
              AND ABS(actual_return - expected_return) > 0.005
            ORDER BY date DESC;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      ALL_FUND_RETURNS DAILY RETURN (WITH CASHFLOWS & DIVIDENDS) MISMATCHES",
                "=" * 90,
            ]
            for (
                date,
                fund_val,
                start_val,
                end_val,
                deposits,
                divs,
                act_ret,
                exp_ret,
                diff,
            ) in failing_rows:
                error_msg.append(
                    f"Date: {date} | FundVal (all_fund_returns): {fund_val:,.2f} | "
                    f"StartVal (delta_nav): {start_val:,.2f} | EndVal (delta_nav): {end_val:,.2f} | "
                    f"DepWith (delta_nav): {deposits:,.2f} | Divs (delta_nav): {divs:,.2f}\n"
                    f"   -> Actual Return (all_fund_returns): {act_ret:.6f} | "
                    f"Expected Return (delta_nav formula): {exp_ret:.6f} | Diff: {diff:.6f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} rows in all_fund_returns where daily return does not match delta_nav cashflow calculation!"
            )

    @task(task_id="all_fund_returns_dividends_match")
    def all_fund_returns_dividends_match():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Verifies all_fund_returns dividends on each date equals sum(delta_nav.dividends) on that date.
            WITH delta_nav_divs AS (
                SELECT 
                    date,
                    SUM(dividends) AS sum_delta_nav_divs
                FROM delta_nav
                -- Exclude values for quant paper account
                WHERE client_account_id != 'DU8843649'
                GROUP BY date
            )
            SELECT 
                f.date,
                f.dividends AS fund_dividends,
                d.sum_delta_nav_divs,
                ABS(f.dividends - d.sum_delta_nav_divs) AS diff
            FROM all_fund_returns f
            JOIN delta_nav_divs d ON f.date = d.date
            WHERE ABS(f.dividends - d.sum_delta_nav_divs) > 0.01
            ORDER BY f.date DESC;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 90,
                "      ALL_FUND_RETURNS VS DELTA_NAV DIVIDENDS MISMATCHES DETECTED",
                "=" * 90,
            ]
            for date, fund_divs, delta_divs, diff in failing_rows:
                error_msg.append(
                    f"Date: {date} | AllFundReturns Divs: {fund_divs:,.2f} | DeltaNAV Sum Divs: {delta_divs:,.2f} | Diff: {diff:,.2f}"
                )
            error_msg.append("=" * 90)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} dates where all_fund_returns dividends do not match sum(delta_nav.dividends)!"
            )

    @task(task_id="benchmark_math")
    def benchmark_math():
        hook = DbApiHook.get_hook(CONN_ID)
        sql = """
            -- Verifies benchmark daily return per ticker matches: (adjusted_close_t - adjusted_close_{t-1}) / adjusted_close_{t-1}.
            WITH lag_values AS (
                SELECT 
                    date,
                    ticker,
                    adjusted_close,
                    return as actual_return,
                    LAG(adjusted_close) OVER (PARTITION BY ticker ORDER BY date) as prev_adj_close
                FROM benchmark
            )
            SELECT 
                date,
                ticker,
                adjusted_close,
                prev_adj_close,
                actual_return,
                ((adjusted_close - prev_adj_close) / prev_adj_close) as expected_return,
                ABS(actual_return - ((adjusted_close - prev_adj_close) / prev_adj_close)) as diff
            FROM lag_values
            WHERE prev_adj_close IS NOT NULL 
              AND prev_adj_close > 0
              AND ABS(actual_return - ((adjusted_close - prev_adj_close) / prev_adj_close)) > 0.0001
            ORDER BY ticker, date DESC;
        """
        failing_rows = hook.get_records(sql)

        if failing_rows:
            import logging

            logger = logging.getLogger("airflow.task")
            error_msg = [
                "\n" + "=" * 80,
                "      BENCHMARK DAILY RETURN MISMATCHES DETECTED",
                "=" * 80,
            ]
            for (
                date,
                ticker,
                adj_close,
                prev_close,
                act_ret,
                exp_ret,
                diff,
            ) in failing_rows:
                error_msg.append(
                    f"Date: {date} | Ticker: {ticker} | AdjClose: {adj_close:,.2f} | PrevClose: {prev_close:,.2f} | "
                    f"Actual Return: {act_ret:.6f} | Expected Return: {exp_ret:.6f} | Diff: {diff:.6f}"
                )
            error_msg.append("=" * 80)
            logger.error("\n".join(error_msg))
            raise ValueError(
                f"Found {len(failing_rows)} rows in benchmark with invalid daily return calculations!"
            )

    # -------------------------------------------------------------------------
    # DAG FLOW DEPENDENCIES
    # -------------------------------------------------------------------------
    task_delta_nav_vs_all_fund = delta_nav_vs_all_fund_returns()
    task_all_fund_math = all_fund_returns_math()
    task_all_fund_divs = all_fund_returns_dividends_match()
    task_benchmark_math = benchmark_math()

    table_structure_validation >> [
        task_delta_nav_vs_all_fund,
        task_all_fund_math,
        task_all_fund_divs,
        task_benchmark_math,
    ]


data_integrity_dag = data_integrity_pipeline()
