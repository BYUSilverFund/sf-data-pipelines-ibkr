# Silver Fund IBKR Data Pipelines

Silver Fund's data pipelines for IBKR flex query reporting data.

## Setup

### Prerequisites

- [Docker Desktop](https://docs.docker.com/get-docker/) installed and running.
- Python 3.11+ (for local linting, pre-commit, and test workflows).

### 1. Environment Configuration

Copy `example.env` to `.env` and fill in your AWS, RDS, and API credentials:

```bash
cp example.env .env
```

Refer to [`example.env`](example.env) for full variable descriptions:
- **`AIRFLOW_UID` / `AIRFLOW_GID`**: Host user and group ID for Airflow container permissions (`50000` / `0`).
- **`USER_ACCESS_KEY_ID` / `USER_SECRET_ACCESS_KEY`**: AWS IAM credentials for S3 bucket ingestion.
- **`DB_*`**: RDS PostgreSQL database connection credentials.
- **`FRED_API_KEY` / `APCA_*`**: FRED risk-free rate & Alpaca market data API keys.
- **`_AIRFLOW_WWW_USER_*`**: Local web UI admin credentials.
- **`SLACK_*`**: Slack bot token and alert channel ID.
- **IBKR Tokens**: Individual Flex Query tokens for portfolio accounts (`GRAD_TOKEN`, `UNDERGRAD_TOKEN`, etc.).

### 2. Local Python Environment & Pre-commit Hooks

Create a virtual environment and install development dependencies for local linting and pre-commit checks:

```bash
python -m venv .venv
```

```bash
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
.venv\Scripts\Activate.ps1
```

```bash
pip install -r requirements.txt -r requirements-dev.txt
pre-commit install
```

### 3. Local SSL Certificates (Optional for HTTPS)

For local development, generate self-signed certificates using OpenSSL (you will only need to do this once). Alternatively, you can run Airflow over plain HTTP by commenting out the `nginx` and `certbot` containers in `docker-compose.yaml`:

Generate the `fullchain.pem` and `privkey.pem` certs:
```bash
openssl req -x509 -newkey rsa:4096 -keyout privkey.pem -out fullchain.pem -days 365 -nodes -subj "/C=US/ST=Utah/L=Provo/O=SilverFund/CN=localhost"
```

Place both `fullchain.pem` and `privkey.pem` in `certbot/conf/live/airflow.silverfund.byu.edu/` (or the directory referenced by `nginx.conf`).

### 4. Running Airflow

Spin up the cluster:

```bash
docker compose up --build
```

Access the web UI at:
- HTTP: [http://localhost:8080](http://localhost:8080)
- HTTPS (if using reverse proxy certs): [https://localhost](https://localhost)

Default credentials (or whatever you set in `.env`):
- **Username**: `airflow`
- **Password**: `airflow`

Shut down containers:

```bash
docker compose down
```

## Reverse Proxy Nginx Server (HTTPS)

The reverse proxy is an Nginx server running in a Docker container as part of the Docker Compose cluster. It accepts traffic on ports 80 and 443, performs HTTPS redirection, and forwards traffic to the `airflow-apiserver` on port 8080.

#### TLS Certificate Management:

##### Production

To issue certificates using Certbot, run the following command (one-time setup):

```bash
docker compose run certbot certonly --webroot -w /var/www/certbot -d airflow.silverfund.byu.edu
```

To renew certificates, run the following command:

```bash
docker compose run certbot renew
```

The above command is run daily using a systemd timer on the EC2 instance.

Copy the `certbot-renew.service` and `certbot-renew.timer` files to the following locations:

- **Service file location:** `/etc/systemd/system/certbot-renew.service`
- **Timer file location:** `/etc/systemd/system/certbot-renew.timer`

- **List running timers:**
  ```bash
  systemctl list-timers --all
  ```
- **Enable and start the renewal timer:**
  ```bash
  sudo systemctl enable --now certbot-renew.timer
  ```
- **View renewal logs:**
  ```bash
  sudo journalctl -u certbot-renew.service
  ```

#### Notes:

- Let's Encrypt's Certbot does not issue certificates for local testing because it requires a publicly resolvable domain name to verify ownership.
- On the production server, the Certbot container will manage certificates.
- For local development, use OpenSSL to create certificates or exclude the Nginx and Certbot containers from Docker Compose.

### Infrastructure Notes

- Airflow is hosted on an EC2 instance (named `airflow`).
- This EC2 is not managed by Terraform and does not have a dev environment.
- Environment variables are on the EC2 in a `.env` file within the airflow directory at `/home/ec2-user/airflow/.env`.
- The airflow instance is updated on merge to the `prod` branch by an AWS CodePipeline managed by Terraform.
  - **Manual deployment fallback**: Connect to the EC2, navigate to `/home/ec2-user/airflow`, pull changes with `git pull`, and restart with `docker compose down && docker compose up --build -d`.

## Code Quality

We use **Ruff** for both linting and formatting. GitHub Actions is configured to fail if Ruff checks fail.

### Format Code

Format all Python files:

```bash
ruff format
```

Check formatting without making changes:

```bash
ruff format --check
```

### Lint Code

Run linter:

```bash
ruff check
```

Auto-fix linting issues:

```bash
ruff check --fix
```

### DAGS

- **`dashboard_dag_daily`** *(Schedule: `0 10 * * *` / 3:00 AM MST)*: Primary daily pipeline. 
  From IBKR
    - Flex Statements -> S3 (ibkr-flex-query-files) -> RDS (tables: positions, trades, dividends, delta_nav, all_fund_returns, fund_returns)
  From S3 (barra-stock-history)
    - Historical Barra Stock Prices -> RDS (tables: historical_data, symbol_barra_mapping)
  From NYSE Market Calendar (`pandas_market_calendars`)
    - Trading Calendar Schedule -> RDS (tables: calendar)
  From FRED
    - Risk Free Rate -> RDS (tables: risk_free_rate)
  From Yahoo Finance
    - IWV Benchmark Prices -> RDS (tables: benchmark)
    
- **`data_integrity`**: Automated quality validation DAG. Runs the following integrity checks:
  - Table Structure (`table_structure_validation`) -> Verifies required RDS table schemas exist
  - NAV Consistency (`delta_nav_vs_all_fund_returns`, `delta_nav_vs_fund_returns`) -> fund return tables are built from delta_nav, this checks that they were built correctly
  - Return Math Verification (`all_fund_returns_math`, `fund_returns_math`, `benchmark_math`) -> ensures returns in tables were calculated correctly
  - Dividend Reconciliation (`all_fund_returns_dividends_match`) -> checks dividends across tables match.
  - Historical Price Integrity (`positions_vs_historical_symbols`) -> Verifies that our historical data mark_price matches position mark_price for the same symbol every day (within 5% variance and $0.01 for sub-cent stocks)
  - Trade vs Position Reconciliation (`trades_vs_positions_qty`) -> Verifies daily position quantity changes match net executed trade volume
  - Dividend Account Reconciliation (`orphan_dividends`) -> Ensures all dividend payouts are attributed to valid registered client accounts
  - Zero/Negative NAV Check (`zero_negative_nav`) -> Flags invalid zero or negative account ending NAV values in delta_nav
  - Future Date & Data Freshness (`date_sync_across_tables`) -> Ensures no dates are in the future and all max(date) for each table match.
  - Trading Calendar Completeness (`missing_calendar_dates`) -> Verifies every NYSE trading date since 2023-07-17 is present in positions, holding_returns, fund_returns, and all_fund_returns
  
- **`dashboard_dag_backfill`** *(Manual Trigger)*: Backfills data for a specified date range (`from_date` to `to_date`):
  - **IBKR Extraction (`ibkr_to_s3_backfill`)**: Pulls available dates within the 1-year API window and uploads CSVs to S3 bucket `ibkr-flex-query-files`. For dates older than 1 year, skips the API call (since IBKR does not retain data older than 365 days).
  - **S3 to RDS Load (`s3_to_rds_backfill`)**: Reads matching date ranges across bucket `ibkr-flex-query-files`,attaches nearest IWV 1 minute bar price from Alpaca to all trades and merges rows into RDS (`trades`, `positions`, `dividends`, `delta_nav`).
  - **External Sources**: Fetches full historical trading calendar, FRED risk-free rates, and daily benchmark returns.
  - **Return Materializations**: Recomputes `holding_returns`, `fund_returns`, and `all_fund_returns` for the backfilled range.

- **`dashboard_dag_reload`** *(Manual Trigger)*: Re-ingests all stored S3 data from `ibkr-flex-query-files` directly into RDS and recomputes all returns without hitting the IBKR API.