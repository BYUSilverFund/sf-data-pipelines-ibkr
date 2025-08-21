# Silver Fund IBKR Data Pipelines
Silver Fund's data pipelines for IBKR flex query reporting data.

## Setup
Install docker by following this [guide](https://docs.docker.com/desktop/setup/install/mac-install/)

## Development
Spin up the containers using

```bash
docker compose up --build
```

Access the web UI at
[http://localhost:8080](http://localhost:8080)

Login using

- username: airflow

- password: airflow

Shut down containers and remove volumes using

```bash
docker compose down --volumes --rmi all
```
