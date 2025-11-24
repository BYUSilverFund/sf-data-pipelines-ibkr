# Silver Fund IBKR Data Pipelines

Silver Fund's data pipelines for IBKR flex query reporting data.

## Setup

Install docker by following this [guide](https://docs.docker.com/desktop/setup/install/mac-install/)

## Development

Install pre-commit hooks

```bash
pre-commit install
```

Spin up the containers using

```bash
docker compose up --build
```

Access the web UI at
[http://localhost:8080](http://localhost:8080)
or if you are using self signed certs and the reverse proxy then
[https://localhost](https://localhost)

Login using

- username: airflow

- password: airflow

Shut down containers and remove volumes using

```bash
docker compose down --volumes --rmi all
```

## Reverse Proxy Nginx Server (HTTPS)

The reverse proxy is an Nginx server running in a Docker container as part of the Docker Compose cluster. It accepts traffic on ports 80 and 443, performs HTTPS redirection, and forwards traffic to the `airflow-apiserver` on port 8080.

#### TLS Certificate Management:

##### Local Development

For local development, generate self-signed certificates using OpenSSL:

```bash
openssl req -x509 -newkey rsa:4096 -keyout privkey.pem -out fullchain.pem -days 365 -nodes -subj "/C=US/ST=Utah/L=Provo/O=SilverFund/CN=localhost"
```

After generating the certificates, place both `fullchain.pem` and `privkey.pem` in the `certbot/letsencrypt/live/airflow.silverfund.byu.edu/` directory on your local machine (or the directory referenced by your local `nginx.conf`). This allows Nginx to use the self-signed certificates for HTTPS during development.

##### Production

To issue certificates using Certbot, run the following command (one time setup):

```bash
docker-compose run certbot certonly --webroot -w /var/www/certbot -d airflow.silverfund.byu.edu
```

To renew certificates, run the following command:

```bash
docker-compose run certbot renew
```

the above ^^ command is ran daily using a systemd timer on the EC2 instance.

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

- **Service file location:** `/etc/systemd/system/certbot-renew.service`
- **Timer file location:** `/etc/systemd/system/certbot-renew.timer`

#### Notes:

- Let's Encrypt's Certbot does not issue certificates for local testing because it requires a publicly resolvable domain name to verify ownership. However, for local testing, you can generate certificates using self-signed certificates.
- On the production server, the Certbot container will manage certificates.
- For local development, you will need to use OpenSSL to create certificates. Alternatively, you can comment out the SSL server section in your `nginx.conf` file, or simply exclude the Nginx and Certbot containers from your Docker Compose setup. These components are only required for production environments where HTTPS and certificate management are necessary.

## Production Deployment (CI/CD)

Deployment to the production EC2 instance is now **fully automated** using GitHub Actions and AWS SSM.

### How it Works

- On every push to the `main` branch (except for changes to `README.md`), a GitHub Actions workflow is triggered.
- The workflow:
  1. Starts the EC2 instance if it is stopped.
  2. Uses AWS SSM to run a deployment script on the EC2 instance.
  3. The script:
     - Pulls the latest code from GitHub.
     - Installs or updates Docker and Git if needed.
     - Brings down any running containers and rebuilds/starts them with Docker Compose.
     - Issues or renews SSL certificates using Certbot (with Nginx installer).
     - Ensures systemd timers for automatic certificate renewal are up to date.

### No Manual Steps Required

- **No SSH required:** All deployment steps are handled automatically via SSM.
- **No manual certificate management:** Certbot and systemd timers handle certificate issuance and renewal.
- **No manual container management:** Docker Compose is used for all container lifecycle operations.

### To Trigger a Deployment

- **Push any change to the `main` branch** (except for `README.md`).
- The workflow will automatically deploy the latest code to the production EC2 instance.

### Infrastructure Notes

- Airflow is hosted on an EC2 instance (named `airflow`).
- This EC2 is not managed by Terraform and does not have a dev environment.
- Environment variables are configured directly on the EC2 from the AWS console or CLI.

## Code Quality

We use **Ruff** for both linting and formatting. Make sure to lint and format before pushing to Github. Github Actions is set up to fail ruff fails.

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
