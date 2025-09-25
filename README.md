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

## Reverse Proxy Nginx Server (HTTPS)

The reverse proxy is an Nginx server running in a Docker container as part of the Docker Compose cluster. It accepts traffic on ports 80 and 443, performs HTTPS redirection, and forwards traffic to the `airflow-apiserver` on port 8080.

#### TLS Certificate Management:

To issue certificates using Certbot, run the following command (one time setup):

```bash
docker-compose run certbot certonly --webroot -w /var/www/certbot -d airflow.silverfund.byu.edu
```

To renew certificates, run the following command:

```bash
docker-compose run certbot renew
```

the above command is ran daily using a systemd timer on the EC2 instance.

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
- For development, use OpenSSL to create certificates and place them in `/certbot/www` (or the directory defined in the `docker-compose.yaml` file under the Nginx service volumes).
