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

### Certificates

- **Management**: Certificates are manually managed. Auto-renewal software is not used. Due to the reverse proxy being within the EC2 instance, Amazon Certificate Manager cannot be used for certificates.
- **Creation**: Certificates are created by connecting to the EC2 instance and using Let's Encrypt to generate them.

#### Notes:

- Let's Encrypt's Certbot does not issue certificates for local testing because it requires a publicly resolvable domain name to verify ownership. However, for local testing, you can generate certificates using self-signed certificates.
- On the production server, the Certbot container will manage certificates.
- For development, use OpenSSL to create certificates and place them in `/certbot/www` (or the directory defined in the `docker-compose.yaml` file under the Nginx service volumes).

#### Command to Issue Certificates:

To issue certificates using Certbot, run the following command:

```bash
docker-compose run certbot certonly --webroot -w /var/www/certbot -d airflow.silverfund.byu.edu
```
