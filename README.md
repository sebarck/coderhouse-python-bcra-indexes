# :rocket: BCRA Indexes script

CoderHouse project to pull BCRA information from this API https://estadisticasbcra.com/ and store it in a Database. In this case we use Amazon Redshift.

## :memo: Table of Contents

- [:rocket: BCRA Indexes script](#rocket-bcra-indexes-script)
  - [:memo: Table of Contents](#memo-table-of-contents)
  - [:package: Installation](#package-installation)
  - [:wrench: Configuration](#wrench-configuration)
  - [:computer: Usage](#computer-usage)

## :package: Installation 

1. Clone the repository.

2. Follow the Configuration section to create the required variable file with the required parameters for this app.

3. Run the Docker compose with the command in "Usage" section.
## :wrench: Configuration

To configure the application, create a .env file in the root directory and provide the following environment variables:

```makefile
AIRFLOW_VAR_REDSHIFT_HOST=your_redshift_host
AIRFLOW_VAR_REDSHIFT_PORT=your_redshift_port
AIRFLOW_VAR_REDSHIFT_DBNAME=your_redshift_dbname
AIRFLOW_VAR_REDSHIFT_USER=your_redshift_user
AIRFLOW_VAR_REDSHIFT_PASSWORD=your_redshift_password
AIRFLOW_VAR_OPTIONS=your_redshift_options
AIRFLOW_VAR_BEARER_TOKEN=your_bearer_token
AIRFLOW__SMTP__SMTP_HOST=smtp.sendgrid.net
AIRFLOW__SMTP__SMTP_STARTTLS=False
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=apikey
AIRFLOW__SMTP__SMTP_PASSWORD=your_sendgrid_apikey
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_MAIL_FROM=your_from_email
AIRFLOW_VAR_MAIL_TO=where_to_send_emails
AIRFLOW_VAR_ALERT_THRESHOLD=your_desired_threshold_for_alerts
```
If you don't have them, please contact smonti@eurekalabs.io.

To obtain the API Token, you need to register your email here: https://estadisticasbcra.com/api/registracion

To configure the SendGrid API, please create a free account and setup it here: https://sendgrid.com/

## :computer: Usage

You need to only run the script:

```bash
docker compose up
```
Follow the prompts to follow the different execution results and keep track of the errors if any