# :rocket: Project Name

CoderHouse project to pull BCRA information from this API https://estadisticasbcra.com/ and store it in a Database. In this case we use Amazon Redshift.

## :memo: Table of Contents

- [:rocket: Project Name](#rocket-project-name)
  - [:memo: Table of Contents](#memo-table-of-contents)
  - [:package: Installation](#package-installation)
  - [:wrench: Configuration](#wrench-configuration)
  - [:computer: Usage](#computer-usage)

## :package: Installation 

1. Clone the repository

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```
## :wrench: Configuration

To configure the application, create a .env file in the root directory and provide the following environment variables:

```makefile
REDSHIFT_HOST=your_redshift_host
REDSHIFT_PORT=your_redshift_port
REDSHIFT_DBNAME=your_redshift_dbname
REDSHIFT_USER=your_redshift_user
REDSHIFT_PASSWORD=your_redshift_password
OPTIONS=your_redshift_options
BEARER_TOKEN=your_bearer_token
```
If you don't have them, please contact smonti@eurekalabs.io.

To obtain the API Token, you need to register your email here: https://estadisticasbcra.com/api/registracion

## :computer: Usage

You need to only run the script:

```bash
python main.py
```
Follow the prompts to follow the different execution results and keep track of the errors if any