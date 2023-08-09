from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load environment variables from the .env file
# If you don't have them, please ask to smonti@eurekalabs.io
load_dotenv()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'uva_historical_data_dag',
    description='DAG to retrieve UVA index historical data and store it in Redshift',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
)


def retrieve_uva_data():
    # API endpoint URL to retrieve UVA index historical values
    api_url = "https://api.estadisticasbcra.com/uva"

    headers = {
        "Authorization": f"Bearer {Variable.get('BEARER_TOKEN')}"
    }

    response = requests.get(api_url, headers=headers)
    data = response.json()

    # Create a dictionary to store the data
    historical_data_uva = {}

    # Store the API response in the dictionary
    for entry in data:
        date = entry['d']
        value = entry['v']
        historical_data_uva[date] = value

    # Create the Panda DataFrame with the dictionary data
    df_uva = pd.DataFrame.from_dict(
        historical_data_uva, orient='index', columns=['value'])

    return df_uva


def store_uva_data(**kwargs):
    # Retrieve 'df_uva' from XCom
    df_uva = kwargs['ti'].xcom_pull(task_ids='retrieve_uva_data')

    try:
        conn = psycopg2.connect(
            host=Variable.get('REDSHIFT_HOST'),
            port=Variable.get('REDSHIFT_PORT'),
            dbname=Variable.get('REDSHIFT_DBNAME'),
            user=Variable.get('REDSHIFT_USER'),
            password=Variable.get('REDSHIFT_PASSWORD'),
            options=Variable.get('OPTIONS', '')
        )
        print("Connected to the database successfully.")
    except Exception as e:
        print("Error connecting to the database:", e)

    # Create the Redshift table IF NOT EXISTS. If exists will success anyways, consider that
    try:
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS uva_historical_values (date DATE PRIMARY KEY SORTKEY, value FLOAT)")
        print("Table created successfully or already exists.")
    except Exception as e:
        print("Error creating the database. Details: ", e)

    # The insert will be made in batches so the performance is increased.
    batch_size = 1000

    try:
        # Truncate the table so every time the script runs, we ensure to refresh all the historical data
        cursor.execute("TRUNCATE TABLE uva_historical_values;")
        print("Table truncated successfully.")

        # Data insertion in batches
        for i in range(0, len(df_uva), batch_size):
            batch = df_uva.iloc[i:i + batch_size]
            insert_values = ", ".join(
                "(TO_DATE('%s', 'YYYY-MM-DD'), %s)" % (index, row['value']) for index, row in batch.iterrows()
            )
            insert_query = "INSERT INTO uva_historical_values (date, value) VALUES " + \
                insert_values + ";"
            cursor.execute(insert_query)

        conn.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)

    finally:
        cursor.close()
        conn.close()
        print("Closed connection. Bye!")


def retrieve_blue_dollar_data():
    # API endpoint URL to retrieve Blue dollar historical values
    api_url = "https://api.bluelytics.com.ar/v2/evolution.json"

    response = requests.get(api_url)
    data = response.json()

    # Create a dictionary to store the data
    historical_data_dollar = {}

    # Store the API response in the dictionary
    for entry in data:
        date = entry['date']
        source = entry['source']
        value_sell = entry['value_sell']
        value_buy = entry['value_buy']
        historical_data_dollar[(date, source)] = {
            'value_sell': value_sell, 'value_buy': value_buy}

    # Create the Panda DataFrame with the dictionary data
    df_dollar = pd.DataFrame(historical_data_dollar).T.reset_index()
    df_dollar.rename(columns={'level_0': 'date',
                     'level_1': 'source'}, inplace=True)

    return df_dollar


def store_blue_dollar_data(**kwargs):
    # Retrieve 'df_dollar' from XCom
    df_dollar = kwargs['ti'].xcom_pull(task_ids='retrieve_dollar_data')

    try:
        conn = psycopg2.connect(
            host=Variable.get('REDSHIFT_HOST'),
            port=Variable.get('REDSHIFT_PORT'),
            dbname=Variable.get('REDSHIFT_DBNAME'),
            user=Variable.get('REDSHIFT_USER'),
            password=Variable.get('REDSHIFT_PASSWORD'),
            options=Variable.get('OPTIONS', '')
        )
        print("Connected to the database successfully.")
    except Exception as e:
        print("Error connecting to the database:", e)

    # Create the Redshift table IF NOT EXISTS. If exists will success anyways, consider that
    try:
        cursor = conn.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS dollar_historical_values (
                date DATE PRIMARY KEY SORTKEY,
                source VARCHAR(255),
                value_sell FLOAT,
                value_buy FLOAT
            );
            """
        cursor.execute(create_table_query)
        conn.commit()
        print("Table created successfully or already exists.")
    except Exception as e:
        print("Error creating the table. Details: ", e)

    # The insert will be made in batches so the performance is increased.
    batch_size = 1000

    try:
        # Truncate the table
        cursor.execute("TRUNCATE TABLE dollar_historical_values;")
        conn.commit()
        print("Table truncated successfully.")

        # Data insertion in batches
        for i in range(0, len(df_dollar), batch_size):
            batch = df_dollar.iloc[i:i + batch_size]
            insert_values = ", ".join(
                "(TO_DATE('%s', 'YYYY-MM-DD'), '%s', %s, %s)" % (row['date'], row['source'], row['value_sell'], row['value_buy']) for index, row in batch.iterrows()
            )
            insert_query = "INSERT INTO dollar_historical_values (date, source, value_sell, value_buy) VALUES " + \
                insert_values + ";"
            cursor.execute(insert_query)

        conn.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)

    finally:
        cursor.close()
        conn.close()
        print("Closed connection. Bye!")


def calculate_trending(**kwargs):
    conn = psycopg2.connect(
        host=Variable.get('REDSHIFT_HOST'),
        port=Variable.get('REDSHIFT_PORT'),
        dbname=Variable.get('REDSHIFT_DBNAME'),
        user=Variable.get('REDSHIFT_USER'),
        password=Variable.get('REDSHIFT_PASSWORD'),
        options=Variable.get('OPTIONS', '')
    )

    # Calculate the date range for the last 7 days
    current_date = datetime.now().date()
    week_ago_date = current_date - timedelta(days=7)

    # Query the data from both tables within the date range
    uva_query = f"SELECT * FROM uva_historical_values WHERE date >= '{week_ago_date}' ORDER BY date ASC;"
    dollar_query = f"SELECT * FROM dollar_historical_values WHERE date >= '{week_ago_date}' AND source = 'Blue' ORDER BY date ASC;"

    uva_data = pd.read_sql_query(uva_query, conn)
    dollar_data = pd.read_sql_query(dollar_query, conn)

    # Convert data dictionaries to DataFrames
    uva_data_df = pd.DataFrame(uva_data)
    dollar_data_df = pd.DataFrame(dollar_data)

    # Convert 'date' column to datetime
    uva_data_df['date'] = pd.to_datetime(uva_data_df['date'])
    dollar_data_df['date'] = pd.to_datetime(dollar_data_df['date'])

    # Calculate daily and weekly trending changes for UVA data
    uva_current_data = uva_data_df[uva_data_df['date']
                                   == pd.Timestamp(current_date)]
    uva_previous_day_data = uva_data_df[uva_data_df['date'] == pd.Timestamp(
        current_date - timedelta(days=1))]
    uva_previous_week_data = uva_data_df[uva_data_df['date'] == pd.Timestamp(
        week_ago_date)]

    # Calculate the daily trending change in percentage for UVA data
    uva_daily_trending_percentage = (
        (uva_current_data['value'].values[0] - uva_previous_day_data['value'].values[0]) / uva_previous_day_data['value'].values[0]) * 100

    # Calculate the weekly trending change in percentage for UVA data
    uva_weekly_trending_percentage = (
        (uva_current_data['value'].values[0] - uva_previous_week_data['value'].values[0]) / uva_previous_week_data['value'].values[0]) * 100

    # Calculate daily and weekly trending changes for Dollar data (Sell and Buy)
    dollar_current_data = dollar_data_df[dollar_data_df['date']
                                         == pd.Timestamp(current_date)]
    dollar_previous_day_data = dollar_data_df[dollar_data_df['date']
                                              == pd.Timestamp(current_date - timedelta(days=1))]
    dollar_previous_week_data = dollar_data_df[dollar_data_df['date']
                                               == pd.Timestamp(week_ago_date)]

    # Calculate the daily trending change in percentage for Dollar data (Sell)
    dollar_daily_trending_sell_percentage = (
        (dollar_current_data['value_sell'].values[0] - dollar_previous_day_data['value_sell'].values[0]) / dollar_previous_day_data['value_sell'].values[0]) * 100

    # Calculate the weekly trending change in percentage for Dollar data (Sell)
    dollar_weekly_trending_sell_percentage = (
        (dollar_current_data['value_sell'].values[0] - dollar_previous_week_data['value_sell'].values[0]) / dollar_previous_week_data['value_sell'].values[0]) * 100

    # Print the calculated daily and weekly trending changes
    print("UVA Data:")
    print("Daily Trending:", uva_daily_trending_percentage)
    print("Weekly Trending:", uva_weekly_trending_percentage)

    print("\nDollar Data (Sell):")
    print("Daily Trending (Sell):", dollar_daily_trending_sell_percentage)
    print("Weekly Trending (Sell):", dollar_weekly_trending_sell_percentage)

    # Close the connection
    conn.close()

    return uva_daily_trending_percentage, uva_weekly_trending_percentage, dollar_daily_trending_sell_percentage, dollar_weekly_trending_sell_percentage


def send_email(**kwargs):
    # Retrieve the calculated percentages from the previous task
    task_instance = kwargs['ti']
    (
        uva_daily_percentage,
        uva_weekly_percentage,
        dollar_daily_percentage_sell,
        dollar_weekly_percentage_sell
    ) = task_instance.xcom_pull(task_ids='calculate_trending')

    # Get the threshold percentage from the environment variable
    threshold = float(Variable.get('TRENDING_THRESHOLD', 0.0))

    # Check if any of the percentages exceed the threshold
    if (uva_daily_percentage > threshold) or \
       (uva_weekly_percentage > threshold) or \
       (dollar_daily_percentage_sell > threshold) or \
       (dollar_weekly_percentage_sell > threshold):

        # Send an email alert
        email_subject = "Trending Change Alert"
        email_body = (
            "<p style='font-size: 16px;'>"
            "Some of the trending percentages have exceeded the threshold of "
            f"<strong>{threshold:.2f}%</strong>.<br><br>"
            "<strong>UVA Daily Trending:</strong> {:.2f}%<br>"
            "<strong>UVA Weekly Trending:</strong> {:.2f}%<br>"
            "<strong>Dollar Daily Trending (Sell):</strong> {:.2f}%<br>"
            "<strong>Dollar Weekly Trending (Sell):</strong> {:.2f}%<br><br>"
            "Please take appropriate action."
            "</p>"
        ).format(
            uva_daily_percentage, uva_weekly_percentage,
            dollar_daily_percentage_sell, dollar_weekly_percentage_sell
        )

        email_task = EmailOperator(
            task_id='send_trending_change_email',
            to=Variable.get('MAIL_TO'),
            subject=email_subject,
            html_content=email_body,
            dag=dag,
        )
        email_task.execute(kwargs)


# Task 1: Retrieve UVA data from API
retrieve_uva_data_task = PythonOperator(
    task_id='retrieve_uva_data',
    python_callable=retrieve_uva_data,
    dag=dag,
)

# Task 2: Store UVA data in Redshift
store_uva_data_task = PythonOperator(
    task_id='store_uva_data',
    python_callable=store_uva_data,
    dag=dag,
)

# Task 3: Retrieve Dollar data from API
retrieve_dollar_data_task = PythonOperator(
    task_id='retrieve_dollar_data',
    python_callable=retrieve_blue_dollar_data,
    dag=dag,
)

# Task 4: Store Dollar data in Redshift
store_dollar_data_task = PythonOperator(
    task_id='store_dollar_data',
    python_callable=store_blue_dollar_data,
    dag=dag,
)

# Task 5: Calculate trending for UVA and Dollar data
calculate_trending_task = PythonOperator(
    task_id='calculate_trending',
    python_callable=calculate_trending,
    provide_context=True,
    dag=dag,
)

# Task 6: Evaluate threshold and send email
send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email,
    provide_context=True,
    dag=dag,
)


# Define the task dependencies
retrieve_uva_data_task >> store_uva_data_task
retrieve_dollar_data_task >> store_dollar_data_task
store_uva_data_task >> calculate_trending_task
store_dollar_data_task >> calculate_trending_task
calculate_trending_task >> send_email_task
