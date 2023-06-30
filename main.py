import requests
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
# If you don't have them, please ask to smonti@eurekalabs.io
load_dotenv()

# API endpoint URL to retrieve UVA index historical values
api_url = "https://api.estadisticasbcra.com/uva"

headers = {
    "Authorization": f"Bearer {os.environ.get('BEARER_TOKEN')}"
}

response = requests.get(api_url, headers=headers)
data = response.json()

# Create a dictionary to store the data
historical_data = {}

# Store the API response in the dictionary
for entry in data:
    date = entry['d']
    value = entry['v']
    historical_data[date] = value

# Create the Panda DataFrame with the dictionary data
df = pd.DataFrame.from_dict(historical_data, orient='index', columns=['value'])

try:
    conn = psycopg2.connect(
        host=os.environ.get('REDSHIFT_HOST'),
        port=os.environ.get('REDSHIFT_PORT'),
        dbname=os.environ.get('REDSHIFT_DBNAME'),
        user=os.environ.get('REDSHIFT_USER'),
        password=os.environ.get('REDSHIFT_PASSWORD'),
        options=os.environ.get('OPTIONS', '')
    )
    print("Connected to the database successfully.")
except Exception as e:
    print("Error connecting to the database:", e)

# Create the Redshift table IF NOT EXISTS. If exists will success anyways, consider that
try: 
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS uva_historical_values (date DATE PRIMARY KEY SORTKEY, value FLOAT)")
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
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        insert_values = ", ".join(
            "(TO_DATE('%s', 'YYYY-MM-DD'), %s)" % (index, row['value']) for index, row in batch.iterrows()
        )
        insert_query = "INSERT INTO uva_historical_values (date, value) VALUES " + insert_values + ";"
        cursor.execute(insert_query)

    conn.commit()
    print("Data inserted successfully!")

except Exception as e:
    print("Error:", e)

finally:
    cursor.close()
    conn.close()
    print("Closed connection. Bye!")