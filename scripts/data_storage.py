# store_data.py

import psycopg2
from kafka import KafkaConsumer

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic_transformed_data = 'transformed_data'

# PostgreSQL configuration
pg_host = 'localhost'
pg_port = '5432'
pg_dbname = 'airflow'
pg_user = 'airflow'
pg_password = 'airflow'

# Connect to PostgreSQL
conn = psycopg2.connect(host=pg_host, port=pg_port,
                        dbname=pg_dbname, user=pg_user, password=pg_password)
cur = conn.cursor()

# Kafka consumer configuration
consumer = KafkaConsumer(kafka_topic_transformed_data,
                         bootstrap_servers=kafka_bootstrap_servers,
                         auto_offset_reset='earliest')

# Define function to store data in PostgreSQL
def store_data_in_postgress(data, cur, conn):
    """
    Function to store data in PostgreSQL.
    
    Args:
    - data: Data to be stored in PostgreSQL.
    - cur: PostgreSQL cursor object.
    - conn: PostgreSQL connection object.
    """
    try:
        # Execute SQL query to insert data into PostgreSQL table
        cur.execute("INSERT INTO transformed_data (UserID, Timestamp, Action) VALUES (%s, %s, %s)",
                    (data['UserID'], data['Timestamp'], data['Action']))
        
        # Commit the transaction
        conn.commit()
        
        print("Data stored successfully in PostgreSQL.")
    except Exception as e:
        # Rollback the transaction in case of error
        conn.rollback()
        
        print("Error occurred while storing data in PostgreSQL:", e)



# Store data in PostgreSQL
for message in consumer:
    data = message.value
    store_data_in_postgresql(data, cur, conn)

# Close the Kafka consumer
consumer.close()

# Close the PostgreSQL connection
cur.close()
conn.close()
