import json
import csv
from kafka import KafkaProducer
from avro import schema, datafile, io

ad_impressions_file_path = 'data/ad_impressions.json'
clicks_conversions_file_path =  'data/clicks_conversions.csv'
bid_requests_file_path = 'data/bid_requests.avro'

# Function to read JSON data and publish to Kafka
def ingest_json_data(data, topic, producer):
    producer.send(topic, json.dumps(data).encode('utf-8'))
    producer.flush()

# Function to read CSV data and publish to Kafka
def ingest_csv_data(file_path, topic, producer):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            producer.send(topic, json.dumps(row).encode('utf-8'))
            producer.flush()

# Function to read Avro data and publish to Kafka
def ingest_avro_data(file_path, topic, producer):
    with open(file_path, 'rb') as avrofile:
        datum_reader = io.DatumReader()
        df_reader = datafile.DataFileReader(avrofile, datum_reader)
        for record in df_reader:
            producer.send(topic, json.dumps(record).encode('utf-8'))
            producer.flush()

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic_ad_impressions = 'ad_impressions'
kafka_topic_clicks_conversions = 'clicks_conversions'
kafka_topic_bid_requests = 'bid_requests'

producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

# Read and ingest data from various sources

ingest_json_data(ad_impressions_file_path, kafka_topic_ad_impressions, producer)
ingest_csv_data(clicks_conversions_file_path, kafka_topic_clicks_conversions, producer)
ingest_avro_data(bid_requests_file_path, kafka_topic_bid_requests, producer)

# Close the Kafka producer
producer.close()