# data_transformation.py

from kafka import KafkaConsumer
from kafka import KafkaProducer
from datetime import datetime

# Function to process ad impressions
def process_impressions(impressions):
    # Example: Transformations
    transformed_impressions = []
    for imp in impressions:
        transformed_impression = {
            'adcreativeid': imp['AdCreativeID'].lower(),
            'userid': imp['UserID'].lower(),
            'date': imp['Timestamp'].split('T')[0],
            'time': imp['Timestamp'].split('T')[1],
            'website': imp['Website'].lower()
        }
        transformed_impressions.append(transformed_impression)
    return transformed_impressions

# Function to process clicks and conversions
def process_clicks_conversions(data):
    # Example: Transformations
    transformed_data = []
    for item in data:
        transformed_item = {
            'userid': item['UserID'].lower(),
            'adcampaignid': item['AdCampaignID'].lower(),
            'date': item['Timestamp'].split('T')[0],
            'time': item['Timestamp'].split('T')[1],
            'conversiontype': item['ConversionType'].lower()
        }
        transformed_data.append(transformed_item)
    return transformed_data

# Function to correlate ad impressions with clicks and conversions
def correlate_data(impressions, clicks_conversions):
    correlated_data = []
    for imp in impressions:
        for cc in clicks_conversions:
            if imp['userid'] == cc['userid']:
                correlated_data.append({'userid': imp['userid'], 'date': imp['date'], 'time': imp['time'], 'action': 'click' if 'adcampaignid' in cc else 'conversion'})
    return correlated_data

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic_ad_impressions = 'ad_impressions'
kafka_topic_clicks_conversions = 'clicks_conversions'
kafka_topic_bid_requests = 'bid_requests'
kafka_topic_transformed_data = 'transformed_data'

consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap_servers,
                         auto_offset_reset='earliest')

# Subscribe to input topics
consumer.subscribe([kafka_topic_ad_impressions,
                    kafka_topic_clicks_conversions,
                    kafka_topic_bid_requests])

producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

# Transformation logic
for message in consumer:
    # Perform transformation
    if message.topic == kafka_topic_ad_impressions:
        impressions = process_impressions(message.value)
    elif message.topic == kafka_topic_clicks_conversions:
        clicks_conversions = process_clicks_conversions(message.value)
    elif message.topic == kafka_topic_bid_requests:
        pass  # Add processing logic for bid requests if needed

    if 'impressions' in locals() and 'clicks_conversions' in locals():
        transformed_data = correlate_data(impressions, clicks_conversions)
        # Publish transformed data to output topic
        producer.send(kafka_topic_transformed_data, transformed_data)

# Close the Kafka consumer and producer
consumer.close()
producer.close()
