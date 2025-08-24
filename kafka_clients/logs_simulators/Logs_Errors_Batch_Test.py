"""
Logs Schema Kafka Test Producer

This script generates and sends test messages to a Kafka topic. It's designed to test
data processing pipelines that consume messages following the 'logs_schema' structure.

How to run:
python3 producer.py <bootstrap-servers> <test-topic>

What it generates:
- Total of 15 individual messages sent to the specified Kafka topic.
- 8 valid messages and 7 invalid messages, sent in the following order:
  1-5.   Valid records
  6-12.  Invalid records (one of each type):
         - Missing DataEntries
         - Empty DataEntries
         - Invalid type DataEntry (not array)
         - Invalid Timestamp
         - Missing VinNumber
         - Missing DataName
         - Missing Timestamp
  13-15. Valid records

Message Structure:
{
    "Campaign_ID": string,
    "DataEntries": [
        {
            "DataName": string,
            "DataType": string,
            "DataValue": string,
            "TimeStamp": string (13-digit epoch milliseconds)
        },
        ...
    ],
    "VinNumber": string
}
"""

import argparse
import json
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
from datetime import datetime
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_epoch_time():
    return int(datetime.utcnow().timestamp() * 1000)

def create_valid_data_entry():
    data_types = ["String", "Integer", "Float"]
    data_names = ["101", "102", "103"]
    return {
        "DataName": f"{random.choice(data_names)}",
        "DataType": random.choice(data_types),
        "DataValue": str(random.randint(0, 100)),
        "TimeStamp": str(get_epoch_time())
    }

def create_valid_record(campaign_id, vin):
    return {
        "Campaign_ID": campaign_id,
        "DataEntries": [create_valid_data_entry() for _ in range(random.randint(1, 5))],
        "VinNumber": vin
    }

def create_invalid_records(campaign_ids, vin):
    return [
        # 1. Missing DataEntries
        {
            "Campaign_ID": campaign_ids[0],
            "VinNumber": vin
        },
        # 2. Empty DataEntries
        {
            "Campaign_ID": campaign_ids[1],
            "DataEntries": [],
            "VinNumber": vin
        },
        # 3. Invalid type DataEntry (not array)
        {
            "Campaign_ID": campaign_ids[2],
            "DataEntries": create_valid_data_entry(),  # Single object instead of array
            "VinNumber": vin
        },
        # 4. Invalid Timestamp
        {
            "Campaign_ID": campaign_ids[3],
            "DataEntries": [{**create_valid_data_entry(), "TimeStamp": "invalid_timestamp"}],
            "VinNumber": vin
        },
        # 5. Missing VinNumber
        {
            "Campaign_ID": campaign_ids[4],
            "DataEntries": [create_valid_data_entry()]
        },
        # 6. Missing DataName
        {
            "Campaign_ID": campaign_ids[5],
            "DataEntries": [{k: v for k, v in create_valid_data_entry().items() if k != "DataName"}],
            "VinNumber": vin
        },
        # 7. Missing Timestamp
        {
            "Campaign_ID": campaign_ids[6],
            "DataEntries": [{k: v for k, v in create_valid_data_entry().items() if k != "TimeStamp"}],
            "VinNumber": vin
        }
    ]

def create_topic_if_not_exists(admin_client, topic_name):
    try:
        logger.info(f"Attempting to create topic: {topic_name}")
        new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        logger.info(f"Topic '{topic_name}' created successfully")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists")
    except Exception as e:
        logger.error(f"Error creating topic '{topic_name}': {str(e)}")
        raise

def send_test_messages(bootstrap_servers, topic):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 5, 1)
    )

    create_topic_if_not_exists(admin_client, topic)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        api_version=(3, 5, 1),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    campaign_ids = [f"CAMPAIGN_{i:03d}" for i in range(1, 16)]  # Increased to 16 to have enough IDs
    vin = "YGEGFL7PRPC815346"  # Using the same VIN for all records as requested

    messages = [
        # 5 valid records
        create_valid_record(campaign_ids[i], vin) for i in range(5)
    ] + create_invalid_records(campaign_ids[5:12], vin) + [
        # 3 more valid records
        create_valid_record(campaign_ids[i], vin) for i in range(12, 15)
    ]

    try:
        for i, message in enumerate(messages, 1):
            future = producer.send(topic, message)
            record_metadata = future.get(timeout=10)
            logger.info(f"Message {i} sent successfully to {record_metadata.topic} [{record_metadata.partition}]")
            logger.info(f"Message {i} content: {json.dumps(message, indent=2)}")
    except KafkaError as e:
        logger.error(f"Failed to send message: {e}")
    finally:
        producer.close()
        admin_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send individual test messages to Kafka with valid and specific invalid records for logs schema.")
    parser.add_argument('bootstrap_servers', help='Comma-separated list of Kafka bootstrap servers')
    parser.add_argument('topic', help='Kafka topic to send the messages to')
    args = parser.parse_args()

    bootstrap_servers = args.bootstrap_servers.split(',')

    send_test_messages(bootstrap_servers, args.topic)