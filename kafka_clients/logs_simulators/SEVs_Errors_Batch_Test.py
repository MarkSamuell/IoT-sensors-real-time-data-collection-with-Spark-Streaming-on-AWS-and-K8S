# sends batch of 4 messages 1 valid message and the other three with different error records 
# python3 producer.py <bootstrap-servers> <topic-name>

import argparse
import json
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
VIN_NUMBER = "YGEGFL7PRPC815346"
VALID_IDS = ["001", "002", "003", "004"]

def get_epoch_time():
    return int(datetime.utcnow().timestamp() * 1000)

def create_valid_record(id_index, network_types, network_ids):
    return {
        "ID": VALID_IDS[id_index % len(VALID_IDS)],
        "IoC": {
            "Parameters": [
                {
                    "Timestamp": str(get_epoch_time()),
                    "InternalParameter": {
                        "ParameterName": "CPU",
                        "ParameterType": "Usage",
                        "ParameterValue": "50",
                        "ParameterUnit": "%"
                    }
                }
            ]
        },
        "NetworkID": network_ids[id_index % len(network_ids)],
        "NetworkType": network_types[id_index % len(network_types)],
        "Origin": "ECU",
        "SEV_Msg": "Valid message",
        "Severity": "Low",
        "Timestamp": str(get_epoch_time()),
        "VinNumber": VIN_NUMBER
    }

def create_invalid_record(invalid_type, id_index):
    base_record = {
        "ID": VALID_IDS[id_index % len(VALID_IDS)],
        "VinNumber": VIN_NUMBER
    }

    if invalid_type == "missing_fields":
        return base_record  # Missing required fields
    elif invalid_type == "invalid_timestamp":
        return {**base_record, "Timestamp": "invalid_timestamp"}  # Invalid timestamp
    elif invalid_type == "invalid_severity":
        return {**base_record, "Timestamp": str(get_epoch_time()), "Severity": 123}  # Invalid Severity type
    else:
        raise ValueError(f"Unknown invalid type: {invalid_type}")

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

def send_test_messages(bootstrap_servers, topic, network_types, network_ids):
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

    messages = [
        # Message 1: All valid records
        [create_valid_record(i, network_types, network_ids) for i in range(4)],

        # Message 2: One invalid record (missing fields) + valid records
        [create_invalid_record("missing_fields", 0)] + [create_valid_record(i, network_types, network_ids) for i in range(1, 4)],

        # Message 3: One invalid record (invalid timestamp) + valid records
        [create_invalid_record("invalid_timestamp", 0)] + [create_valid_record(i, network_types, network_ids) for i in range(1, 4)],

        # Message 4: One invalid record (invalid severity) + valid records
        [create_invalid_record("invalid_severity", 0)] + [create_valid_record(i, network_types, network_ids) for i in range(1, 4)]
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
    parser = argparse.ArgumentParser(description="Send test messages to Kafka with valid and invalid records.")
    parser.add_argument('bootstrap_servers', help='Comma-separated list of Kafka bootstrap servers')
    parser.add_argument('topic', help='Kafka topic to send the messages to')
    args = parser.parse_args()

    bootstrap_servers = args.bootstrap_servers.split(',')
    network_types = ["CAN", "Ethernet"]
    network_ids = ["12", "36", "65", "78", "90"]

    send_test_messages(bootstrap_servers, args.topic, network_types, network_ids)