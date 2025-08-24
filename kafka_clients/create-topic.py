"""
# Create a single topic
python topic_manager.py localhost:9092 my-topic --action create

# Create multiple topics with configuration
python topic_manager.py localhost:9092 topic1 topic2 --action create --partitions 3 --replication-factor 2 --config retention.ms=86400000

# Delete a single topic
python topic_manager.py localhost:9092 my-topic --action delete

# Delete multiple topics
python topic_manager.py localhost:9092 topic1 topic2 topic3 --action delete

"""

import argparse
import logging
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable, UnknownTopicOrPartitionError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_topic(admin_client, topic_name, num_partitions=4, replication_factor=2, config=None):
    """
    Create a new Kafka topic with the specified configuration.
    
    Args:
        admin_client: KafkaAdminClient instance
        topic_name (str): Name of the topic to create
        num_partitions (int): Number of partitions for the topic
        replication_factor (int): Replication factor for the topic
        config (dict): Optional topic configuration parameters
    
    Returns:
        bool: True if topic was created successfully, False if it already exists
    """
    try:
        logger.info(f"Attempting to create topic: {topic_name}")
        logger.info(f"Configuration: partitions={num_partitions}, replication={replication_factor}")
        
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=config or {}
        )
        
        admin_client.create_topics([new_topic])
        logger.info(f"Topic '{topic_name}' created successfully")
        return True
        
    except TopicAlreadyExistsError:
        logger.warning(f"Topic '{topic_name}' already exists")
        return False
    except Exception as e:
        logger.error(f"Error creating topic '{topic_name}': {str(e)}")
        raise

def delete_topics(admin_client, topics):
    """
    Delete one or more Kafka topics.
    
    Args:
        admin_client: KafkaAdminClient instance
        topics (list): List of topic names to delete
    
    Returns:
        dict: Dictionary with topic names as keys and deletion success status as values
    """
    results = {}
    
    # First, check which topics exist
    existing_topics = set(admin_client.list_topics())
    topics_to_delete = [topic for topic in topics if topic in existing_topics]
    
    if not topics_to_delete:
        logger.warning("None of the specified topics exist")
        return {topic: False for topic in topics}
    
    try:
        logger.info(f"Attempting to delete topics: {topics_to_delete}")
        admin_client.delete_topics(topics_to_delete)
        logger.info("Topics deleted successfully")
        results = {topic: True for topic in topics_to_delete}
        
        # Mark non-existent topics as False
        for topic in topics:
            if topic not in topics_to_delete:
                results[topic] = False
                logger.warning(f"Topic '{topic}' did not exist")
        
        return results
        
    except UnknownTopicOrPartitionError as e:
        logger.error(f"One or more topics not found: {str(e)}")
        return {topic: False for topic in topics}
    except Exception as e:
        logger.error(f"Error deleting topics: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Create or delete Kafka topics")
    parser.add_argument('bootstrap_servers', help='Comma-separated list of Kafka bootstrap servers')
    parser.add_argument('topics', nargs='+', help='One or more topic names')
    parser.add_argument('--action', choices=['create', 'delete'], required=True,
                       help='Action to perform (create or delete topics)')
    
    # Create-specific arguments
    parser.add_argument('--partitions', type=int, default=4,
                       help='Number of partitions (default: 4)')
    parser.add_argument('--replication-factor', type=int, default=2,
                       help='Replication factor (default: 2)')
    parser.add_argument('--config', nargs='*',
                       help='Topic configurations in key=value format')
    
    args = parser.parse_args()
    
    # Parse topic configurations if provided
    config = {}
    if args.config:
        for conf in args.config:
            try:
                key, value = conf.split('=')
                config[key.strip()] = value.strip()
            except ValueError:
                logger.error(f"Invalid configuration format: {conf}")
                return

    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=args.bootstrap_servers.split(','),
            api_version=(3, 7, 1)
        )
        
        if args.action == 'create':
            # Create each specified topic
            for topic in args.topics:
                try:
                    create_topic(
                        admin_client,
                        topic,
                        num_partitions=args.partitions,
                        replication_factor=args.replication_factor,
                        config=config
                    )
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {str(e)}")
        
        else:  # delete action
            try:
                results = delete_topics(admin_client, args.topics)
                for topic, success in results.items():
                    if success:
                        logger.info(f"Successfully deleted topic: {topic}")
                    else:
                        logger.warning(f"Failed to delete topic: {topic}")
            except Exception as e:
                logger.error(f"Failed to delete topics: {str(e)}")
                
    except NoBrokersAvailable:
        logger.error("Could not connect to Kafka brokers. Please check your bootstrap servers.")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    main()