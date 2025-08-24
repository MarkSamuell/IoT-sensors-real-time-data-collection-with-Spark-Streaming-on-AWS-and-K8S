import argparse
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

def create_kafka_topics(bootstrap_servers, topics, num_partitions, replication_factor):
    """
    Create Kafka topics with specified partitions and replication factor.
    
    Args:
        bootstrap_servers (str): Comma-separated list of Kafka broker addresses
        topics (list): List of topic names to create
        num_partitions (int): Number of partitions for each topic
        replication_factor (int): Replication factor for each topic
    """
    print(f"Connecting to Kafka at {bootstrap_servers}...")
    
    try:
        # Create admin client with longer timeouts
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='topic-creator',
            request_timeout_ms=60000,
            connections_max_idle_ms=60000
        )
        
        print("Connected to Kafka successfully.")
        
        # Get existing topics
        existing_topics = admin_client.list_topics()
        print(f"Existing topics: {existing_topics}")
        
        # Prepare new topics to create
        new_topics = []
        skipped_topics = []
        
        for topic in topics:
            if topic in existing_topics:
                print(f"Topic '{topic}' already exists, skipping.")
                skipped_topics.append(topic)
            else:
                new_topic = NewTopic(
                    name=topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor
                )
                new_topics.append(new_topic)
        
        # Create new topics if any
        if new_topics:
            print(f"Creating {len(new_topics)} topics: {', '.join(t.name for t in new_topics)}")
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            print("Topics created successfully.")
        else:
            print("No new topics to create.")
            
        # Close the admin client
        admin_client.close()
        return True
        
    except TopicAlreadyExistsError:
        print("Some topics already exist (race condition).")
        return True
    except KafkaError as e:
        print(f"Kafka error occurred: {e}")
        return False
    except Exception as e:
        print(f"Error creating topics: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create Kafka topics with specified partitions.')
    parser.add_argument('--bootstrap-servers', '-b', required=True, help='Comma-separated list of Kafka broker addresses')
    parser.add_argument('--topics', '-t', required=True, nargs='+', help='One or more topic names to create')
    parser.add_argument('--partitions', '-p', type=int, default=1, help='Number of partitions (default: 1)')
    parser.add_argument('--replication-factor', '-r', type=int, default=1, help='Replication factor (default: 1)')
    
    args = parser.parse_args()
    
    # Ensure bootstrap_servers has port specification
    bootstrap_servers = args.bootstrap_servers
    if ':' not in bootstrap_servers:
        bootstrap_servers = f"{bootstrap_servers}:9092"
    
    # Create the topics
    success = create_kafka_topics(
        bootstrap_servers=bootstrap_servers,
        topics=args.topics,
        num_partitions=args.partitions,
        replication_factor=args.replication_factor
    )
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)