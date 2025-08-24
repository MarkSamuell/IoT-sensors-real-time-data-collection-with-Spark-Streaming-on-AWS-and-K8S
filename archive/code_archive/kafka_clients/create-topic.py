import boto3
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError

# Initialize AWS MSK client
msk_client = boto3.client('kafka')

# Get cluster information
cluster_arn = 'arn:aws:kafka:eu-central-1:381492251123:cluster/cluster4/f3ab7c63-526e-452e-917f-0d50c2c6f989-3'
response = msk_client.get_bootstrap_brokers(ClusterArn=cluster_arn)
bootstrap_servers = response['BootstrapBrokerStringPublicSaslScram']

# Kafka configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_SSL',  # Changed from SSL to SASL_SSL
    'ssl.ca.location': r'C:\Users\mark.girgis\OneDrive - VxLabs GmbH\DataPlatform\uraeus-dataplatform\configs\Certs\AmazonRootCA1.crt',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'kafka-user',
    'sasl.password': 'kafka-secret'
}

# Create AdminClient
admin_client = AdminClient(conf)

# Define the new topic
topic_name = 'topic1'
num_partitions = 2
replication_factor = 2

new_topic = NewTopic(topic_name, num_partitions, replication_factor)

# Create the topic
try:
    fs = admin_client.create_topics([new_topic])
    
    # Wait for each operation to finish
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{topic}' created")
        except KafkaException as e:
            # Check for specific error codes
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"Topic '{topic}' already exists")
            else:
                print(f"Failed to create topic '{topic}': {e}")
except Exception as e:
    print(f"An error occurred: {str(e)}")
