import time
from elasticsearch import Elasticsearch, exceptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, sha2, concat
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from elasticsearch import Elasticsearch, exceptions

# Elasticsearch configuration
es_host = "10.0.3.216"
es_port = 9200
es_scheme = "http"
raw_es_index = "raw_stream_data_101"
raw_mappings = {
    "properties": {
        "id": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "VinNumber": {"type": "keyword"},
        "DataName": {"type": "keyword"},
        "DataValue": {"type": "keyword"},
        "DataType": {"type": "keyword"}
    }
}

# Create Elasticsearch client
es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

def create_index_if_not_exists(es_client, index_name, mappings):
    try:
        if not es_client.indices.exists(index=index_name):
            print(f"Index '{index_name}' does not exist. Creating index...")
            es_client.indices.create(
                index=index_name,
                body={"mappings": mappings}
            )
            print(f"Index '{index_name}' created successfully.")
        else:
            print(f"Index '{index_name}' already exists.")
    except exceptions.RequestError as e:
        print(f"RequestError: {e.info}")
    except exceptions.ConnectionError as e:
        print(f"ConnectionError: {e}")
    except Exception as e:
        print(f"Error creating index: {e}")

# Create the index in Elasticsearch
create_index_if_not_exists(es, raw_es_index, raw_mappings)

# Define the updated schema
schema = StructType([
    StructField("Campaign_ID", StringType(), True),
    StructField("DataEntries", ArrayType(StructType([
        StructField("DataName", StringType(), True),
        StructField("DataType", StringType(), True),
        StructField("DataValue", StringType(), True),
        StructField("TimeStamp", StringType(), True)
    ])), True),
    StructField("VinNumber", StringType(), True)
])

# Kafka parameters
kafka_bootstrap_servers = "b-2-public.kafkapublic.cp877p.c3.kafka.eu-central-1.amazonaws.com:9198,b-1-public.kafkapublic.cp877p.c3.kafka.eu-central-1.amazonaws.com:9198"
kafka_topic = "test-topic"

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaToElasticsearch") \
    .config("spark.sql.streaming.checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/checkpoint/") \
    .getOrCreate()

# Create a DataFrame representing the stream of input lines from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
    .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
    .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
    .load()

# Convert the value column from Kafka to a string and parse JSON
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Flatten the structure for easier querying
flattened_df = parsed_df.select(
    col("data.VinNumber"),
    explode(col("data.DataEntries")).alias("entry")
).select(
    col("VinNumber"),
    col("entry.DataName"),
    col("entry.DataValue"),
    col("entry.DataType"),
    col("entry.TimeStamp").alias("timestamp")
)

# Generate a unique ID using SHA256 hash of VinNumber, timestamp, and DataName
flattened_df = flattened_df.withColumn("id", sha2(concat(col("VinNumber"), col("timestamp"), col("DataName")), 256))

# Remove any duplicate columns if they exist
flattened_df = flattened_df.dropDuplicates(["id"])

# Elasticsearch configuration for Spark
es_write_conf = {
    "es.nodes": es_host,
    "es.port": str(es_port),
    "es.resource": f"{raw_es_index}",
    "es.mapping.id": "id",
    "es.write.operation": "upsert"
}

# Write data to Elasticsearch
query = flattened_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .options(**es_write_conf) \
        .mode("append") \
        .save()) \
    .start()

# Await termination of the stream
query.awaitTermination()
