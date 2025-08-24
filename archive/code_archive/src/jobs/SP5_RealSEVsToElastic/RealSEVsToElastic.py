import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_json, struct, explode, sha2, concat_ws, 
    when, lit, array, coalesce, size, from_unixtime
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, LongType
from elasticsearch import Elasticsearch, exceptions

# Elasticsearch configuration
es_host = "10.0.3.216"
es_port = 9200
es_scheme = "http"
es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

# Define mappings for both indices
security_events_mappings = {
    "properties": {
        "ID": {"type": "keyword"},
        "VinNumber": {"type": "keyword"},
        "Timestamp": {"type": "date"},
        "Severity": {"type": "keyword"},
        "SEV_Msg": {"type": "text"},
        "Origin": {"type": "keyword"},
        "NetworkType": {"type": "keyword"},
        "NetworkID": {"type": "keyword"}
    }
}

sevs_logs_mappings = {
    "properties": {
        "ID": {"type": "keyword"},
        "VinNumber": {"type": "keyword"},
        "Timestamp": {"type": "date"},
        "ParameterName": {"type": "keyword"},
        "ParameterValue": {"type": "keyword"},
        "ParameterUnit": {"type": "keyword"},
        "ParameterType": {"type": "keyword"}
    }
}

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

# Create both indices in Elasticsearch
create_index_if_not_exists(es, "security_events", security_events_mappings)
create_index_if_not_exists(es, "sevs_logs", sevs_logs_mappings)

# Create SparkSession
spark = SparkSession.builder \
    .appName("SEVsToElasticsearch") \
    .config("spark.sql.streaming.checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/") \
    .getOrCreate()

# Update the schema to match the new structure
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("IoC", StructType([
        StructField("Parameters", ArrayType(StructType([
            StructField("Timestamp", StringType(), True),
            StructField("InternalParameter", StructType([
                StructField("ParameterName", StringType(), True),
                StructField("ParameterType", StringType(), True),  # Fixed typo
                StructField("ParameterValue", StringType(), True),
                StructField("ParameterUnit", StringType(), True)
            ]), True)
        ])), True)
    ]), True),
    StructField("NetworkID", StringType(), True),
    StructField("NetworkType", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("SEV_Msg", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("VinNumber", StringType(), True)
])

array_schema = ArrayType(schema)

# Kafka parameters
kafka_bootstrap_servers = "b-1-public.kafkapublic2.wqjg8g.c3.kafka.eu-central-1.amazonaws.com:9198,b-2-public.kafkapublic2.wqjg8g.c3.kafka.eu-central-1.amazonaws.com:9198"
kafka_topic = "sevs-topic2"

# Create a DataFrame representing the stream of input lines from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
    .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
    .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
    .load()
    # .option("kafka.security.protocol", "SASL_SSL") \
    # .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
    # .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
    # .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \


# Convert the value column from Kafka to a string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON data using the array schema
parsed_df = kafka_df.select(from_json(col("value"), array_schema).alias("data"))

# Explode the array of records
exploded_df = parsed_df.select(explode("data").alias("record"))

# Update the security events DataFrame selection
security_events_df = exploded_df.select(
    coalesce(col("record.ID"), lit("")).alias("ID"),
    coalesce(col("record.VinNumber"), lit("")).alias("VinNumber"),
    from_unixtime(col("record.Timestamp") / 1000).cast(TimestampType()).alias("Timestamp"),
    coalesce(col("record.SEV_Msg"), lit("")).alias("SEV_Msg"),
    coalesce(col("record.Severity"), lit("")).alias("Severity"),
    coalesce(col("record.Origin"), lit("")).alias("Origin"),
    coalesce(col("record.NetworkType"), lit("")).alias("NetworkType"),
    coalesce(col("record.NetworkID"), lit("")).alias("NetworkID")
)

def insert_to_elasticsearch(df, epoch_id, index_name):
    es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])
    
    # Calculate the document ID once for the entire DataFrame
    df_with_id = df.withColumn("doc_id", sha2(concat_ws("|", *df.columns), 256))
    
    for row in df_with_id.collect():
        doc = row.asDict()
        doc_id = doc.pop("doc_id")  # Remove doc_id from the document before indexing
        try:
            es.index(index=index_name, id=doc_id, body=doc)
        except Exception as e:
            print(f"Error indexing document: {e}")
    
    es.close()

def process_sevs_logs(df, epoch_id):
    # Explode the array of records
    exploded_df = df.select(explode("data").alias("record"))

    # Handle non-empty IoC parameters
    non_empty_ioc = exploded_df.filter(size(col("record.IoC.Parameters")) > 0)
    params_df = non_empty_ioc.select(
        col("record.ID").alias("ID"),
        col("record.VinNumber"),
        from_unixtime(col("record.Timestamp") / 1000).cast(TimestampType()).alias("Timestamp"),
        explode("record.IoC.Parameters").alias("Parameter")
    )

    # Flatten the Parameter structure
    flattened_df = params_df.select(
        col("ID"),
        col("VinNumber"),
        col("Timestamp"),
        from_unixtime(col("Parameter.Timestamp") / 1000).cast(TimestampType()).alias("ParameterTimestamp"),
        col("Parameter.InternalParameter.ParameterName").alias("ParameterName"),
        col("Parameter.InternalParameter.ParameterType").alias("ParameterType"),
        col("Parameter.InternalParameter.ParameterValue").alias("ParameterValue"),
        col("Parameter.InternalParameter.ParameterUnit").alias("ParameterUnit")
    )

    # Insert to Elasticsearch
    insert_to_elasticsearch(flattened_df, epoch_id, "sevs_logs")


# Write security events to Elasticsearch
security_events_query = security_events_df.writeStream \
    .foreachBatch(lambda df, epoch_id: insert_to_elasticsearch(df, epoch_id, "security_events")) \
    .option("checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/security_events/") \
    .trigger(processingTime='10 seconds') \
    .start()

# Write IoC logs to Elasticsearch
sevs_logs_query = parsed_df.writeStream \
    .foreachBatch(process_sevs_logs) \
    .option("checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/sevs_logs/") \
    .trigger(processingTime='10 seconds') \
    .start()

# Await termination of both streams
security_events_query.awaitTermination()
sevs_logs_query.awaitTermination()