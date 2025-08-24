from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, explode, lit, expr, when, to_timestamp, array_min, transform
)
from pyspark.sql.types import ArrayType, StructType
from streaming_pipeline.shared.schema.schema_utils import json_schema_to_spark_schema
from streaming_pipeline.shared.schema.schema_validator import SchemaValidator
from kafka import KafkaProducer, KafkaAdminClient
from configs.config import Config
from kafka.admin import NewTopic
import logging
import json
import traceback
import time
import uuid


logger = logging.getLogger(__name__)

class KafkaHandler:
    def __init__(self, bootstrap_servers, topic, schema_validator, watermark_duration="1 hour", skip_late_handling=False):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.schema_validator = schema_validator
        self.watermark_duration = watermark_duration
        self.late_topic = f"late_{topic}"
        self.skip_late_handling = skip_late_handling
        if not skip_late_handling:
            self._ensure_late_topic_exists()

    def _ensure_late_topic_exists(self):
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        try:
            # Get topic metadata
            topic_metadata = admin_client.describe_topics([self.topic])
            if not topic_metadata:
                raise ValueError(f"Topic {self.topic} not found")
            
            # Get partition count from metadata dictionary
            original_partitions = len(topic_metadata[0]['partitions'])
            
            # Check if late topic exists
            existing_topics = admin_client.list_topics()
            
            if self.late_topic not in existing_topics:
                # Get cluster metadata
                cluster_metadata = admin_client.describe_cluster()
                broker_count = len(cluster_metadata['brokers'])
                
                # Create late topic with same partitions and replication
                new_topic = NewTopic(
                    name=self.late_topic,
                    num_partitions=original_partitions,
                    replication_factor=min(broker_count, 3)
                )
                admin_client.create_topics([new_topic])
                logger.info(f"Created late topic {self.late_topic} with {original_partitions} partitions")
        except Exception as e:
            logger.error(f"Error ensuring late topic exists: {str(e)}")
            raise
        finally:
            admin_client.close()

    def create_kafka_stream(self, spark: SparkSession):
        logger.info(f"Creating Kafka stream for topic: {self.topic}")
        logger.info(f"Using bootstrap servers: {self.bootstrap_servers}")
        
        try:
            # Test connection
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='test_client'
            )
            topics = admin_client.list_topics()
            logger.info(f"Available Kafka topics: {topics}")
            admin_client.close()

            unique_group_id = f"spark-{uuid.uuid4().hex[:8]}-{int(time.time())}"
            
            logger.info(f"Creating Kafka stream for topic: {self.topic}")
            logger.info(f"Using bootstrap servers: {self.bootstrap_servers}")
            logger.info(f"Using consumer group ID: {unique_group_id}")
            
            # Rest of your method with improved timeout settings...
            stream_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.topic) \
                .option("kafka.group.id", unique_group_id) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .option("maxOffsetsPerTrigger", 10000) \
                .option("kafkaConsumer.pollTimeoutMs", 2000) \
                .option("spark.streaming.kafka.consumer.cache.timeout", "30s") \
                .option("minPartitions", Config.NUM_EXECUTORS * 2) \
                .option("includeHeaders", "false") \
                .load()
            
            return stream_df

        except Exception as e:
            logger.error(f"Error creating Kafka stream: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def parse_without_validation(self, spark: SparkSession, df: DataFrame, schema_name: str):
        try:
            messages_df = df.selectExpr(
                "*",
                f"from_json(cast(value as string), 'struct<DataEntries:array<struct<TimeStamp:string>>>') as parsed_value"
            )

            # Only perform late message handling if not skipped
            if not self.skip_late_handling:
                # Find oldest timestamp in each message
                messages_with_timestamp = messages_df.selectExpr(
                    "*",
                    "array_min(transform(parsed_value.DataEntries, x -> cast(x.TimeStamp as double))) as oldest_timestamp"
                ).withColumn(
                    "event_time",
                    when(
                        col("oldest_timestamp") > 1e11,
                        to_timestamp(col("oldest_timestamp") / 1000)
                    ).otherwise(
                        to_timestamp(col("oldest_timestamp"))
                    )
                )

                watermark_threshold = expr(f"current_timestamp() - interval {self.watermark_duration}")

                # Split into current and late messages
                late_messages = messages_with_timestamp.filter(col("event_time") < watermark_threshold)
                current_messages = messages_with_timestamp.filter(col("event_time") >= watermark_threshold)

                # Forward late messages to late topic
                if late_messages.take(1):
                    def send_to_kafka(partition_iter):
                        producer = KafkaProducer(
                            bootstrap_servers=self.bootstrap_servers,
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                            acks=1,
                            retries=3,
                            retry_backoff_ms=1000,
                            request_timeout_ms=30000
                        )
                        try:
                            for row in partition_iter:
                                producer.send(
                                    self.late_topic,
                                    key=row.key.encode('utf-8') if row.key else None,
                                    value=json.loads(row.value)
                                )
                            producer.flush()
                        finally:
                            producer.close()

                    late_messages.select(
                        col("key").cast("string"),
                        col("value").cast("string"),
                        col("timestamp")
                    ).foreachPartition(send_to_kafka)
                    logger.info(f"Forwarded late messages to topic: {self.late_topic}")
            else:
                current_messages = messages_df
                logger.debug("Skipping late message handling as requested")

            # Parse messages
            schema = self.schema_validator.schemas[schema_name]
            # Handle both wrapped and direct schema formats
            schema = schema.get('schema', schema) if isinstance(schema, dict) and 'schema' in schema else schema
            spark_schema = json_schema_to_spark_schema(schema)

            current_df = current_messages.select(
                col("key").cast("string").alias("message_id"),
                from_json(col("value").cast("string"), spark_schema).alias("parsed_value"),
                col("value").cast("string").alias("raw_kafka_value"),  # Always include raw data
                col("timestamp").alias("kafka_timestamp"),
                col("offset").alias("kafka_offset")  # Include Kafka offset for debugging
            )

            if spark_schema.typeName() == "array":
                valid_df = current_df.select(
                    col("message_id"),
                    explode("parsed_value").alias("event"),
                    col("raw_kafka_value"),
                    col("kafka_timestamp"),
                    col("kafka_offset")
                ).select("message_id", "event.*", "raw_kafka_value", "kafka_timestamp", "kafka_offset")
            else:
                valid_df = current_df.select("message_id", "parsed_value.*", "raw_kafka_value", "kafka_timestamp", "kafka_offset")

            error_df = spark.createDataFrame([], valid_df.schema)
            return valid_df, error_df

        except Exception as e:
            error_df = df.select(
                lit("PARSING_ERROR").alias("key"),
                col("key").alias("message_id"),
                col("value").alias("event"),
                col("timestamp").alias("error_timestamp"),
                lit("PARSING_ERROR").alias("error_type"),
                lit(str(e)).alias("error_details")
            )
            logger.error(f"Error parsing messages: {str(e)}")
            logger.error(traceback.format_exc())
            return spark.createDataFrame([], df.schema), error_df

    def __del__(self):
        if hasattr(self, 'producer') and self.producer:
            try:
                self.producer.flush(timeout=5)
                self.producer.close()
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {str(e)}")