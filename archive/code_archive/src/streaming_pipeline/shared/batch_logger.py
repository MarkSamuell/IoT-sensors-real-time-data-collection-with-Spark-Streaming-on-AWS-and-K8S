from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit, when, array
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, ArrayType, TimestampType
)
from datetime import datetime
from streaming_pipeline.shared.dal.elasticsearch_handler import ElasticsearchHandler

class BatchLogger:
    def __init__(self, es_handler: ElasticsearchHandler, spark: SparkSession, index_prefix="logging_"):
        self.es_handler = es_handler
        self.spark = spark
        self.index_prefix = index_prefix
        self.total_messages = 0
        self.schema = StructType([
            StructField("job_type", StringType(), False),
            StructField("step_id", StringType(), False),
            StructField("batch_number", IntegerType(), False),
            StructField("kafka_timestamp", StringType(), True),
            StructField("time_received_from_kafka", StringType(), True),
            StructField("time_inserted_to_elasticsearch", StringType(), True),
            StructField("total_messages_received_from_kafka_so_far", IntegerType(), False),
            StructField("number_messages_in_this_batch", IntegerType(), False),
            StructField("is_error", BooleanType(), False),
            StructField("error_types", ArrayType(StringType(), True), True)
        ])

    def create_logging_index_if_not_exists(self, job_type: str):
        index_name = f"{self.index_prefix}{job_type}"
        if not self.es_handler.index_exists(index_name):
            mappings = {
                "mappings": {
                    "properties": {
                        "job_type": {"type": "keyword"},
                        "step_id": {"type": "keyword"},
                        "batch_number": {"type": "integer"},
                        "kafka_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                        "time_received_from_kafka": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                        "time_inserted_to_elasticsearch": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                        "total_messages_received_from_kafka_so_far": {"type": "integer"},
                        "number_messages_in_this_batch": {"type": "integer"},
                        "is_error": {"type": "boolean"},
                        "error_types": {"type": "keyword"},
                        "processing_time_in_seconds": {
                            "type": "double",
                            "script": {
                                "lang": "painless",
                                "source": """
                                if (doc['time_inserted_to_elasticsearch'].size() == 0 || 
                                    doc['time_received_from_kafka'].size() == 0) {
                                    emit(0.0);
                                } else {
                                    long milliseconds = doc['time_inserted_to_elasticsearch'].value.toInstant().toEpochMilli() - 
                                                     doc['time_received_from_kafka'].value.toInstant().toEpochMilli();
                                    emit((double)milliseconds / 1000.0);
                                }
                                """
                            }
                        },
                        "total_latency_in_seconds": {
                            "type": "double",
                            "script": {
                                "lang": "painless",
                                "source": """
                                if (doc['time_inserted_to_elasticsearch'].size() == 0 || 
                                    doc['kafka_timestamp'].size() == 0) {
                                    emit(0.0);
                                } else {
                                    long milliseconds = doc['time_inserted_to_elasticsearch'].value.toInstant().toEpochMilli() - 
                                                     doc['kafka_timestamp'].value.toInstant().toEpochMilli();
                                    emit((double)milliseconds / 1000.0);
                                }
                                """
                            }
                        },
                        "batch_status": {
                            "type": "keyword",
                            "script": {
                                "lang": "painless",
                                "source": """
                                if (doc['is_error'].value) {
                                    emit('FAILED');
                                } else if (doc['number_messages_in_this_batch'].value == 0) {
                                    emit('EMPTY');
                                } else {
                                    emit('SUCCESS');
                                }
                                """
                            }
                        }
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "index.mapping.total_fields.limit": 2000,
                    "index.mapping.ignore_malformed": True,
                    "index.max_result_window": 10000
                }
            }
            self.es_handler.create_index_if_not_exists(index_name, mappings)

    def log_batch(self, job_type: str, step_id: str, batch_number: int, kafka_df: DataFrame,
                  kafka_timestamp: str, time_received_from_kafka: str, 
                  is_error: bool = False, error_types: list = None):
        """
        Log a single entry for a batch, either on successful completion or when errors occur.
        
        Args:
            job_type: Type of the job (e.g., 'event_logs', 'sevs')
            step_id: Unique identifier for the processing step
            batch_number: Batch number being processed
            kafka_df: The original Kafka DataFrame
            kafka_timestamp: Timestamp from Kafka
            time_received_from_kafka: Time when the batch was received
            is_error: Whether the batch encountered errors
            error_types: List of error types if is_error is True
        """
        index_name = f"{self.index_prefix}{job_type}"
        
        # Count messages from the original Kafka DataFrame
        batch_messages = kafka_df.count()
        self.total_messages += batch_messages

        # Prepare log document
        log_data = [{
            "job_type": job_type,
            "step_id": step_id,
            "batch_number": batch_number,
            "kafka_timestamp": kafka_timestamp,
            "time_received_from_kafka": time_received_from_kafka,
            "time_inserted_to_elasticsearch": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_messages_received_from_kafka_so_far": self.total_messages,
            "number_messages_in_this_batch": batch_messages,
            "is_error": bool(is_error),
            "error_types": error_types if error_types else []
        }]

        # Create DataFrame with explicit schema
        log_df = self.spark.createDataFrame(log_data, self.schema)

        # Insert log document into Elasticsearch
        self.es_handler.insert_dataframe(
            df=log_df,
            index_name=index_name
        )