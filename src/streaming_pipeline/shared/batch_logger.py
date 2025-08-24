from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, ArrayType
)
from datetime import datetime
from streaming_pipeline.shared.dal.elasticsearch_handler import ElasticsearchHandler
import logging
import time
import traceback

logger = logging.getLogger(__name__)

class BatchLogger:
    def __init__(self, es_handler: ElasticsearchHandler, spark: SparkSession, index_prefix="logging_", max_retries=3):
        self.es_handler = es_handler
        self.spark = spark
        self.index_prefix = index_prefix
        self.max_retries = max_retries
        self.schema = StructType([
            StructField("job_type", StringType(), False),
            StructField("step_id", StringType(), False),
            StructField("batch_number", IntegerType(), False),
            StructField("kafka_timestamp", StringType(), True),
            StructField("time_received_from_kafka", StringType(), True),
            StructField("time_inserted_to_elasticsearch", StringType(), True),
            StructField("number_messages_in_this_batch", IntegerType(), False),
            StructField("total_messages_received_from_kafka_so_far", IntegerType(), False),
            StructField("is_error", BooleanType(), False),
            StructField("error_types", ArrayType(StringType(), True), True),
            StructField("processing_time_in_seconds", IntegerType(), True),
            StructField("total_latency_in_seconds", IntegerType(), True)
        ])

    def _get_total_messages(self, client, index_name: str, step_id: str, batch_number: int) -> int:
        """Get total messages from previous batches efficiently"""
        try:
            if batch_number == 0:
                return 0
                
            sum_body = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"step_id": step_id}},
                            {"range": {"batch_number": {"lt": batch_number}}}
                        ]
                    }
                },
                "aggs": {
                    "total_messages": {"sum": {"field": "number_messages_in_this_batch"}}
                },
                "_source": False  # Don't retrieve document contents
            }
            
            result = client.search(index=index_name, body=sum_body, size=0)
            return int(result['aggregations']['total_messages']['value'])
                
        except Exception as e:
            logger.warning(f"Error getting total messages: {e}, returning 0")
            return 0

    def _calculate_basic_metrics(self, time_inserted_to_elasticsearch: str, 
                               time_received_from_kafka: str,
                               kafka_timestamp: str) -> tuple:
        """Calculate simple processing time and latency metrics"""
        try:
            if not time_inserted_to_elasticsearch:
                return 0, 0
                
            # Convert string to datetime if needed
            if isinstance(time_inserted_to_elasticsearch, str):
                time_inserted_to_elasticsearch = datetime.strptime(time_inserted_to_elasticsearch, "%Y-%m-%d %H:%M:%S")
            if isinstance(time_received_from_kafka, str):
                time_received_from_kafka = datetime.strptime(time_received_from_kafka, "%Y-%m-%d %H:%M:%S")
            if isinstance(kafka_timestamp, str) and kafka_timestamp:
                kafka_timestamp = datetime.strptime(kafka_timestamp, "%Y-%m-%d %H:%M:%S")
            
            processing_time = int((time_inserted_to_elasticsearch - time_received_from_kafka).total_seconds())
            total_latency = int((time_inserted_to_elasticsearch - kafka_timestamp).total_seconds()) if kafka_timestamp else 0
            
            # Ensure values are within reasonable bounds
            if not (0 <= processing_time <= 3600):  # max 1 hour
                processing_time = 0
            if not (0 <= total_latency <= 86400):  # max 24 hours
                total_latency = 0
                
            return processing_time, total_latency
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Error calculating metrics: {e}")
            return 0, 0

    def _log_batch_with_retry(self, job_type: str, step_id: str, batch_number: int, 
                            kafka_df: DataFrame, kafka_timestamp: str, 
                            time_received_from_kafka: str, 
                            time_inserted_to_elasticsearch: str = None, 
                            is_error: bool = False, 
                            error_types: list = None, retry_count=0):
        """Log batch with retry logic"""
        index_name = f"{self.index_prefix}{job_type}"
        client = self.es_handler._create_client()
        
        try:
            # Get total messages from previous batches
            prev_total = self._get_total_messages(client, index_name, step_id, batch_number)
            
            # Count current batch messages efficiently
            def count_partition(iterator):
                count = 0
                for _ in iterator:
                    count += 1
                yield count
            
            batch_messages = kafka_df.rdd.mapPartitions(count_partition).sum()
            total_messages = prev_total + batch_messages
            
            # Calculate basic metrics
            processing_time, total_latency = self._calculate_basic_metrics(
                time_inserted_to_elasticsearch,
                time_received_from_kafka,
                kafka_timestamp
            )
            
            # Prepare document
            doc = {
                "job_type": job_type,
                "step_id": step_id,
                "batch_number": batch_number,
                "kafka_timestamp": kafka_timestamp,
                "time_received_from_kafka": time_received_from_kafka,
                "time_inserted_to_elasticsearch": time_inserted_to_elasticsearch,
                "number_messages_in_this_batch": batch_messages,
                "total_messages_received_from_kafka_so_far": total_messages,
                "is_error": bool(is_error),
                "error_types": error_types if error_types else [],
                "processing_time_in_seconds": processing_time,
                "total_latency_in_seconds": total_latency
            }
            
            # Index document with optimistic concurrency control
            client.index(
                index=index_name,
                id=f"{step_id}_{batch_number}",
                body=doc,
                op_type="create",
                refresh=False  # Don't force refresh for better performance
            )
            
            logger.info(f"Successfully logged batch {batch_number} for step {step_id} "
                       f"with {batch_messages} messages (total: {total_messages})")
            
        except Exception as e:
            if retry_count < self.max_retries:
                retry_delay = 0.1 * (2 ** retry_count)  # Exponential backoff
                logger.warning(f"Error logging batch {batch_number}, retrying in {retry_delay}s ({retry_count + 1}/{self.max_retries})")
                time.sleep(retry_delay)
                self._log_batch_with_retry(
                    job_type, step_id, batch_number, kafka_df, 
                    kafka_timestamp, time_received_from_kafka, 
                    time_inserted_to_elasticsearch,
                    is_error, error_types, retry_count + 1
                )
            else:
                logger.error(f"Failed to log batch {batch_number} after {self.max_retries} retries")
                raise
        finally:
            client.close()

    def log_batch(self, job_type: str, step_id: str, batch_number: int, kafka_df: DataFrame,
                 kafka_timestamp: str, time_received_from_kafka: str,
                 time_inserted_to_elasticsearch: str = None,  
                 is_error: bool = False, error_types: list = None):
        """Main log_batch method with optimistic concurrency control"""
        self._log_batch_with_retry(
            job_type, step_id, batch_number, kafka_df, 
            kafka_timestamp, time_received_from_kafka,
            time_inserted_to_elasticsearch,
            is_error, error_types
        )

    def create_logging_index_if_not_exists(self, job_type: str):
        """Create logging index with optimized mappings if it doesn't exist"""
        index_name = f"{self.index_prefix}{job_type}"
        if not self.es_handler.index_exists(index_name):
            mappings = {
                "mappings": {
                    "properties": {
                        "job_type": {"type": "keyword"},
                        "step_id": {"type": "keyword"},
                        "batch_number": {"type": "integer"},
                        "kafka_timestamp": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||epoch_millis"
                        },
                        "time_received_from_kafka": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||epoch_millis"
                        },
                        "time_inserted_to_elasticsearch": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||epoch_millis"
                        },
                        "number_messages_in_this_batch": {"type": "integer"},
                        "total_messages_received_from_kafka_so_far": {"type": "integer"},
                        "is_error": {"type": "boolean"},
                        "error_types": {"type": "keyword"},
                        "processing_time_in_seconds": {"type": "integer"},
                        "total_latency_in_seconds": {"type": "integer"}
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "refresh_interval": "30s"  # Reduce refresh frequency for better write performance
                }
            }
            self.es_handler.create_index_if_not_exists(index_name, mappings)