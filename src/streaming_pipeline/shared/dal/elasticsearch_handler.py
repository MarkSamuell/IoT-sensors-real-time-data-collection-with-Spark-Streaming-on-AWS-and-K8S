from elasticsearch import Elasticsearch, BadRequestError
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, col, udf, spark_partition_id, when, to_timestamp, date_format, current_timestamp, lit
from pyspark.sql.types import StringType
import logging
import uuid
from elasticsearch.helpers import bulk

logger = logging.getLogger(__name__)


class ElasticsearchHandler:
    def __init__(self, host, port, scheme, fixed_namespace_uuid, username=None, password=None):
            # Log parameters before any processing
        logger.error(f"DEBUGGING - ElasticsearchHandler init params: host={host!r}, port={port!r}, scheme={scheme!r}")

        self.host = host
        self.port = port
        self.scheme = scheme
        self.username = username
        self.password = password
        self.fixed_namespace_uuid = self._ensure_uuid(fixed_namespace_uuid)

    def _ensure_uuid(self, value):
        if isinstance(value, uuid.UUID):
            return value
        elif isinstance(value, str):
            return uuid.UUID(value)
        else:
            raise ValueError("fixed_namespace_uuid must be a UUID object or a string")

    @staticmethod
    def parse_timestamp(df: DataFrame, timestamp_column: str, event_time_alias: str = "event_time", formatted_alias: str = "timestamp", date_format_string: str = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"):
        """
        Unified timestamp parsing function that handles various timestamp formats:
        - Unix timestamps in seconds (like 1751181550)
        - Unix timestamps in milliseconds (like 1751181550000)
        - Already formatted date strings
        
        Args:
            df: DataFrame to process
            timestamp_column: Name of the column containing the timestamp
            event_time_alias: Alias for the event_time column (default: "event_time")
            formatted_alias: Alias for the formatted timestamp string (default: "timestamp")
            date_format_string: Format string for the output timestamp (default: ISO format)
            
        Returns:
            DataFrame with event_time and formatted timestamp columns
        """
        try:
            # Convert timestamp to double for processing
            timestamp_col = col(timestamp_column).cast("double")
            
            # Detect timestamp format and convert appropriately
            # If timestamp > 1e10, it's in milliseconds; otherwise, it's in seconds
            event_time = when(
                timestamp_col > lit(1e10),
                to_timestamp(timestamp_col / 1000)  # Milliseconds -> seconds
            ).otherwise(
                to_timestamp(timestamp_col)  # Already in seconds
            )
            
            # Add both event_time and formatted timestamp
            result_df = df.withColumn(event_time_alias, event_time) \
                         .withColumn(formatted_alias, date_format(col(event_time_alias), date_format_string))
            
            logger.debug(f"Successfully parsed timestamp column '{timestamp_column}' to '{event_time_alias}' and '{formatted_alias}'")
            return result_df
            
        except Exception as e:
            logger.error(f"Error parsing timestamp column '{timestamp_column}': {str(e)}")
            # Return original DataFrame with null timestamp columns as fallback
            return df.withColumn(event_time_alias, lit(None).cast("timestamp")) \
                     .withColumn(formatted_alias, lit(None).cast("string"))

    def _create_client(self):

        if self.host is None:
            raise ValueError("Cannot create Elasticsearch client: host is None")

        return Elasticsearch(
            [{'host': self.host, 'port': self.port, 'scheme': self.scheme}],
            basic_auth=(self.username, self.password) if self.username and self.password else None,
            verify_certs=False,
            ssl_show_warn=False,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3
        )
    
    def get_es_write_conf(self, index_name: str, mapping_id: str = None, operation: str = "index"):
        """Get Elasticsearch write configurations"""
        conf = {
            "es.nodes": self.host,
            "es.port": str(self.port),
            "es.resource": index_name,
            "es.nodes.wan.only": "true",
            "es.batch.write.refresh": "false",
            "es.mapping.date.rich": "false",
            "es.batch.write.retry.count": "3",
            "es.batch.write.retry.wait": "2s",
            "es.write.operation": operation,
            "es.http.retries": "3"
        }

        # Add SSL configuration only if using HTTPS
        if self.scheme == 'https':
            conf.update({
                "es.net.ssl": "true",
                "es.net.ssl.cert.allow.self.signed": "true",
            })

        if mapping_id:
            conf["es.mapping.id"] = mapping_id

        if self.username and self.password:
            conf.update({
                "es.net.http.auth.user": self.username,
                "es.net.http.auth.pass": self.password
            })

        return conf

    def insert_aggregations(self, df: DataFrame, index_name: str):
            """Insert aggregations using bulk operations per partition"""
            if not self.index_exists(index_name):
                raise ValueError(f"Elasticsearch index '{index_name}' does not exist.")

            def process_partition(iterator):
                """Process a single partition of data"""
                client = self._create_client()
                try:
                    # Convert partition data to actions
                    actions = [self._es_update_action(row.asDict(), index_name) for row in iterator]
                    
                    if actions:  # Only process if partition has data
                        # Perform bulk operation for this partition
                        success, failed = bulk(client, actions, raise_on_error=False, stats_only=True)
                        logger.info(f"Bulk inserted batch of {success} records into {index_name}" + 
                                f" ({len(failed) if failed else 0} failed)")
                        
                        if failed:
                            logger.error(f"Failed to update {len(failed)} documents in partition")
                except Exception as e:
                    logger.error(f"Error processing partition: {e}")
                    raise
                finally:
                    client.close()

            try:
                # Process partitions
                df.foreachPartition(process_partition)
                logger.info(f"Completed bulk insertion into {index_name}")
            except Exception as e:
                logger.error(f"Error performing bulk operations: {e}")
                raise

    def _es_update_action(self, doc, index_name):
        """Create an Elasticsearch update action for aggregations"""
        return {
            "_op_type": "update",
            "_index": index_name,
            "_id": doc["es_id"],
            "script": {
                "source": "ctx._source.count += params.count",
                "lang": "painless",
                "params": {"count": doc["count"]}
            },
            "upsert": doc
        }

    def insert_dataframe(self, df: DataFrame, index_name: str, mapping_id: str = None):
        """Insert data without aggregation"""
        if not self.index_exists(index_name):
            raise ValueError(f"Elasticsearch index '{index_name}' does not exist.")

        try:
            logger.info(f"Inserting records into {index_name}")
            logger.info(f"Starting insert into {index_name} with mapping_id {mapping_id}")
            
            es_write_conf = self.get_es_write_conf(
                index_name=index_name,
                mapping_id=mapping_id,
                operation="index"
            )
            
            df.write \
                .format("org.elasticsearch.spark.sql") \
                .options(**es_write_conf) \
                .mode("append") \
                .save(index_name)
            
            logger.info(f"Successfully inserted records into {index_name}")
            
        except Exception as e:
            logger.error(f"Error inserting data into Elasticsearch: {e}")
            logger.error(f"DataFrame schema: {df.schema}")
            raise

    def index_exists(self, index_name):
        client = self._create_client()
        try:
            # Try GET request instead of exists API
            try:
                info = client.indices.get(index=index_name)
                return index_name in info
            except Exception as e:
                if hasattr(e, 'status_code') and e.status_code == 404:
                    return False
                
                # For logging indices, handle errors differently
                if index_name.startswith('logging_'):
                    logger.warning(f"Error checking logging index '{index_name}': {str(e)}")
                    # Use HEAD request as fallback for logging indices
                    try:
                        return client.indices.exists(index=index_name)
                    except Exception:
                        logger.warning(f"Fallback check failed for logging index '{index_name}'")
                        return False
                
                # For other indices, propagate the error
                logger.error(f"Error checking index '{index_name}': {str(e)}")
                raise
        finally:
            client.close()

    def add_log_id(self, df: DataFrame, log_id_column_name: str, columns_list: list):
        """Add a UUID5-based ID column to the DataFrame"""
        return df.withColumn(
            log_id_column_name, 
            self.generate_uuid5_udf(
                concat_ws("|", *[col(col_name) for col_name in columns_list])
            )
        )

    def generate_uuid5(self, name: str) -> str:
        """Generate a UUID5 from a string"""
        if name is None:
            return None
        return str(uuid.uuid5(self.fixed_namespace_uuid, str(name)))
    
        
    def create_index_if_not_exists(self, index_name, mappings):
        client = self._create_client()
        try:
            exists = self.index_exists(index_name)
            if not exists:
                try:
                    client.indices.create(index=index_name, mappings=mappings["mappings"], settings=mappings["settings"])
                    logger.info(f"Created index '{index_name}'")
                except BadRequestError as e:
                    # Check if the error is because index already exists
                    if "resource_already_exists_exception" in str(e):
                        logger.info(f"Index '{index_name}' already exists (created by another process)")
                        return
                    logger.error(f"Error creating index '{index_name}': {str(e)}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error creating index '{index_name}': {str(e)}")
                    raise
            else:
                logger.info(f"Index '{index_name}' already exists")
        finally:
            client.close()


    @property
    def generate_uuid5_udf(self):
        return udf(self.generate_uuid5, StringType())