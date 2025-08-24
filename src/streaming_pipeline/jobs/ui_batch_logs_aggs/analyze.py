from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, date_format, window, count, concat,
    lit, to_date, coalesce, udf
)
import logging
import uuid
from datetime import datetime, timedelta
from configs.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_uuid5(name: str) -> str:
    """Generate a UUID5 from a string"""
    if name is None:
        return None
    FIXED_NAMESPACE = uuid.UUID('32f51344-0933-43d2-b923-304b8d8f7896')
    return str(uuid.uuid5(FIXED_NAMESPACE, str(name)))

generate_uuid5_udf = udf(generate_uuid5)

def analyze(spark):
    """Main analysis function for batch Event Logs processing."""
    logger.info("Initializing Batch Event Logs Aggregation")
   
    # Create SparkSession with ES configs
    spark = SparkSession.builder \
        .appName("EventLogsBatchAggregator") \
        .config("es.nodes", Config.ES_HOST) \
        .config("es.port", str(Config.ES_PORT)) \
        .config("es.nodes.wan.only", "true") \
        .getOrCreate()

    try:
        # Read from Elasticsearch with specific fields
        read_conf = {
            "es.nodes": Config.ES_HOST,
            "es.port": str(Config.ES_PORT),
            "es.nodes.wan.only": "true",
            "es.read.field.include": "Timestamp,VIN,Signal_Name"  # Only include fields we need
        }

        raw_data = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .options(**read_conf) \
            .load(Config.ELASTICSEARCH_EVENT_LOGS_INDEX)

        # Filter data for desired date range
        # end_date = datetime.now()
        # start_date = end_date - timedelta(days=1)

        # Or use specific dates
        start_date = datetime(2024, 11, 29)
        end_date = datetime(2024, 11, 30)

        filtered_data = raw_data.filter(
            (to_date(raw_data.Timestamp) >= start_date.strftime("%Y-%m-%d")) &
            (to_date(raw_data.Timestamp) <= end_date.strftime("%Y-%m-%d"))
        )

        # Create aggregations with window
        aggregated_df = filtered_data \
            .groupBy(
                window(to_timestamp("Timestamp"), "1 hour"),
                col("VIN"),
                col("Signal_Name")
            ) \
            .agg(count("*").alias("count"))

        # Prepare data for Elasticsearch
        flat_aggregated_df = aggregated_df.select(
            date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
            date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
            col("VIN"),
            col("Signal_Name"),
            col("count")
        )

        # Generate ID for upsert
        final_df = flat_aggregated_df.withColumn(
            "es_id",
            generate_uuid5_udf(concat(
                col("VIN"),
                col("window_start"),
                col("Signal_Name")
            ))
        )

        # Write to Elasticsearch
        write_conf = {
            "es.nodes": Config.ES_HOST,
            "es.port": str(Config.ES_PORT),
            "es.resource": "ui_event_logs_aggs",
            "es.write.operation": "upsert",
            "es.mapping.id": "es_id",
            "es.nodes.wan.only": "true"
        }

        final_df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**write_conf) \
            .mode("append") \
            .save()

        logger.info(f"Successfully processed and inserted {final_df.count()} aggregation records")

    except Exception as e:
        logger.error(f"Error in Batch Event Logs Aggregation: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()