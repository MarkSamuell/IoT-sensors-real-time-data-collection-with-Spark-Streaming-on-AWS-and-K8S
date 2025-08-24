from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_format, window, count, 
    lit, to_date
)
import logging
from datetime import datetime
from streaming_pipeline.shared.dal.elasticsearch_handler import ElasticsearchHandler
from configs.config import Config

logger = logging.getLogger(__name__)

def analyze(spark):
    """Main analysis function for batch SEVs processing."""
    logger.info("Initializing Batch SEVs Aggregation")
   
    try:
        es_handler = ElasticsearchHandler(
            Config.ES_HOST, 
            Config.ES_PORT, 
            Config.ES_SCHEME, 
            Config.FIXED_NAMESPACE_UUID
        )

        # Read from source index
        read_conf = {
            "es.nodes": Config.ES_HOST,
            "es.port": str(Config.ES_PORT),
            "es.nodes.wan.only": "true",
            "es.read.field.include": "ID,VinNumber,Timestamp,Severity,fleet_name,sev_name"
        }

        raw_data = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .options(**read_conf) \
            .load(Config.ELASTICSEARCH_SECURITY_EVENTS_INDEX) \
            .select(
                "ID",
                "Timestamp",
                "Severity", 
                "VinNumber",
                col("fleet_name").alias("Fleet"),
                col("sev_name").alias("SEV_Name")
            )

        # Filter date range
        start_date = datetime(2025, 1, 4)
        end_date = datetime(2025, 1, 8)
       
        filtered_data = raw_data.filter(
            (to_date(raw_data.Timestamp) >= start_date.strftime("%Y-%m-%d")) &
            (to_date(raw_data.Timestamp) <= end_date.strftime("%Y-%m-%d"))
        )

        # Aggregate
        aggregated_df = filtered_data \
            .groupBy(
                window(to_timestamp("Timestamp"), "1 hour"),
                "ID",
                "SEV_Name",
                "Severity",
                "VinNumber",
                "Fleet"
            ) \
            .agg(count("*").alias("count"))

        # Format for insertion
        final_df = aggregated_df.select(
            date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
            date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
            "ID",
            "SEV_Name",
            "Severity",
            col("VinNumber").alias("VIN"),
            "Fleet",
            "count"
        )

        # Add ID using handler
        final_df_with_id = es_handler.add_log_id(
            final_df,
            "es_id",
            ["VIN", "window_start", "ID", "SEV_Name"]
        )

        # Insert using handler
        es_handler.insert_aggregations(
            final_df_with_id,
            Config.ELASTICSEARCH_SECURITY_EVENTS_AGGS_INDEX
        )

        logger.info(f"Successfully processed and inserted {final_df_with_id.count()} records")

    except Exception as e:
        logger.error(f"Error in Batch SEVs Aggregation: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()