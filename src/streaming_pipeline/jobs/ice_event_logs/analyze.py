import traceback
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, explode, from_unixtime, to_timestamp, year, month, dayofmonth,
    when, lit
)
from streaming_pipeline.shared.dal import KafkaHandler, PostgresHandler, DataFrameStorageType, IcebergHandler
from streaming_pipeline.shared.schema.schema_validator import SchemaValidator
from streaming_pipeline.shared.schema.schema_utils import process_error_records, get_schema_from_registry
from configs.config import Config

logger = logging.getLogger(__name__)

def process_and_enrich_df(df: DataFrame, vehicles_lookup_df: DataFrame, signals_lookup_df: DataFrame):
    try:
        processed_df = (df
            .select(
                col("VinNumber").alias("VIN"),
                col("Campaign_ID"),
                explode(col("DataEntries")).alias("entry")
            )
            .select(
                col("VIN"),
                col("Campaign_ID"),
                col("entry.DataName").alias("Signal_ID"),
                col("entry.DataValue").alias("Signal_Value"),
                col("entry.DataType").alias("Datatype"),
                to_timestamp(from_unixtime(col("entry.TimeStamp").cast("double") / 1000)).alias("Timestamp")
            )
            .withColumn(
                "Signal_Value",
                when(
                    ~col("Signal_ID").isin("101", "102", "103") & 
                    col("Signal_Value").isin("0", "1"),
                    when(col("Signal_Value") == "0", "passed").otherwise("failed")
                ).otherwise(col("Signal_Value"))
            )
            .withColumn("Log_ID", 
                       lit(None).cast("string")  # Placeholder for auto-generated ID
            )
            .withColumn("year", year("Timestamp"))
            .withColumn("month", month("Timestamp"))
            .withColumn("day", dayofmonth("Timestamp"))
        )
        
        if vehicles_lookup_df is not None:
            processed_df = processed_df.join(
                vehicles_lookup_df, 
                processed_df.VIN == vehicles_lookup_df.vin, 
                "left"
            ).drop(vehicles_lookup_df.vin)

        if signals_lookup_df is not None:
            processed_df = processed_df.join(
                signals_lookup_df, 
                processed_df.Signal_ID == signals_lookup_df.signal_id, 
                "left"
            )

        return processed_df.select(
            "Log_ID",
            "Timestamp",
            col("signal_name").alias("Signal_Name"),
            "Signal_Value",
            col("signal_unit").alias("Signal_Unit"),
            "VIN",
            col("vehicle_model").alias("Vehicle_Model"),
            col("fleet_name").alias("Fleet_Name"),
            "Campaign_ID",
            col("component").alias("Component"),
            "Datatype",
            col("min_value").alias("Min"),
            col("max_value").alias("Max"),
            col("signal_fully_qualified_name").alias("Signal_Fully_Qualified_Name"),
            "year", "month", "day"
        )

    except Exception as e:
        logger.error(f"Error processing and enriching data: {e}")
        logger.error(traceback.format_exc())
        raise

def process_batch(spark: SparkSession, df: DataFrame, epoch_id: int, 
                 kafka_handler: KafkaHandler, iceberg_handler: IcebergHandler,
                 vehicles_lookup_df: DataFrame, signals_lookup_df: DataFrame):
    if df.isEmpty():
        logger.info(f"Batch {epoch_id}: Received empty DataFrame")
        return

    try:
        valid_records, error_df = kafka_handler.parse_without_validation(spark, df, "logs_schema")
        
        if error_df.count() > 0:
            process_error_records(error_df, epoch_id, Config, 'ice_event_logs')

        if not valid_records.isEmpty():
            enriched_df = process_and_enrich_df(valid_records, vehicles_lookup_df, signals_lookup_df)
            
            iceberg_handler.insert_dataframe(
                enriched_df,
                f"{Config.ICEBERG_DATABASE}.{Config.ICEBERG_EVENT_LOGS_TABLE}",
                mode="merge",
                id_column="Log_ID"
            )
            
            logger.info(f"Successfully processed {enriched_df.count()} records")

    except Exception as e:
        logger.error(f"Error processing batch {epoch_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def analyze(spark: SparkSession):
    logger.info("Initializing Iceberg Event Logs analysis")

    with PostgresHandler(Config.POSTGRES_URL, Config.POSTGRES_PROPERTIES) as ps_handler:
        try:
            iceberg_handler = IcebergHandler(Config.ICEBERG_WAREHOUSE, Config.ICEBERG_CATALOG_NAME)

            vehicles_lookup_df = ps_handler.load_by_query(
                spark,
                """
                SELECT 
                    v.vin, 
                    vm.model_name AS vehicle_model, 
                    fc."Fleet Name" AS fleet_name
                FROM vehicle v
                LEFT JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                LEFT JOIN fleet_cars fc ON v.vin = fc."VIN Number"
                """,
                "vehicles_lookup_df",
                DataFrameStorageType.CACHED
            )

            signals_lookup_df = ps_handler.load_by_query(
                spark,
                """
                SELECT
                    signal_id,
                    signal_name,
                    signal_unit,
                    component,
                    min_value,
                    max_value,
                    signal_fully_qualified_name
                FROM cars_signals
                """,
                "signals_lookup_df",
                DataFrameStorageType.BROADCASTED
            )

            if vehicles_lookup_df is None or signals_lookup_df is None:
                raise ValueError("Failed to load required lookup tables")

            schema_validator = SchemaValidator()
            schema_validator.register_schema_in_validator(
                "logs_schema",
                get_schema_from_registry(
                    Config.GLUE_SCHEMA_REGISTRY_NAME,
                    Config.GLUE_SCHEMA_REGISTRY_REGION,
                    "logs_schema"
                )
            )
            
            kafka_handler = KafkaHandler(
                Config.KAFKA_BOOTSTRAP_SERVERS,
                Config.KAFKA_LOGS_TOPIC,
                schema_validator
            )

            kafka_df = kafka_handler.create_kafka_stream(spark)
            
            query = kafka_df.writeStream \
                .foreachBatch(lambda df, epoch_id: process_batch(
                    spark, df, epoch_id, kafka_handler, iceberg_handler,
                    vehicles_lookup_df, signals_lookup_df
                )) \
                .option("checkpointLocation", Config.S3_CHECKPOINT_LOCATION_LOGS) \
                .trigger(processingTime=Config.PROCESSING_TIME) \
                .start()

            logger.info("Streaming query started successfully")
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in Iceberg Event Logs analysis: {str(e)}")
            logger.error(traceback.format_exc())
            if 'query' in locals() and query.isActive:
                query.stop()
            raise