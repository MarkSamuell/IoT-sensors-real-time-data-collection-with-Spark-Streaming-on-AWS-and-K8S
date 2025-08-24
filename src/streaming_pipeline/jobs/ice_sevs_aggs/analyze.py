import traceback
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, to_timestamp, window, broadcast, when, count,
    concat_ws, concat, element_at, expr, lit, year, month, dayofmonth
)
from streaming_pipeline.shared.dal import KafkaHandler, PostgresHandler, DataFrameStorageType, IcebergHandler
from streaming_pipeline.shared.schema.optimized_schema_validator import OptSchemaValidator
from streaming_pipeline.shared.schema.schema_utils import process_error_records, get_schema_from_registry
from configs.config import Config

logger = logging.getLogger(__name__)

def transform_records(df: DataFrame, security_events_lookup_df: DataFrame):
    try:
        df = df.withColumn(
            "event_time",
            to_timestamp(col("Timestamp").cast("double") / 1000)
        ).withColumn(
            "first_parameter",
            when(col("ID") == '005', element_at(col("IoC.Parameters"), 1))
        ).withColumn(
            "transformed_id",
            when(
                col("ID") == '005',
                concat_ws("-", col("ID"), expr("first_parameter.InternalParameter.ParameterValue"))
            ).otherwise(col("ID"))
        )

        df = df.join(broadcast(security_events_lookup_df), df.ID == security_events_lookup_df.sev_id, "left")
            
        df = df.withColumn(
            "final_sev_name",
            when(
                col("ID") == '005',
                concat(col("sev_name"), concat(lit("-"), expr("first_parameter.InternalParameter.ParameterType")))
            ).otherwise(col("sev_name"))
        )

        return df.select(
            col("transformed_id").alias("ID"),
            col("final_sev_name").alias("SEV_Name"),
            col("VinNumber").alias("VIN"),
            "Severity",
            "event_time"
        )

    except Exception as e:
        logger.error(f"Error transforming records: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_current_events(df: DataFrame, vehicles_lookup_df: DataFrame, 
                         security_events_lookup_df: DataFrame, iceberg_handler: IcebergHandler):
    try:
        transformed_records = transform_records(df, security_events_lookup_df)
        windowed_records = transformed_records.withColumn(
            "window",
            window(col("event_time"), "1 hour")
        )

        window_spec = Window.partitionBy(
            "window", "ID", "SEV_Name", "VIN", "Severity"
        )

        aggregated_df = windowed_records.withColumn(
            "count",
            count("*").over(window_spec)
        ).select(
            "window.start",
            "window.end",
            "ID",
            "SEV_Name",
            "Severity",
            "VIN",
            "count"
        ).distinct()

        enriched_df = aggregated_df.join(
            broadcast(vehicles_lookup_df), 
            col("VIN") == vehicles_lookup_df.vin,
            "left"
        ).withColumn(
            "Fleet",
            when(col("fleet_name").isNull(), "Unknown").otherwise(col("fleet_name"))
        )

        final_df = enriched_df.select(
            "start",
            "end",
            "ID",
            "SEV_Name",
            "Severity",
            "VIN",
            "Fleet",
            "count"
        ).withColumn(
            "year", year("start")
        ).withColumn(
            "month", month("start")
        ).withColumn(
            "day", dayofmonth("start")
        )

        iceberg_handler.insert_aggregations(
            final_df,
            f"{Config.ICEBERG_DATABASE}.{Config.ICEBERG_SECURITY_EVENTS_AGGS_TABLE}",
            groupby_columns=["start", "ID", "SEV_Name", "Severity", "VIN"],
            agg_column="count"
        )

    except Exception as e:
        logger.error(f"Error processing current events: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_batch(spark: SparkSession, df: DataFrame, batch_id: int, 
                 kafka_handler: KafkaHandler, iceberg_handler: IcebergHandler,
                 vehicles_lookup_df: DataFrame, security_events_lookup_df: DataFrame):
    if df.isEmpty():
        return

    try:
        valid_records, error_df = kafka_handler.parse_without_validation(spark, df, "sevs_schema")
        
        if error_df.count() > 0:
            process_error_records(error_df, batch_id, Config, 'ice_sevs_aggs')

        if valid_records.count() > 0:
            process_current_events(
                valid_records, 
                vehicles_lookup_df,
                security_events_lookup_df,
                iceberg_handler
            )

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def analyze(spark: SparkSession):
    logger.info("Initializing Iceberg SEVs Aggregations")

    with PostgresHandler(Config.POSTGRES_URL, Config.POSTGRES_PROPERTIES) as ps_handler:
        try:
            iceberg_handler = IcebergHandler(Config.ICEBERG_WAREHOUSE, Config.ICEBERG_CATALOG_NAME)

            vehicles_lookup_df = ps_handler.load_by_query(
                spark,
                """SELECT DISTINCT
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

            security_events_lookup_df = ps_handler.load_by_query(
                spark,
                """SELECT DISTINCT
                    sev_id, 
                    sev_name
                FROM security_event_lookup
                """,
                "security_events_lookup_df",
                DataFrameStorageType.BROADCASTED
            )

            schema_validator = OptSchemaValidator()
            schema_validator.register_schema_in_validator(
                "sevs_schema",
                get_schema_from_registry(
                    Config.GLUE_SCHEMA_REGISTRY_NAME,
                    Config.GLUE_SCHEMA_REGISTRY_REGION,
                    "sevs_schema"
                )
            )
            
            kafka_handler = KafkaHandler(
                Config.KAFKA_BOOTSTRAP_SERVERS,
                Config.KAFKA_SEVS_TOPIC,
                schema_validator
            )

            kafka_df = kafka_handler.create_kafka_stream(spark)
            
            query = kafka_df.writeStream \
                .foreachBatch(lambda df, epoch_id: process_batch(
                    spark, df, epoch_id, kafka_handler, iceberg_handler,
                    vehicles_lookup_df, security_events_lookup_df
                )) \
                .option("checkpointLocation", Config.S3_CHECKPOINT_LOCATION_SEVS_AGGS) \
                .trigger(processingTime=Config.PROCESSING_TIME) \
                .start()

            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in Iceberg SEVs Aggregations: {str(e)}")
            logger.error(traceback.format_exc())
            if 'query' in locals() and query.isActive:
                query.stop()
            raise