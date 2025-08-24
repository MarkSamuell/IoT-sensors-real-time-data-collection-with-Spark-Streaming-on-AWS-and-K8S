import traceback
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_unixtime, to_timestamp, year, month, dayofmonth,
    when, concat_ws, concat, element_at, expr, lit
)
from streaming_pipeline.shared.dal import KafkaHandler, PostgresHandler, DataFrameStorageType, IcebergHandler
from streaming_pipeline.shared.schema.schema_validator import SchemaValidator
from streaming_pipeline.shared.schema.schema_utils import process_error_records, get_schema_from_registry
from configs.config import Config

logger = logging.getLogger(__name__)

def enrich_sevs_df(df: DataFrame, vehicles_lookup_df: DataFrame, security_events_lookup_df: DataFrame):
    try:
        # Handle array access safely before joins
        df = df.withColumn(
            "first_parameter",
            when(
                col("ID") == '005',
                element_at(col("IoC.Parameters"), 1)
            )
        )

        # Join with lookup data
        if vehicles_lookup_df is not None:
            df = df.join(vehicles_lookup_df, df.VinNumber == vehicles_lookup_df.vin, "left")
        if security_events_lookup_df is not None:
            df = df.join(security_events_lookup_df, df.ID == security_events_lookup_df.sev_id, "left")

        # Transform ID and SEV name
        df = df.withColumn(
            "Alert_ID",
            concat_ws("-", col("ID"), col("VinNumber"), col("Timestamp"))
        ).withColumn(
            "transformed_id",
            when(
                col("ID") == '005',
                concat_ws("-", col("ID"), expr("first_parameter.InternalParameter.ParameterValue"))
            ).otherwise(col("ID"))
        ).withColumn(
            "final_sev_name",
            when(
                col("ID") == '005',
                concat(col("sev_name"), concat(lit("-"), expr("first_parameter.InternalParameter.ParameterType")))
            ).otherwise(col("sev_name"))
        )

        # Select final columns matching Iceberg schema
        return df.select(
            "Alert_ID",
            col("transformed_id").alias("ID"),
            "Timestamp",
            col("VinNumber").alias("VIN"),
            "Severity",
            "SEV_Msg",
            "Origin", 
            "NetworkType",
            "NetworkID",
            col("vehicle_model").alias("Vehicle_Model"),
            col("fleet_name").alias("Fleet_Name"),
            col("final_sev_name").alias("SEV_Name"),
            col("rule_id").alias("Rule_ID"), 
            col("description").alias("Description"),
            lit("unhandled").alias("SEV_Status"),
            "year",
            "month", 
            "day"
        )

    except Exception as e:
        logger.error(f"Error enriching security events data: {e}")
        logger.error(traceback.format_exc())
        return df.withColumn("SEV_Status", lit("unhandled"))

def process_sevs_batch(spark: SparkSession, df: DataFrame, epoch_id: int, 
                      kafka_handler: KafkaHandler, iceberg_handler: IcebergHandler,
                      vehicles_lookup_df: DataFrame, security_events_lookup_df: DataFrame):
    if df.isEmpty():
        logger.info(f"Batch {epoch_id}: Received empty DataFrame")
        return

    try:
        # Parse Kafka messages
        valid_records, error_df = kafka_handler.parse_without_validation(spark, df, "sevs_schema")
        
        if error_df.count() > 0:
            process_error_records(error_df, epoch_id, Config, 'ice_sevs')

        if not valid_records.isEmpty():
            # Format timestamp and add partition columns
            valid_records = valid_records.withColumn(
                "Timestamp",
                to_timestamp(from_unixtime(col("Timestamp").cast("double") / 1000))
            ).withColumn(
                "year", year("Timestamp")
            ).withColumn(
                "month", month("Timestamp")
            ).withColumn(
                "day", dayofmonth("Timestamp")
            )

            # Enrich data
            enriched_df = enrich_sevs_df(valid_records, vehicles_lookup_df, security_events_lookup_df)

            # Insert into Iceberg table
            iceberg_handler.insert_dataframe(
                enriched_df,
                f"{Config.ICEBERG_DATABASE}.{Config.ICEBERG_SECURITY_EVENTS_TABLE}",
                mode="merge",
                id_column="Alert_ID"
            )
            
            logger.info(f"Successfully processed {enriched_df.count()} records")

    except Exception as e:
        logger.error(f"Error processing batch {epoch_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def analyze(spark: SparkSession):
    logger.info("Initializing Iceberg SEVs analysis")

    with PostgresHandler(Config.POSTGRES_URL, Config.POSTGRES_PROPERTIES) as ps_handler:
        try:
            # Initialize Iceberg handler (but don't configure spark)
            iceberg_handler = IcebergHandler(Config.ICEBERG_WAREHOUSE, Config.ICEBERG_CATALOG_NAME)

            # Load lookup tables
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

            security_events_lookup_df = ps_handler.load_by_query(
                spark,
                """
                SELECT
                    sev_id, 
                    rule_id, 
                    sev_name, 
                    description
                FROM security_event_lookup
                """,
                "security_events_lookup_df",
                DataFrameStorageType.BROADCASTED
            )

            if vehicles_lookup_df is None or security_events_lookup_df is None:
                raise ValueError("Failed to load required lookup tables")

            # Initialize schema validator and Kafka handler
            schema_validator = SchemaValidator()
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

            # Create and process stream
            kafka_df = kafka_handler.create_kafka_stream(spark)
            
            query = kafka_df.writeStream \
                .foreachBatch(lambda df, epoch_id: process_sevs_batch(
                    spark, df, epoch_id, kafka_handler, iceberg_handler,
                    vehicles_lookup_df, security_events_lookup_df
                )) \
                .option("checkpointLocation", Config.S3_CHECKPOINT_LOCATION_SEVS) \
                .trigger(processingTime=Config.PROCESSING_TIME) \
                .start()

            logger.info("Streaming query started successfully")
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in Iceberg SEVs analysis: {str(e)}")
            logger.error(traceback.format_exc())
            if 'query' in locals() and query.isActive:
                query.stop()
            raise