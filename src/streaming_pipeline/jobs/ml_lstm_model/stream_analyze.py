from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import (
    col, pandas_udf, PandasUDFType, when, lit, 
    current_timestamp, to_json, struct, from_json, expr,
    date_format, concat_ws, format_number, from_unixtime, unix_timestamp
)
import pandas as pd
import numpy as np
import tensorflow as tf
import h5py
import logging
import os
import boto3
import traceback
from datetime import datetime

from streaming_pipeline.shared.dal import ElasticsearchHandler, KafkaHandler, PostgresHandler, DataFrameStorageType
from streaming_pipeline.shared.batch_logger import BatchLogger
from streaming_pipeline.jobs.ml_lstm_model.data_processing import load_and_process_data
from configs.config import Config

logger = logging.getLogger(__name__)

# Define schema for parsing Kafka messages
kafka_schema = StructType([
    StructField("VinNumber", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("Arbitration_ID", StringType(), True),
    StructField("DLC", StringType(), True),
    StructField("Data", StringType(), True),
    StructField("Class", StringType(), True),
    StructField("SubClass", StringType(), True)
])

class MLPredictor:
    def __init__(self):
        self.model = None
    
    def predict(self, timestamps, arb_ids, data):
        if self.model is None:
            self._initialize_model()
            
        pdf = pd.DataFrame({
            'Timestamp': timestamps.astype('float64'),
            'Arbitration_ID': arb_ids,
            'Data': data
        })
        
        if pdf.empty:
            return pd.Series()
            
        # Sort by timestamp before processing
        pdf = pdf.sort_values('Timestamp')
        
        # Use preprocess_batch for data cleaning
        # cleaned_pdf = preprocess_batch(pdf)
        processed_df = load_and_process_data(df=pdf)
        reshaped_data = processed_df.values.reshape(processed_df.shape[0], processed_df.shape[1], 1)
        
        # Get predictions and print raw values
        predictions = self.model.predict(reshaped_data) 
        predicted_classes = np.argmax(predictions, axis=1)
        
        # Convert to 1-dimensional pandas Series
        return pd.Series(predicted_classes)

    def _initialize_model(self):
        if not os.path.exists(Config.ML_MODEL_LOCAL_PATH):
            s3 = boto3.client('s3')
            s3.download_file(Config.ML_MODEL_S3_BUCKET,
                           Config.ML_MODEL_S3_KEY,
                           Config.ML_MODEL_LOCAL_PATH)
        
        self.model = tf.keras.models.load_model(Config.ML_MODEL_LOCAL_PATH)
        tf.keras.backend.set_learning_phase(0)

# def preprocess_batch(key_pdf: pd.DataFrame) -> pd.DataFrame:
#     """Preprocess pandas DataFrame batch with NaN handling"""
#     # Fill NaN values
#     pdf = key_pdf.fillna({
#         'Timestamp': 0.0,
#         'Arbitration_ID': '0',
#         'Data': '00 00 00 00 00 00 00 00'
#     })
    
#     # Format Arbitration IDs safely
#     def safe_hex_convert(x):
#         try:
#             return str(int(str(x).replace('0x', '').strip().upper(), 16))
#         except (ValueError, AttributeError):
#             return '0'
    
#     pdf['Arbitration_ID'] = pdf['Arbitration_ID'].apply(safe_hex_convert)
    
#     # Format data fields safely 
#     def format_data(x):
#         try:
#             if pd.isna(x) or not x:
#                 return "00 00 00 00 00 00 00 00"
#             parts = str(x).strip().split()
#             hex_values = [f"{int(p, 16):02X}" for p in parts[:8]]
#             return ' '.join(hex_values + ['00'] * (8 - len(hex_values)))
#         except ValueError:
#             return "00 00 00 00 00 00 00 00"
    
#     pdf['Data'] = pdf['Data'].apply(format_data)
    
#     # Sort by timestamp before returning to ensure monotonic order
#     pdf = pdf[['Timestamp', 'Arbitration_ID', 'Data']].sort_values('Timestamp')
#     return pdf

def process_streaming_batch(spark: SparkSession, df: DataFrame, epoch_id: int, 
                          es_handler: ElasticsearchHandler, batch_logger: BatchLogger,
                          predictor: MLPredictor,
                          vehicles_lookup_df: DataFrame,
                          security_events_lookup_df: DataFrame):
    """Process each streaming batch with comprehensive debugging and verification"""
    
    error_types = set()
    kafka_timestamp = None
    time_received_from_kafka = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    time_inserted_to_elasticsearch = None
    
    # Batch start logging
    logger.info("\n" + "="*50)
    logger.info(f"PROCESSING BATCH {epoch_id}")
    logger.info("="*50)
    
    if df.isEmpty():
        logger.info(f"Batch {epoch_id}: Received empty DataFrame. Skipping processing.")
        batch_logger.log_batch(
            job_type="can_ml",
            step_id=str(spark.sparkContext.applicationId),
            batch_number=epoch_id,
            kafka_df=df,
            kafka_timestamp=kafka_timestamp,
            time_received_from_kafka=time_received_from_kafka,
            time_inserted_to_elasticsearch=None,
            is_error=False,
            error_types=None
        )
        return

    try:
        # 1. Extract Kafka timestamp
        kafka_timestamp_row = df.select("timestamp").agg({"timestamp": "min"}).collect()[0]
        if kafka_timestamp_row[0] is not None:
            kafka_timestamp = kafka_timestamp_row[0].strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Kafka timestamp: {kafka_timestamp}")

        # 2. Log raw input sample
        logger.info("\nRaw input sample:")
        df.select("value").show(5, truncate=False)

        # 3. Parse Kafka messages
        logger.info("\nParsing Kafka messages...")
        parsed_df = df.select(
            from_json(expr("cast(value as string)"), kafka_schema).alias("parsed_value")
        ).select("parsed_value.*")
        
        logger.info("Parsed data sample:")
        parsed_df.show(5, truncate=False)

        # 4. Clean data
        cleaned_df = parsed_df.select(
            col("VinNumber"),
            col("Timestamp").cast("double").alias("timestamp_value"),
            col("Arbitration_ID"),
            when(col("Data").isNull() | (col("Data") == ""), "00 00 00 00 00 00 00 00")
            .otherwise(col("Data")).alias("Data")
        ).orderBy("timestamp_value")

        logger.info("\nCleaned data sample:")
        cleaned_df.show(5, truncate=False)

        # 5. Create and apply prediction UDF
        predict_udf = pandas_udf(
            predictor.predict,
            returnType=DoubleType(),
            functionType=PandasUDFType.SCALAR
        )

        result_df = cleaned_df.withColumn(
            "anomaly_score",
            predict_udf(
                col("timestamp_value"),
                col("Arbitration_ID"),
                col("Data")
            )
        )

        # 6. Format for Elasticsearch
        enriched_df = result_df.select(
            col("timestamp_value"),
            col("VinNumber"),
            col("Arbitration_ID"),
            col("Data"),
            col("anomaly_score"),
            date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("Timestamp")
        )

        # 7. Generate document IDs
        final_df = es_handler.add_log_id(
            enriched_df,
            "doc_id",
            ["timestamp_value", "Arbitration_ID"]
        )

        # 8. Insert into Elasticsearch
        es_handler.insert_dataframe(
            final_df,
            Config.ELASTICSEARCH_CAN_ML_INDEX,
            mapping_id="doc_id"
        )
        
        # Process high-score anomalies for security events index
        high_anomaly_df = result_df.filter(col("anomaly_score") >= 1)

        if high_anomaly_df.count() > 0:
            # First join with vehicle lookup data
            enriched_df = high_anomaly_df.join(
                vehicles_lookup_df,
                high_anomaly_df.VinNumber == vehicles_lookup_df.vin,
                "left"
            )

            # Create base security event with proper ID based on score
            security_events_df = enriched_df.select(
                when(col("anomaly_score") == 1.0, lit("006"))
                .when(col("anomaly_score") == 2.0, lit("007"))
                .alias("ID"),
                col("VinNumber"),
                col("timestamp_value"),  # Keep this column
                date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("Timestamp"),
                lit("CAN").alias("NetworkType"),
                lit("AI Anomaly Detection").alias("Origin"),
                when(col("anomaly_score") == 1, lit("HIGH"))
                .when(col("anomaly_score") == 2, lit("HIGH"))
                .alias("Severity"),
                col("Arbitration_ID").alias("NetworkID"),
                col("vehicle_model"),
                col("fleet_name"),
                col("Data"),
                lit("unhandled").alias("SEV_Status")
            )

            # Step 1: Generate Alert_ID for security events first
            security_events_df_with_id = es_handler.add_log_id(
                security_events_df,
                "Alert_ID",
                ["ID", "VinNumber", "Timestamp", "Severity", "timestamp_value", "NetworkID", "Data"]
            )


            # Join with security events lookup
            joined_sev_df = security_events_df_with_id.join(
                security_events_lookup_df,
                security_events_df_with_id.ID == security_events_lookup_df.sev_id,
                "left"
            )

            # Generate SEV message and final security events DataFrame
            security_events_df_with_msg = joined_sev_df.withColumn(
                "SEV_Msg", 
                concat_ws(" ",
                    lit("CAN Frame Anomaly Detected:"),
                    col("sev_name"),
                    lit("CAN Frame ID:"),
                    col("NetworkID")
                )
            )

            # Final selection for security events
            final_sev_df = security_events_df_with_msg.select(
                "Alert_ID",
                "ID",
                "VinNumber",
                "Timestamp",
                "NetworkType",
                "Origin",
                "Severity",
                "SEV_Msg",
                "NetworkID",
                "vehicle_model",
                "fleet_name",
                "sev_name",
                "rule_id",
                "description",
                "SEV_Status"
            )

            final_sev_df.show(10)

            # Create logs DataFrame using the SAME Alert_ID
            can_logs_df = security_events_df_with_msg.select(
                col("ID"),
                col("Alert_ID"),
                col("VinNumber"),
                col("Timestamp"),
                date_format(
                    from_unixtime(
                        unix_timestamp(col("Timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") + 
                        col("timestamp_value")
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                ).alias("ParameterTimestamp"),
                lit("AI Anomaly Detection").alias("ParameterName"),
                col("sev_name").alias("ParameterType"),
                col("Data").alias("ParameterValue")
            )

            can_logs_df.show(10)

            # Generate ONLY Log_ID for logs, keeping the original Alert_ID
            logs_df_with_id = es_handler.add_log_id(
                can_logs_df,
                "Log_ID",
                ["ID", "Alert_ID", "VinNumber", "Timestamp", "ParameterTimestamp", "ParameterType", "ParameterValue"]
            )

            logs_df_with_id.show(10)

            # Insert security events
            es_handler.insert_dataframe(
                final_sev_df,
                Config.ELASTICSEARCH_SECURITY_EVENTS_INDEX,
                mapping_id="Alert_ID"
            )

            # Insert logs
            es_handler.insert_dataframe(
                logs_df_with_id,
                Config.ELASTICSEARCH_SEVS_LOGS_INDEX,
                mapping_id="Log_ID"
            )
        
        # Log processing summary
        time_inserted_to_elasticsearch = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info("\nProcessing Summary:")
        logger.info(f"- Records processed: {final_df.count()}")
        logger.info(f"- Processing time: {time_inserted_to_elasticsearch}")
        logger.info("- Status: Success")

    except Exception as e:
        error_types.add("PROCESSING_ERROR")
        logger.error("\nError processing batch:")
        logger.error(str(e))
        logger.error(traceback.format_exc())
        raise
        
    finally:
        try:
            batch_logger.log_batch(
                job_type="can_ml",
                step_id=str(spark.sparkContext.applicationId),
                batch_number=epoch_id,
                kafka_df=df,
                kafka_timestamp=kafka_timestamp,
                time_received_from_kafka=time_received_from_kafka,
                time_inserted_to_elasticsearch=time_inserted_to_elasticsearch,
                is_error=bool(error_types),
                error_types=list(error_types) if error_types else None
            )
            logger.info("\nBatch logging completed")
            logger.info("="*50 + "\n")
            
        except Exception as logging_error:
            logger.error(f"Failed to log batch status: {str(logging_error)}")
            logger.error(traceback.format_exc())

def analyze(spark: SparkSession):
    """Main analysis function for streaming CAN ML processing"""
    logger.info("Initializing CAN ML Streaming analysis")

    # Use context manager for PostgresHandler
    with PostgresHandler(Config.POSTGRES_URL, Config.POSTGRES_PROPERTIES) as ps_handler:
        try:
            # Initialize Elasticsearch handler
            es_handler = ElasticsearchHandler(
                Config.ES_HOST,
                Config.ES_PORT,
                Config.ES_SCHEME,
                Config.FIXED_NAMESPACE_UUID
            )

            # Initialize batch logger
            batch_logger = BatchLogger(es_handler, spark)
            batch_logger.create_logging_index_if_not_exists('can_ml')

            # Load lookup tables once at the start
            logger.info("Loading lookup tables...")
            
            # Load vehicle information
            vehicles_lookup_df = ps_handler.load_by_query(
                spark,
                """
                SELECT 
                    v.vin, 
                    vm.name AS vehicle_model, 
                    fc."Fleet Name" AS fleet_name
                FROM 
                    vehicle v
                LEFT JOIN 
                    vehicle_model vm ON v.vehicle_model_id = vm.id
                LEFT JOIN 
                    fleet_cars fc ON v.vin = fc."VIN Number"
                """,
                "vehicles_lookup_df",
                DataFrameStorageType.CACHED
            )

            # Load security events lookup
            security_events_lookup_df = ps_handler.load_by_query(
                spark,
                """
                SELECT
                    sev_id, 
                    rule_id, 
                    sev_name, 
                    description
                FROM 
                    security_event_lookup
                """,
                "security_events_lookup_df",
                DataFrameStorageType.CACHED
            )

            if vehicles_lookup_df is None or security_events_lookup_df is None:
                raise ValueError("Failed to load required lookup tables")

            logger.info("Successfully loaded all lookup tables")

            # Reset Elasticsearch indices if needed
            if es_handler.index_exists(Config.ELASTICSEARCH_CAN_ML_INDEX):
                client = es_handler._create_client()
                try:
                    client.indices.delete(index=Config.ELASTICSEARCH_CAN_ML_INDEX)
                    logger.info(f"Deleted existing index {Config.ELASTICSEARCH_CAN_ML_INDEX}")
                finally:
                    client.close()

            # Create Elasticsearch indices with proper mappings
            can_ml_mapping = {
                "mappings": {
                    "properties": {
                        "doc_id": {"type": "keyword"},
                        "timestamp_value": {"type": "double"},
                        "VinNumber": {"type": "keyword"},
                        "Arbitration_ID": {"type": "keyword"},
                        "Data": {"type": "keyword"},
                        "anomaly_score": {"type": "double"},
                        "processing_timestamp": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss"
                        }
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                }
            }
            
            es_handler.create_index_if_not_exists(
                Config.ELASTICSEARCH_CAN_ML_INDEX,
                can_ml_mapping
            )

            # Create predictor instance
            predictor = MLPredictor()

            # Create Kafka stream
            logger.info("Creating Kafka stream")
            kafka_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", Config.KAFKA_CAN_TOPIC) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load()

            # Process the stream
            query = kafka_df.writeStream \
                .foreachBatch(lambda df, epoch_id: process_streaming_batch(
                    spark, df, epoch_id, es_handler, batch_logger, predictor,
                    vehicles_lookup_df, security_events_lookup_df
                )) \
                .trigger(processingTime=Config.CAN_ML_PROCESSING_TIME) \
                .option("checkpointLocation", Config.S3_CHECKPOINT_LOCATION_CAN_ML_STREAM) \
                .start()

            logger.info("CAN ML Streaming analysis started successfully")
            query.awaitTermination()

        except Exception as e:
            logger.error(f"Fatal error in CAN ML Streaming analysis: {str(e)}")
            logger.error(traceback.format_exc())
            if 'query' in locals() and query.isActive:
                query.stop()
            raise

        finally:
            # Clean up cached DataFrames
            if 'ps_handler' in locals():
                ps_handler.cleanup_all()