from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, udf, pandas_udf, PandasUDFType, monotonically_increasing_id, when, lit
import pandas as pd
import logging
import tensorflow as tf
import os
from datetime import datetime
import boto3
import tempfile
import numpy as np

from streaming_pipeline.jobs.ml_lstm_model.data_processing import load_and_process_data
from streaming_pipeline.jobs.ml_lstm_model.classifier import classify_can_frames

logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("CAN_ML_Batch_Job") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def create_model():
    """Create the base model"""
    input_layer = tf.keras.layers.Input(shape=(11, 1))
    x = tf.keras.layers.LSTM(64)(input_layer)
    x = tf.keras.layers.Dense(32, activation='relu')(x)
    output_layer = tf.keras.layers.Dense(1, activation='sigmoid')(x)
    model = tf.keras.Model(inputs=input_layer, outputs=output_layer)
    return model

def preprocess_dataframe(df):
    """Preprocess DataFrame to match the format expected by the existing model code"""
    df['Timestamp'] = pd.to_numeric(df['Timestamp'])
    
    def pad_data(data_str):
        try:
            if pd.isna(data_str) or data_str is None:
                # Return default padding for missing data
                return '00 00 00 00 00 00 00 00'
            
            # Clean and split the string
            parts = str(data_str).strip().split()
            
            # Validate hex values and pad
            clean_parts = []
            for part in parts[:8]:  # Take up to 8 parts
                try:
                    # Try to validate as hex
                    int(part, 16)
                    clean_parts.append(part)
                except ValueError:
                    clean_parts.append('00')
            
            # Pad to 8 bytes
            while len(clean_parts) < 8:
                clean_parts.append('00')
            
            return ' '.join(clean_parts[:8])  # Ensure exactly 8 parts
        except Exception as e:
            logger.warning(f"Error processing data value: {str(e)}")
            return '00 00 00 00 00 00 00 00'
    
    # Fill NA values and apply padding
    df['Data'] = df['Data'].fillna('00 00 00 00 00 00 00 00').apply(pad_data)
    return df[['Timestamp', 'Arbitration_ID', 'Data']]

def process_batch(spark, input_path, output_path):
    """Process CAN data batch using ML model"""
    try:
        # Define schema for input data
        schema = StructType([
            StructField("Timestamp", StringType(), True),
            StructField("Arbitration_ID", StringType(), True),
            StructField("DLC", StringType(), True),
            StructField("Data", StringType(), True),
            StructField("Class", StringType(), True),
            StructField("SubClass", StringType(), True)
        ])

        # Read input data
        logger.info(f"Reading input data from {input_path}")
        raw_df = spark.read.csv(input_path, header=True, schema=schema)
        
        # Clean data at Spark level before passing to UDF
        input_df = raw_df.select(
            col("Timestamp"),
            col("Arbitration_ID"),
            when(col("Data").isNull() | (col("Data") == ""), "00 00 00 00 00 00 00 00")
            .otherwise(col("Data")).alias("Data")
        )
        
        # Convert to pandas for ML processing
        @pandas_udf("double", PandasUDFType.SCALAR)
        def predict_pandas_udf(timestamps, arb_ids, data):
            try:
                # Set up model path
                model_path = os.path.join(tempfile.gettempdir(), "LSTM_model.h5")
                
                # Download model if not exists
                if not os.path.exists(model_path):
                    s3_client = boto3.client('s3')
                    s3_client.download_file(
                        'elasticmapreduce-uraeusdev',
                        'spark_build/LSTM_model.h5',
                        model_path
                    )
                    logger.info(f"Downloaded model to {model_path}")

                # Create and load model
                if not hasattr(predict_pandas_udf, 'model'):
                    predict_pandas_udf.model = create_model()
                    logger.info("Model created successfully")

                # Create DataFrame and preprocess
                pdf = pd.DataFrame({
                    'Timestamp': timestamps,
                    'Arbitration_ID': arb_ids,
                    'Data': data
                })
                
                # Preprocess to match expected format
                preprocessed_df = preprocess_dataframe(pdf)
                processed_data = load_and_process_data(df=preprocessed_df)
                
                # Reshape and predict
                reshaped_data = processed_data.values.reshape(-1, 11, 1)
                predictions = predict_pandas_udf.model.predict(reshaped_data, verbose=0)
                return pd.Series(predictions.flatten())
                
            except Exception as e:
                logger.error(f"Error in prediction UDF: {str(e)}", exc_info=True)
                # Return default score for errors
                return pd.Series([0.5] * len(timestamps))

        # Add predictions
        result_df = input_df \
            .withColumn(
                "score",
                predict_pandas_udf(
                    col("Timestamp"),
                    col("Arbitration_ID"),
                    col("Data")
                )
            )

        # Write results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path_with_timestamp = f"{output_path}/predictions_{timestamp}"
        
        logger.info(f"Writing results to {output_path_with_timestamp}")
        
        # Write only Timestamp, Arbitration_ID, and score
        (result_df.select(
            "Timestamp", 
            "Arbitration_ID", 
            "score"
        )
        .coalesce(1)  # Write to single file
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path_with_timestamp))

        logger.info("Batch processing completed successfully")

    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}", exc_info=True)
        raise

def analyze(spark: SparkSession):
    """Main analysis function for CAN ML batch processing"""
    try:
        # S3 paths
        input_path = "s3://elasticmapreduce-uraeusdev/can_data/Fin_host_session_submit_S.csv"
        output_path = "s3://elasticmapreduce-uraeusdev/can_data/predictions"

        # Process the batch
        process_batch(spark, input_path, output_path)

    except Exception as e:
        logger.error(f"Error in CAN ML batch analysis: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    spark = create_spark_session()
    try:
        analyze(spark)
    finally:
        spark.stop()