import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr
from typing import Optional

logger = logging.getLogger(__name__)

class SparkJobMonitor:
    def __init__(self, spark: SparkSession):
        """
        Initialize SparkMonitoring with a SparkSession
        
        Args:
            spark (SparkSession): Active Spark session
        """
        self.spark = spark
        self.sc = spark.sparkContext
        
    def mark_job(self, marker: str):
        """
        Set job group for tracking in Spark UI
        
        Args:
            marker (str): Name to identify the job group in Spark UI
        """
        self.sc.setJobGroup(marker, marker)
        
    def explain_plan(self, df: DataFrame, marker: str):
        """
        Log physical execution plan and DataFrame details for debugging
        
        Args:
            df (DataFrame): DataFrame to explain
            marker (str): Identifier for this explanation in logs
        """
        num_partitions = df.rdd.getNumPartitions()
        logger.info(f"\n{'='*150}")
        logger.info(f"Analysis for: {marker}")
        logger.info(f"Number of partitions: {num_partitions}")
        # logger.info(f"Schema:\n{df.schema}")
        # logger.info("\nExecution Plan:")
        # logger.info(df._jdf.queryExecution().toString())
        # logger.info(f"{'='*150}")
        
    def __enter__(self):
        """Context manager entry point"""
        return self
        
    def __exit__(self, exc_type: Optional[type], 
                 exc_val: Optional[Exception], 
                 exc_tb: Optional[type]):
        """
        Context manager exit point - clears job group
        
        Args:
            exc_type: Exception type if an error occurred
            exc_val: Exception value if an error occurred
            exc_tb: Exception traceback if an error occurred
        """
        self.sc.setJobGroup(None, None)