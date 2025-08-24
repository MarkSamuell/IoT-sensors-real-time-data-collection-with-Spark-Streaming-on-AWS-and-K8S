from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel
import logging
from typing import Optional, Dict, Union
from pyspark.sql.functions import broadcast
from enum import Enum

logger = logging.getLogger(__name__)

class DataFrameStorageType(Enum):
    CACHED = "cached"
    BROADCASTED = "broadcasted"
    NONE = "none"

class PostgresHandler:
    def __init__(self, url: str, properties: dict):
        """
        Initialize PostgresHandler with connection details and storage management.
        
        Args:
            url (str): JDBC URL for PostgreSQL connection
            properties (dict): Connection properties including credentials
        """
        self.url = url
        self.properties = properties
        self._stored_dataframes: Dict[str, tuple[DataFrame, DataFrameStorageType]] = {}

    def load_table(self, spark: SparkSession, table_name: str, 
                  storage_type: DataFrameStorageType = DataFrameStorageType.NONE) -> Optional[DataFrame]:
        """
        Load a table from PostgreSQL with optional caching or broadcasting.
        
        Args:
            spark (SparkSession): Active Spark session
            table_name (str): Name of the table to load
            storage_type (DataFrameStorageType): How to store the DataFrame
        
        Returns:
            Optional[DataFrame]: The loaded DataFrame or None if loading failed
        """
        try:
            df = spark.read.jdbc(url=self.url, table=table_name, properties=self.properties)
            return self._process_storage(df, table_name, storage_type, spark)
        except Exception as e:
            logger.error(f"Error loading table {table_name} from PostgreSQL: {e}")
            return None

    def load_by_query(self, spark: SparkSession, query: str, df_name: str, 
                     storage_type: DataFrameStorageType = DataFrameStorageType.NONE) -> Optional[DataFrame]:
        """
        Load data using a custom query with optional caching or broadcasting.
        
        Args:
            spark (SparkSession): Active Spark session
            query (str): SQL query to execute
            df_name (str): Name to identify the DataFrame
            storage_type (DataFrameStorageType): How to store the DataFrame
        
        Returns:
            Optional[DataFrame]: The loaded DataFrame or None if loading failed
        """
        # Check if DataFrame is already stored
        if df_name in self._stored_dataframes:
            logger.info(f"Returning stored data for {df_name} ({self._stored_dataframes[df_name][1].value})")
            return self._stored_dataframes[df_name][0]
        
        try:
            df = spark.read.jdbc(url=self.url, table=f"({query}) as {df_name}", properties=self.properties)
            return self._process_storage(df, df_name, storage_type, spark)
        except Exception as e:
            logger.error(f"Error executing query and loading data into {df_name}: {e}")
            return None

    def _process_storage(self, df: DataFrame, name: str, 
                        storage_type: DataFrameStorageType, spark: SparkSession) -> DataFrame:
        """
        Process DataFrame storage based on the specified type.
        
        Args:
            df (DataFrame): DataFrame to process
            name (str): Identifier for the DataFrame
            storage_type (DataFrameStorageType): How to store the DataFrame
            spark (SparkSession): Active Spark session
        
        Returns:
            DataFrame: The processed DataFrame
        """
        if storage_type == DataFrameStorageType.CACHED:
            df.persist(StorageLevel.MEMORY_AND_DISK)
            df.count()  # Force materialization
            processed_df = df
            logger.info(f"Cached DataFrame: {name}")
        elif storage_type == DataFrameStorageType.BROADCASTED:
            collected_data = df.collect()
            broadcasted_df = spark.createDataFrame(collected_data, df.schema)
            processed_df = spark.sparkContext.broadcast(broadcasted_df)
        else:
            processed_df = df
            logger.info(f"Loaded DataFrame without caching/broadcasting: {name}")

        if storage_type != DataFrameStorageType.NONE:
            self._stored_dataframes[name] = (processed_df, storage_type)
            
        return processed_df

    def cleanup_dataframe(self, name: str) -> bool:
        """
        Clean up a specific stored DataFrame.
        
        Args:
            name (str): Identifier of the DataFrame to clean up
        
        Returns:
            bool: True if cleanup was successful, False otherwise
        """
        try:
            if name in self._stored_dataframes:
                df, storage_type = self._stored_dataframes[name]
                if storage_type == DataFrameStorageType.CACHED:
                    df.unpersist()
                elif storage_type == DataFrameStorageType.BROADCASTED:
                    df.unpersist()
                del self._stored_dataframes[name]
                logger.info(f"Cleaned up {storage_type.value} DataFrame: {name}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error cleaning up DataFrame {name}: {e}")
            return False

    def cleanup_all(self) -> None:
        """Clean up all stored DataFrames."""
        try:
            for name in list(self._stored_dataframes.keys()):
                self.cleanup_dataframe(name)
            logger.info("Cleaned up all stored DataFrames")
        except Exception as e:
            logger.error(f"Error during cleanup of all DataFrames: {e}")

    def get_storage_info(self) -> Dict[str, str]:
        """
        Get information about all stored DataFrames.
        
        Returns:
            Dict[str, str]: Dictionary with DataFrame names and their storage types
        """
        return {name: storage_type.value 
                for name, (_, storage_type) in self._stored_dataframes.items()}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup_all()