from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, List, Any, Union
import logging
from datetime import datetime
from configs.config import Config
from pyspark.sql.functions import current_timestamp, lit

logger = logging.getLogger(__name__)

class IcebergHandler:
    """
    Handler for Iceberg table operations in the data pipeline.
    Provides functionality for managing and writing to Iceberg tables.
    """

    def __init__(self, warehouse_path: str, catalog_name: str = "spark_catalog"):
        """
        Initialize the Iceberg handler.

        Args:
            warehouse_path: Base path for Iceberg tables (e.g., 's3://my-bucket/warehouse')
            catalog_name: Name of the Iceberg catalog to use
        """
        self.warehouse_path = Config.ICEBERG_WAREHOUSE
        self.catalog_name = Config.ICEBERG_CATALOG_NAME

    def get_iceberg_configs(self, spark: SparkSession) -> SparkSession:
        """
        Configure Spark session with required Iceberg settings for AWS.

        Args:
            spark: Existing SparkSession

        Returns:
            SparkSession: Configured SparkSession with AWS Glue catalog settings
        """
        return spark.conf.set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ).conf.set(
            f"spark.sql.catalog.{self.catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog"
        ).conf.set(
            f"spark.sql.catalog.{self.catalog_name}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog"
        ).conf.set(
            f"spark.sql.catalog.{self.catalog_name}.warehouse",
            self.warehouse_path
        ).conf.set(
            f"spark.sql.catalog.{self.catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )

    def create_table(
        self, 
        spark: SparkSession, 
        table_name: str, 
        schema: Dict[str, Any],
        partition_by: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Create a new Iceberg table if it doesn't exist.

        Args:
            spark: SparkSession
            table_name: Full table name (e.g., 'db.table')
            schema: Dictionary defining the table schema
            partition_by: List of partition columns
            properties: Additional table properties

        Returns:
            bool: True if table was created, False if it already existed
        """
        try:
            full_table_name = f"{self.catalog_name}.{table_name}"
            
            # Check if table exists
            if spark.catalog.tableExists(full_table_name):
                logger.info(f"Table {full_table_name} already exists")
                return False

            # Build CREATE TABLE statement
            create_stmt = f"CREATE TABLE {full_table_name} "
            
            # Add schema
            schema_str = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
            create_stmt += f"({schema_str}) "
            
            # Add partitioning
            if partition_by:
                partition_cols = ", ".join(partition_by)
                create_stmt += f"PARTITIONED BY ({partition_cols}) "
            
            # Add properties
            if properties:
                props_str = ", ".join([f"'{k}' = '{v}'" for k, v in properties.items()])
                create_stmt += f"TBLPROPERTIES ({props_str})"
            
            # Execute creation
            spark.sql(create_stmt)
            logger.info(f"Successfully created table {full_table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating Iceberg table {table_name}: {str(e)}")
            raise

    def insert_dataframe(
        self, 
        df: DataFrame, 
        table_name: str,
        mode: str = "merge",
        id_column: Optional[str] = None
    ) -> None:
        """
        Insert/upsert data into Iceberg table.
        
        Args:
            df: DataFrame to insert
            table_name: Target table
            mode: Write mode ('merge' or 'append')
            id_column: Column for merge key (e.g. 'Log_ID', 'Alert_ID')
        """
        try:
            full_table_name = f"{self.catalog_name}.{table_name}"
            df_with_meta = df.withColumn("_inserted_timestamp", current_timestamp())

            if mode == "merge" and id_column:
                df_with_meta.createOrReplaceTempView("source_data")
                
                merge_sql = f"""
                MERGE INTO {full_table_name} t
                USING source_data s
                ON t.{id_column} = s.{id_column}
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """
                df_with_meta.sparkSession.sql(merge_sql)
            else:
                df_with_meta.write \
                    .format("iceberg") \
                    .mode("append") \
                    .saveAsTable(full_table_name)

            logger.info(f"Inserted {df.count()} records into {full_table_name}")
                
        except Exception as e:
            logger.error(f"Error inserting into {table_name}: {str(e)}")
            raise

    def insert_aggregations(
        self,
        df: DataFrame, 
        table_name: str,
        groupby_columns: List[str],
        agg_column: str = "count"
    ) -> None:
        """
        Insert aggregations with increment logic.
        
        Args:
            df: DataFrame with aggregated data  
            table_name: Target table
            groupby_columns: Columns to group by
            agg_column: Column to aggregate
        """
        try:
            full_table_name = f"{self.catalog_name}.{table_name}"
            df_with_meta = df.withColumn("_inserted_timestamp", current_timestamp())
            
            df_with_meta.createOrReplaceTempView("source_aggs") 
            
            merge_sql = f"""
            MERGE INTO {full_table_name} t
            USING source_aggs s 
            ON {' AND '.join(f't.{col} = s.{col}' for col in groupby_columns)}
            WHEN MATCHED THEN
                UPDATE SET {agg_column} = t.{agg_column} + s.{agg_column},
                        _inserted_timestamp = s._inserted_timestamp
            WHEN NOT MATCHED THEN
                INSERT *
            """
            df_with_meta.sparkSession.sql(merge_sql)
                
            logger.info(f"Merged aggregations into {full_table_name}")
                
        except Exception as e:
            logger.error(f"Error merging aggregations into {table_name}: {str(e)}")
            raise

    def optimize_table(
        self,
        spark: SparkSession,
        table_name: str,
        min_files_per_partition: int = 1,
        target_file_size_bytes: int = 536870912  # 512MB
    ) -> None:
        """
        Optimize table by compacting small files.

        Args:
            spark: SparkSession
            table_name: Table to optimize
            min_files_per_partition: Minimum number of files per partition
            target_file_size_bytes: Target file size for compaction
        """
        try:
            full_table_name = f"{self.catalog_name}.{table_name}"
            
            spark.sql(f"""
            CALL {self.catalog_name}.system.rewrite_data_files(
                table => '{table_name}',
                options => map(
                    'min-input-files', '{min_files_per_partition}',
                    'target-file-size-bytes', '{target_file_size_bytes}'
                )
            )
            """)
            
            logger.info(f"Successfully optimized table {full_table_name}")
            
        except Exception as e:
            logger.error(f"Error optimizing Iceberg table {table_name}: {str(e)}")
            raise

    def expire_snapshots(
        self,
        spark: SparkSession,
        table_name: str,
        retention_hours: int = 168  # 7 days
    ) -> None:
        """
        Expire old snapshots to clean up storage.

        Args:
            spark: SparkSession
            table_name: Table to clean up
            retention_hours: Hours to retain snapshots
        """
        try:
            full_table_name = f"{self.catalog_name}.{table_name}"
            
            spark.sql(f"""
            CALL {self.catalog_name}.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}' - INTERVAL {retention_hours} HOURS,
                retain_last => 1
            )
            """)
            
            logger.info(f"Successfully expired snapshots for {full_table_name}")
            
        except Exception as e:
            logger.error(f"Error expiring snapshots for table {table_name}: {str(e)}")
            raise

    def add_partitions(
        self,
        spark: SparkSession,
        table_name: str,
        partition_fields: List[Dict[str, str]]
    ) -> None:
        """
        Add new partition fields to an existing table.

        Args:
            spark: SparkSession
            table_name: Table to modify
            partition_fields: List of partition field definitions
        """
        try:
            full_table_name = f"{self.catalog_name}.{table_name}"
            
            for field in partition_fields:
                transform = field.get('transform', 'identity')
                name = field['name']
                source = field.get('source_column', name)
                
                spark.sql(f"""
                ALTER TABLE {full_table_name}
                ADD PARTITION FIELD {transform}({source}) AS {name}
                """)
            
            logger.info(f"Successfully added partition fields to {full_table_name}")
            
        except Exception as e:
            logger.error(f"Error adding partition fields to table {table_name}: {str(e)}")
            raise

    def set_table_properties(
        self,
        spark: SparkSession,
        table_name: str,
        properties: Dict[str, str]
    ) -> None:
        """
        Set or update table properties.

        Args:
            spark: SparkSession
            table_name: Table to modify
            properties: Properties to set
        """
        try:
            full_table_name = f"{self.catalog_name}.{table_name}"
            
            props_str = ", ".join([f"'{k}' = '{v}'" for k, v in properties.items()])
            spark.sql(f"""
            ALTER TABLE {full_table_name} 
            SET TBLPROPERTIES ({props_str})
            """)
            
            logger.info(f"Successfully set properties for {full_table_name}")
            
        except Exception as e:
            logger.error(f"Error setting properties for table {table_name}: {str(e)}")
            raise