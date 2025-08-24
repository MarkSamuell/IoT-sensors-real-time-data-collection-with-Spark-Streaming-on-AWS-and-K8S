# %%configure -f 
# {
#     "conf": {
#         "spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
#         "spark.sql.catalog.demo.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
#         "spark.sql.catalog.demo.warehouse": "s3://elasticmapreduce-uraeusdev/ICEBERG/",
#         "spark.sql.extensions":"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
#     }
# }

def create_tables():
    """Create tables in Glue catalog using demo namespace"""
    try:
        # Create database if not exists
        print("\nCreating database...")
        spark.sql("CREATE DATABASE IF NOT EXISTS demo.uraeus_db")

        # Create event_logs table
        print("\nCreating event_logs table...")
        spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.uraeus_db.event_logs (
            Log_ID string,
            Timestamp timestamp,
            Signal_Name string,
            Signal_Value string,
            Signal_Unit string,
            VIN string,
            Vehicle_Model string,
            Fleet_Name string,
            Campaign_ID string,
            Component string,
            Datatype string,
            Min double,
            Max double,
            Signal_Fully_Qualified_Name string,
            year int,
            month int, 
            day int,
            _inserted_timestamp timestamp
        )
        USING iceberg
        PARTITIONED BY (year, month, day)
        LOCATION 's3://elasticmapreduce-uraeusdev/ICEBERG/uraeus_db/event_logs'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '536870912',
            'format-version' = '2'
        )
        """)
        
        # Create security_events table
        print("\nCreating security_events table...")
        spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.uraeus_db.security_events (
            Alert_ID string,
            ID string,
            Timestamp timestamp,
            VIN string,
            Severity string,
            SEV_Msg string,
            Origin string,
            NetworkType string,
            NetworkID string,
            Vehicle_Model string,
            Fleet_Name string,
            SEV_Name string,
            Rule_ID string,
            Description string,
            SEV_Status string,
            year int,
            month int,
            day int,
            _inserted_timestamp timestamp
        )
        USING iceberg
        PARTITIONED BY (year, month, day, Severity)
        LOCATION 's3://elasticmapreduce-uraeusdev/ICEBERG/uraeus_db/security_events'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '536870912',
            'format-version' = '2'
        )
        """)

        # Create event logs aggregations table
        spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.uraeus_db.event_logs_aggs (
            start timestamp,
            end timestamp,
            VIN string,
            Signal_ID string,
            Signal_Name string,
            count long,
            Fleet_Name string,
            year int,
            month int,
            day int,
            _inserted_timestamp timestamp
        )
        USING iceberg
        PARTITIONED BY (year, month, day)
        LOCATION 's3://elasticmapreduce-uraeusdev/ICEBERG/uraeus_db/event_logs_aggs'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'format-version' = '2'
        )
        """)

        # Create security events aggregations table
        spark.sql("""
        CREATE TABLE IF NOT EXISTS demo.uraeus_db.security_events_aggs (
            start timestamp,
            end timestamp,
            ID string,
            SEV_Name string,
            VIN string,
            Severity string,
            Fleet string,
            count long,
            year int,
            month int,
            day int,
            _inserted_timestamp timestamp
        )
        USING iceberg
        PARTITIONED BY (year, month, day, Severity)
        LOCATION 's3://elasticmapreduce-uraeusdev/ICEBERG/uraeus_db/security_events_aggs'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'format-version' = '2'
        )
        """)

        # Verify creation
        print("\nVerifying tables:")
        spark.sql("SHOW TABLES IN demo.uraeus_db").show()
        
        print("\nVerifying table locations:")
        spark.sql("DESCRIBE TABLE EXTENDED demo.uraeus_db.event_logs").show(truncate=False)
        spark.sql("DESCRIBE TABLE EXTENDED demo.uraeus_db.security_events").show(truncate=False)

    except Exception as e:
        print(f"Error creating tables: {str(e)}")
        raise

# Run the creation script
try:
    create_tables()
    print("\nTables created successfully!")
except Exception as e:
    print(f"\nError during table creation: {str(e)}")
    raise