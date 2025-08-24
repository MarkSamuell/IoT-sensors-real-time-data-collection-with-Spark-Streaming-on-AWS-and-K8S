import boto3
from botocore.exceptions import ClientError
import json
import logging
from typing import Dict, Any
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, LongType, 
    FloatType, DoubleType, BooleanType, ArrayType, MapType, 
    DateType, TimestampType, BinaryType, ShortType, ByteType, DecimalType)
from pyspark.sql.functions import to_json, struct, date_format, col, lit, current_timestamp
from configs.config import Config

logger = logging.getLogger(__name__)

class GlueSchemaRegistryHandler:
    def __init__(self, registry_name, region_name):
        self.registry_name = registry_name
        self.region_name = region_name

    def get_latest_schema(self, schema_name):
        glue_client = boto3.client('glue', region_name=self.region_name)
        try:
            response = glue_client.get_schema_version(
                SchemaId={'RegistryName': self.registry_name, 'SchemaName': schema_name},
                SchemaVersionNumber={'LatestVersion': True}
            )
            schema_definition = response['SchemaDefinition']
            return json.loads(schema_definition)
        except ClientError as e:
            logger.error(f"Error retrieving schema from Glue Schema Registry: {e}")
            raise

def get_schema_from_registry(registry_name: str, region_name: str, schema_name: str) -> Dict[str, Any]:
    glue_handler = GlueSchemaRegistryHandler(registry_name, region_name)
    schema = glue_handler.get_latest_schema(schema_name)
    logger.info(f"Returned schema from Glue: {schema_name}")
    logger.info(f"Schema content from Glue: {json.dumps(schema, indent=2)}")
    return schema

def json_schema_to_spark_schema(json_schema):
    """
    Convert a JSON schema to a Spark StructType schema.
    
    Args:
    json_schema (dict): A JSON schema definition from Glue Schema Registry
    
    Returns:
    StructType: Equivalent Spark StructType schema
    """
    def _convert_field(field_name, field_schema):
        if "anyOf" in field_schema:
            types = [t.get("type") for t in field_schema["anyOf"]]
            if "string" in types and "number" in types:
                return StructField(field_name, StringType(), True)
        
        field_type = field_schema.get("type")
        if isinstance(field_type, list):
            non_null_types = [t for t in field_type if t != "null"]
            field_type = non_null_types[0] if non_null_types else "string"
        
        if field_type == "object":
            return StructField(field_name, _convert_object(field_schema), True)
        elif field_type == "array":
            item_schema = field_schema.get("items", {})
            return StructField(field_name, ArrayType(_convert_field("element", item_schema).dataType), True)
        elif field_type == "string":
            return StructField(field_name, StringType(), True)
        elif field_type in ["number", "integer"]:
            return StructField(field_name, LongType(), True)
        elif field_type == "boolean":
            return StructField(field_name, BooleanType(), True)
        else:
            return StructField(field_name, StringType(), True)

    def _convert_object(obj_schema):
        if "properties" not in obj_schema:
            logger.warning(f"No properties found in schema: {obj_schema}")
            return StructType([])
            
        fields = [
            _convert_field(name, schema)
            for name, schema in obj_schema["properties"].items()
        ]
        return StructType(fields)

    # Get the actual schema definition
    schema_def = json_schema.get("schema", json_schema)
    
    # Handle array type at root level
    if schema_def.get("type") == "array":
        # For array type, create ArrayType with the item schema
        item_schema = schema_def.get("items", {})
        if item_schema.get("type") == "object":
            return ArrayType(_convert_object(item_schema))
        else:
            return ArrayType(_convert_field("element", item_schema).dataType)
    else:
        # For object type or others, convert normally
        return _convert_object(schema_def)

def spark_type_to_json_type(spark_type):
    """
    Convert a Spark SQL type to a JSON schema type.
    
    Args:
    spark_type: A Spark SQL DataType
    
    Returns:
    dict: Equivalent JSON schema type definition
    """
    if isinstance(spark_type, StringType):
        return {"type": "string"}
    elif isinstance(spark_type, (IntegerType, LongType, ShortType, ByteType)):
        return {"type": "integer"}
    elif isinstance(spark_type, (FloatType, DoubleType)):
        return {"type": "number"}
    elif isinstance(spark_type, BooleanType):
        return {"type": "boolean"}
    elif isinstance(spark_type, BinaryType):
        return {"type": "string", "contentEncoding": "base64"}
    elif isinstance(spark_type, DateType):
        return {"type": "string", "format": "date"}
    elif isinstance(spark_type, TimestampType):
        return {"type": "string", "format": "date-time"}
    elif isinstance(spark_type, DecimalType):
        return {"type": "number"}
    elif isinstance(spark_type, ArrayType):
        return {
            "type": "array",
            "items": spark_type_to_json_type(spark_type.elementType)
        }
    elif isinstance(spark_type, MapType):
        return {
            "type": "object",
            "additionalProperties": spark_type_to_json_type(spark_type.valueType)
        }
    elif isinstance(spark_type, StructType):
        return {
            "type": "object",
            "properties": {field.name: spark_type_to_json_type(field.dataType) for field in spark_type.fields}
        }
    else:
        raise ValueError(f"Unsupported Spark type: {spark_type}")
    
def process_error_records(error_df, epoch_id, config: Config, job_type: str):
    """Process error records and write them to S3."""
    if error_df.count() > 0:
        try:
            # Standardize error DataFrame structure
            standardized_error_df = error_df.select(
                col("key"),
                col("error_timestamp"),
                col("error_type"),
                col("error_details"),
                col("event").alias("event_data")
            )

            final_error_df = standardized_error_df.withColumn(
                "date", 
                date_format(col("error_timestamp"), "yyyy-MM-dd")
            )

            s3_error_path = {
                'sevs': config.S3_SEVS_ERROR_RECORDS_PATH,
                'event_logs': config.S3_LOGS_ERROR_RECORDS_PATH,
                'sevs_aggs': config.S3_SEVS_AGGS_ERROR_RECORDS_PATH,
                'event_logs_aggs': config.S3_LOGS_AGGS_ERROR_RECORDS_PATH
            }.get(job_type)

            if not s3_error_path:
                raise ValueError(f"Unknown job type: {job_type}")
            
            final_error_df.write \
                .partitionBy("date", "error_type") \
                .mode("append") \
                .json(s3_error_path)
            
        except Exception as e:
            logger.error(f"Error writing error records to S3: {str(e)}")
            error_df.printSchema()
            error_df.show(2, truncate=False)

## we want to delete this
def create_error_record(df, error_type: str, error_details: str):
    """Create standardized error records."""
    try:
        return df.select(
            lit(error_type).alias("key"),
            to_json(struct(*df.columns)).alias("event"),
            current_timestamp().alias("error_timestamp"),
            lit(error_type).alias("error_type"),
            lit(error_details).alias("error_details")
        )
    except Exception as e:
        logger.error(f"Error creating error record: {str(e)}")
        return df.select(
            lit(error_type).alias("key"),
            lit("error_conversion_failed").alias("event"),
            current_timestamp().alias("error_timestamp"),
            lit(error_type).alias("error_type"),
            lit(str(e)).alias("error_details")
        )