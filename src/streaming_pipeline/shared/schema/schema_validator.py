import json
from jsonschema import validate, ValidationError
from typing import Dict, Any, Tuple, Union, List
import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, BooleanType, StringType

logger = logging.getLogger(__name__)

class SchemaValidator:
    """
    The SchemaValidator class is responsible for validating JSON data against predefined schemas.
    It provides methods to register schemas, validate data, and create a UDF for use in Spark DataFrames.
    """

    def __init__(self):

        self.schemas: Dict[str, Dict[str, Any]] = {}

    def register_schema_in_validator(self, schema_name: str, schema: Dict[str, Any]) -> None:
        self.schemas[schema_name] = schema
        logger.info(f"Registered schema: {schema_name}")
        logger.info(f"Schema content: {json.dumps(schema, indent=2)}")

    """
    Validates a single item against the given schema.
    
    Parameters:
    - item (Dict[str, Any]): The individual item to validate
    - schema (Dict[str, Any]): The schema to validate against
    
    Returns:
    - Tuple[bool, str, str]: A tuple containing:
      - is_valid (bool): Whether the item is valid
      - error_type (str): The type of error if invalid, empty string if valid
      - error_message (str): Detailed error message if invalid, empty string if valid
    
    This method uses the jsonschema library's validate function to perform the actual validation.
    It catches ValidationError exceptions to handle validation failures.
    """
    def _validate_single_item(self, item: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, str, str]:
        logger.info(f"Validating single item: {json.dumps(item, indent=2)}")
        try:
            validate(instance=item, schema=schema)
            logger.info("Item is valid")
            return True, "", ""
        except ValidationError as e:
            logger.error(f"Validation error for item: {json.dumps(item, indent=2)}")
            logger.error(f"Validation error details: {str(e)}")
            return False, "SCHEMA_VALIDATION_ERROR", str(e)

    """
    Validates data against a given schema.
    
    Parameters:
    - data (Union[Dict[str, Any], List[Dict[str, Any]]]): The data to validate, either a single item or a list of items
    - schema (Dict[str, Any]): The schema to validate against
    
    Returns:
    - Tuple[bool, str, str]: A tuple containing:
      - is_valid (bool): Whether the data is valid
      - error_type (str): The type of error if invalid, empty string if valid
      - error_message (str): Detailed error message if invalid, empty string if valid
    
    This method handles both single items and lists of items. For lists, it validates each item
    and aggregates the results. It uses map() and filter() for efficient processing of list data.
    """
    def validate_data(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], schema: Dict[str, Any]) -> Tuple[bool, str, str]:
        logger.info(f"Validating data: {json.dumps(data, indent=2)}")
        if isinstance(data, list):
            results = list(map(lambda item: self._validate_single_item(item, schema), data))
            invalid_results = list(filter(lambda x: not x[0], results))
            if invalid_results:
                error_messages = "; ".join(map(lambda x: f"Item: {x[2]}", invalid_results))
                return False, "SCHEMA_VALIDATION_ERRORS", error_messages
            return True, "", ""
        else:
            return self._validate_single_item(data, schema)

    """
    Validates a raw message string against a named schema.
    
    Parameters:
    - raw_message (str): The raw JSON string to validate
    - schema_name (str): The name of the schema to validate against
    
    Returns:
    - Tuple[bool, str, str]: A tuple containing:
      - is_valid (bool): Whether the message is valid
      - error_type (str): The type of error if invalid, empty string if valid
      - error_message (str): Detailed error message if invalid, empty string if valid
    
    This method first attempts to parse the raw_message as JSON, then validates the parsed data
    against the specified schema. It handles JSON parsing errors and missing schemas.
    """
    def validate_raw_message(self, raw_message: str, schema_name: str) -> Tuple[bool, str, str]:
        logger.info(f"Validating message for schema '{schema_name}': {raw_message[:100]}...")
        schema = self.schemas.get(schema_name)
        if not schema:
            logger.error(f"Schema '{schema_name}' not found")
            return False, "SCHEMA_NOT_FOUND", f"Schema '{schema_name}' not registered"
        
        try:
            message_json = json.loads(raw_message)
            logger.info(f"Parsed message: {json.dumps(message_json, indent=2)}")
            # Handle both wrapped and direct schema formats
            schema = schema.get('schema', schema) if isinstance(schema, dict) and 'schema' in schema else schema
            return self.validate_data(message_json, schema)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
            logger.info(f"Raw message: {raw_message}")
            return False, "JSON_DECODE_ERROR", str(e)

    """
    Creates a User Defined Function (UDF) for use in Spark DataFrames to validate messages.
    
    Returns:
    - udf: A Spark UDF that takes a raw message string and schema name, and returns a struct with:
      - is_valid (bool): Whether the message is valid
      - error_type (str): The type of error if invalid, empty string if valid
      - error_message (str): Detailed error message if invalid, empty string if valid
    
    This method wraps the validate_raw_message method in a UDF, allowing it to be used
    directly in Spark DataFrame operations. It includes error handling to ensure the UDF
    doesn't fail even if an unexpected error occurs during validation.
    """
    def create_validate_message_udf(self):
        def validate_message(raw_message: str, schema_name: str):
            try:
                logger.info(f"Validating message: {raw_message[:100]}... for schema: {schema_name}")
                result = self.validate_raw_message(raw_message, schema_name)
                logger.info(f"Validation result: {result}")
                return (bool(result[0]), str(result[1]), str(result[2]))
            except Exception as e:
                logger.error(f"Error in validate_message_udf: {str(e)}", exc_info=True)
                return (False, "UDF_ERROR", str(e))
        
        return udf(validate_message, StructType([
            StructField("is_valid", BooleanType(), False),
            StructField("error_type", StringType(), True),
            StructField("error_message", StringType(), True)
        ]))