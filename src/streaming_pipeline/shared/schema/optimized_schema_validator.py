import json
from typing import Dict, Any, Tuple, Union, List
import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, BooleanType, StringType
from functools import lru_cache
from jsonschema import Draft7Validator, validators
import fastjsonschema
import threading

logger = logging.getLogger(__name__)

class OptSchemaValidator:
    """
    Optimized Schema Validator that maintains JSON Schema validation with improved performance.
    Uses fastjsonschema for validation and implements caching strategies.
    """
    def __init__(self):
        self.schemas: Dict[str, Dict[str, Any]] = {}
        self.compiled_validators: Dict[str, Any] = {}
        self._lock = threading.Lock()
        
    def _create_optimized_validator(self, schema: Dict[str, Any]):
        """
        Create an optimized validator using fastjsonschema.
        Falls back to Draft7Validator if compilation fails.
        """
        try:
            # Try to compile with fastjsonschema for better performance
            return fastjsonschema.compile(schema)
        except Exception as e:
            logger.warning(f"Failed to compile with fastjsonschema: {e}. Falling back to Draft7Validator")
            return Draft7Validator(schema)

    def register_schema_in_validator(self, schema_name: str, schema: Dict[str, Any]) -> None:
        """
        Register and compile schema with thread-safe handling.
        """
        with self._lock:
            self.schemas[schema_name] = schema
            try:
                if 'schema' in schema:
                    actual_schema = schema['schema']
                else:
                    actual_schema = schema
                
                self.compiled_validators[schema_name] = self._create_optimized_validator(actual_schema)
                logger.info(f"Successfully registered and compiled schema: {schema_name}")
            except Exception as e:
                logger.error(f"Error compiling schema {schema_name}: {e}")
                raise

    @lru_cache(maxsize=1000)
    def _validate_cached_message(self, message_str: str, schema_name: str) -> Tuple[bool, str, str]:
        """
        Cached validation for identical messages.
        Note: Use with caution as it stores results in memory.
        """
        return self._validate_message_internal(message_str, schema_name)

    def _validate_message_internal(self, message_str: str, schema_name: str) -> Tuple[bool, str, str]:
        """
        Internal validation method using compiled validator.
        """
        validator = self.compiled_validators.get(schema_name)
        if not validator:
            return False, "SCHEMA_NOT_FOUND", f"Schema '{schema_name}' not registered"

        try:
            message_json = json.loads(message_str)
            
            try:
                # Fast validation with compiled schema
                if isinstance(validator, fastjsonschema.compiled.CompiledFastJsonschema):
                    validator(message_json)
                else:
                    errors = list(validator.iter_errors(message_json))
                    if errors:
                        return False, "SCHEMA_VALIDATION_ERROR", "; ".join(str(e) for e in errors)
                
                return True, "", ""
                
            except fastjsonschema.JsonSchemaException as e:
                return False, "SCHEMA_VALIDATION_ERROR", str(e)
            except Exception as e:
                return False, "VALIDATION_ERROR", str(e)
                
        except json.JSONDecodeError as e:
            return False, "JSON_DECODE_ERROR", str(e)
        except Exception as e:
            return False, "UNEXPECTED_ERROR", str(e)

    def validate_batch(self, messages: List[str], schema_name: str) -> List[Tuple[bool, str, str]]:
        """
        Validate multiple messages in a batch for better performance.
        """
        return [self._validate_message_internal(msg, schema_name) for msg in messages]

    def create_validate_message_udf(self):
        """
        Create a Spark UDF for validation with optimized performance.
        """
        def validate_message(raw_message: str, schema_name: str):
            try:
                # Use cached validation for frequent messages
                if len(raw_message) < 1000:  # Only cache smaller messages
                    result = self._validate_cached_message(raw_message, schema_name)
                else:
                    result = self._validate_message_internal(raw_message, schema_name)
                    
                return (bool(result[0]), str(result[1]), str(result[2]))
            except Exception as e:
                logger.error(f"Error in validate_message_udf: {str(e)}", exc_info=True)
                return (False, "UDF_ERROR", str(e))
        
        return udf(validate_message, StructType([
            StructField("is_valid", BooleanType(), False),
            StructField("error_type", StringType(), True),
            StructField("error_message", StringType(), True)
        ]))

class BatchValidationOptimizer:
    """
    Helper class for optimizing batch validation operations.
    """
    def __init__(self, validator: OptSchemaValidator):
        self.validator = validator
        self.batch_size = 1000
        
    def validate_dataframe_batch(self, df, schema_name: str, value_column: str = "value"):
        """
        Optimize validation of DataFrame batches.
        """
        # Collect values to validate in batches
        values = [row[value_column] for row in df.select(value_column).collect()]
        
        # Process in batches for better performance
        results = []
        for i in range(0, len(values), self.batch_size):
            batch = values[i:i + self.batch_size]
            results.extend(self.validator.validate_batch(batch, schema_name))
            
        return results