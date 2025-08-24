import uuid
import os
import logging
from functools import lru_cache
from datetime import datetime

logger = logging.getLogger(__name__)

# Get environment before class definition to avoid method call issues
ENVIRONMENT = os.environ.get('ENVIRONMENT')

class Config:
    # Static class variables
    region_name = "eu-central-1"
    _ssm_client = None

    # EMR configs
    NUM_EXECUTORS = 2
    EXECUTOR_CORES = 1
    OPTIMAL_PARTITIONS = NUM_EXECUTORS * EXECUTOR_CORES * 3

    # Get current date for checkpoint folders
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # Environment setting
    ENVIRONMENT = ENVIRONMENT
    
    # Checkpoint base path
    checkpoint_base = f's3a://elasticmapreduce-uraeusdev/{ENVIRONMENT}_checkpoint'
    
    # S3 CHECKPOINT_LOCATION configuration with date folders
    S3_CHECKPOINT_LOCATION_SEVS = f'{checkpoint_base}/sevs/'
    S3_CHECKPOINT_LOCATION_LOGS = f'{checkpoint_base}/logs/'
    S3_CHECKPOINT_LOCATION_LOGS_LATE_AGGS = f'{checkpoint_base}/late_logs_aggs/'
    S3_CHECKPOINT_LOCATION_BATCH_LATE_SEVS = f'{checkpoint_base}/batch_late_sevs/'
    S3_CHECKPOINT_LOCATION_BATCH_LATE_LOGS = f'{checkpoint_base}/batch_late_logs/'
    S3_CHECKPOINT_LOCATION_CAN_ML_BATCH = f'{checkpoint_base}/can_ml_batch/'
    S3_CHECKPOINT_LOCATION_CAN_ML_STREAM = f'{checkpoint_base}/can_ml_stream/'
    S3_CHECKPOINT_LOCATION_API_LOGS = f'{checkpoint_base}/api_logs/'
    
    # Elasticsearch configuration
    # Get Elasticsearch host from available environment variables
    ES_HOST = os.environ.get('URAEUS_ELASTIC_SEARCH_ES_HTTP_SERVICE_HOST')
    ES_PORT = int(os.environ.get('URAEUS_ELASTIC_SEARCH_ES_HTTP_SERVICE_PORT', '9200'))

    # Log the raw values from environment variable with WARNING level
    logger.warning(f"DEBUGGING - ES_HOST from environment: {ES_HOST!r}")
    logger.warning(f"DEBUGGING - ES_PORT from environment: {ES_PORT!r}")

    # Determine scheme based on hostname pattern
    if ES_HOST and (".local" in ES_HOST or ES_HOST.startswith("172") or ES_HOST == "localhost"):
        ES_SCHEME = "http"
        logger.warning(f"DEBUGGING - Using HTTP scheme for internal endpoint: {ES_HOST!r}")
    else:
        ES_SCHEME = "https"
        logger.warning(f"DEBUGGING - Using HTTPS scheme for external endpoint: {ES_HOST!r}")

    ES_USERNAME = os.environ.get('ELASTIC_USERNAME')
    ES_PASSWORD = os.environ.get('ELASTIC_PASSWORD')

    # Also log final configuration
    logger.warning(f"DEBUGGING - Final Elasticsearch config: Host={ES_HOST}, Port={ES_PORT}, Scheme={ES_SCHEME}")
    logger.warning(f"DEBUGGING - Elasticsearch auth: Username={'Set' if ES_USERNAME else 'Not set'}, Password={'Set' if ES_PASSWORD else 'Not set'}")
        
    # Kafka configuration - read from env vars
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BROKERS')
    KAFKA_SEVS_TOPIC = 'security-events-topic'
    KAFKA_LOGS_TOPIC = 'logs-topic'
    KAFKA_API_LOGS_TOPIC = 'api-logs-topic'
   
    # Elasticsearch index names
    ELASTICSEARCH_SECURITY_EVENTS_INDEX = 'ui_security_events'
    ELASTICSEARCH_SEVS_LOGS_INDEX = 'ui_sevs_logs'
    ELASTICSEARCH_EVENT_LOGS_INDEX = 'ui_event_logs'
    ELASTICSEARCH_SECURITY_EVENTS_AGGS_INDEX = 'ui_security_events_aggs'
    ELASTICSEARCH_EVENT_LOGS_AGGS_INDEX = 'ui_event_logs_aggs'
    
    # API Elasticsearch index names
    ELASTICSEARCH_API_LOGS_INDEX = 'api_logs'
    ELASTICSEARCH_API_SEVS_INDEX = 'api_sevs'
    ELASTICSEARCH_API_LOGS_AGGS_INDEX = 'api_logs_aggs'
    ELASTICSEARCH_API_SEVS_AGGS_INDEX = 'api_sevs_aggs'
   
    # S3 ERROR_RECORDS configuration
    S3_SEVS_ERROR_RECORDS_PATH = 's3a://elasticmapreduce-uraeusdev/error-records/sevs'
    S3_LOGS_ERROR_RECORDS_PATH = 's3a://elasticmapreduce-uraeusdev/error-records/logs'
    S3_SEVS_AGGS_ERROR_RECORDS_PATH = 's3a://elasticmapreduce-uraeusdev/error-records/sevs_aggs'
    S3_LOGS_AGGS_ERROR_RECORDS_PATH = 's3a://elasticmapreduce-uraeusdev/error-records/logs_aggs'
    
    # API S3 ERROR_RECORDS configuration
    S3_API_LOGS_ERROR_RECORDS_PATH = 's3a://elasticmapreduce-uraeusdev/error-records/api_logs'
    S3_API_SEVS_ERROR_RECORDS_PATH = 's3a://elasticmapreduce-uraeusdev/error-records/api_sevs'
    S3_API_LOGS_AGGS_ERROR_RECORDS_PATH = 's3a://elasticmapreduce-uraeusdev/error-records/api_logs_aggs'
    S3_API_SEVS_AGGS_ERROR_RECORDS_PATH = 's3a://elasticmapreduce-uraeusdev/error-records/api_sevs_aggs'
 
    # Processing configuration
    PROCESSING_TIME = '0 seconds'
    ACK_INTERVAL = 0

    # Fixed UUID for SEVsAnalyzer
    FIXED_NAMESPACE_UUID = uuid.UUID('32f51344-0933-43d2-b923-304b8d8f7896')

    # Date format for UI
    UI_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

    # Date format for logging
    LOGGING_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

    # Scheme
    GLUE_SCHEMA_REGISTRY_NAME = 'default_registry'
    GLUE_SCHEMA_REGISTRY_REGION = 'eu-central-1'

    # Add late events configuration
    LATE_EVENTS_WATERMARK_DURATION = "24 hour"
    LATE_EVENTS_PROCESSING_WINDOW = "24 hour"
    LATE_EVENTS_MAX_RETRIES = 3
    LATE_EVENTS_BATCH_SIZE = 1000

    # Iceberg configuration
    ICEBERG_CATALOG_NAME = "demo"
    ICEBERG_WAREHOUSE = "s3://elasticmapreduce-uraeusdev/ICEBERG/"
   
    # Database and table names
    ICEBERG_DATABASE = "uraeus_db"
    ICEBERG_EVENT_LOGS_TABLE = "event_logs"
    ICEBERG_SECURITY_EVENTS_TABLE = "security_events"
    ICEBERG_EVENT_LOGS_AGGS_TABLE = "event_logs_aggs"
    ICEBERG_SECURITY_EVENTS_AGGS_TABLE = "security_events_aggs"

    # Database configuration from environment variables
    POSTGRES_URL = f"jdbc:postgresql://{os.environ.get('DATABASE_HOST')}:{os.environ.get('DATABASE_PORT')}/{os.environ.get('DATABASE')}"
    POSTGRES_PROPERTIES = {
        "user": os.environ.get('DATABASE_USER'),
        "password": os.environ.get('DATABASE_PASSWORD'),
        "driver": "org.postgresql.Driver"
    }


    NRC_KEYWORDS = ["NRC", "negative response"]
    DTC_KEYWORDS = ["DTC", "diagnostic trouble code"]


    # SQL queries for lookup tables
    VEHICLES_LOOKUP_QUERY = """
    SELECT 
        v.vin,
        vm.name as vehicle_model,
        vm.region as region,
        f.name as fleet_name,
        f.id as fleet_id
    FROM vehicle v
    LEFT JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
    LEFT JOIN fleet_vehicles_vehicle fvv ON fvv.vehicle_id = v.id
    LEFT JOIN fleet f ON f.id = fvv.fleet_id
    WHERE v.is_deleted = false 
    AND (vm.is_deleted IS NULL OR vm.is_deleted = false)
    AND (f.is_deleted IS NULL OR f.is_deleted = false)
    """
    
    SECURITY_EVENTS_LOOKUP_QUERY = """
    SELECT
        business_id as sev_id, 
        business_id as rule_id, 
        name as sev_name, 
        description
    FROM 
        security_event
    WHERE is_deleted = false
    """
    
    API_SECURITY_EVENTS_LOOKUP_QUERY = """
    SELECT
        sev_id, 
        sev_name, 
        description,
        severity
    FROM 
        api_security_event_lookup
    """
    
    SIGNALS_LOOKUP_QUERY = """
    SELECT
        business_id as signal_id,
        name as signal_name,
        unit as signal_unit,
        CASE 
            WHEN min::text = 'NaN' THEN NULL 
            ELSE min 
        END as min_value,
        CASE 
            WHEN max::text = 'NaN' THEN NULL 
            ELSE max 
        END as max_value,
        signal_fully_qualified_name
    FROM signal
    """

    # Log configuration info
    @classmethod
    def log_config(cls):
        logger.info(f"Running in {cls.ENVIRONMENT} environment")
        logger.info(f"Final Elasticsearch config: Host={cls.ES_HOST}, Port={cls.ES_PORT}, Scheme={cls.ES_SCHEME}")
        logger.info(f"Elasticsearch auth: Username={'Yes' if cls.ES_USERNAME else 'No'}")
        logger.info(f"Using Kafka: {cls.KAFKA_BOOTSTRAP_SERVERS or 'Not configured'}")
        logger.info(f"Using PostgreSQL: {cls.POSTGRES_URL}")
        logger.info(f"Using checkpoint base: {cls.checkpoint_base}")

# Log config information
Config.log_config()