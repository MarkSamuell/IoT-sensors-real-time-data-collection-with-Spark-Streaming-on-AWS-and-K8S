import os
import sys
from elasticsearch import Elasticsearch
import logging

# Add the project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))  # dal directory
shared_dir = os.path.dirname(current_dir)  # shared directory
streaming_dir = os.path.dirname(shared_dir)  # streaming_pipeline directory
src_dir = os.path.dirname(streaming_dir)  # src directory
root_dir = os.path.dirname(src_dir)  # project root
sys.path.insert(0, root_dir)

from configs.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ES_HOST = "localhost"  # Config.ES_HOST
ES_PORT = 9200  # Config.ES_PORT
ES_SCHEME = 'http'

# Safely get credentials, defaulting to None
try:
    ES_USERNAME = 'elastic'
    ES_PASSWORD = 'Tc3o1a1yP6G9d0d4DOm815Im'
except AttributeError:
    ES_USERNAME = None
    ES_PASSWORD = None

custom_analyzer = {
    "analysis": {
        "normalizer": {
            "lowercase_normalizer": {
                "type": "custom",
                "char_filter": [],
                "filter": ["lowercase"]
            }
        }
    }
}

def case_insensitive_field(field_type):
    if isinstance(field_type, dict):
        # Handle nested objects and other complex field types
        if field_type.get('type') == 'date':
            return field_type
        elif field_type.get('type') == 'nested':
            # For nested types, preserve the structure and process properties recursively
            nested_field = field_type.copy()
            if 'properties' in nested_field:
                nested_field['properties'] = {
                    key: case_insensitive_field(value) 
                    for key, value in nested_field['properties'].items()
                }
            return nested_field
        elif 'type' in field_type:
            # Handle other dict-based field definitions
            return field_type
        else:
            # Unknown dict structure, return as-is
            return field_type
    elif field_type == "keyword":
        return {
            "type": "keyword",
            "normalizer": "lowercase_normalizer"
        }
    elif field_type == "text":
        return {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "normalizer": "lowercase_normalizer"
                }
            }
        }
    else:
        return {"type": field_type}

def create_mapping(properties, id_field):
    return {
        "settings": {
            **custom_analyzer,
            "number_of_shards": 2,
            "number_of_replicas": 0,
            "refresh_interval": "30s",
            "index": {
                "write.wait_for_active_shards": 1,
                "translog.durability": "async",
                "translog.sync_interval": "30s",
                "translog.flush_threshold_size": "256mb",
                "merge.scheduler.max_thread_count": "8",
                "merge.policy.floor_segment": "2mb",
                "merge.policy.max_merge_at_once": "10",
                "merge.policy.segments_per_tier": "4",
                "routing.allocation.total_shards_per_node": 4
            }
        },
        "mappings": {
            "properties": {key: case_insensitive_field(value) for key, value in properties.items()},
            "_meta": {
                "id_field": id_field
            }
        }
    }

# Properties definitions remain unchanged
security_events_properties = {
    "message_id": "keyword",
    "Alert_ID": "keyword",
    "ID": "keyword",
    "VinNumber": "keyword",
    "Vehicle_Model": "text",
    "Fleet": "text",
    "SEV_Name": "text",
    "Rule_ID": "keyword",
    "SEV_Msg": "text",
    "SEV_Desc": "text",
    "SEV_Status": "text",
    "Severity": "text",
    "NetworkID": "keyword",
    "NetworkType": "text",
    "Origin": "text",
    "Timestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"}
}

sevs_logs_properties = {
    "message_id": "keyword",
    "Log_ID": "keyword",
    "Alert_ID": "keyword",
    "ID": "keyword",
    "VinNumber": "keyword",
    "ParameterName": "text",
    "ParameterTimestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
    "ParameterType": "keyword",
    "ParameterUnit": "keyword",
    "ParameterValue": "keyword",
    "Timestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"}    
}

security_events_aggs_properties = {
    "message_id": "keyword",
    "es_id": "keyword",
    "ID": "keyword",
    "SEV_Name": "text",
    "Severity": "text",
    "VIN": "keyword",
    "Fleet": "text",
    "count": "long",
    "window_end": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
    "window_start": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"}
}

event_logs_aggs_properties = {
    "message_id": "keyword",
    "es_id": "keyword",
    "Signal_Name": "text",
    "VIN": "keyword",
    "count": "long",
    "window_end": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
    "window_start": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"}
}

event_logs_properties = {
    "message_id": "keyword",
    "Log_ID": "keyword",
    "Campaign_Id": "keyword",
    "VIN": "keyword",
    "Vehicle_Model": "text",
    "Fleet_Name": "text",
    "Component": "text",
    "Datatype": "keyword",
    "Signal_Name": "text",
    "Signal_Unit": "keyword",
    "Signal_Value": "keyword",
    "Max": "float",
    "Min": "float",
    "Signal_Fully_Qualified_Name": "text",
    "Timestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"}
}

# API Logs properties - Fixed mapping with proper nested structure
api_logs_properties = {
    "request_id": "long",
    "message_type": "keyword",      # Added for request/response separation
    "request": {
        "type": "object",
        "properties": {
            "source_ip": {"type": "keyword"},
            "method": {"type": "keyword"},
            "service_port": {"type": "integer"},
            "agent_id": {"type": "keyword"},
            "path": {"type": "keyword"},
            "campaign_id": {"type": "keyword"},
            "headers": {"type": "text"},      # Headers as text (JSON string)
            "body": {"type": "text"},         # Body as text (JSON string)
            "query_params": {"type": "text"}  # Query params as text (JSON string)
        }
    },
    "response": {
        "type": "object",
        "properties": {
            "status_code": {"type": "integer"},
            "headers": {"type": "text"}       # Headers as text (JSON string)
        }
    },
    "timestamp": {"type": "date"},
    "event_time": {"type": "date"}, # Added event_time field
    "ingestion_time": {"type": "date"},
    "has_response": {"type": "boolean"},
    "Log_ID": {"type": "keyword"},
}

# API SEVs properties
api_sevs_properties = {
    "timestamp": {"type": "date"},
    "sev_id": "keyword",
    "sev_name": "keyword",
    "description": "text",
    "severity": "keyword",
    "source_ip": "keyword",
    "type": "keyword",
    "Alert_ID": "keyword",
    "iocs": {
        "type": "nested",
        "properties": {
            "request_id": {"type": "long"},
            "timestamp": {"type": "date"},
            "path": {"type": "keyword"},
            "response_code": {"type": "integer"}
        }
    }
}

# API Logs Aggregations properties (aggregated by source_ip and campaign_id)
api_logs_aggs_properties = {
    "es_id": "keyword",
    "window_start": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
    "window_end": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
    "source_ip": "keyword",
    "campaign_id": "keyword",
    "count": "long"
}

# API SEVs Aggregations properties (aggregated only by source_ip)
api_sevs_aggs_properties = {
    "es_id": "keyword",
    "window_start": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
    "window_end": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
    "source_ip": "keyword",
    "sev_id": "keyword",
    "sev_name": "text",
    "severity": "text",
    "type": "keyword",
    "count": "long"
}

def create_elasticsearch_client():
    return Elasticsearch([{'host': ES_HOST, 'port': ES_PORT, 'scheme': ES_SCHEME}],
                        basic_auth=(ES_USERNAME, ES_PASSWORD) if ES_USERNAME and ES_PASSWORD else None,
                        verify_certs=False,
                        ssl_show_warn=False
                         )

def create_index(client, index_name, properties, id_field):
    mapping = create_mapping(properties, id_field)
    try:
        if not client.indices.exists(index=index_name):
            client.indices.create(index=index_name, body=mapping)
            logger.info(f"Index '{index_name}' created successfully.")
        else:
            logger.info(f"Index '{index_name}' already exists. Skipping creation.")
    except Exception as e:
        logger.error(f"Error creating index '{index_name}': {str(e)}")

def main():
    client = create_elasticsearch_client()

    # Create all indexes
    # create_index(client, "ui_security_events", security_events_properties, "Alert_ID")
    # create_index(client, "ui_sevs_logs", sevs_logs_properties, "Log_ID")
    # create_index(client, "ui_security_events_aggs", security_events_aggs_properties, "es_id")
    # create_index(client, "ui_event_logs_aggs", event_logs_aggs_properties, "es_id")
    # create_index(client, "ui_event_logs", event_logs_properties, "Log_ID")
    
    # Create API indexes
    create_index(client, "api_logs", api_logs_properties, "Log_ID")
    create_index(client, "api_sevs", api_sevs_properties, "Alert_ID")
    create_index(client, "api_logs_aggs", api_logs_aggs_properties, "es_id")
    create_index(client, "api_sevs_aggs", api_sevs_aggs_properties, "es_id")

    client.close()

if __name__ == "__main__":
    main()