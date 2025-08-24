#!/usr/bin/env python3
"""
EQL Security Event Detector Flask API

This service provides a Flask API for managing EQL queries that are executed
against Elasticsearch to detect security events.
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
import traceback

import requests
import yaml
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from werkzeug.utils import secure_filename
import atexit

import os
from flask import render_template, send_from_directory, url_for
import dateutil.parser

# Configure logging
# Configure logging with only console output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('eql_detector')

# Create directories for static files
os.makedirs('static/css', exist_ok=True)

class ElasticsearchClient:
    """Client for interacting with Elasticsearch"""
    
    def __init__(self, host: str, port: int, scheme: str = 'http', 
                 username: Optional[str] = None, password: Optional[str] = None):
        self.base_url = f"{scheme}://{host}:{port}"
        self.auth = (username, password) if username and password else None
        self.headers = {"Content-Type": "application/json"}
        logger.info(f"Initialized Elasticsearch client for {self.base_url}")
        
    def test_connection(self) -> bool:
        """Test connection to Elasticsearch"""
        try:
            response = requests.get(
                f"{self.base_url}/_cluster/health",
                auth=self.auth,
                headers=self.headers,
                timeout=10
            )
            if response.status_code == 200:
                logger.info("Successfully connected to Elasticsearch")
                return True
            else:
                logger.error(f"Failed to connect to Elasticsearch: {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to Elasticsearch: {str(e)}")
            return False
            
    def execute_eql_query(self, index: str, query: str, timestamp_field: str = "@timestamp",
                        size: int = 100) -> Dict[str, Any]:
        """Execute an EQL query against an Elasticsearch index"""
        eql_payload = {
            "query": query,
            "timestamp_field": timestamp_field,
            "size": size
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/{index}/_eql/search",
                auth=self.auth,
                headers=self.headers,
                data=json.dumps(eql_payload),
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"Error executing EQL query: {response.text}")
                return {}
                
            results = response.json()
            # Return the full response for the caller to handle appropriately
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error during EQL query execution: {str(e)}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {str(e)}")
            return {}
            
    def bulk_index_documents(self, index: str, documents: List[Dict[str, Any]]) -> bool:
        """
        Robustly index multiple documents with improved timestamp handling
        """
        if not documents:
            logger.info("No documents to index")
            return True
        
        # Prepare bulk data with comprehensive validation and timestamp conversion
        bulk_data = []
        validated_documents = []
        
        for doc in documents:
            try:
                # Deep copy to avoid modifying original
                cleaned_doc = json.loads(json.dumps(doc))
                
                # Convert nested timestamps to ISO format
                def convert_timestamps(obj):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if isinstance(v, str) and 'T' in v and '+' in v:
                                # Ensure it's a valid datetime string
                                try:
                                    datetime.fromisoformat(v.replace('Z', '+00:00'))
                                    obj[k] = v
                                except ValueError:
                                    # If invalid, remove or default
                                    obj[k] = datetime.now(timezone.utc).isoformat()
                            elif isinstance(v, (dict, list)):
                                convert_timestamps(v)
                    elif isinstance(obj, list):
                        for item in obj:
                            convert_timestamps(item)
                
                # Process timestamps
                convert_timestamps(cleaned_doc)
                
                # Add index operation
                bulk_data.append({"index": {"_index": index}})
                bulk_data.append(cleaned_doc)
                validated_documents.append(cleaned_doc)
            
            except Exception as e:
                logger.error(f"Error preparing document for indexing: {e}")
                logger.error(f"Problematic document: {doc}")
        
        if not bulk_data:
            logger.warning("No valid documents to index after validation")
            return False
        
        try:
            # Convert to NDJSON
            bulk_payload = "\n".join([json.dumps(item) for item in bulk_data]) + "\n"
            
            # Perform bulk indexing with timeout and detailed error handling
            response = requests.post(
                f"{self.base_url}/_bulk",
                auth=self.auth,
                headers={"Content-Type": "application/x-ndjson"},
                data=bulk_payload,
                timeout=30
            )
            
            # Parse response
            if response.status_code not in [200, 201]:
                logger.error(f"Bulk indexing failed: {response.status_code}")
                logger.error(f"Response body: {response.text}")
                return False
            
            # Parse response for more detailed information
            response_data = response.json()
            
            # Log detailed indexing results
            failed_items = response_data.get('items', [])
            failed_count = sum(1 for item in failed_items if 'error' in item['index'])
            
            if failed_count > 0:
                logger.warning(f"Partial indexing failure: {failed_count} out of {len(validated_documents)} documents failed")
                # Log specific failures
                for item in failed_items:
                    if 'error' in item['index']:
                        logger.error(f"Indexing error: {item['index']['error']}")
                        logger.error(f"Problematic document: {json.dumps(item['index'], indent=2)}")
            
            logger.info(f"Successfully indexed {len(validated_documents) - failed_count} documents to {index}")
            return failed_count == 0
        
        except Exception as e:
            logger.error(f"Unexpected error during bulk indexing: {e}")
            logger.error(traceback.format_exc())
            return False
            
    def create_index_if_not_exists(self, index: str, mappings: Dict[str, Any]) -> bool:
        """Create an index with the specified mappings if it doesn't exist"""
        try:
            # Check if index exists
            response = requests.head(
                f"{self.base_url}/{index}",
                auth=self.auth,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Index {index} already exists")
                return True
                
            # Create index with mappings
            response = requests.put(
                f"{self.base_url}/{index}",
                auth=self.auth,
                headers=self.headers,
                data=json.dumps(mappings),
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully created index {index}")
                return True
            else:
                logger.error(f"Error creating index {index}: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error during index creation: {str(e)}")
            return False
            
    def get_last_detection_timestamp(self, index: str, query_id: str) -> str:
        """
        Get the timestamp of the most recent detection for a specific query
        to avoid reprocessing events
        """
        query = {
            "size": 1,
            "query": {
                "term": {
                    "query_id": query_id
                }
            },
            "sort": [
                {
                    "detection_time": {
                        "order": "desc"
                    }
                }
            ]
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/{index}/_search",
                auth=self.auth,
                headers=self.headers,
                data=json.dumps(query),
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"Error querying for last detection time: {response.text}")
                return None
                
            results = response.json()
            hits = results.get("hits", {}).get("hits", [])
            
            if hits and len(hits) > 0:
                detection_time = hits[0].get("_source", {}).get("detection_time")
                if detection_time:
                    return detection_time
                    
            return None
            
        except Exception as e:
            logger.error(f"Error getting last detection timestamp: {str(e)}")
            return None
    
    def count_documents(self, index: str, query: Dict[str, Any]) -> int:
        """Count documents in an index that match a query"""
        try:
            response = requests.post(
                f"{self.base_url}/{index}/_count",
                auth=self.auth,
                headers=self.headers,
                data=json.dumps(query),
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"Error counting documents: {response.text}")
                return 0
                
            results = response.json()
            return results.get("count", 0)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error counting documents: {str(e)}")
            return 0

class QueryThread(threading.Thread):
    """Thread class for running an EQL query detector"""
    
    def __init__(self, query_config: Dict[str, Any], es_client: ElasticsearchClient, 
                 target_index: str, query_id: str):
        super().__init__(daemon=True)
        self.query_config = query_config
        self.query_id = query_id
        self.es_client = es_client
        self.target_index = target_index
        self.stop_event = threading.Event()
        self.event_count = 0
        self.last_run_time = None
        self.status = "initialized"
        self.last_error = None
        # Store the last timestamp we've seen to avoid duplicate processing
        self.last_timestamp = None
        # Initialize with current time minus interval to ensure we don't process too much history
        interval_seconds = self.query_config.get('interval', 10)
        self.last_timestamp = (datetime.now(timezone.utc) - timedelta(seconds=interval_seconds)).isoformat()
        
    def _generate_time_filter(self) -> str:
        """
        Generate a time range filter for the EQL query to avoid reprocessing old events
        """
        timestamp_field = self.query_config.get('timestamp_field', '@timestamp')
        # Only look at events after the last timestamp we've processed
        return f" and {timestamp_field} >= \"{self.last_timestamp}\""
        
    def _process_query_results(self, sequences: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Advanced processing of EQL query results with robust error handling and comprehensive event generation.
        
        Args:
            sequences (List[Dict[str, Any]]): Raw EQL query result sequences
        
        Returns:
            List[Dict[str, Any]]: Processed security events
        """
        def safe_extract_value(source: Dict[str, Any], path: str, default: Any = None) -> Any:
            """
            Safely extract nested values from a dictionary using dot notation.
            
            Args:
                source (dict): Source dictionary
                path (str): Dot-separated path to the value
                default (Any, optional): Default value if path not found
            
            Returns:
                Value at the specified path or default
            """
            try:
                parts = path.split('.')
                current = source
                for part in parts:
                    if isinstance(current, dict):
                        current = current.get(part)
                    elif isinstance(current, list) and part.isdigit():
                        current = current[int(part)]
                    else:
                        return default
                    
                    if current is None:
                        return default
                return current
            except (TypeError, IndexError, ValueError):
                return default

        def normalize_timestamp(timestamp: Any) -> str:
            """
            Normalize timestamp to ISO 8601 format.
            
            Args:
                timestamp (Any): Input timestamp
            
            Returns:
                str: Normalized timestamp in ISO 8601 format
            """
            try:
                # If it's already a string that looks like a timestamp
                if isinstance(timestamp, str):
                    # Parse and convert to UTC
                    dt = dateutil.parser.parse(timestamp)
                    return dt.astimezone(timezone.utc).isoformat()
                
                # If it's a datetime object
                elif isinstance(timestamp, datetime):
                    return timestamp.astimezone(timezone.utc).isoformat()
                
                # If it's a number (assume timestamp)
                elif isinstance(timestamp, (int, float)):
                    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
                
                # Fallback
                return datetime.now(timezone.utc).isoformat()
            
            except Exception as e:
                logger.warning(f"Error normalizing timestamp: {e}")
                return datetime.now(timezone.utc).isoformat()

        # Set up deduplication tracking
        processed_events = []
        processed_hashes = set()
        detection_time = normalize_timestamp(datetime.now(timezone.utc))
        newest_timestamp = self.last_timestamp

        # Process each sequence or event
        events_list = []
        
        # Handle different response formats
        if self.query_id == 'sql_injection':
            # For non-sequence queries, process individual events
            events_list = sequences
        else:
            # For sequence queries, extract events from sequences
            for sequence in sequences:
                if sequence.get('events'):
                    events_list.extend(sequence.get('events', []))

        for event in events_list:
            try:
                # Get the source data
                source = event.get('_source', {})
                
                # Track the newest timestamp seen
                event_timestamp = safe_extract_value(source, self.query_config.get('timestamp_field', '@timestamp'))
                if event_timestamp:
                    normalized_timestamp = normalize_timestamp(event_timestamp)
                    if normalized_timestamp > newest_timestamp:
                        newest_timestamp = normalized_timestamp

                # Create a unique hash for deduplication
                event_hash = hash((
                    safe_extract_value(source, 'request.source_ip'),
                    safe_extract_value(source, 'request.path'),
                    str(event_timestamp)  # Use the timestamp as part of the hash
                ))

                # Skip if this event has already been processed
                if event_hash in processed_hashes:
                    logger.debug(f"Skipping duplicate event hash: {event_hash}")
                    continue

                processed_hashes.add(event_hash)

                # Extract dynamic fields based on configuration
                sev_config = self.query_config['sev_config']
                dynamic_fields = {}

                for field_name, field_path in sev_config.get('dynamic_fields', {}).items():
                    try:
                        # Special case for count
                        if field_path == "_count":
                            dynamic_fields[field_name] = 1  # For individual events, count is 1
                            continue

                        # Extract value using safe method
                        value = safe_extract_value(source, field_path)
                        if value is not None:
                            dynamic_fields[field_name] = value
                    except Exception as e:
                        logger.warning(f"Error extracting dynamic field {field_name}: {e}")

                # Prepare IOC (Information of Compromise)
                ioc = {
                    "request_id": safe_extract_value(source, 'request_id'),
                    "timestamp": normalize_timestamp(safe_extract_value(source, 'timestamp')),
                    "source_ip": safe_extract_value(source, 'request.source_ip'),
                    "method": safe_extract_value(source, 'request.method'),
                    "path": safe_extract_value(source, 'request.path'),
                    "status_code": safe_extract_value(source, 'response.status_code')
                }
                # Remove None values
                ioc = {k: v for k, v in ioc.items() if v is not None}

                # Create security event
                security_event = {
                    "sev_id": f"{sev_config['id_prefix']}-{uuid.uuid4().hex[:8]}",
                    "sev_name": sev_config['name'],
                    "description": sev_config['description'].format(**dynamic_fields) 
                        if dynamic_fields else sev_config['description'],
                    "severity": sev_config['severity'],
                    "type": self.query_config['type'],
                    "detection_time": detection_time,
                    "timestamp": normalize_timestamp(datetime.now(timezone.utc)),
                    "source_index": self.query_config['source_index'],
                    "query_id": self.query_config['id'],
                    "iocs": [ioc],  # Just use the single IOC
                    "metadata": {
                        "event_count": 1,
                        "source_ips": [ioc.get('source_ip')] if ioc.get('source_ip') else [],
                    },
                    **dynamic_fields  # Flatten dynamic fields
                }

                processed_events.append(security_event)

            except Exception as e:
                logger.error(f"Error processing event: {e}")
                logger.error(traceback.format_exc())

        # Update the last timestamp to avoid reprocessing the same events
        if newest_timestamp > self.last_timestamp:
            self.last_timestamp = newest_timestamp
            logger.info(f"Updated last timestamp to {self.last_timestamp}")

        logger.info(f"Processed {len(processed_events)} unique security events")
        return processed_events
        
    def run(self) -> None:
        """Run the detection thread"""
        self.status = "running"
        interval = self.query_config.get('interval', 10)  # Default to 10 seconds
        
        logger.info(f"Starting detection thread for query {self.query_id} (interval: {interval}s)")
        
        # Initialize the timestamp from the last detection in the database
        last_timestamp = self.es_client.get_last_detection_timestamp(self.target_index, self.query_id)
        if last_timestamp:
            self.last_timestamp = last_timestamp
            logger.info(f"Starting detection from last recorded timestamp: {self.last_timestamp}")
        else:
            # No previous detections - start from current time
            self.last_timestamp = datetime.now(timezone.utc).isoformat()
            logger.info(f"No previous detections found, starting from current time: {self.last_timestamp}")
        
        # Create a set to track detected sequences to avoid duplicates within a session
        detected_sequence_hashes = set()
        
        while not self.stop_event.is_set():
            try:
                self.last_run_time = datetime.now(timezone.utc).isoformat()
                logger.info(f"Running detection for query {self.query_id}")
                
                # Execute the EQL query with time filtering
                eql_query = self.query_config['eql_query']
                
                # Add time filtering to the query for both sequence and non-sequence queries
                timestamp_field = self.query_config.get('timestamp_field', '@timestamp')
                
                if self.query_id == 'sql_injection':
                    # For non-sequence queries, add time filter directly to the 'any where' clause
                    if 'any where (' in eql_query:
                        # Insert time filter at the beginning of the where clause
                        filtered_query = eql_query.replace(
                            'any where (', 
                            f'any where {timestamp_field} > "{self.last_timestamp}" and ('
                        )
                    else:
                        # Keep original if query format doesn't match expected pattern
                        filtered_query = eql_query
                        logger.warning(f"Could not add time filter to SQL injection query: unexpected format")
                elif self.query_id == 'brute_force_login':
                    # For brute force sequence query, we'll use the built-in response handling and deduplication
                    # But we need to add timestamp filtering to the first event in the sequence
                    if 'sequence by' in eql_query and '[any where' in eql_query:
                        # Find the first condition and add timestamp filter
                        parts = eql_query.split('[any where', 1)
                        if len(parts) > 1:
                            # Insert timestamp filter at the beginning of the first condition
                            filtered_query = f"{parts[0]}[any where {timestamp_field} > \"{self.last_timestamp}\" and {parts[1]}"
                        else:
                            filtered_query = eql_query
                            logger.warning(f"Could not add time filter to brute force query: unexpected format")
                    else:
                        filtered_query = eql_query
                        logger.warning(f"Could not add time filter to brute force query: unexpected format")
                else:
                    # Keep other queries as is
                    filtered_query = eql_query
                
                logger.debug(f"Running query with filtering: {filtered_query}")

                # Execute EQL query with proper result handling based on query type
                try:
                    result = self.es_client.execute_eql_query(
                        self.query_config['source_index'],
                        filtered_query,
                        self.query_config.get('timestamp_field', '@timestamp'),
                        self.query_config.get('size', 100)
                    )
                    
                    if self.query_id == 'sql_injection':
                        # Format is different for non-sequence queries
                        events = result.get('hits', {}).get('events', [])
                        logger.info(f"SQL Injection query returned {len(events)} matches")
                        
                        if events:
                            logger.warning(f"SECURITY ALERT: Query {self.query_id} detected {len(events)} potential security issues")
                            security_events = self._process_query_results(events)
                            self.event_count += len(security_events)
                            if security_events:
                                self.es_client.bulk_index_documents(self.target_index, security_events)
                                # Update the last timestamp to most recent detection time
                                self.last_timestamp = security_events[0].get("detection_time", self.last_timestamp)
                    else:
                        # For sequence queries like brute force
                        sequences = result.get('hits', {}).get('sequences', [])
                        
                        # Deduplicate sequences based on source_ip and timestamp
                        unique_sequences = []
                        for sequence in sequences:
                            # Extract key information for deduplication
                            if 'events' in sequence and len(sequence['events']) > 0:
                                first_event = sequence['events'][0].get('_source', {})
                                source_ip = first_event.get('request', {}).get('source_ip', '')
                                event_time = first_event.get(timestamp_field, '')
                                
                                # Create a hash for this sequence
                                sequence_hash = hash(f"{source_ip}_{event_time}")
                                
                                # Only add if we haven't seen this sequence before
                                if sequence_hash not in detected_sequence_hashes:
                                    detected_sequence_hashes.add(sequence_hash)
                                    unique_sequences.append(sequence)
                        
                        if unique_sequences:
                            logger.warning(f"SECURITY ALERT: Query {self.query_id} detected {len(unique_sequences)} potential security issues (after deduplication)")
                            security_events = self._process_query_results(unique_sequences)
                            self.event_count += len(security_events)
                            if security_events:
                                self.es_client.bulk_index_documents(self.target_index, security_events)
                                # Update the last timestamp to most recent detection time
                                self.last_timestamp = security_events[0].get("detection_time", self.last_timestamp)
                except Exception as e:
                    logger.error(f"Error executing EQL query for {self.query_id}: {str(e)}")
                    logger.error(f"Query was: {filtered_query}")
                    self.last_error = str(e)
                
            except Exception as e:
                logger.error(f"Error in detection loop for query {self.query_id}: {str(e)}")
                logger.error(traceback.format_exc())
                self.last_error = str(e)
                self.status = "error"
                
            # Sleep until next interval
            time.sleep(interval)
        
        self.status = "stopped"
        logger.info(f"Detection thread for query {self.query_id} stopped")
        
    def stop(self) -> None:
        """Signal the thread to stop"""
        logger.info(f"Stopping detection thread for query {self.query_id}")
        self.stop_event.set()
        
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the query thread"""
        return {
            "query_id": self.query_id,
            "status": self.status,
            "event_count": self.event_count,
            "last_run_time": self.last_run_time,
            "last_error": self.last_error,
            "type": self.query_config['type'],
            "name": self.query_config.get('name', '') or self.query_config['sev_config']['name'],
            "source_index": self.query_config['source_index'],
            "interval": self.query_config.get('interval', 60)
        }

class EQLDetectorService:
    """Service that manages EQL query threads"""
    
    def __init__(self):
        self.query_threads = {}  # Dict of query_id -> QueryThread
        self.es_client = None
        self.config = {}
        self.target_index = ""
        
    def load_config(self, config_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Load configuration from dict"""
        try:
            # Basic validation
            required_keys = ['elasticsearch', 'target_index', 'queries']
            if not all(k in config_data for k in required_keys):
                missing = [k for k in required_keys if k not in config_data]
                return False, f"Missing required configuration keys: {missing}"
            
            self.config = config_data
            self.target_index = config_data['target_index']
            
            # Initialize Elasticsearch client
            es_config = config_data['elasticsearch']
            self.es_client = ElasticsearchClient(
                host=es_config['host'],
                port=es_config['port'],
                scheme=es_config.get('scheme', 'http'),
                username=es_config.get('username'),
                password=es_config.get('password')
            )
            
            # Test connection
            if not self.es_client.test_connection():
                return False, "Failed to connect to Elasticsearch"
                
            # Setup required indices
            self._setup_indices()
            
            return True, "Configuration loaded successfully"
            
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            return False, f"Error loading configuration: {str(e)}"
            
    def _setup_indices(self) -> None:
        """Create necessary indices if they don't exist"""
        for index_config in self.config.get('indices', []):
            index_name = index_config['name']
            mappings = index_config['mappings']
            
            self.es_client.create_index_if_not_exists(index_name, mappings)
    
    def start_query(self, query_config: Dict[str, Any]) -> Tuple[bool, str]:
        """Start a new query thread"""
        try:
            query_id = query_config['id']
            
            # Check if query is already running
            if query_id in self.query_threads and self.query_threads[query_id].is_alive():
                return False, f"Query with ID {query_id} is already running"
                
            # Create and start thread
            thread = QueryThread(
                query_config=query_config,
                es_client=self.es_client,
                target_index=self.target_index,
                query_id=query_id
            )
            thread.start()
            
            # Store thread
            self.query_threads[query_id] = thread
            
            return True, f"Query {query_id} started successfully"
            
        except Exception as e:
            logger.error(f"Error starting query: {str(e)}")
            return False, f"Error starting query: {str(e)}"
    
    def stop_query(self, query_id: str) -> Tuple[bool, str]:
        """Stop a running query thread"""
        if query_id not in self.query_threads:
            return False, f"Query with ID {query_id} not found"
            
        thread = self.query_threads[query_id]
        if not thread.is_alive():
            return False, f"Query with ID {query_id} is not running"
            
        thread.stop()
        # Give the thread a moment to stop
        time.sleep(0.5)
        
        return True, f"Query {query_id} stopped successfully"
    
    def delete_query(self, query_id: str) -> Tuple[bool, str]:
        """Delete a query thread"""
        if query_id not in self.query_threads:
            return False, f"Query with ID {query_id} not found"
            
        thread = self.query_threads[query_id]
        if thread.is_alive():
            thread.stop()
            # Give the thread a moment to stop
            time.sleep(0.5)
            
        del self.query_threads[query_id]
        
        return True, f"Query {query_id} deleted successfully"
    
    def get_query_status(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a query thread"""
        if query_id not in self.query_threads:
            return None
            
        return self.query_threads[query_id].get_status()
    
    def get_all_queries(self) -> List[Dict[str, Any]]:
        """Get status of all query threads"""
        return [thread.get_status() for thread in self.query_threads.values()]
    
    def start_from_config(self) -> Tuple[bool, str]:
        """Start all queries defined in the configuration"""
        if not self.config or not self.es_client:
            return False, "No configuration loaded"
            
        success_count = 0
        failure_count = 0
        
        for query_config in self.config.get('queries', []):
            success, message = self.start_query(query_config)
            if success:
                success_count += 1
            else:
                failure_count += 1
                logger.error(f"Failed to start query {query_config.get('id')}: {message}")
        
        if failure_count == 0:
            return True, f"Started {success_count} queries successfully"
        else:
            return False, f"Started {success_count} queries, but {failure_count} failed"
    
    def stop_all_queries(self) -> None:
        """Stop all running query threads"""
        for query_id, thread in list(self.query_threads.items()):
            if thread.is_alive():
                logger.info(f"Stopping query thread {query_id}")
                thread.stop()
        
        # Wait for all threads to stop
        time.sleep(1)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Initialize the EQL detector service
detector_service = EQLDetectorService()

# Ensure the logs directory exists
# os.makedirs('logs', exist_ok=True)

# Configure uploads directory
UPLOAD_FOLDER = 'config_uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB max upload

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "eql-detector"
    })

@app.route('/api/config', methods=['POST'])
def upload_config():
    """Upload and apply configuration"""
    if 'file' in request.files:
        # Handle file upload
        file = request.files['file']
        if file.filename == '':
            return jsonify({
                "success": False,
                "message": "No file selected"
            }), 400
            
        if file:
            # Save file
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            # Load config from file
            try:
                with open(file_path, 'r') as f:
                    config_data = yaml.safe_load(f)
            except Exception as e:
                return jsonify({
                    "success": False,
                    "message": f"Error parsing config file: {str(e)}"
                }), 400
    else:
        # Handle JSON body
        if not request.is_json:
            return jsonify({
                "success": False,
                "message": "Request must be JSON or file upload"
            }), 400
            
        config_data = request.json
    
    # Stop any existing queries
    detector_service.stop_all_queries()
    
    # Load configuration
    success, message = detector_service.load_config(config_data)
    if not success:
        return jsonify({
            "success": False,
            "message": message
        }), 400
    
    # Start queries
    success, message = detector_service.start_from_config()
    
    return jsonify({
        "success": success,
        "message": message
    })

@app.route('/api/queries', methods=['GET'])
def get_queries():
    """Get all running queries"""
    queries = detector_service.get_all_queries()
    return jsonify({
        "success": True,
        "queries": queries,
        "count": len(queries)
    })

@app.route('/api/queries/<query_id>', methods=['GET'])
def get_query(query_id):
    """Get status of a specific query"""
    status = detector_service.get_query_status(query_id)
    if status is None:
        return jsonify({
            "success": False,
            "message": f"Query with ID {query_id} not found"
        }), 404
    
    return jsonify({
        "success": True,
        "query": status
    })

@app.route('/api/queries/<query_id>/stop', methods=['POST'])
def stop_query(query_id):
    """Stop a running query"""
    success, message = detector_service.stop_query(query_id)
    return jsonify({
        "success": success,
        "message": message
    }), 200 if success else 400

@app.route('/api/queries/<query_id>/restart', methods=['POST'])
def restart_query(query_id):
    """Restart a query"""
    # Get query config
    status = detector_service.get_query_status(query_id)
    if status is None:
        return jsonify({
            "success": False,
            "message": f"Query with ID {query_id} not found"
        }), 404
    
    # First, stop the query if it's running
    detector_service.stop_query(query_id)
    
    # Find the query configuration
    query_config = None
    for q in detector_service.config.get('queries', []):
        if q['id'] == query_id:
            query_config = q
            break
    
    if query_config is None:
        return jsonify({
            "success": False,
            "message": f"Configuration for query ID {query_id} not found"
        }), 404
    
    # Start the query again
    success, message = detector_service.start_query(query_config)
    
    return jsonify({
        "success": success,
        "message": message
    }), 200 if success else 400

@app.route('/api/queries/<query_id>', methods=['DELETE'])
def delete_query(query_id):
    """Delete a query"""
    success, message = detector_service.delete_query(query_id)
    return jsonify({
        "success": success,
        "message": message
    }), 200 if success else 400

@app.route('/api/queries', methods=['POST'])
def add_query():
    """Add a new query"""
    if not request.is_json:
        return jsonify({
            "success": False,
            "message": "Request must be JSON"
        }), 400
    
    query_config = request.json
    
    # Validate query config
    required_keys = ['id', 'type', 'source_index', 'eql_query', 'sev_config']
    if not all(k in query_config for k in required_keys):
        missing = [k for k in required_keys if k not in query_config]
        return jsonify({
            "success": False,
            "message": f"Missing required configuration keys: {missing}"
        }), 400
    
    # Start the query
    success, message = detector_service.start_query(query_config)
    
    # Add to configuration if successful
    if success:
        # Add to in-memory config
        detector_service.config.setdefault('queries', []).append(query_config)
    
    return jsonify({
        "success": success,
        "message": message
    }), 200 if success else 400

@app.route('/api/queries/stop-all', methods=['POST'])
def stop_all_queries():
    """Stop all running queries"""
    detector_service.stop_all_queries()
    return jsonify({
        "success": True,
        "message": "All queries stopped"
    })

@app.route('/api/events/count', methods=['GET'])
def get_event_count():
    """Get count of security events by type"""
    query_id = request.args.get('query_id')
    
    if not detector_service.es_client or not detector_service.target_index:
        return jsonify({
            "success": False,
            "message": "No configuration loaded"
        }), 400
    
    try:
        counts = {}
        
        # If query_id is provided, count only for that query
        if query_id:
            query = {
                "query": {
                    "term": {
                        "query_id": query_id
                    }
                }
            }
            count = detector_service.es_client.count_documents(
                detector_service.target_index,
                query
            )
            counts[query_id] = count
        else:
            # Count events for each query
            queries = detector_service.get_all_queries()
            for query in queries:
                query_id = query['query_id']
                query = {
                    "query": {
                        "term": {
                            "query_id": query_id
                        }
                    }
                }
                count = detector_service.es_client.count_documents(
                    detector_service.target_index,
                    query
                )
                counts[query_id] = count
        
        return jsonify({
            "success": True,
            "counts": counts
        })
    except Exception as e:
        logger.error(f"Error getting event counts: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Error getting event counts: {str(e)}"
        }), 500

# Register cleanup function
@atexit.register
def cleanup():
    """Stop all queries when the application exits"""
    logger.info("Shutting down EQL Detector service")
    detector_service.stop_all_queries()

# Add these routes to serve the UI
@app.route('/')
def index():
    """Render the main dashboard page"""
    return render_template('index.html')

@app.route('/static/<path:path>')
def static_files(path):
    """Serve static files"""
    return send_from_directory('static', path)


os.makedirs('templates', exist_ok=True)


# Save styles.css to static/css/styles.css
def setup_ui_files():
    """Set up the UI files"""
    # Create directories
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static/css', exist_ok=True)
    
    # Only write files if they don't exist
    if not os.path.exists('static/css/styles.css'):
        with open('static/css/styles.css', 'w') as f:
            f.write('''/* Paste the styles.css content here */''')
    
    if not os.path.exists('templates/index.html'):
        with open('templates/index.html', 'w') as f:
            f.write('''<!-- Paste the index.html content here -->''')

def main():
    """Main entry point for the EQL Detector Flask API"""
    parser = argparse.ArgumentParser(description='EQL Security Event Detector Flask API')
    parser.add_argument('-c', '--config', help='Path to initial configuration file')
    parser.add_argument('-p', '--port', type=int, default=5000, help='Port to run the Flask API on')
    parser.add_argument('-d', '--debug', action='store_true', help='Run in debug mode')
    args = parser.parse_args()
    
    # Load initial configuration if provided
    if args.config:
        try:
            with open(args.config, 'r') as f:
                config_data = yaml.safe_load(f)
                
            success, message = detector_service.load_config(config_data)
            if success:
                detector_service.start_from_config()
                logger.info("Initial configuration loaded and queries started")
            else:
                logger.error(f"Failed to load initial configuration: {message}")
        except Exception as e:
            logger.error(f"Error loading initial configuration: {str(e)}")
    
    # Start the Flask app
    app.run(host='0.0.0.0', port=args.port, debug=args.debug)

if __name__ == "__main__":
    # Set up UI files
    setup_ui_files()
    
    # Start the Flask app
    main()