from elasticsearch import Elasticsearch, helpers
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
import logging
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class AckHandler:
    """Handler for managing message acknowledgments with optimized batch processing."""
    
    def __init__(self, es_handler, ack_index: str = "event_logs_ack", 
                 batch_size: int = 5000, flush_interval: int = 3):
        self.es_handler = es_handler
        self.ack_index = ack_index
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        # Thread-safe queues for batch processing
        self.raw_queue = Queue()
        self.aggs_queue = Queue()
        
        # Start background processing
        self._start_background_processor()
        self._ensure_ack_index()
        
    def _ensure_ack_index(self):
        """Create optimized acknowledgment index if it doesn't exist."""
        if not self.es_handler.index_exists(self.ack_index):
            mappings = {
                "mappings": {
                    "properties": {
                        "message_id": {"type": "keyword"},
                        "raw_ack": {"type": "byte"},
                        "aggs_ack": {"type": "byte"},
                        "timestamp": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||epoch_millis"
                        }
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "refresh_interval": "30s",  # Reduced refresh frequency
                    "index.mapping.total_fields.limit": 2000,
                    "index.number_of_routing_shards": 1,
                    "index.translog.durability": "async",  # Async translog updates
                    "index.translog.sync_interval": "30s",
                    "index.translog.flush_threshold_size": "256mb"
                }
            }
            self.es_handler.create_index_if_not_exists(self.ack_index, mappings)
            logger.info(f"Created acknowledgment index: {self.ack_index}")

    def _start_background_processor(self):
        """Start background thread for processing acknowledgments."""
        self.should_run = True
        self.processor_thread = threading.Thread(target=self._process_queues)
        self.processor_thread.daemon = True
        self.processor_thread.start()

    def _process_queues(self):
        """Background processor for batching acknowledgments."""
        while self.should_run:
            try:
                raw_batch = []
                aggs_batch = []
                batch_start = time.time()
                
                # Keep collecting until either batch is full or timeout reached
                while time.time() - batch_start < self.flush_interval:
                    raw_messages_added = 0
                    aggs_messages_added = 0
                    
                    # Process raw queue
                    while not self.raw_queue.empty() and len(raw_batch) < self.batch_size:
                        raw_batch.append(self.raw_queue.get_nowait())
                        raw_messages_added += 1
                        
                    # Process aggs queue    
                    while not self.aggs_queue.empty() and len(aggs_batch) < self.batch_size:
                        aggs_batch.append(self.aggs_queue.get_nowait())
                        aggs_messages_added += 1
                    
                    # If either batch is full, break early
                    if len(raw_batch) >= self.batch_size or len(aggs_batch) >= self.batch_size:
                        break
                        
                    # If no new messages were added this iteration, sleep briefly
                    if raw_messages_added == 0 and aggs_messages_added == 0:
                        time.sleep(0.1)
                
                # Process batches if they have any messages
                if raw_batch:
                    logger.info(f"Flushing raw batch of {len(raw_batch)} messages after {time.time() - batch_start:.2f}s")
                    self._bulk_acknowledge(raw_batch, 'raw')
                    
                if aggs_batch:
                    logger.info(f"Flushing aggs batch of {len(aggs_batch)} messages after {time.time() - batch_start:.2f}s")
                    self._bulk_acknowledge(aggs_batch, 'aggs')
                    
            except Exception as e:
                logger.error(f"Error in background processor: {e}")
                time.sleep(1)

    def _bulk_acknowledge(self, message_ids: List[str], ack_type: str):
        """Bulk acknowledge messages using ES bulk API."""
        client = self.es_handler._create_client()
        try:
            actions = []
            current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            
            for message_id in message_ids:
                # Convert bytearray to string if needed
                if isinstance(message_id, bytearray):
                    message_id = message_id.decode('utf-8')
                
                action = {
                    "_op_type": "update",
                    "_index": self.ack_index,
                    "_id": str(message_id),  # Ensure string type
                    "doc": {
                        f"{ack_type}_ack": 1,
                        "message_id": str(message_id),  # Store original ID
                        "timestamp": current_time
                    },
                    "doc_as_upsert": True
                }
                actions.append(action)
                
            success, failed = helpers.bulk(
                client, 
                actions, 
                chunk_size=self.batch_size,
                raise_on_error=False,
                raise_on_exception=False
            )
            
            if failed:
                logger.warning(f"Failed to acknowledge {len(failed)} messages for {ack_type}")
                
        except Exception as e:
            logger.error(f"Error in bulk acknowledge for {ack_type}: {e}")
            for action in actions[:5]:  # Log first 5 actions for debugging
                logger.error(f"Problem action: {action}")
        finally:
            client.close()

    def acknowledge_message(self, message_id: str, ack_type: str):
        """Queue message for acknowledgment."""
        if ack_type == 'raw':
            self.raw_queue.put(message_id)
        elif ack_type == 'aggs':
            self.aggs_queue.put(message_id)
        else:
            raise ValueError("ack_type must be either 'raw' or 'aggs'")

    def acknowledge_batch(self, message_ids: List[str], ack_type: str):
        """Queue multiple messages for acknowledgment."""
        queue = self.raw_queue if ack_type == 'raw' else self.aggs_queue
        for message_id in message_ids:
            queue.put(message_id)

    def shutdown(self):
        """Gracefully shutdown background processor."""
        self.should_run = False
        if self.processor_thread.is_alive():
            self.processor_thread.join(timeout=30)
            
        # Process any remaining messages
        self._process_queues()