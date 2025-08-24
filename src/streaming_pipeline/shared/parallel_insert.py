"""
Parallel insertion utility for Elasticsearch operations
Provides parallel execution of raw data and aggregation insertions for better performance
"""

import time
import logging
from datetime import datetime
from typing import Optional, Dict, List, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from streaming_pipeline.shared.dal.elasticsearch_handler import ElasticsearchHandler
from streaming_pipeline.shared.job_monitor import SparkJobMonitor

logger = logging.getLogger(__name__)


class ParallelInserter:
    """Utility class for parallel Elasticsearch insertions"""
    
    def __init__(self, es_handler: ElasticsearchHandler, monitor: SparkJobMonitor, batch_id: int):
        self.es_handler = es_handler
        self.monitor = monitor
        self.batch_id = batch_id
        self.max_workers = 4
    
    def execute_parallel_insertions(self, insertion_tasks: List[Dict]) -> Optional[str]:
        """
        Execute multiple insertion tasks in parallel
        
        Args:
            insertion_tasks: List of dictionaries with keys:
                - 'name': Task name for logging
                - 'function': Function to execute
                - 'description': Description for monitoring
        
        Returns:
            Timestamp string if successful, raises exception if any task fails
        """
        logger.info(f"🚀 Starting parallel insertion for batch {self.batch_id} with {len(insertion_tasks)} tasks")
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="ES_Insert") as executor:
            # Submit all tasks
            future_to_task = {}
            for task in insertion_tasks:
                future = executor.submit(self._execute_task, task)
                future_to_task[future] = task
            
            # Wait for all tasks to complete
            results = {}
            exceptions = []
            
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                task_name = task['name']
                try:
                    result = future.result()
                    results[task_name] = result
                    logger.info(f"✅ {task_name}: {result}")
                except Exception as e:
                    exceptions.append(f"{task_name}: {str(e)}")
                    logger.error(f"❌ {task_name} failed: {str(e)}")
            
            # Check if any insertions failed
            if exceptions:
                logger.error(f"🚨 {len(exceptions)} parallel insertions failed:")
                for exc in exceptions:
                    logger.error(f"  - {exc}")
                raise Exception(f"Parallel insertion failed: {'; '.join(exceptions)}")
            
            elapsed_time = time.time() - start_time
            logger.info(f"🎉 All {len(insertion_tasks)} parallel insertions completed successfully in {elapsed_time:.2f}s")
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def _execute_task(self, task: Dict) -> str:
        """Execute a single insertion task with monitoring"""
        try:
            with self.monitor as m:
                m.mark_job(f"Parallel Insert: {task['description']}")
                result = task['function']()
                return result
        except Exception as e:
            logger.error(f"Task {task['name']} failed: {str(e)}")
            raise


# Utility functions for common insertion patterns
def create_raw_insertion_task(name: str, df: DataFrame, es_handler: ElasticsearchHandler, 
                             index_name: str, mapping_id: str, operation: str = "index") -> Dict:
    """Create a task for raw data insertion"""
    def insert_raw():
        if operation == "upsert":
            es_write_conf = es_handler.get_es_write_conf(
                index_name=index_name,
                mapping_id=mapping_id,
                operation="upsert"
            )
            df.write \
                .format("org.elasticsearch.spark.sql") \
                .options(**es_write_conf) \
                .mode("append") \
                .save(index_name)
        else:
            es_handler.insert_dataframe(df, index_name, mapping_id)
        return f"{name}_success"
    
    return {
        'name': name,
        'function': insert_raw,
        'description': name
    }


def create_aggregation_insertion_task(name: str, df: DataFrame, es_handler: ElasticsearchHandler, 
                                     index_name: str) -> Dict:
    """Create a task for aggregation insertion"""
    def insert_aggs():
        es_handler.insert_aggregations(df, index_name)
        return f"{name}_success"
    
    return {
        'name': name,
        'function': insert_aggs,
        'description': name
    }


def create_conditional_task(name: str, condition: bool, task_function: Callable) -> Dict:
    """Create a conditional task that only runs if condition is True"""
    def conditional_function():
        if not condition:
            return f"{name}_skipped"
        return task_function()
    
    return {
        'name': name,
        'function': conditional_function,
        'description': name
    }


# High-level parallel insertion functions for common patterns
def insert_sevs_data_parallel(es_handler: ElasticsearchHandler,
                              raw_df: DataFrame,
                              ioc_df: Optional[DataFrame],
                              agg_df: DataFrame,
                              monitor: SparkJobMonitor,
                              batch_id: int,
                              config) -> Optional[str]:
    """Parallel insertion for SEVs data (3 insertions: raw, IoC, aggregations)"""
    logger.info(f"🚀 Starting parallel insertion for SEVs batch {batch_id}")
    start_time = time.time()
    
    parallel_inserter = ParallelInserter(es_handler, monitor, batch_id)
    
    insertion_tasks = [
        create_raw_insertion_task(
            "Raw SEVs",
            raw_df,
            es_handler,
            config.ELASTICSEARCH_SECURITY_EVENTS_INDEX,
            "Alert_ID"
        ),
        create_conditional_task(
            "IoC Data",
            ioc_df is not None and not ioc_df.isEmpty(),
            lambda: create_raw_insertion_task(
                "IoC Data",
                ioc_df,
                es_handler,
                config.ELASTICSEARCH_SEVS_LOGS_INDEX,
                "Log_ID"
            )["function"]()
        ),
        create_aggregation_insertion_task(
            "SEVs Aggregations",
            agg_df,
            es_handler,
            config.ELASTICSEARCH_SECURITY_EVENTS_AGGS_INDEX
        )
    ]
    
    try:
        result = parallel_inserter.execute_parallel_insertions(insertion_tasks)
        elapsed_time = time.time() - start_time
        logger.info(f"🎉 SEVs parallel insertion completed in {elapsed_time:.2f}s")
        return result
    except Exception as e:
        logger.error(f"❌ SEVs parallel insertion failed: {str(e)}")
        raise


def insert_event_logs_data_parallel(es_handler: ElasticsearchHandler,
                                   raw_df: DataFrame,
                                   agg_df: DataFrame,
                                   monitor: SparkJobMonitor,
                                   batch_id: int,
                                   config) -> Optional[str]:
    """Parallel insertion for Event Logs data (2 insertions: raw, aggregations)"""
    logger.info(f"🚀 Starting parallel insertion for Event Logs batch {batch_id}")
    start_time = time.time()
    
    parallel_inserter = ParallelInserter(es_handler, monitor, batch_id)
    
    insertion_tasks = [
        create_raw_insertion_task(
            "Raw Event Logs",
            raw_df,
            es_handler,
            config.ELASTICSEARCH_EVENT_LOGS_INDEX,
            "Log_ID"
        ),
        create_aggregation_insertion_task(
            "Event Logs Aggregations",
            agg_df,
            es_handler,
            config.ELASTICSEARCH_EVENT_LOGS_AGGS_INDEX
        )
    ]
    
    try:
        result = parallel_inserter.execute_parallel_insertions(insertion_tasks)
        elapsed_time = time.time() - start_time
        logger.info(f"🎉 Event Logs parallel insertion completed in {elapsed_time:.2f}s")
        return result
    except Exception as e:
        logger.error(f"❌ Event Logs parallel insertion failed: {str(e)}")
        raise


def insert_api_logs_data_parallel(es_handler: ElasticsearchHandler,
                                 raw_logs_df: DataFrame,
                                 agg_logs_df: DataFrame,
                                 raw_sevs_df: Optional[DataFrame],
                                 agg_sevs_df: Optional[DataFrame],
                                 monitor: SparkJobMonitor,
                                 batch_id: int,
                                 config) -> Optional[str]:
    """Parallel insertion for API Logs data (4 insertions: logs raw+agg, sevs raw+agg)"""
    logger.info(f"🚀 Starting parallel insertion for API Logs batch {batch_id}")
    start_time = time.time()
    
    parallel_inserter = ParallelInserter(es_handler, monitor, batch_id)
    
    # Create custom upsert task for API logs (maintains original logic)
    def insert_api_logs_with_upsert():
        es_write_conf = es_handler.get_es_write_conf(
            index_name=config.ELASTICSEARCH_API_LOGS_INDEX,
            mapping_id="Log_ID",
            operation="upsert"
        )
        raw_logs_df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_conf) \
            .mode("append") \
            .save(config.ELASTICSEARCH_API_LOGS_INDEX)
        return "api_logs_upsert_success"
    
    insertion_tasks = [
        {
            'name': 'API Logs Raw (Upsert)',
            'function': insert_api_logs_with_upsert,
            'description': 'API Logs Raw with Upsert'
        },
        create_aggregation_insertion_task(
            "API Logs Aggregations",
            agg_logs_df,
            es_handler,
            config.ELASTICSEARCH_API_LOGS_AGGS_INDEX
        ),
        create_conditional_task(
            "API SEVs Raw",
            raw_sevs_df is not None,
            lambda: create_raw_insertion_task(
                "API SEVs Raw",
                raw_sevs_df,
                es_handler,
                config.ELASTICSEARCH_API_SEVS_INDEX,
                "Alert_ID"
            )["function"]()
        ),
        create_conditional_task(
            "API SEVs Aggregations",
            agg_sevs_df is not None,
            lambda: create_aggregation_insertion_task(
                "API SEVs Aggregations",
                agg_sevs_df,
                es_handler,
                config.ELASTICSEARCH_API_SEVS_AGGS_INDEX
            )["function"]()
        )
    ]
    
    try:
        result = parallel_inserter.execute_parallel_insertions(insertion_tasks)
        elapsed_time = time.time() - start_time
        logger.info(f"🎉 API Logs parallel insertion completed in {elapsed_time:.2f}s")
        return result
    except Exception as e:
        logger.error(f"❌ API Logs parallel insertion failed: {str(e)}")
        raise