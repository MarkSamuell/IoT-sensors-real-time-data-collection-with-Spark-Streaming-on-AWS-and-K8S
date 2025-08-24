from elasticsearch import Elasticsearch
import time
import logging
from typing import Dict, Any
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ElasticsearchPerformanceTest:
    def __init__(self, host: str = '127.0.0.1', port: int = 9002, scheme: str = 'http'):
        self.client = Elasticsearch(
            [{'host': host, 'port': port, 'scheme': scheme}],
            verify_certs=False,
            ssl_show_warn=False,
            timeout=300,  # 5 minutes timeout
            max_retries=3,
            retry_on_timeout=True
        )
        
    def perform_aggregation(self) -> Dict[str, Any]:
        """
        Scans all data and performs severity count aggregations by day at 12 PM
        """
        start_time = time.time()
        
        try:
            # First get total document count
            count_result = self.client.count(index="ui_security_events")
            total_docs = count_result['count']
            logger.info(f"Total documents in index: {total_docs:,}")

            query = {
                "track_total_hits": True,
                "size": 0,
                "query": {
                    "script": {
                        "script": {
                            "source": "doc['Timestamp'].value.hour == params.target_hour",
                            "lang": "painless",
                            "params": {
                                "target_hour": 12
                            }
                        }
                    }
                },
                "aggs": {
                    "by_day": {
                        "date_histogram": {
                            "field": "Timestamp",
                            "calendar_interval": "day",
                            "format": "yyyy-MM-dd",
                            "min_doc_count": 1
                        },
                        "aggs": {
                            "severity_counts": {
                                "terms": {
                                    "field": "Severity.keyword",
                                    "size": 100
                                }
                            }
                        }
                    },
                    "total_severity": {
                        "terms": {
                            "field": "Severity.keyword",
                            "size": 100
                        }
                    }
                }
            }

            # Execute query
            response = self.client.search(
                index="ui_security_events",
                body=query
            )

            # Calculate metrics
            end_time = time.time()
            execution_time = end_time - start_time

            # Get actual hits count
            hits_count = response['hits']['total']['value']

            # Log performance metrics
            logger.info("\nElasticsearch Aggregation Performance:")
            logger.info(f"Total documents in index: {total_docs:,}")
            logger.info(f"Documents processed: {hits_count:,}")
            logger.info(f"Execution time: {execution_time:.2f} seconds")
            logger.info(f"Processing rate: {total_docs/execution_time:.2f} documents/second")

            # Log overall severity distribution
            logger.info("\nOverall Severity Distribution:")
            total_events = sum(bucket['doc_count'] for bucket in response['aggregations']['total_severity']['buckets'])
            for bucket in response['aggregations']['total_severity']['buckets']:
                percentage = (bucket['doc_count'] / total_events) * 100 if total_events > 0 else 0
                logger.info(f"  {bucket['key']}: {bucket['doc_count']:,} events ({percentage:.2f}%)")

            # Log sample of daily results
            logger.info("\nSeverity Counts at 12 PM (Sample of first 5 days):")
            for day_bucket in response['aggregations']['by_day']['buckets'][:5]:
                logger.info(f"\nDate: {day_bucket['key_as_string']}")
                day_total = sum(sev['doc_count'] for sev in day_bucket['severity_counts']['buckets'])
                for severity in day_bucket['severity_counts']['buckets']:
                    percentage = (severity['doc_count'] / day_total) * 100 if day_total > 0 else 0
                    logger.info(f"  {severity['key']}: {severity['doc_count']} events ({percentage:.2f}%)")

            return {
                "total_documents": total_docs,
                "processed_documents": hits_count,
                "execution_time": execution_time,
                "documents_per_second": total_docs/execution_time,
                "aggregations": response['aggregations']
            }

        except Exception as e:
            logger.error(f"Error performing Elasticsearch aggregation: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        tester = ElasticsearchPerformanceTest()
        results = tester.perform_aggregation()
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")