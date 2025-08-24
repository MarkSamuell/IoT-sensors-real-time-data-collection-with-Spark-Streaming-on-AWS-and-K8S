from pyiceberg.catalog import load_catalog
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any
import json
import boto3
from collections import defaultdict
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IcebergPerformanceTest:
    def __init__(self):
        session = boto3.Session(
            region_name='eu-central-1'
        )
        credentials = session.get_credentials()
        
        self.catalog = load_catalog(
            "demo",
            **{
                "type": "glue",
                "warehouse": "s3://elasticmapreduce-uraeusdev/ICEBERG/",
                "region": "eu-central-1",
                "aws_access_key_id": credentials.access_key,
                "aws_secret_access_key": credentials.secret_key,
                "aws_session_token": credentials.token
            }
        )
        self.table = self.catalog.load_table("uraeus_db.security_events")
        
    def _get_epoch_ms(self, date_str: str) -> int:
        """Convert date string to epoch milliseconds"""
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        except Exception as e:
            logger.error(f"Error converting date {date_str}: {e}")
            return 0
        
    def perform_aggregation(self) -> Dict[str, Any]:
        """
        Processes all data and aggregates severity counts at 12 PM for each day
        """
        start_time = time.time()
        
        try:
            # Create scan for all fields we need
            scan = self.table.scan(
                selected_fields=[
                    "Severity", "Timestamp", "year", "month", "day"
                ]
            )
            
            # Initialize aggregation structures
            daily_counts = defaultdict(lambda: defaultdict(int))  # {date: {severity: count}}
            total_severity_counts = defaultdict(int)  # {severity: count}
            
            # Get data as PyArrow table
            table = scan.to_arrow()
            total_docs = len(table)
            
            logger.info(f"Starting to process {total_docs:,} records...")
            
            # Convert to pandas for easier datetime handling
            df = table.to_pandas()
            df['hour'] = pd.to_datetime(df['Timestamp']).dt.hour
            
            # Process all records for total counts
            severity_counts = df['Severity'].value_counts().to_dict()
            total_severity_counts.update(severity_counts)
            
            # Filter for noon records and process
            noon_records = df[df['hour'] == 12]
            
            # Group and count noon records
            for _, row in noon_records.iterrows():
                day_key = f"{row['year']:04d}-{row['month']:02d}-{row['day']:02d}"
                severity = row['Severity']
                daily_counts[day_key][severity] += 1

            # Calculate metrics
            end_time = time.time()
            execution_time = end_time - start_time
            total_events = sum(total_severity_counts.values())
            
            # Log performance metrics
            logger.info("\nIceberg Aggregation Performance:")
            logger.info(f"Total documents processed: {total_docs:,}")
            logger.info(f"Execution time: {execution_time:.2f} seconds")
            logger.info(f"Processing rate: {total_docs/execution_time:.2f} documents/second")

            # Log overall severity distribution
            logger.info("\nOverall Severity Distribution:")
            for severity, count in sorted(total_severity_counts.items()):
                percentage = (count / total_events) * 100 if total_events > 0 else 0
                logger.info(f"  {severity}: {count:,} events ({percentage:.2f}%)")

            # Log sample of daily results
            logger.info("\nSeverity Counts at 12 PM (Sample of first 5 days):")
            for day in sorted(daily_counts.keys())[:5]:
                logger.info(f"\nDate: {day}")
                day_total = sum(daily_counts[day].values())
                for severity, count in sorted(daily_counts[day].items()):
                    percentage = (count / day_total) * 100 if day_total > 0 else 0
                    logger.info(f"  {severity}: {count} events ({percentage:.2f}%)")

            # Format aggregations to match Elasticsearch structure
            aggregations = {
                "by_day": {
                    "buckets": [
                        {
                            "key_as_string": day,
                            "key": self._get_epoch_ms(day),
                            "severity_counts": {
                                "buckets": [
                                    {
                                        "key": severity,
                                        "doc_count": count
                                    }
                                    for severity, count in sorted(severity_counts.items())
                                ]
                            }
                        }
                        for day, severity_counts in sorted(daily_counts.items())
                    ]
                },
                "total_severity": {
                    "buckets": [
                        {
                            "key": severity,
                            "doc_count": count
                        }
                        for severity, count in sorted(total_severity_counts.items())
                    ]
                }
            }

            return {
                "total_documents": total_docs,
                "execution_time": execution_time,
                "documents_per_second": total_docs/execution_time,
                "aggregations": aggregations
            }

        except Exception as e:
            logger.error(f"Error performing Iceberg aggregation: {str(e)}")
            logger.exception("Full traceback:")
            raise

if __name__ == "__main__":
    try:
        tester = IcebergPerformanceTest()
        results = tester.perform_aggregation()
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")