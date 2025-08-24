from trino import dbapi
from contextlib import contextmanager
from typing import List, Dict, Any
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class TrinoClient:
    def __init__(self, host: str, port: int = 8889, user: str = 'hadoop',
                 catalog: str = 'iceberg', schema: str = 'uraeus_db'):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema

    @contextmanager
    def get_connection(self):
        conn = dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str) -> pd.DataFrame:
        logger.info(f"Executing query: {query}")
        with self.get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(query)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                return pd.DataFrame(rows, columns=columns)
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                raise
            finally:
                cur.close()

    def get_security_events(self, limit: int = 1000) -> pd.DataFrame:
        query = f"""
        SELECT severity, count(*) as count
        FROM security_events
        group by severity
        """
        return self.execute_query(query)

    def get_event_logs(self, limit: int = 1000) -> pd.DataFrame:
        query = f"""
        SELECT *
        FROM event_logs
        LIMIT {limit}
        """
        return self.execute_query(query)

# Usage example
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    try:
        # Initialize client
        trino_client = TrinoClient(
            host='localhost',  # EMR primary node private IP
            port=9010,
            user='trino'         # Default user name for Trino
        )

        # Get security events
        security_events_df = trino_client.get_security_events()
        print("Security Events:")
        print(security_events_df)

        # Get event logs
        event_logs_df = trino_client.get_event_logs(limit=5)
        print("\nEvent Logs:")
        print(event_logs_df)

    except Exception as e:
        logger.error(f"Error: {str(e)}")