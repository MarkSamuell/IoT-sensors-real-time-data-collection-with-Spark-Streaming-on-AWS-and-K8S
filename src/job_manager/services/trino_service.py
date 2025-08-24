from trino import dbapi
import pandas as pd
import logging
import io
import csv
from urllib3.exceptions import MaxRetryError, NewConnectionError
import socket

logger = logging.getLogger(__name__)

class TrinoService:
    def __init__(self, host=None, port=None, user='hadoop', catalog='iceberg', schema='uraeus_db'):
        """
        Initialize Trino service with connection parameters
        
        Args:
            host (str): Trino server hostname
            port (str): Trino server port
            user (str): Username for Trino
            catalog (str): Catalog name
            schema (str): Schema name
        """
        self.host = host
        self.port = port  # Keep as string here, convert to int when needed
        self.user = user
        self.catalog = catalog
        self.schema = schema
        
    def test_connection(self):
        """Test if Trino server is reachable"""
        if not self.host or not self.port:
            return False
            
        try:
            with socket.create_connection((self.host, int(self.port)), timeout=5):
                return True
        except (socket.timeout, ConnectionRefusedError, socket.gaierror) as e:
            logging.error(f"Connection failed: {e}")
            return False
    
    def get_connection(self):
        """Create a connection with error handling"""
        if not self.test_connection():
            raise ConnectionError(f"Cannot connect to Trino server at {self.host}:{self.port}")
        
        try:
            conn = dbapi.connect(
                host=self.host,
                port=int(self.port),  # Convert port to int here
                user=self.user,
                catalog=self.catalog,
                schema=self.schema
            )
            return conn
        except Exception as e:
            logging.error(f"Connection error: {e}")
            raise ConnectionError(f"Failed to establish Trino connection: {e}")
    
    def execute_query(self, query):
        """
        Execute a query with robust type conversion and error handling
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            dict: {
                'success': bool,
                'columns': list[str],
                'data': list[dict],
                'row_count': int,
                'message': str
            }
        """
        if not self.host or not self.port:
            return {
                'success': False,
                'message': 'Connection parameters not set',
                'columns': [],
                'data': [],
                'row_count': 0
            }

        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(query)
                
                # Get column names (handle case where description is None)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                
                # Fetch all rows and convert to proper Python types
                rows = cur.fetchall()
                data = []
                
                for row in rows:
                    row_dict = {}
                    for idx, value in enumerate(row):
                        col_name = columns[idx]
                        
                        # Convert numeric values (including strings that represent numbers)
                        if value is not None:
                            try:
                                # Handle PostgreSQL numeric types (Decimal)
                                if hasattr(value, 'to_eng_string'):
                                    value = float(value)
                                # Handle string representations of numbers
                                elif isinstance(value, str) and value.replace('.', '', 1).isdigit():
                                    value = float(value)
                            except (ValueError, AttributeError):
                                pass
                        
                        row_dict[col_name] = value
                    
                    data.append(row_dict)
                
                # Ensure count columns are properly typed
                for col in columns:
                    if col.lower().startswith('count') or col.lower().endswith('count'):
                        for row in data:
                            if row[col] is not None:
                                row[col] = int(float(row[col]))
                
                return {
                    'success': True,
                    'columns': columns,
                    'data': data,
                    'row_count': len(data),
                    'message': f"Successfully returned {len(data)} rows"
                }
                
        except ConnectionError as e:
            logging.error(f"Connection error executing query: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f"Database connection failed: {str(e)}",
                'columns': [],
                'data': [],
                'row_count': 0
            }
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}", exc_info=True)
            return {
                'success': False,
                'message': f"Query execution failed: {str(e)}",
                'columns': [],
                'data': [],
                'row_count': 0
            }
    
    def get_csv_data(self, query):
        """
        Execute a query and return CSV data
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            tuple: (csv_data, error_message)
        """
        try:
            result = self.execute_query(query)
            
            if not result['success']:
                return None, result['message']
                
            # Convert to CSV
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow(result['columns'])
            
            # Write data rows
            for row in result['data']:
                writer.writerow([row[col] for col in result['columns']])
                
            return output.getvalue(), None
        except Exception as e:
            logger.error(f"Error generating CSV: {str(e)}")
            return None, f"Error generating CSV: {str(e)}"
    
    def get_tables(self):
        """
        Get list of available tables in the schema
        
        Returns:
            dict: Tables with status information
        """
        if not self.host or not self.port:
            return {
                'success': False,
                'message': 'Connection parameters not set'
            }
            
        query = f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = '{self.schema}'
        ORDER BY table_schema, table_name
        """
        return self.execute_query(query)