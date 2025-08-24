from flask import render_template, request, jsonify, make_response
import os
import logging
from services.trino_service import TrinoService

logger = logging.getLogger(__name__)

def register_query_tool_routes(app):
    """Register query tool routes with the Flask app"""
    
    @app.route('/query_tool')
    def query_tool():
        """Render the query tool page with default connection settings"""
        # Default connection settings - don't set host/port by default to prevent auto-connection
        connection = {
            'host': '',
            'port': '',
            'user': os.getenv('TRINO_USER', 'hadoop'),
            'catalog': os.getenv('TRINO_CATALOG', 'iceberg'),
            'schema': os.getenv('TRINO_SCHEMA', 'uraeus_db')
        }
        
        # Check if there are connection parameters in the query string
        if request.args.get('host'):
            connection['host'] = request.args.get('host')
        if request.args.get('port'):
            connection['port'] = request.args.get('port')
        if request.args.get('user'):
            connection['user'] = request.args.get('user')
        if request.args.get('catalog'):
            connection['catalog'] = request.args.get('catalog')
        if request.args.get('schema'):
            connection['schema'] = request.args.get('schema')
        
        return render_template('query_tool.html', connection=connection)

    @app.route('/api/get_tables', methods=['POST'])
    def get_tables():
        """Get list of available tables with connection parameters"""
        try:
            data = request.json
            
            # Get connection parameters
            host = data.get('host')
            port = data.get('port')
            user = data.get('user', 'hadoop')
            catalog = data.get('catalog', 'iceberg')
            schema = data.get('schema', 'uraeus_db')
            
            # Validate required parameters
            if not host or not port:
                return jsonify({
                    'success': False,
                    'message': 'Host and port are required'
                })
            
            # Create Trino service with the provided connection details
            service = TrinoService(host, port, user, catalog, schema)
            
            result = service.get_tables()
            return jsonify(result)
        
        except ConnectionError as e:
            logger.error(f"Connection error fetching tables: {e}")
            return jsonify({
                'success': False,
                'message': f'Connection Error: {str(e)}'
            }), 503  # Service Unavailable
        
        except Exception as e:
            logger.exception("Table listing error")
            return jsonify({
                'success': False,
                'message': f'Error fetching tables: {str(e)}'
            }), 500

    @app.route('/api/execute_query', methods=['POST'])
    def execute_query():
        """Execute a Trino query and return results"""
        data = request.json
        query = data.get('query', '')
        
        # Get connection parameters
        host = data.get('host')
        port = data.get('port')
        user = data.get('user', 'hadoop')
        catalog = data.get('catalog', 'iceberg')
        schema = data.get('schema', 'uraeus_db')
        
        # Validate query and connection parameters
        if not query.strip():
            return jsonify({
                'success': False,
                'message': 'Query cannot be empty'
            })
            
        if not host or not port:
            return jsonify({
                'success': False,
                'message': 'Host and port are required'
            })
        
        # Create Trino service with the provided connection details
        service = TrinoService(host, port, user, catalog, schema)
        
        try:
            result = service.execute_query(query)
            return jsonify(result)
        except Exception as e:
            logger.exception("Query execution error")
            return jsonify({
                'success': False,
                'message': f'Error executing query: {str(e)}'
            })

    @app.route('/api/download_csv', methods=['POST'])
    def download_csv():
        """Download query results as CSV"""
        data = request.json
        query = data.get('query', '')
        
        # Get connection parameters
        host = data.get('host')
        port = data.get('port')
        user = data.get('user', 'hadoop')
        catalog = data.get('catalog', 'iceberg')
        schema = data.get('schema', 'uraeus_db')
        
        # Validate query and connection parameters
        if not query.strip():
            return jsonify({
                'success': False,
                'message': 'Query cannot be empty'
            })
            
        if not host or not port:
            return jsonify({
                'success': False,
                'message': 'Host and port are required'
            })
        
        # Create Trino service with the provided connection details
        service = TrinoService(host, port, user, catalog, schema)
        
        try:
            csv_data, error = service.get_csv_data(query)
            
            if error:
                return jsonify({
                    'success': False,
                    'message': error
                })
            
            response = make_response(csv_data)
            response.headers["Content-Disposition"] = "attachment; filename=query_results.csv"
            response.headers["Content-Type"] = "text/csv"
            
            return response
        except Exception as e:
            logger.exception("CSV download error")
            return jsonify({
                'success': False,
                'message': f'Error generating CSV: {str(e)}'
            })