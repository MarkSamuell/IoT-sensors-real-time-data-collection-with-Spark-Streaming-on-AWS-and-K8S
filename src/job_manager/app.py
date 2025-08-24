import os
from flask import Flask, make_response, render_template, request, redirect, url_for, flash, jsonify
from dotenv import load_dotenv
from services.emr_service import EMRJobManager
from services.trino_service import TrinoService
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__, 
            static_folder='static',
            static_url_path='/static')
app.secret_key = os.getenv('SECRET_KEY', 'your_default_secret_key')

# Initialize EMR Job Manager
emr_manager = EMRJobManager()

# Global Trino Service singleton
trino_service = None

def get_trino_service(host=None, port=None, user=None, catalog=None, schema=None):
    """Get or create Trino service instance with optional parameter override"""
    global trino_service
    
    # If specific parameters are provided, create a new instance
    if host and port:
        return TrinoService(host, port, 
                           user or os.getenv('TRINO_USER', 'hadoop'),
                           catalog or os.getenv('TRINO_CATALOG', 'iceberg'),
                           schema or os.getenv('TRINO_SCHEMA', 'uraeus_db'))
    
    # Otherwise, use or create the singleton
    if trino_service is None:
        trino_service = TrinoService(
            os.getenv('TRINO_HOST', ''),  # Default to empty to prevent auto-connection
            os.getenv('TRINO_PORT', ''),  # Default to empty to prevent auto-connection
            os.getenv('TRINO_USER', 'hadoop'),
            os.getenv('TRINO_CATALOG', 'iceberg'),
            os.getenv('TRINO_SCHEMA', 'uraeus_db')
        )
    return trino_service

#
# Main App Routes
#

@app.route('/')
def index():
    """Render the main dashboard with available jobs and quick actions."""
    try:
        active_clusters = emr_manager.list_active_clusters()
        return render_template('index.html', clusters=active_clusters)
    except Exception as e:
        flash(f"Error retrieving clusters: {str(e)}", 'error')
        return render_template('index.html', clusters=[])

@app.route('/submit_job', methods=['GET', 'POST'])
def submit_job():
    """Handle job submission form."""
    if request.method == 'POST':
        try:
            job_type = request.form.get('job_type')
            cluster_id = request.form.get('cluster_id')
            
            # Submit the job
            step_id = emr_manager.submit_job(cluster_id, job_type)
            
            flash(f"Job {job_type} submitted successfully. Step ID: {step_id}", 'success')
            return redirect(url_for('index'))
        except Exception as e:
            flash(f"Error submitting job: {str(e)}", 'error')
    
    # Get available job types and clusters for the form
    job_types = emr_manager.get_available_job_types()
    clusters = emr_manager.list_active_clusters()
    
    # Get pre-selected cluster ID from query parameters
    preselected_cluster_id = request.args.get('cluster_id')
    preselected_job_type = request.args.get('job_type')
    
    return render_template('submit_job.html', 
                          job_types=job_types, 
                          clusters=clusters, 
                          preselected_cluster_id=preselected_cluster_id,
                          preselected_job_type=preselected_job_type)

@app.route('/job_status/<cluster_id>')
def job_status(cluster_id):
    """Show status of jobs in a specific cluster."""
    try:
        step_id = request.args.get('step_id')
        steps = emr_manager.list_cluster_steps(cluster_id)
        cluster_details = emr_manager.describe_cluster(cluster_id)
        
        if step_id:
            # If a specific step is requested, find it and pass it to the template
            selected_step = next((step for step in steps if step['Id'] == step_id), None)
            return render_template('job_status.html', 
                                 steps=steps, 
                                 cluster_id=cluster_id, 
                                 cluster_details=cluster_details,
                                 selected_step=selected_step)
        
        return render_template('job_status.html', 
                             steps=steps, 
                             cluster_id=cluster_id, 
                             cluster_details=cluster_details)
    except Exception as e:
        flash(f"Error retrieving job status: {str(e)}", 'error')
        return redirect(url_for('index'))

@app.route('/stop_job', methods=['POST'])
def stop_job():
    """Stop a running job."""
    try:
        step_id = request.form.get('step_id')
        cluster_id = request.form.get('cluster_id')
        
        emr_manager.cancel_step(cluster_id, step_id)
        
        flash(f"Step {step_id} cancelled successfully", 'success')
        return redirect(url_for('job_status', cluster_id=cluster_id))
    except Exception as e:
        flash(f"Error stopping job: {str(e)}", 'error')
        return redirect(url_for('index'))

@app.route('/all_jobs/<cluster_id>')
def all_jobs(cluster_id):
    """View all jobs for a specific cluster"""
    try:
        # Get cluster details
        cluster_details = emr_manager.describe_cluster(cluster_id)
        
        # Get all jobs (including completed, failed, running)
        steps = emr_manager.list_cluster_steps(
            cluster_id, 
            state_filter=['PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED']
        )
        
        # Sort steps more safely
        def get_start_time(step):
            try:
                timeline = step.get('Status', {}).get('Timeline', {})
                start_time = timeline.get('StartDateTime')
                if start_time:
                    return start_time
                return timeline.get('CreationDateTime', datetime.min)
            except:
                return datetime.min
        
        steps = sorted(steps, key=get_start_time, reverse=True)
        
        return render_template(
            'all_jobs.html', 
            cluster_id=cluster_id, 
            cluster_details=cluster_details, 
            steps=steps
        )
    except Exception as e:
        logger.error(f"Error retrieving jobs: {str(e)}", exc_info=True)
        flash(f"Error retrieving jobs: {str(e)}", 'error')
        return redirect(url_for('index'))

@app.route('/job_details/<cluster_id>/<step_id>')
def job_details(cluster_id, step_id):
    """Display detailed information for a specific job step."""
    try:
        # Get cluster details
        cluster_details = emr_manager.describe_cluster(cluster_id)
        
        # Get step information
        steps = emr_manager.list_cluster_steps(cluster_id)
        step = next((s for s in steps if s.get('Id') == step_id), None)
        
        if not step:
            flash(f"Step {step_id} not found in cluster {cluster_id}", "error")
            return redirect(url_for('all_jobs', cluster_id=cluster_id))
        
        # Extract all available step information as flat key-value pairs
        step_info = {}
        
        # Basic step info
        step_info['Step ID'] = step.get('Id', 'N/A')
        step_info['Name'] = step.get('Name', 'N/A')
        step_info['Status'] = step.get('Status', {}).get('State', 'N/A')
        
        # Extract ALL configuration details recursively
        if 'Config' in step:
            # Add action on failure
            step_info['Action On Failure'] = step['Config'].get('ActionOnFailure', 'N/A')
            
            # Add HadoopJarStep details if present
            if 'HadoopJarStep' in step['Config']:
                jar_step = step['Config']['HadoopJarStep']
                step_info['JAR File'] = jar_step.get('Jar', 'N/A')
                if jar_step.get('MainClass'):
                    step_info['Main Class'] = jar_step.get('MainClass')
                
                # Store arguments separately for special formatting
                if 'Args' in jar_step and jar_step['Args']:
                    step_info['Arguments'] = jar_step['Args']
        
        # Add timeline information
        if 'Status' in step and 'Timeline' in step['Status']:
            timeline = step['Status']['Timeline']
            if 'CreationDateTime' in timeline:
                step_info['Created At'] = timeline['CreationDateTime'].strftime('%Y-%m-%d %H:%M:%S')
            if 'StartDateTime' in timeline:
                step_info['Started At'] = timeline['StartDateTime'].strftime('%Y-%m-%d %H:%M:%S')
            if 'EndDateTime' in timeline:
                step_info['Ended At'] = timeline['EndDateTime'].strftime('%Y-%m-%d %H:%M:%S')
        
        # Pass the original step object too for advanced use cases
        is_running = step.get('Status', {}).get('State') == 'RUNNING'
        
        debug_mode = app.config.get('DEBUG', False)
        return render_template(
            'job_details.html',
            cluster_id=cluster_id,
            cluster_details=cluster_details,
            step=step,
            step_info=step_info,
            is_running=is_running,
            debug_mode=debug_mode
        )
    except Exception as e:
        logger.error(f"Error retrieving job details: {str(e)}")
        flash(f"Error retrieving job details: {str(e)}", "error")
        return redirect(url_for('index'))

@app.route('/download_logs/<cluster_id>/<step_id>')
def download_logs(cluster_id, step_id):
    """Download the log file for a specific job step."""
    try:
        logs = emr_manager.get_step_logs(cluster_id, step_id)
        step = emr_manager.get_step(cluster_id, step_id)
        step_name = step.get('Name', 'unknown').replace(' ', '_').lower()
        
        # Create a response with the logs
        response = make_response(logs)
        response.headers["Content-Disposition"] = f"attachment; filename={step_name}_{step_id}.log"
        response.headers["Content-Type"] = "text/plain"
        return response
    except Exception as e:
        logger.error(f"Error downloading logs: {str(e)}")
        flash(f"Error downloading logs: {str(e)}", "error")
        return redirect(url_for('job_details', cluster_id=cluster_id, step_id=step_id))

#
# API Routes
#

@app.route('/api/cluster_steps/<cluster_id>')
def get_cluster_steps(cluster_id):
    """Get steps for a specific cluster."""
    try:
        logger.info(f"Fetching steps for cluster: {cluster_id}")
        steps = emr_manager.list_cluster_steps(cluster_id)
        
        logger.info(f"Retrieved {len(steps)} steps for cluster {cluster_id}")
        
        # Sort steps by creation time (newest first)
        steps.sort(
            key=lambda x: x.get('Status', {}).get('Timeline', {}).get('CreationDateTime', ''),
            reverse=True
        )
        
        # Limit to the 10 most recent steps
        recent_steps = steps[:10]
        logger.info(f"Returning {len(recent_steps)} recent steps")
        return jsonify(recent_steps)
    except Exception as e:
        logger.error(f"Error retrieving steps for cluster {cluster_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/job_counts')
def get_job_counts():
    """Get counts of running, completed, and failed jobs."""
    try:
        clusters = emr_manager.list_active_clusters()
        
        running = 0
        completed = 0
        failed = 0
        
        # Get counts from all active clusters
        for cluster in clusters:
            steps = emr_manager.list_cluster_steps(cluster['Id'])
            for step in steps:
                state = step['Status']['State']
                if state in ['RUNNING', 'PENDING']:
                    running += 1
                elif state == 'COMPLETED':
                    completed += 1
                elif state in ['FAILED', 'CANCELLED']:
                    failed += 1
        
        return jsonify({
            'running': running,
            'completed': completed,
            'failed': failed
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'running': 0,
            'completed': 0,
            'failed': 0
        }), 500

@app.route('/api/running_jobs')
def get_running_jobs():
    """Get all currently running jobs across all active clusters."""
    try:
        running_jobs = []
        
        # Get all active clusters
        clusters = emr_manager.list_active_clusters()
        
        # Iterate through clusters and fetch their running steps
        for cluster in clusters:
            steps = emr_manager.list_cluster_steps(
                cluster['Id'], 
                state_filter=['RUNNING', 'PENDING']
            )
            
            # Transform steps into a standardized format
            for step in steps:
                running_jobs.append({
                    'step_id': step['Id'],
                    'job_name': step['Name'],
                    'cluster_id': cluster['Id'],
                    'cluster_name': cluster['Name']
                })
        
        return jsonify(running_jobs)
    except Exception as e:
        logger.error(f"Error fetching running jobs: {str(e)}")
        return jsonify({
            'error': str(e),
            'running_jobs': []
        }), 500

@app.route('/api/cluster_details/<cluster_id>')
def get_cluster_details(cluster_id):
    """Get detailed information about a specific cluster."""
    try:
        cluster_details = emr_manager.describe_cluster(cluster_id)
        
        # Format the response to match expected structure
        return jsonify({
            'id': cluster_details.get('Id', ''),
            'name': cluster_details.get('Name', ''),
            'status': cluster_details.get('Status', {}).get('State', ''),
            'created': cluster_details.get('Status', {}).get('Timeline', {}).get('CreationDateTime', '').isoformat() if 
                       cluster_details.get('Status', {}).get('Timeline', {}).get('CreationDateTime') else '',
            'masterDns': cluster_details.get('MasterPublicDnsName', ''),
            'instanceCount': str(len(cluster_details.get('InstanceGroups', []))),
            'applications': [app.get('Name', '') for app in cluster_details.get('Applications', [])]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/step_details/<step_id>')
def get_step_details(step_id):
    """Get detailed information about a specific step."""
    try:
        cluster_id = request.args.get('cluster_id')
        if not cluster_id:
            return jsonify({"error": "Missing cluster_id parameter"}), 400
            
        # Get all steps for the cluster
        steps = emr_manager.list_cluster_steps(cluster_id)
        
        # Find the specific step
        step = next((s for s in steps if s.get('Id') == step_id), None)
        
        if not step:
            return jsonify({"error": f"Step {step_id} not found"}), 404
            
        return jsonify(step)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/logs/<cluster_id>/<step_id>')
def get_logs(cluster_id, step_id):
    """Get logs for a specific job step."""
    try:
        # Get logs from EMR
        logs = emr_manager.get_step_logs(cluster_id, step_id)
        return jsonify({
            'logs': logs,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error retrieving logs: {str(e)}")
        return jsonify({
            'error': str(e),
            'logs': None
        }), 500

#
# Trino Query Tool Routes
#

@app.route('/query_tool')
def query_tool():
    """Render the query tool page with default connection settings."""
    # Default connection settings - leave host/port empty to prevent auto-connection
    connection = {
        'host': '',  # Start with empty to prevent automatic connection
        'port': '',  # Start with empty to prevent automatic connection
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
    """Get list of available tables with connection parameters."""
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
        service = get_trino_service(host, port, user, catalog, schema)
        
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
    """Execute a Trino query and return results."""
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
    service = get_trino_service(host, port, user, catalog, schema)
    
    try:
        result = service.execute_query(query)
        return jsonify(result)
    except Exception as e:
        logger.exception("Query execution error")
        return jsonify({
            'success': False,
            'message': f'Error executing query: {str(e)}'
        })

@app.route('/api/download_csv', methods=['GET'])
def download_csv():
    """Download query results as CSV using GET method with query parameters."""
    # Get parameters from query string (GET request)
    query = request.args.get('query', '')
    
    # Get connection parameters
    host = request.args.get('host')
    port = request.args.get('port')
    user = request.args.get('user', 'hadoop')
    catalog = request.args.get('catalog', 'iceberg')
    schema = request.args.get('schema', 'uraeus_db')
    
    # Validate query and connection parameters
    if not query.strip():
        flash('Query cannot be empty', 'error')
        return redirect(url_for('query_tool'))
        
    if not host or not port:
        flash('Host and port are required', 'error')
        return redirect(url_for('query_tool'))
    
    # Create Trino service with the provided connection details
    service = get_trino_service(host, port, user, catalog, schema)
    
    try:
        csv_data, error = service.get_csv_data(query)
        
        if error:
            flash(f'Error generating CSV: {error}', 'error')
            return redirect(url_for('query_tool'))
        
        # Create filename based on timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"query_results_{timestamp}.csv"
        
        response = make_response(csv_data)
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        response.headers["Content-Type"] = "text/csv"
        
        return response
    except Exception as e:
        logger.exception("CSV download error")
        flash(f'Error generating CSV: {str(e)}', 'error')
        return redirect(url_for('query_tool'))
    
#
# Visualizer routes
#

@app.route('/api/get_columns', methods=['POST'])
def get_columns():
    """Get column information for a specific table"""
    try:
        data = request.json
        
        # Get table information
        schema_name = data.get('schema')
        table_name = data.get('table')
        
        # Get connection parameters
        host = data.get('host')
        port = data.get('port')
        user = data.get('user', 'hadoop')
        catalog = data.get('catalog', 'iceberg')
        
        # Validate required parameters
        if not host or not port:
            return jsonify({
                'success': False,
                'message': 'Host and port are required'
            })
        
        if not schema_name or not table_name:
            return jsonify({
                'success': False,
                'message': 'Schema and table names are required'
            })
        
        # Create Trino service with the provided connection details
        service = get_trino_service(host, port, user, catalog, schema_name)
        
        # Build query to get column information
        query = f"""
            SELECT 
                column_name, 
                data_type,
                ordinal_position
            FROM 
                {catalog}.information_schema.columns
            WHERE 
                table_schema = '{schema_name}'
                AND table_name = '{table_name}'
            ORDER BY 
                ordinal_position
        """
        
        # Execute query
        result = service.execute_query(query)
        
        if not result['success']:
            return jsonify({
                'success': False,
                'message': result.get('message', 'Failed to retrieve column information')
            })
        
        return jsonify({
            'success': True,
            'table': table_name,
            'schema': schema_name,
            'columns': result['data']
        })
    
    except Exception as e:
        logger.exception("Error fetching column information")
        return jsonify({
            'success': False,
            'message': f'Error retrieving column information: {str(e)}'
        }), 500

#
# Error Handlers
#

@app.errorhandler(404)
def page_not_found(e):
    """Custom 404 error handler."""
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    """Custom 500 error handler."""
    return render_template('500.html'), 500

#
# Main Entry Point
#

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)