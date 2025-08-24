import os
import boto3
from botocore.exceptions import ClientError
from .job_config import JOB_CONFIGURATIONS
import logging

logger = logging.getLogger(__name__)

class EMRJobManager:
    def __init__(self, region_name='eu-central-1'):
        """
        Initialize EMR Job Manager with AWS boto3 client
        
        Args:
            region_name (str): AWS region name
        """
        self.emr_client = boto3.client('emr', region_name=region_name)
        self.logs_client = boto3.client('logs', region_name=region_name)
    
    def list_active_clusters(self):
        """
        List all active EMR clusters
        
        Returns:
            list: Active EMR clusters with their details
        """
        try:
            response = self.emr_client.list_clusters(
                ClusterStates=['RUNNING', 'WAITING']
            )
            return response.get('Clusters', [])
        except ClientError as e:
            logger.error(f"Error listing clusters: {e}")
            return []
    
    def get_available_job_types(self):
        """
        Get list of available job types
        
        Returns:
            list: Available job types
        """
        return list(JOB_CONFIGURATIONS.keys())
    
    def submit_job(self, cluster_id, job_type):
        """
        Submit a job to a specific EMR cluster
        
        Args:
            cluster_id (str): EMR Cluster ID
            job_type (str): Type of job to submit
        
        Returns:
            str: Step ID of the submitted job
        """
        # Validate job type
        if job_type not in JOB_CONFIGURATIONS:
            raise ValueError(f"Invalid job type: {job_type}")
        
        # Get job configuration
        job_config = JOB_CONFIGURATIONS[job_type]
        
        try:
            response = self.emr_client.add_job_flow_steps(
                JobFlowId=cluster_id,
                Steps=[{
                    'Name': job_config['name'],
                    'ActionOnFailure': job_config.get('action_on_failure', 'CONTINUE'),
                    'HadoopJarStep': job_config['hadoop_jar_step']
                }]
            )
            
            # Return the Step ID
            return response['StepIds'][0]
        except ClientError as e:
            logger.error(f"Error submitting job: {e}")
            raise
    
    def list_cluster_steps(self, cluster_id, state_filter=None):
        """
        List steps for a specific cluster
        
        Args:
            cluster_id (str): EMR Cluster ID
            state_filter (list, optional): Filter steps by state
        
        Returns:
            list: Cluster steps
        """
        try:
            # Default filter to show all steps
            if not state_filter:
                state_filter = ['PENDING', 'RUNNING', 'COMPLETED', 'CANCELLED', 'FAILED']
            
            response = self.emr_client.list_steps(
                ClusterId=cluster_id,
                StepStates=state_filter
            )
            return response.get('Steps', [])
        except ClientError as e:
            logger.error(f"Error listing cluster steps: {e}")
            return []
    
    def describe_cluster(self, cluster_id):
        """
        Get detailed information about a specific cluster
        
        Args:
            cluster_id (str): EMR Cluster ID
        
        Returns:
            dict: Cluster details
        """
        try:
            response = self.emr_client.describe_cluster(ClusterId=cluster_id)
            return response.get('Cluster', {})
        except ClientError as e:
            logger.error(f"Error describing cluster: {e}")
            return {}
    
    def cancel_step(self, cluster_id, step_id):
        """
        Cancel a running step
        
        Args:
            cluster_id (str): EMR Cluster ID
            step_id (str): Step ID to cancel
        """
        try:
            self.emr_client.cancel_steps(
                ClusterId=cluster_id,
                StepIds=[step_id]
            )
        except ClientError as e:
            logger.error(f"Error cancelling step: {e}")
            raise
            
    def get_step(self, cluster_id, step_id):
        """
        Get detailed information about a specific step
        
        Args:
            cluster_id (str): EMR Cluster ID
            step_id (str): Step ID
            
        Returns:
            dict: Step details
        """
        try:
            steps = self.list_cluster_steps(cluster_id)
            for step in steps:
                if step['Id'] == step_id:
                    return step
            return None
        except Exception as e:
            logger.error(f"Error getting step: {e}")
            return None
            
    def get_step_logs(self, cluster_id, step_id):
        """
        Get logs for a specific step
        
        Args:
            cluster_id (str): EMR Cluster ID
            step_id (str): Step ID
            
        Returns:
            str: Logs content
        """
        try:
            # Get cluster details to find log location
            cluster = self.describe_cluster(cluster_id)
            if not cluster or not cluster.get('LogUri'):
                return "Log URI not available for this cluster"
                
            # Get step details
            step = self.get_step(cluster_id, step_id)
            if not step:
                return "Step not found"
                
            # For now, let's just return a dummy log message
            # In a real implementation, you would fetch logs from S3 or CloudWatch
            return f"Logs for step {step_id} in cluster {cluster_id}\n\n" + \
                   f"Step name: {step.get('Name', 'Unknown')}\n" + \
                   f"Status: {step.get('Status', {}).get('State', 'Unknown')}\n" + \
                   f"Log location: {cluster.get('LogUri')}\n\n" + \
                   "This is a placeholder for the actual logs content."
                
        except Exception as e:
            logger.error(f"Error getting step logs: {e}")
            return f"Error retrieving logs: {str(e)}"