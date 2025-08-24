import os
import sys
import logging

# Add the directory containing the wheel file to Python path
wheel_dir = '/mnt/tmp'
sys.path.insert(0, wheel_dir)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting spark_runner.py")
    try:
        if len(sys.argv) != 2:
            logger.error("Usage: spark_runner.py <job_type>")
            sys.exit(1)
        
        # Print Python path for debugging
        logger.info(f"Python path: {sys.path}")
        
        # Import main after setting up path
        from streaming_pipeline.main import main
        
        job_type = sys.argv[1]
        logger.info(f"Job type: {job_type}")
        main(job_type)
        logger.info("Job completed successfully")
    except ImportError as e:
        logger.error(f"Import error: {e}")
        logger.error(f"Python path: {sys.path}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"An error occurred while running the job: {str(e)}")
        sys.exit(1)