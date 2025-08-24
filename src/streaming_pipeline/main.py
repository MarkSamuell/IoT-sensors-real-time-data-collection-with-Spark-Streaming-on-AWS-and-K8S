import sys
import logging
from pyspark.sql import SparkSession
from streaming_pipeline.jobs.ui_sevs import analyze as ui_sevs_analyze
from streaming_pipeline.jobs.ui_event_logs import analyze as ui_event_logs_analyze
from streaming_pipeline.jobs.ui_sevs_aggs import analyze as ui_sevs_aggs_analyze
from streaming_pipeline.jobs.ui_event_logs_aggs import analyze as ui_event_logs_aggs_analyze
from streaming_pipeline.jobs.ui_batch_sevs_aggs import analyze as ui_batch_sevs_analyze
from streaming_pipeline.jobs.ui_batch_logs_aggs import analyze as ui_batch_logs_analyze
from streaming_pipeline.jobs.ui_batch_late_events_aggs.analyze import analyze as ui_batch_late_logs_analyze
from streaming_pipeline.jobs.ice_event_logs import analyze as ice_event_logs_analyze
from streaming_pipeline.jobs.ice_sevs import analyze as ice_sevs_analyze
from streaming_pipeline.jobs.ice_event_logs_aggs import analyze as ice_event_logs_aggs_analyze
from streaming_pipeline.jobs.ice_sevs_aggs import analyze as ice_sevs_aggs_analyze
from streaming_pipeline.jobs.api_logs import analyze as api_security_analyze
from configs.config import Config

# Try to import ML modules, but continue if they're not available
try:
    from streaming_pipeline.jobs.ml_lstm_model.batch_analyze import analyze as can_ml_batch_analyze
    from streaming_pipeline.jobs.ml_lstm_model.stream_analyze import analyze as can_ml_stream_analyze
    ml_modules_available = True
except ImportError:
    ml_modules_available = False
    logging.warning("ML modules not available - can_ml_batch and can_ml_stream jobs will be disabled")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(job_type):
    logger.info(f"Starting job with type: {job_type}")

    builder = SparkSession.builder \
        .appName(f"DataProcessing_{job_type}") \
        .config("spark.executor.instances", Config.NUM_EXECUTORS) \
        .config("spark.dynamicAllocation.maxExecutors", Config.NUM_EXECUTORS * 3) \
        .config("spark.dynamicAllocation.minExecutors", Config.NUM_EXECUTORS - 1) \
        .config("spark.dynamicAllocation.initialExecutors", Config.NUM_EXECUTORS) \
        .config("spark.executor.cores", Config.EXECUTOR_CORES) \
        .config("spark.sql.shuffle.partitions", Config.OPTIMAL_PARTITIONS) \
        .config("spark.default.parallelism", Config.OPTIMAL_PARTITIONS) \
        .config("spark.streaming.concurrentJobs=", Config.OPTIMAL_PARTITIONS) \
        .config("spark.task.cpus", Config.EXECUTOR_CORES) \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", Config.NUM_EXECUTORS) \
        .config("spark.streaming.kafka.allowNonConsecutiveOffsets", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "20") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.executor.memory", "3g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.executor.memoryOverhead", "1g")
    
    if job_type.startswith('ice_'):
        builder = builder \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.demo.warehouse", Config.ICEBERG_WAREHOUSE) \
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    
    if job_type.startswith('can_ml') and ml_modules_available:
        builder = builder \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")

    spark = builder.getOrCreate()
    logger.info(f"Created SparkSession with {Config.OPTIMAL_PARTITIONS} shuffle partitions")

    # UI Jobs (Elasticsearch)
    if job_type == 'ui_sevs':
        logger.info("Running UI SEVs analysis (Elasticsearch)")
        ui_sevs_analyze(spark)
    elif job_type == 'ui_event_logs':
        logger.info("Running UI Event Logs analysis (Elasticsearch)")
        ui_event_logs_analyze(spark)
    elif job_type == 'ui_sevs_aggs':
        logger.info("Running UI SEVs Aggregations (Elasticsearch)")
        ui_sevs_aggs_analyze(spark)
    elif job_type == 'ui_event_logs_aggs':
        logger.info("Running UI Event Logs Aggregations (Elasticsearch)")
        ui_event_logs_aggs_analyze(spark)
    elif job_type == 'ui_batch_sevs_aggs':
        logger.info("Running UI Batch SEVs Aggregation (Elasticsearch)")
        ui_batch_sevs_analyze(spark)
    elif job_type == 'ui_batch_logs_aggs':
        logger.info("Running UI Batch Event Logs Aggregation (Elasticsearch)")
        ui_batch_logs_analyze(spark)
    elif job_type == 'ui_batch_late_logs':
        logger.info("Running UI Batch Late SEVs Aggregation (Elasticsearch)")
        ui_batch_late_logs_analyze(spark)
        
    # Iceberg Jobs
    elif job_type == 'ice_event_logs':
        logger.info("Running Event Logs analysis (Iceberg)")
        ice_event_logs_analyze(spark)
    elif job_type == 'ice_sevs':
        logger.info("Running SEVs analysis (Iceberg)")
        ice_sevs_analyze(spark)
    elif job_type == 'ice_event_logs_aggs':
        logger.info("Running Event Logs Aggregations (Iceberg)")
        ice_event_logs_aggs_analyze(spark)
    elif job_type == 'ice_sevs_aggs':
        logger.info("Running SEVs Aggregations (Iceberg)")
        ice_sevs_aggs_analyze(spark)
        
    # ML Jobs - Only run if modules are available
    elif job_type == 'can_ml_batch':
        if ml_modules_available:
            logger.info("Running CAN Messages ML Batch Scoring")
            can_ml_batch_analyze(spark)
        else:
            logger.error("ML modules not available - cannot run can_ml_batch job")
            sys.exit(1)
    elif job_type == 'can_ml_stream':
        if ml_modules_available:
            logger.info("Running CAN Messages ML Stream Scoring")
            can_ml_stream_analyze(spark)
        else:
            logger.error("ML modules not available - cannot run can_ml_stream job")
            sys.exit(1)

    # Api_logs_job
    elif job_type == 'api_logs':
        logger.info("Running API Security Events Processing")
        api_security_analyze(spark)
                             
    else:
        logger.error(f"Unknown job type: {job_type}")
        sys.exit(1)
        
    logger.info("Job completed successfully")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python main.py <job_type>")
        sys.exit(1)
    main(sys.argv[1])