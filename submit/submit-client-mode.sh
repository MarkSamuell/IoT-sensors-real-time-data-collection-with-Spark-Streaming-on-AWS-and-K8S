#!/bin/bash

# Check if all required arguments are provided
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <cluster-id> <job-name-prefix> <job-type> <local-log-dir>"
    echo "job-type should be 'sevs', 'event_logs', 'sevs_aggs', 'event_logs_aggs', 'batch_sevs_aggs', 'batch_event_logs_aggs', or 'all'"
    exit 1
fi

CLUSTER_ID=$1
JOB_NAME_PREFIX=$2
JOB_TYPE=$3
LOG_DIR=$4

# Create log directory if it doesn't exist
mkdir -p "${LOG_DIR}"

# Update S3 path
S3_PATH="s3://elasticmapreduce-uraeusdev/spark_build/"

# Function to create and submit a job step
submit_job() {
    local job_type=$1
    local job_name="${JOB_NAME_PREFIX}_${job_type}"
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local log_file="${LOG_DIR}/${job_name}_${timestamp}.log"
    
    echo "Starting job '${job_name}' at ${timestamp}"
    echo "Logs will be written to: ${log_file}"

    # Get master node address
    MASTER_DNS=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.MasterPublicDnsName' --output text)
    
    # Submit spark job in client mode
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3 \
        --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 \
        --conf spark.pyspark.python=/usr/bin/python3 \
        --conf spark.pyspark.driver.python=/usr/bin/python3 \
        --conf spark.executor.memory=512m \
        --conf spark.executor.cores=1 \
        --conf spark.driver.memory=512m \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j-driver.properties" \
        --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j-executor.properties" \
        --conf "spark.hadoop.yarn.timeline-service.enabled=false" \
        --conf "spark.yarn.submit.waitAppCompletion=true" \
        --conf "spark.eventLog.enabled=true" \
        --conf "spark.eventLog.dir=${LOG_DIR}/spark-events" \
        --py-files "${S3_PATH}/streaming_pipeline-0.0.1-py3-none-any.whl" \
        --files "${S3_PATH}/jaas.conf,log4j-driver.properties,log4j-executor.properties" \
        --conf "spark.submit.pyFiles=${S3_PATH}/streaming_pipeline-0.0.1-py3-none-any.whl" \
        --verbose \
        "${S3_PATH}/spark_runner.py" \
        "${job_type}" 2>&1 | tee "${log_file}"

    echo "Job completed. Logs saved to: ${log_file}"
}

# Create log4j configuration files
cat > log4j-driver.properties << EOL
log4j.rootCategory=INFO, console, file
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=${LOG_DIR}/driver.log
log4j.appender.file.MaxFileSize=10MB
log4j.appender.file.MaxBackupIndex=10
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
EOL

cat > log4j-executor.properties << EOL
log4j.rootCategory=INFO, console, file
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=${LOG_DIR}/executor.log
log4j.appender.file.MaxFileSize=10MB
log4j.appender.file.MaxBackupIndex=10
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
EOL

# Main logic to submit job(s)
if [ "$JOB_TYPE" == "all" ]; then
    # Only run streaming jobs
    submit_job "sevs"
    submit_job "event_logs"
    submit_job "sevs_aggs"
    submit_job "event_logs_aggs"
elif [[ $JOB_TYPE =~ ^(sevs|event_logs|sevs_aggs|event_logs_aggs|batch_sevs_aggs|batch_event_logs_aggs)$ ]]; then
    submit_job "$JOB_TYPE"
else
    echo "Error: Invalid job-type"
    echo "Valid types: 'sevs', 'event_logs', 'sevs_aggs', 'event_logs_aggs', 'batch_sevs_aggs', 'batch_event_logs_aggs', or 'all'"
    echo "Note: 'all' will only run streaming jobs"
    exit 1
fi
