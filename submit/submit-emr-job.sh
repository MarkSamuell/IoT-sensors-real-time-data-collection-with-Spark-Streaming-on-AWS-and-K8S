#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <cluster-id> <job-name-prefix> <job-type>"
    echo -e "\nAvailable job types:\n"
    
    echo "UI Jobs (Elasticsearch):"
    echo "  ui_sevs                  : UI SEVs processing"
    echo "  ui_event_logs            : UI Event Logs processing"
    echo "  ui_sevs_aggs             : UI SEVs aggregations"
    echo "  ui_event_logs_aggs       : UI Event Logs aggregations"
    echo "  ui_batch_sevs_aggs       : UI Batch SEVs aggregations"
    echo "  ui_batch_logs_aggs       : UI Batch Event Logs aggregations"
    echo "  ui_batch_late_sevs       : UI Batch Late SEVs processing"
    echo "  ui_batch_late_logs       : UI Batch Late Event Logs processing"
    echo "  ui_batch_late_all        : All UI Batch Late processing"
    echo "  api_logs                 : API Logs and SEVs processing"  # Added new job type
    
    echo -e "\nIceberg Jobs:"
    echo "  ice_event_logs           : Event Logs to Iceberg"
    echo "  ice_sevs                 : SEVs to Iceberg"
    echo "  ice_event_logs_aggs      : Event Logs Aggregations to Iceberg" 
    echo "  ice_sevs_aggs            : SEVs Aggregations to Iceberg"
        
    echo -e "\nML Jobs:"
    echo "  can_ml_batch             : CAN Messages ML Batch Scoring"
    echo "  can_ml_stream            : CAN Messages ML Stream Scoring"
    
    echo -e "\nSpecial Commands:"
    echo "  all_ui                   : Run all UI jobs"
    echo "  all_ice                  : Run all Iceberg jobs"
    echo "  all                      : Run all jobs"
    exit 1
fi

CLUSTER_ID=$1
JOB_NAME_PREFIX=$2
JOB_TYPE=$3
S3_PATH="s3://elasticmapreduce-uraeusdev/spark_build/"

submit_job() {
    local job_type=$1
    local job_name="${JOB_NAME_PREFIX}_${job_type}"
    
    local base_configs=(
        "--master" "yarn"
        "--deploy-mode" "client"
        "--conf" "spark.scheduler.mode=FAIR"
        "--conf" "spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3"
        "--conf" "spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3"
        "--conf" "spark.pyspark.python=/usr/bin/python3"
        "--conf" "spark.pyspark.driver.python=/usr/bin/python3"
        "--conf" "spark.executor.memory=2g"
        "--conf" "spark.driver.memory=4g"
        "--conf" "spark.sql.autoBroadcastJoinThreshold=20485760"
        "--conf" "spark.sql.cbo.enabled=true"
        "--conf" "spark.sql.adaptive.enabled=true"
        "--conf" "spark.yarn.submit.waitAppCompletion=true"
        "--conf" "spark.eventLog.enabled=true"
    )
    
    local extra_configs=()
    if [[ $job_type == ice_* ]]; then
        extra_configs+=(
            "--conf" "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            "--conf" "spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog"
            "--conf" "spark.sql.catalog.demo.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
            "--conf" "spark.sql.catalog.demo.warehouse=s3://elasticmapreduce-uraeusdev/ICEBERG/"
            "--conf" "spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
            "--packages" "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1"
        )
    elif [[ $job_type == can_ml_* ]]; then
        extra_configs+=(
            "--conf" "spark.sql.execution.arrow.pyspark.enabled=true"
            "--files" "${S3_PATH}/LSTM_model.h5"
            "--packages" "org.apache.hadoop:hadoop-aws:3.3.4"
        )
    fi

    local all_args=("${base_configs[@]}" "${extra_configs[@]}")
    all_args+=(
        "--py-files" "${S3_PATH}/streaming_pipeline-0.0.1-py3-none-any.whl"
        "--files" "${S3_PATH}/jaas.conf"
        "--conf" "spark.submit.pyFiles=${S3_PATH}/streaming_pipeline-0.0.1-py3-none-any.whl"
        "--verbose"
        "${S3_PATH}/spark_runner.py"
        "${job_type}"
    )

    local json_args="["
    for arg in "${all_args[@]}"; do
        json_args+="\""${arg}"\","
    done
    json_args=${json_args%,}"]"

    local step_json='{
        "Type": "Spark",
        "Name": "'${job_name}'",
        "ActionOnFailure": "CONTINUE",
        "Args": '${json_args}'
    }'
    
    STEP_ID=$(aws emr add-steps --cluster-id ${CLUSTER_ID} --steps "[${step_json}]" --query 'StepIds[0]' --output text)
    echo "Job '${job_name}' submitted. Step ID: $STEP_ID"
}

submit_ui_jobs() {
    submit_job "ui_sevs"
    submit_job "ui_event_logs"
    submit_job "ui_sevs_aggs"
    submit_job "ui_event_logs_aggs"
}

submit_ice_jobs() {
    submit_job "ice_event_logs"
    submit_job "ice_sevs"
    submit_job "ice_event_logs_aggs"
    submit_job "ice_sevs_aggs"
}

case "$JOB_TYPE" in
    "all")
        submit_ui_jobs
        submit_ice_jobs
        ;;
    "all_ui")
        submit_ui_jobs
        ;;
    "all_ice")
        submit_ice_jobs
        ;;
    "ui_batch_late_all")
        submit_job "ui_batch_late_sevs"
        submit_job "ui_batch_late_logs"
        ;;
    "ui_sevs"|"ui_event_logs"|"ui_sevs_aggs"|"ui_event_logs_aggs"|\
    "ui_batch_sevs_aggs"|"ui_batch_logs_aggs"|"ui_batch_late_sevs"|\
    "ui_batch_late_logs"|"ice_event_logs"|"ice_sevs"|"ice_event_logs_aggs"|\
    "ice_sevs_aggs"|"can_ml_batch"|"can_ml_stream"|"api_logs")  # Added api_logs here
        submit_job "$JOB_TYPE"
        ;;
    *)
        echo "Error: Invalid job type '$JOB_TYPE'"
        exit 1
        ;;
esac

echo "Check status: aws emr list-steps --cluster-id ${CLUSTER_ID}"