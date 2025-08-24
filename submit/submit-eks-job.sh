#!/bin/bash

# Parse arguments
ENABLE_LOCAL_MODE=false

# Process options
while [[ $# -gt 0 ]]; do
    case $1 in
        --local)
            ENABLE_LOCAL_MODE=true
            shift
            ;;
        -*)
            echo "Unknown option $1"
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

# Check for minimum required arguments
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 [--local] <environment> <job-name-prefix> <job-type1> [job-type2] [job-type3] ..."
    echo "Environment must be either 'dev' or 'stg'"
    echo "Options:"
    echo "  --local       Enable local features (monitoring, logging, Spark UI port forwarding)"
    echo -e "\nAvailable job types:\n"
    
    echo "UI Jobs (Elasticsearch):"
    echo "  ui_sevs                  : UI SEVs processing"
    echo "  ui_event_logs            : UI Event Logs processing"
    echo "  ui_batch_sevs_aggs       : UI Batch SEVs aggregations"
    echo "  ui_batch_logs_aggs       : UI Batch Event Logs aggregations"
    echo "  ui_batch_late_sevs       : UI Batch Late SEVs processing"
    echo "  ui_batch_late_logs       : UI Batch Late Event Logs processing"
    echo "  ui_batch_late_all        : All UI Batch Late processing"
    echo "  api_logs                 : API Logs and SEVs processing"
    echo "  unified_sevs             : Unified SEVs processing (Raw + Aggregations)"
    
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
    
    echo -e "\nExamples:"
    echo "  $0 dev job-prefix ui_sevs                   : Run UI SEVs job in dev environment"
    echo "  $0 --local dev job-prefix ui_sevs           : Run UI SEVs job with local features (monitoring, logging, Spark UI)"
    echo "  $0 stg job-prefix ice_event_logs            : Run Iceberg Event Logs job in staging environment"
    echo "  $0 dev job-prefix ui_sevs ui_event_logs     : Run multiple jobs in dev environment"
    exit 1
fi

ENVIRONMENT=$1
JOB_NAME_PREFIX=$2
# Shift arguments to remove the first two args (environment and prefix)
shift 2
# Remaining arguments are job types
JOB_TYPES=("$@")

IMAGE="703671895821.dkr.ecr.eu-central-1.amazonaws.com/fleetconnect-dataplatform:spark-k8s"

# Global arrays to track submitted jobs for log collection and port forwarding
declare -a SUBMITTED_JOBS=()
declare -a PORT_FORWARD_PIDS=()
declare -a LOG_PATHS=()  # Cache log paths to avoid creating multiple directories
declare -a TRACKED_PORTS=()  # Track assigned ports to avoid conflicts
declare -i NEXT_PORT=4040

# Validate environment
if [[ "$ENVIRONMENT" != "dev" && "$ENVIRONMENT" != "stg" ]]; then
    echo "Error: Environment must be either 'dev' or 'stg'"
    exit 1
fi

# Set namespace based on environment
NAMESPACE="$ENVIRONMENT"

# Set environment-specific values
if [[ "$ENVIRONMENT" == "dev" ]]; then
    DB_HOST_KEY="dev_db_host"
else
    DB_HOST_KEY="stg_db_host"
fi

# Function to create log directory and filename (cached)
create_log_path() {
    local job_type=$1
    local job_name=$2
    
    # Check if log path already exists for this job
    for cached_path in "${LOG_PATHS[@]}"; do
        if [[ "$cached_path" == *"$job_name.log" ]]; then
            echo "$cached_path"
            return
        fi
    done
    
    # Create new log path if not cached
    local timestamp=$(date +"%Y-%m-%d_%H_%M")
    
    # Get the directory where the script is located
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    # Create directory structure: logs/job_type/timestamp/ (relative to project root)
    local log_dir="${script_dir}/../logs/${job_type}/${timestamp}"
    mkdir -p "$log_dir"
    
    # Return the full log path with prefix_job_type format
    local log_path="${log_dir}/${job_name}.log"
    
    # Cache the log path
    LOG_PATHS+=("$log_path")
    
    echo "$log_path"
}

# Function to find available port (improved with better compatibility)
find_available_port() {
    local start_port=$1
    local port=$start_port
    
    echo "Looking for available port starting from $start_port..." >&2
    
    # Use multiple methods to check port availability for maximum compatibility
    while true; do
        local port_in_use=false
        
        # Method 1: Check with netstat (most compatible)
        if netstat -an 2>/dev/null | grep -q ":$port "; then
            port_in_use=true
            echo "Port $port is in use (netstat)" >&2
        fi
        
        # Method 2: Check with lsof (Unix-like systems)
        if [ "$port_in_use" = false ] && command -v lsof >/dev/null 2>&1; then
            if lsof -i :$port >/dev/null 2>&1; then
                port_in_use=true
                echo "Port $port is in use (lsof)" >&2
            fi
        fi
        
        # Method 3: Check with ss (modern Linux)
        if [ "$port_in_use" = false ] && command -v ss >/dev/null 2>&1; then
            if ss -tuln 2>/dev/null | grep -q ":$port "; then
                port_in_use=true
                echo "Port $port is in use (ss)" >&2
            fi
        fi
        
        # Method 4: Check our own tracked ports
        for tracked_port in "${TRACKED_PORTS[@]}"; do
            if [ "$tracked_port" = "$port" ]; then
                port_in_use=true
                echo "Port $port is already tracked by this script" >&2
                break
            fi
        done
        
        if [ "$port_in_use" = false ]; then
            echo "Found available port: $port" >&2
            # Track this port to avoid conflicts within this script run
            TRACKED_PORTS+=($port)
            echo $port
            return 0
        fi
        
        port=$((port + 1))
        
        # Safety check to avoid infinite loop
        if [ $port -gt $((start_port + 100)) ]; then
            echo "Error: Could not find available port in range $start_port-$((start_port + 100))" >&2
            return 1
        fi
    done
}

# Function to set up port forwarding for Spark UI (improved)
setup_port_forward() {
    local job_name=$1
    local namespace=$2
    local preferred_port=$3
    
    echo "Setting up port forwarding for Spark UI: $job_name (trying port $preferred_port)"
    
    # Find an available port starting from the preferred port
    local local_port
    local_port=$(find_available_port $preferred_port)
    if [ $? -ne 0 ]; then
        echo "Warning: Could not find available port for $job_name"
        return 1
    fi
    
    # If we got a different port than preferred, show the actual port and update job info
    if [ "$local_port" != "$preferred_port" ]; then
        echo "Port $preferred_port was busy, using port $local_port instead"
        
        # Update the SUBMITTED_JOBS array with the actual port used
        for i in "${!SUBMITTED_JOBS[@]}"; do
            if [[ "${SUBMITTED_JOBS[$i]}" == "${job_name}:"*":${preferred_port}" ]]; then
                IFS=':' read -r name type port <<< "${SUBMITTED_JOBS[$i]}"
                SUBMITTED_JOBS[$i]="${name}:${type}:${local_port}"
                echo "Updated job info: ${SUBMITTED_JOBS[$i]}"
                break
            fi
        done
    fi
    
    # Start port forwarding in background
    {
        # Wait for the driver pod to be ready (max 10 minutes)
        echo "Waiting for driver pod to be ready for port forwarding..."
        local max_wait=600
        local waited=0
        local driver_pod=""
        
        while [ $waited -lt $max_wait ]; do
            # Look for driver pod
            driver_pod=$(kubectl get pods -n "$namespace" -l "spark-app-name=$job_name,spark-role=driver" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
            
            if [ -n "$driver_pod" ]; then
                # Check if pod is running and ready
                local pod_status=$(kubectl get pod "$driver_pod" -n "$namespace" -o jsonpath='{.status.phase}' 2>/dev/null)
                local ready_status=$(kubectl get pod "$driver_pod" -n "$namespace" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
                
                if [ "$pod_status" = "Running" ] && [ "$ready_status" = "True" ]; then
                    echo "Driver pod ready for port forwarding: $driver_pod"
                    break
                fi
            fi
            
            sleep 10
            waited=$((waited + 10))
        done
        
        if [ -z "$driver_pod" ] || [ $waited -ge $max_wait ]; then
            echo "Warning: Driver pod for $job_name not ready for port forwarding after ${max_wait}s"
            # Remove the tracked port since we're not using it
            TRACKED_PORTS=("${TRACKED_PORTS[@]/$local_port}")
            return 1
        fi
        
        # Double-check port availability right before starting port forwarding
        echo "Double-checking port $local_port availability before starting port forward..."
        local final_port_check=false
        if netstat -an 2>/dev/null | grep -q ":$local_port " || \
           (command -v lsof >/dev/null && lsof -i :$local_port >/dev/null 2>&1) || \
           (command -v ss >/dev/null && ss -tuln 2>/dev/null | grep -q ":$local_port "); then
            final_port_check=true
        fi
        
        if [ "$final_port_check" = true ]; then
            echo "Warning: Port $local_port became unavailable, finding new port..."
            local_port=$(find_available_port $((local_port + 1)))
            if [ $? -ne 0 ]; then
                echo "Error: Could not find alternative port for $job_name"
                return 1
            fi
            echo "Using alternative port: $local_port"
            
            # Update job info again with the final port
            for i in "${!SUBMITTED_JOBS[@]}"; do
                if [[ "${SUBMITTED_JOBS[$i]}" == "${job_name}:"* ]]; then
                    IFS=':' read -r name type port <<< "${SUBMITTED_JOBS[$i]}"
                    SUBMITTED_JOBS[$i]="${name}:${type}:${local_port}"
                    break
                fi
            done
        fi
        
        # Start port forwarding to the driver pod directly
        echo "Starting port forwarding: localhost:$local_port -> $driver_pod:4040"
        kubectl port-forward pod/"$driver_pod" "$local_port:4040" -n "$namespace" >/dev/null 2>&1 &
        
        local pf_pid=$!
        echo "Port forwarding started (PID: $pf_pid) for pod: $driver_pod"
        echo "    ============================================================================"
        echo "✓✓✓ Spark UI available at: http://localhost:$local_port"
        echo "    ============================================================================"
        
        # Store the PID for cleanup
        PORT_FORWARD_PIDS+=($pf_pid)
        
        # Verify the port forwarding is working
        sleep 3
        if ! kill -0 $pf_pid 2>/dev/null; then
            echo "Warning: Port forwarding process died immediately for $job_name"
            # Remove the tracked port since port forwarding failed
            TRACKED_PORTS=("${TRACKED_PORTS[@]/$local_port}")
            return 1
        fi
        
        # Test connectivity (optional)
        echo "Testing port forwarding connectivity..."
        local test_count=0
        while [ $test_count -lt 5 ]; do
            if command -v curl >/dev/null 2>&1; then
                if curl -s --connect-timeout 2 "http://localhost:$local_port" >/dev/null 2>&1; then
                    echo "✓ Spark UI connectivity test passed"
                    break
                fi
            elif command -v nc >/dev/null 2>&1; then
                if nc -z localhost $local_port 2>/dev/null; then
                    echo "✓ Port $local_port is responding"
                    break
                fi
            fi
            test_count=$((test_count + 1))
            sleep 2
        done
        
        if [ $test_count -eq 5 ]; then
            echo "Warning: Port forwarding connectivity test failed for $job_name (but port forwarding process is running)"
        fi
        
        # Keep the port forwarding alive
        wait $pf_pid
        
        # Clean up tracked port when port forwarding ends
        TRACKED_PORTS=("${TRACKED_PORTS[@]/$local_port}")
        
    } &
    
    return 0
}

# Function to collect logs in background
collect_logs() {
    local job_name=$1
    local job_type=$2
    local namespace=$3
    
    echo "Setting up log collection for job: $job_name"
    
    # Create log path
    local log_path=$(create_log_path "$job_type" "$job_name")
    echo "Logs will be saved to: $log_path"
    
    # Start log collection in background
    {
        # Wait for the driver pod to be created (max 5 minutes)
        echo "Waiting for driver pod to be ready..."
        local max_wait=300
        local waited=0
        local driver_pod=""
        
        while [ $waited -lt $max_wait ]; do
            driver_pod=$(kubectl get pods -n "$namespace" -l "spark-app-name=$job_name,spark-role=driver" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
            
            if [ -n "$driver_pod" ]; then
                # Check if pod exists and is not in pending state
                local pod_status=$(kubectl get pod "$driver_pod" -n "$namespace" -o jsonpath='{.status.phase}' 2>/dev/null)
                if [ "$pod_status" != "Pending" ]; then
                    echo "Driver pod found: $driver_pod (status: $pod_status)"
                    break
                fi
            fi
            
            sleep 5
            waited=$((waited + 5))
        done
        
        if [ -z "$driver_pod" ]; then
            echo "Warning: Driver pod for $job_name not found after ${max_wait}s" >> "$log_path"
            return 1
        fi
        
        # Start collecting logs
        echo "Starting log collection for $driver_pod..." >> "$log_path"
        echo "Job: $job_name" >> "$log_path"
        echo "Type: $job_type" >> "$log_path"
        echo "Started: $(date)" >> "$log_path"
        echo "========================================" >> "$log_path"
        
        # Follow logs and append to file
        kubectl logs -f "$driver_pod" -n "$namespace" >> "$log_path" 2>&1
        
        # Add completion timestamp
        echo "========================================" >> "$log_path"
        echo "Log collection completed: $(date)" >> "$log_path"
        
        echo "Logs saved to: $log_path"
        
    } &
    
    # Store the background process PID for potential cleanup
    local log_pid=$!
    echo "Log collection started in background (PID: $log_pid) for job: $job_name"
}

submit_job() {
    local job_type=$1
    local job_name="${JOB_NAME_PREFIX}-${job_type}"
    job_name=$(echo "$job_name" | tr '_' '-')
    
    echo "Creating SparkApplication for $job_name in $ENVIRONMENT environment..."
    echo "Using DB_HOST_KEY: $DB_HOST_KEY"
    
    # Generate the YAML content and pipe directly to kubectl apply
    {
        cat << EOF
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: ${job_name}
  namespace: ${NAMESPACE}
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "${IMAGE}"
  imagePullPolicy: Always
  mainApplicationFile: "local:///opt/spark/work-dir/spark_runner.py"
  arguments:
    - "${job_type}"
  sparkVersion: "3.5.1"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20
  driver:
    serviceAccount: spark-sa
    tolerations:
      - key: node-type
        operator: Equal
        value: "spark"
        effect: NoSchedule
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: node-type
                  operator: In
                  values:
                    - spark
    env:
      - name: PYTHONPATH
        value: "/opt/spark/work-dir:/opt/spark/work-dir/src"
      - name: AWS_REGION
        value: "eu-central-1"
      - name: ENVIRONMENT
        value: "${ENVIRONMENT}"
      - name: ELASTIC_USERNAME
        valueFrom:
          configMapKeyRef:
            name: shared-config
            key: ELASTIC_USERNAME
      - name: KAFKA_BROKERS
        valueFrom:
          configMapKeyRef:
            name: shared-config
            key: KAFKA_BROKERS
      - name: DATABASE_USER
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: username
      - name: DATABASE_PASSWORD
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: password
      - name: DATABASE
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: fleet_db_database
      - name: DATABASE_HOST
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: ${DB_HOST_KEY}
      - name: DATABASE_PORT
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: db_port
      - name: ELASTIC_PASSWORD
        valueFrom:
          secretKeyRef:
            name: uraeus-elastic-search-es-elastic-user
            key: elastic
  executor:
    tolerations:
      - key: node-type
        operator: Equal
        value: "spark"
        effect: NoSchedule
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: node-type
                  operator: In
                  values:
                    - spark
    env:
      - name: PYTHONPATH
        value: "/opt/spark/work-dir:/opt/spark/work-dir/src"
      - name: AWS_REGION
        value: "eu-central-1"
      - name: ELASTIC_USERNAME
        valueFrom:
          configMapKeyRef:
            name: shared-config
            key: ELASTIC_USERNAME
      - name: KAFKA_BROKERS
        valueFrom:
          configMapKeyRef:
            name: shared-config
            key: KAFKA_BROKERS
      - name: DATABASE_USER
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: username
      - name: DATABASE_PASSWORD
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: password
      - name: DATABASE
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: fleet_db_database
      - name: DATABASE_HOST
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: ${DB_HOST_KEY}
      - name: DATABASE_PORT
        valueFrom:
          secretKeyRef:
            name: db-credentials
            key: db_port
      - name: ELASTIC_PASSWORD
        valueFrom:
          secretKeyRef:
            name: uraeus-elastic-search-es-elastic-user
            key: elastic
EOF

        # Add job-specific spark configuration
        if [[ $job_type == ice_* ]]; then
            cat << EOF
  sparkConf:
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.hadoop.fs.s3a.endpoint": "s3.eu-central-1.amazonaws.com"
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    "spark.hadoop.fs.s3a.path.style.access": "false"
    "spark.driver.extraJavaOptions": "-Divy.home=/tmp/.ivy2 -Dokhttp.protocols=TLSv1.2 -Dhttp2.disable=true"
    "spark.executor.extraJavaOptions": "-Divy.home=/tmp/.ivy2 -Dokhttp.protocols=TLSv1.2 -Dhttp2.disable=true"
    "spark.kubernetes.submission.connectionTimeout": "60000"
    "spark.kubernetes.submission.requestTimeout": "60000"
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.demo.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog"
    "spark.sql.catalog.demo.warehouse": "s3://elasticmapreduce-uraeusdev/ICEBERG/"
    "spark.sql.catalog.demo.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1"
EOF
        elif [[ $job_type == can_ml_* ]]; then
            cat << EOF
  sparkConf:
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.hadoop.fs.s3a.endpoint": "s3.eu-central-1.amazonaws.com"
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    "spark.hadoop.fs.s3a.path.style.access": "false"
    "spark.driver.extraJavaOptions": "-Divy.home=/tmp/.ivy2 -Dokhttp.protocols=TLSv1.2 -Dhttp2.disable=true"
    "spark.executor.extraJavaOptions": "-Divy.home=/tmp/.ivy2 -Dokhttp.protocols=TLSv1.2 -Dhttp2.disable=true"
    "spark.kubernetes.submission.connectionTimeout": "60000"
    "spark.kubernetes.submission.requestTimeout": "60000"
    "spark.sql.execution.arrow.pyspark.enabled": "true"
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4"
EOF
        else
            cat << EOF
  sparkConf:
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.hadoop.fs.s3a.endpoint": "s3.eu-central-1.amazonaws.com"
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    "spark.hadoop.fs.s3a.path.style.access": "false"
    "spark.driver.extraJavaOptions": "-Divy.home=/tmp/.ivy2 -Dokhttp.protocols=TLSv1.2 -Dhttp2.disable=true"
    "spark.executor.extraJavaOptions": "-Divy.home=/tmp/.ivy2 -Dokhttp.protocols=TLSv1.2 -Dhttp2.disable=true"
    "spark.kubernetes.submission.connectionTimeout": "60000"
    "spark.kubernetes.submission.requestTimeout": "60000"
EOF
        fi
    } | kubectl apply -f -
    
    # Check if the operation was successful
    if [ $? -eq 0 ]; then
        echo "SparkApplication '${job_name}' created successfully in ${ENVIRONMENT} environment"
        
        # Assign port immediately before starting background processes
        local assigned_port=$NEXT_PORT
        NEXT_PORT=$((NEXT_PORT + 1))
        
        # Add to submitted jobs array with assigned port
        SUBMITTED_JOBS+=("${job_name}:${job_type}:${assigned_port}")
        
        # Wait a moment for the resource to be fully created
        sleep 2
        
        # Get the application name to verify it was created
        app_name=$(kubectl get sparkapplications -n ${NAMESPACE} -o=jsonpath='{.items[?(@.metadata.name=="'${job_name}'")].metadata.name}')
        
        if [ -z "$app_name" ]; then
            echo "Warning: SparkApplication '${job_name}' was applied but may not be visible yet. Please check status manually."
        else
            # Start log collection for this job only if local mode is enabled
            if [ "$ENABLE_LOCAL_MODE" = true ]; then
                collect_logs "$job_name" "$job_type" "$NAMESPACE"
            fi
            
            # Set up port forwarding for Spark UI only if local mode is enabled
            if [ "$ENABLE_LOCAL_MODE" = true ]; then
                (sleep 30 && setup_port_forward "$job_name" "$NAMESPACE" "$assigned_port") &
            fi
        fi
    else
        echo "Failed to create SparkApplication '${job_name}' in ${ENVIRONMENT} environment"
        exit 1
    fi
}

submit_ui_jobs() {
    submit_job "ui_sevs"
    submit_job "ui_event_logs"
    # Removed ui_sevs_aggs and ui_event_logs_aggs as requested
}

submit_ice_jobs() {
    submit_job "ice_event_logs"
    submit_job "ice_sevs"
    submit_job "ice_event_logs_aggs"
    submit_job "ice_sevs_aggs"
}

process_job_types() {
    local job_types=("$@")
    local submitted_jobs=()
    
    for job_type in "${job_types[@]}"; do
        case "$job_type" in
            "all")
                submit_ui_jobs
                submit_ice_jobs
                submitted_jobs+=("all_jobs")
                ;;
            "all_ui")
                submit_ui_jobs
                submitted_jobs+=("all_ui_jobs")
                ;;
            "all_ice")
                submit_ice_jobs
                submitted_jobs+=("all_ice_jobs")
                ;;
            "ui_batch_late_all")
                submit_job "ui_batch_late_sevs"
                submit_job "ui_batch_late_logs"
                submitted_jobs+=("ui_batch_late_all")
                ;;
            # Removed ui_sevs_aggs and ui_event_logs_aggs from valid job types
            "ui_sevs"|"ui_event_logs"|\
            "ui_batch_sevs_aggs"|"ui_batch_logs_aggs"|"ui_batch_late_sevs"|\
            "ui_batch_late_logs"|"ice_event_logs"|"ice_sevs"|"ice_event_logs_aggs"|\
            "ice_sevs_aggs"|"can_ml_batch"|"can_ml_stream"|"api_logs"|"unified_sevs")
                submit_job "$job_type"
                submitted_jobs+=("$job_type")
                ;;
            "ui_sevs_aggs"|"ui_event_logs_aggs")
                echo "Warning: Job type '$job_type' has been removed and is no longer supported - skipping"
                ;;
            *)
                echo "Warning: Invalid job type '$job_type' - skipping"
                ;;
        esac
    done
    
    return_submitted_jobs "${submitted_jobs[@]}"
}

return_submitted_jobs() {
    local submitted_jobs=("$@")
    local job_count=${#submitted_jobs[@]}
    
    if [ $job_count -eq 0 ]; then
        echo "No valid jobs were submitted."
        exit 1
    fi
    
    echo ""
    echo "========================================="
    echo "Job Submission Summary"
    echo "========================================="
    echo "Environment: $ENVIRONMENT"
    echo "Namespace: $NAMESPACE"
    echo "Jobs submitted: $job_count"
    echo ""
    
    # Show log collection and port forwarding info
    if [ ${#SUBMITTED_JOBS[@]} -gt 0 ]; then
        echo "Features:"
        if [ "$ENABLE_LOCAL_MODE" = true ]; then
            echo "✓ Logs being collected automatically"
            echo "✓ Spark UIs will be port-forwarded when pods are ready"
            echo "✓ Live monitoring enabled"
            echo "Log files will be saved in: logs/<job_type>/<date_time>/<prefix_job_type>.log"
        else
            echo "✗ Log collection disabled (use --local to enable)"
            echo "✗ Spark UI port forwarding disabled (use --local to enable)"
            echo "✗ Live monitoring disabled (use --local to enable)"
        fi
        echo ""
        echo "Submitted jobs:"
        for job_info in "${SUBMITTED_JOBS[@]}"; do
            IFS=':' read -r job_name job_type local_port <<< "$job_info"
            echo "  - $job_name (type: $job_type)"
            if [ "$ENABLE_LOCAL_MODE" = true ]; then
                echo "    Spark UI: http://localhost:$local_port (will be available when pod is ready)"
                local actual_log_path=$(create_log_path "$job_type" "$job_name")
                echo "    Logs: $actual_log_path"
            fi
        done
        echo ""
    fi
    
    if [ $job_count -eq 1 ] && [[ "${submitted_jobs[0]}" != all* && "${submitted_jobs[0]}" != ui_batch_late_all ]]; then
        # Single regular job
        local formatted_job_name=${submitted_jobs[0]//\_/-}
        echo "Commands to monitor this job:"
        echo "  Status: kubectl get sparkapplications -n ${NAMESPACE}"
        echo "  Details: kubectl describe sparkapplication/${JOB_NAME_PREFIX}-${formatted_job_name} -n ${NAMESPACE}"
        echo "  Manual logs: kubectl logs -f -n ${NAMESPACE} -l spark-app-name=${JOB_NAME_PREFIX}-${formatted_job_name}"
    else
        # Multiple jobs or special job groups
        echo "Commands to monitor jobs:"
        echo "  Status: kubectl get sparkapplications -n ${NAMESPACE}"
        echo "  Watch all: kubectl get sparkapplications -n ${NAMESPACE} -w"
        
        # If there are a few specific jobs, show their names
        if [ $job_count -lt 5 ] && [[ "${submitted_jobs[0]}" != all* && "${submitted_jobs[0]}" != ui_batch_late_all ]]; then
            echo ""
            echo "Individual job details:"
            for job in "${submitted_jobs[@]}"; do
                local formatted_job_name=${job//\_/-}
                echo "  - ${JOB_NAME_PREFIX}-${formatted_job_name}"
            done
        fi
    fi
    
    echo ""
    echo "Note: Jobs have been submitted successfully."
    if [ "$ENABLE_LOCAL_MODE" = true ]; then
        echo "Local features enabled: monitoring, logging, and Spark UI port forwarding active."
        echo "Check the logs directory for log files."
    else
        echo "Local features disabled. Use --local flag to enable monitoring, logging, and Spark UI."
    fi
    echo "========================================="
    
    # Only start monitoring if local mode is enabled
    if [ "$ENABLE_LOCAL_MODE" = true ]; then
        echo ""
        echo "Monitoring SparkApplications and Related Pods..."
        echo "Press Ctrl+C to stop monitoring and exit."
        echo ""
        
        # Start monitoring
        monitor_jobs
    else
        echo ""
        echo "Jobs submitted successfully. Use 'kubectl get sparkapplications -n ${NAMESPACE}' to check status."
        echo "Add --local flag to enable live monitoring, logging, and Spark UI port forwarding."
    fi
}

# Function to monitor both SparkApplications and related pods (complete implementation)
monitor_jobs() {
    echo "=== SparkApplications in ${NAMESPACE} namespace ==="
    kubectl get sparkapplication -n ${NAMESPACE} -w &
    local spark_apps_pid=$!
    
    echo ""
    echo "=== All Spark-related pods in ${NAMESPACE} namespace ==="
    
    # Create dynamic job pattern based on submitted jobs
    local job_names_pattern=""
    for job_info in "${SUBMITTED_JOBS[@]}"; do
        IFS=':' read -r job_name job_type local_port <<< "$job_info"
        if [ -z "$job_names_pattern" ]; then
            job_names_pattern="$job_name"
        else
            job_names_pattern="${job_names_pattern}|${job_name}"
        fi
    done
    
    # Monitor all Spark-related pods using labels and naming patterns
    while true; do
        # Get all pods and filter for Spark-related pods
        kubectl get pods -n ${NAMESPACE} --no-headers 2>/dev/null | \
        awk -v job_pattern="($job_names_pattern)" -v prefix_pattern="$JOB_NAME_PREFIX" '
            # Match driver pods (any pod ending with -driver)
            $1 ~ /-driver$/ ||
            # Match executor pods (any pod with -exec- pattern)
            $1 ~ /-exec-[0-9]+$/ ||
            # Match pods from our submitted jobs
            $1 ~ job_pattern ||
            # Match any pod starting with our job prefix
            $1 ~ "^" prefix_pattern "-" {
                printf "%-70s %-12s %-20s %-10s %s\n", $1, $2, $3, $4, $5
            }' | \
        while IFS= read -r line; do
            if [ -n "$line" ]; then
                echo "$(date '+%H:%M:%S') $line"
            fi
        done
        sleep 3
    done &
    local pods_pid=$!
    
    # Function to cleanup monitoring processes
    cleanup_monitoring() {
        echo ""
        echo "Stopping monitoring..."
        if [ -n "$spark_apps_pid" ]; then
            kill $spark_apps_pid 2>/dev/null
        fi
        if [ -n "$pods_pid" ]; then
            kill $pods_pid 2>/dev/null
        fi
        wait $spark_apps_pid $pods_pid 2>/dev/null
    }
    
    # Set trap for cleanup
    trap cleanup_monitoring INT TERM
    
    # Wait for monitoring processes
    wait $spark_apps_pid $pods_pid
}

# Check if any job types were provided
if [ ${#JOB_TYPES[@]} -eq 0 ]; then
    echo "Error: No job type specified"
    exit 1
fi

# Process all job types provided as arguments
process_job_types "${JOB_TYPES[@]}"

# Cleanup function for background processes
cleanup() {
    echo ""
    echo "Script interrupted. Cleaning up background processes..."
    
    # Kill port forwarding processes only if local mode was enabled
    if [ "$ENABLE_LOCAL_MODE" = true ] && [ ${#PORT_FORWARD_PIDS[@]} -gt 0 ]; then
        echo "Stopping port forwarding processes..."
        for pid in "${PORT_FORWARD_PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null
                echo "  Stopped port forwarding (PID: $pid)"
            fi
        done
    fi
    
    if [ "$ENABLE_LOCAL_MODE" = true ]; then
        echo "Log collection processes will continue in the background."
        echo "Check the logs directory for ongoing log collection."
        echo ""
        echo "To manually stop all port forwarding later, you can use:"
        echo "  pkill -f 'kubectl port-forward'"
        echo ""
        echo "To restart port forwarding for a specific job:"
        echo "  kubectl port-forward pod/<driver-pod-name> <local-port>:4040 -n <namespace>"
        echo ""
        echo "To check current port usage:"
        echo "  netstat -an | grep :404"
        echo "  lsof -i :4040-4050"
    fi
    
    # Clear tracked ports
    TRACKED_PORTS=()
}

# Set up cleanup on script exit
trap cleanup EXIT