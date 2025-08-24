#!/bin/bash

# Script to automate updating k8s simulator scripts in Kubernetes
# This script creates/updates the ConfigMap and restarts the alpine-tools pod

NAMESPACE="dev"
CONFIGMAP_NAME="k8s-log-simulators"
POD_NAME="alpine-tools"
YAML_FILE="kafka_clients/alpine_tools_for_kafka_producers.yaml"

echo "🚀 Updating K8s Simulator Scripts..."

# Check if we're in the right directory
if [ ! -d "kafka_clients/logs_simulators" ]; then
    echo "❌ Error: kafka_clients/logs_simulators directory not found"
    echo "Please run this script from the project root directory"
    exit 1
fi

# Create/update the ConfigMap
echo "📝 Creating/updating ConfigMap: $CONFIGMAP_NAME"
kubectl create configmap $CONFIGMAP_NAME -n $NAMESPACE \
    --from-file=kafka_clients/logs_simulators/UI_com_logs_producer_to_k8s_kafka.py \
    --from-file=kafka_clients/logs_simulators/UI_sevs_producer_to_k8s_kafka.py \
    --from-file=kafka_clients/logs_simulators/API_logs_producer_to_k8s_kafka.py \
    --from-file=kafka_clients/logs_simulators/car_models_vin.csv \
    --from-file=kafka_clients/logs_simulators/business_ids.csv \
    --dry-run=client -o yaml | kubectl apply -f -

if [ $? -eq 0 ]; then
    echo "✅ ConfigMap updated successfully"
else
    echo "❌ Failed to update ConfigMap"
    exit 1
fi

# Check if pod exists and delete it
echo "🔄 Checking for existing pod: $POD_NAME"
if kubectl get pod $POD_NAME -n $NAMESPACE >/dev/null 2>&1; then
    echo "🗑️  Deleting existing pod: $POD_NAME"
    kubectl delete pod $POD_NAME -n $NAMESPACE
    
    # Wait for pod to be deleted
    echo "⏳ Waiting for pod deletion..."
    kubectl wait --for=delete pod/$POD_NAME -n $NAMESPACE --timeout=60s
fi

# Create the new pod
echo "🆕 Creating new pod: $POD_NAME"
kubectl apply -f $YAML_FILE

if [ $? -eq 0 ]; then
    echo "✅ Pod creation initiated"
    
    # Wait for pod to be ready
    echo "⏳ Waiting for pod to be ready..."
    kubectl wait --for=condition=Ready pod/$POD_NAME -n $NAMESPACE --timeout=300s
    
    if [ $? -eq 0 ]; then
        echo "🎉 Pod is ready!"
        echo ""
        echo "📋 Available scripts in the pod:"
        kubectl exec $POD_NAME -n $NAMESPACE -- ls -la /app/simulators/
        echo ""
        echo "🔗 To connect to the pod:"
        echo "kubectl exec -it $POD_NAME -n $NAMESPACE -- sh"
        echo ""
        echo "🏃 To run the scripts:"
        echo "python3 /app/simulators/UI_com_logs_producer_to_k8s_kafka.py <topic> <bootstrap_servers>"
        echo "python3 /app/simulators/UI_sevs_producer_to_k8s_kafka.py <topic> <bootstrap_servers>"
        echo "python3 /app/simulators/API_logs_producer_to_k8s_kafka.py api-topic <bootstrap_servers>"
    else
        echo "⚠️  Pod creation timed out. Check pod status with:"
        echo "kubectl describe pod $POD_NAME -n $NAMESPACE"
    fi
else
    echo "❌ Failed to create pod"
    exit 1
fi

echo ""
echo "✨ Update complete!"