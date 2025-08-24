#!/bin/bash

sudo python3 -m pip install elasticsearch==7


# Specify the S3 path to the Elasticsearch Spark connector JAR file
S3_JAR_PATH="s3://aws-emr-studio-381492251123-eu-central-1/elasticsearch-spark-30_2.12-8.0.0.jar"

# Download the JAR file from S3
aws s3 cp $S3_JAR_PATH .

# Move the JAR file to the /usr/lib/spark/jars directory
sudo mv elasticsearch-spark-30_2.12-8.0.0.jar /usr/lib/spark/jars/


