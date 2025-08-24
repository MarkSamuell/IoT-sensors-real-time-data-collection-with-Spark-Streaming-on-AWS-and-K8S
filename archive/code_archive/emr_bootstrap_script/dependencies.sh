#!/bin/bash

# Install necessary packages
sudo yum install -y python3-pip
sudo python3 -m pip install pandas boto3 botocore ec2-metadata elasticsearch==8 kafka-python psycopg2-binary

# Install AWS CLI version 2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Specify the S3 path to the folder containing all the JAR files
JARS_FOLDER_PATH="s3://aws-emr-studio-381492251123-eu-central-1/jars/"

# Download all JAR files from the S3 folder to the local directory
aws s3 sync $JARS_FOLDER_PATH .

# Move all downloaded JAR files to the /usr/lib/spark/jars directory
sudo mv *.jar /usr/lib/spark/jars/

# Get Python version and site-packages location
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
SITE_PACKAGES=$(python3 -m site --user-site)

# Set Python path for PySpark
echo "export PYSPARK_PYTHON=/usr/bin/python3" | sudo tee -a /usr/lib/spark/conf/spark-env.sh
echo "export PYTHONPATH=$PYTHONPATH:$SITE_PACKAGES:/usr/local/lib/python$PYTHON_VERSION/site-packages" | sudo tee -a /usr/lib/spark/conf/spark-env.sh

# Create JAAS configuration file
cat << EOF | sudo tee /usr/lib/spark/conf/kafka_jaas.conf
KafkaClient {
  software.amazon.msk.auth.iam.IAMLoginModule required;
};
EOF

# Add JAAS configuration to Spark defaults
echo "spark.driver.extraJavaOptions -Djava.security.auth.login.config=/usr/lib/spark/conf/kafka_jaas.conf" | sudo tee -a /usr/lib/spark/conf/spark-defaults.conf
echo "spark.executor.extraJavaOptions -Djava.security.auth.login.config=/usr/lib/spark/conf/kafka_jaas.conf" | sudo tee -a /usr/lib/spark/conf/spark-defaults.conf

# Install latest AWS MSK IAM Auth library
sudo wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.9/aws-msk-iam-auth-1.1.9-all.jar -O /usr/lib/spark/jars/aws-msk-iam-auth-1.1.9-all.jar

# Set necessary environment variables
echo "export KAFKA_OPTS=-Djava.security.auth.login.config=/usr/lib/spark/conf/kafka_jaas.conf" | sudo tee -a /etc/environment