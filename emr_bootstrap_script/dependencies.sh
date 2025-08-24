#!/bin/bash

# First run the existing package installations for Spark
sudo yum install -y python3-pip

# Install core packages for system Python (3.7)
sudo python3 -m pip install pandas numpy boto3 botocore elasticsearch==8 kafka-python jsonschema aws-glue-schema-registry pyarrow fastjsonschema

# Install packages in smaller groups with --ignore-installed
echo "Installing base ML dependencies..."
sudo python3 -m pip install --ignore-installed dm-tree
sudo python3 -m pip install --ignore-installed scipy

echo "Installing TensorFlow and related packages..."
sudo python3 -m pip install --ignore-installed tensorflow
sudo python3 -m pip install --ignore-installed keras h5py

# Install Python 3.9 dependencies
# sudo yum groupinstall -y "Development Tools"
# sudo yum install -y openssl-devel bzip2-devel libffi-devel xz-devel sqlite-devel

# # Install OpenSSL 1.1.1 for Python 3.9
# cd /tmp
# wget https://www.openssl.org/source/openssl-1.1.1w.tar.gz
# tar xzf openssl-1.1.1w.tar.gz
# cd openssl-1.1.1w
# mkdir /usr/local/python3.9-openssl
# ./config --prefix=/usr/local/python3.9-openssl shared
# make
# sudo make install
# export LDFLAGS="-L/usr/local/python3.9-openssl/lib"
# export CPPFLAGS="-I/usr/local/python3.9-openssl/include"
# export LD_LIBRARY_PATH=/usr/local/python3.9-openssl/lib:$LD_LIBRARY_PATH

# Install Python 3.9 with custom OpenSSL
# cd /tmp
# wget https://www.python.org/ftp/python/3.9.18/Python-3.9.18.tgz
# tar xzf Python-3.9.18.tgz
# cd Python-3.9.18
# ./configure --prefix=/usr/local/python3.9 --with-openssl=/usr/local/python3.9-openssl --enable-optimizations
# sudo make altinstall

# # Create symlinks
# sudo ln -sf /usr/local/python3.9/bin/python3.9 /usr/local/bin/python3.9
# sudo ln -sf /usr/local/python3.9/bin/pip3.9 /usr/local/bin/pip3.9

# # Install Python 3.9 packages
# /usr/local/bin/pip3.9 install \
#    pandas \
#    numpy \
#    pyspark \
#    tensorflow \
#    tf-keras \
#    transformers \
#    h5py \
#    scikit-learn \
#    pyarrow \
#    langchain \
#    langchain-groq \
#    langchain-core \
#    faiss-cpu \
#    openai \
#    sentence-transformers \
#    flask \
#    python-dotenv \
#    pyiceberg

# Install AWS CLI and setup
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# CUDA setup if needed
# if nvidia-smi &>/dev/null; then
#    sudo yum install -y cuda-toolkit-11-8 libcudnn8
# fi

# Setup Spark environment
JARS_FOLDER_PATH="s3://elasticmapreduce-uraeusdev/jars/"
aws s3 sync $JARS_FOLDER_PATH .
sudo mv *.jar /usr/lib/spark/jars/

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
SITE_PACKAGES=$(python3 -m site --user-site)

echo "export PYSPARK_PYTHON=/usr/bin/python3" | sudo tee -a /usr/lib/spark/conf/spark-env.sh
echo "export PYTHONPATH=$PYTHONPATH:$SITE_PACKAGES:/usr/local/lib/python$PYTHON_VERSION/site-packages" | sudo tee -a /usr/lib/spark/conf/spark-env.sh
echo "export TF_CPP_MIN_LOG_LEVEL=2" | sudo tee -a /usr/lib/spark/conf/spark-env.sh

# Verify installations
# echo "Verifying installations..."
# python3 -V
# python3.9 -V

echo "Bootstrap script completed"