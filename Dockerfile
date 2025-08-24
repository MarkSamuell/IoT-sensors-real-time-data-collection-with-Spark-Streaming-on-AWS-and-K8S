FROM apache/spark:3.5.1

USER root

# Install Python dependencies
COPY requirements.txt /opt/
RUN pip3 install --no-cache-dir -r /opt/requirements.txt

# Copy application files
COPY src/ /opt/spark/work-dir/src/
COPY configs/ /opt/spark/work-dir/configs/
COPY spark_runner.py /opt/spark/work-dir/
COPY jars/ /opt/spark/jars/

# Set Python path 
ENV PYTHONPATH=/opt/spark/work-dir:/opt/spark/work-dir/src

# Set working directory
WORKDIR /opt/spark/work-dir

USER 1001