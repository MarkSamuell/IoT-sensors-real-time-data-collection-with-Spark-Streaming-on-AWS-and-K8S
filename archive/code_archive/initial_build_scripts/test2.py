from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, MapType

# Elasticsearch configuration
es_host = "10.0.3.36"
es_port = "9200"
es_index = "test"

# Create Elasticsearch client
es = Elasticsearch([{'host': es_host, 'port': es_port}])

# Create SparkSession
spark = SparkSession.builder \
    .appName("TestToElasticsearch") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.15.2") \
    .getOrCreate()

# Define schema
schema = StructType() \
    .add("eventType", StringType()) \
    .add("customerId", StringType()) \
    .add("productId", StringType()) \
    .add("timestamp", StringType()) \
    .add("metadata", MapType(StringType(), StringType())) \
    .add("quantity", IntegerType()) \
    .add("totalAmount", FloatType()) \
    .add("paymentMethod", StringType())

# Create test data DataFrame
test_data = spark.createDataFrame([{
    'eventType': 'mark',
    'customerId': '333333333333333333333333333333',
    'productId': '67890',
    'timestamp': '2024-07-27T11:44:45',
    'metadata': {'category': 'Books', 'source': 'Advertisement'},
    'quantity': 1,
    'totalAmount': 15.75,
    'paymentMethod': 'Card'
}], schema)

# Elasticsearch configuration for Spark
es_write_conf = {
    "es.nodes": es_host,
    "es.port": es_port,
    "es.index.auto.create": "true"
}

# Write data to Elasticsearch
test_data.write \
    .format("org.elasticsearch.spark.sql") \
    .options(**es_write_conf) \
    .mode("append") \
    .save(es_index)