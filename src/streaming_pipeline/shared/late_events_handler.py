import logging
from datetime import datetime
from typing import Optional, Dict, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, min, when, to_timestamp, expr, struct, to_json
)
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import kafka.errors
import json

logger = logging.getLogger(__name__)

class LateEventsHandler:
    def __init__(self, 
                bootstrap_servers: str,
                main_topic: str,
                watermark_duration: str = "1 hour"):
        self.bootstrap_servers = bootstrap_servers
        self.main_topic = main_topic
        self.late_topic = f"late_{main_topic}"
        self.watermark_duration = watermark_duration
        self._ensure_late_topic_exists()
        self.producer = self._create_producer()

    def _ensure_late_topic_exists(self):
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        try:
            # Get original topic details
            topics = admin_client.describe_topics([self.main_topic])
            if not topics:
                raise ValueError(f"Original topic {self.main_topic} not found")
            
            original_partitions = len(topics[0].partitions)
            
            # Check if late topic exists
            existing_topics = admin_client.list_topics()
            if self.late_topic not in existing_topics:
                new_topic = NewTopic(
                    name=self.late_topic,
                    num_partitions=original_partitions,
                    replication_factor=len(admin_client.describe_cluster().brokers)
                )
                admin_client.create_topics([new_topic])
                logger.info(f"Created late topic {self.late_topic} with {original_partitions} partitions")
            else:
                logger.info(f"Late topic {self.late_topic} already exists")
                
        except kafka.errors.TopicAlreadyExistsError:
            logger.info(f"Late topic {self.late_topic} already exists")
        except Exception as e:
            logger.error(f"Error ensuring late topic exists: {str(e)}")
            raise
        finally:
            admin_client.close()

    def _create_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1,
            retries=3,
            retry_backoff_ms=1000,
            request_timeout_ms=30000
        )

    def identify_late_events(self, df: DataFrame, raw_message_col: str = "value") -> Tuple[DataFrame, DataFrame]:
        """
        Split messages based on their oldest event timestamp.
        If oldest event is late, entire message is considered late.
        """
        try:
            # Parse the JSON messages to extract timestamps
            parsed_df = df.selectExpr(
                "*",
                f"from_json({raw_message_col}, 'struct<DataEntries:array<struct<TimeStamp:string>>>') as parsed_value"
            )

            # Find oldest timestamp per message
            message_timestamps = parsed_df.selectExpr(
                "*",
                "transform(parsed_value.DataEntries, x -> cast(x.TimeStamp as double)) as timestamps"
            ).selectExpr(
                "*",
                "array_min(timestamps) as oldest_timestamp"
            )

            # Convert timestamp and apply watermark
            with_event_time = message_timestamps.withColumn(
                "event_time",
                when(
                    col("oldest_timestamp") > 1e11,
                    to_timestamp(col("oldest_timestamp") / 1000)
                ).otherwise(
                    to_timestamp(col("oldest_timestamp"))
                )
            )

            watermark_threshold = expr(f"current_timestamp() - interval {self.watermark_duration}")

            # Split based on oldest event time
            current_messages = with_event_time.filter(
                col("event_time") >= watermark_threshold
            )
            
            late_messages = with_event_time.filter(
                col("event_time") < watermark_threshold
            )

            # Select original columns only
            current_messages = current_messages.select(df.columns)
            late_messages = late_messages.select(df.columns)

            logger.info(f"Split messages - Current: {current_messages.count()}, Late: {late_messages.count()}")
            return current_messages, late_messages

        except Exception as e:
            logger.error(f"Error identifying late messages: {str(e)}")
            return df, df.limit(0)

    def save_late_events(self, late_messages_df: DataFrame, batch_id: str) -> None:
        """Send late messages to Kafka topic preserving original message structure."""
        try:
            # Convert DataFrame rows to messages
            late_messages = late_messages_df.select(
                col("key").cast("string"),
                col("value").cast("string"),
                col("timestamp")
            ).collect()

            # Send each message
            for message in late_messages:
                try:
                    value = json.loads(message.value)
                    value.update({
                        'batch_id': batch_id,
                        'original_topic': self.main_topic,
                        'late_event_timestamp': datetime.now().isoformat()
                    })
                    
                    self.producer.send(
                        self.late_topic,
                        key=message.key.encode('utf-8') if message.key else None,
                        value=value
                    )
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue

            self.producer.flush()
            logger.info(f"Sent {len(late_messages)} late messages to {self.late_topic}")

        except Exception as e:
            logger.error(f"Error sending late messages to Kafka: {str(e)}")
            raise

    def __del__(self):
        if hasattr(self, 'producer'):
            self.producer.close()