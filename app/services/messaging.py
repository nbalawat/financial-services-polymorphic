# app/services/messaging.py
from confluent_kafka import Producer, Consumer, KafkaError
import json
import logging
import asyncio
from typing import Dict, Any, Callable

class KafkaService:
    def __init__(self, config: Dict[str, str]):
        self.config = config  # Store config for consumer creation
        self.producer = Producer({
            'bootstrap.servers': config['KAFKA_BOOTSTRAP_SERVERS']
        })
        self.logger = logging.getLogger(__name__)

    async def produce_message(self, topic: str, key: str, value: Dict[str, Any]):
        """Produce a message to Kafka asynchronously"""
        try:
            # Run Kafka produce and flush in a thread to avoid blocking
            def _produce_and_flush():
                self.producer.produce(
                    topic,
                    key=key,
                    value=json.dumps(value)
                )
                self.producer.flush()

            await asyncio.to_thread(_produce_and_flush)
        except Exception as e:
            self.logger.error(f"Error producing message: {e}")
            raise

    def create_consumer(self, group_id: str, topics: list):
        return Consumer({
            'bootstrap.servers': self.config['KAFKA_BOOTSTRAP_SERVERS'],
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
