# app/router.py
import asyncio
from typing import Dict, Any, List
from confluent_kafka import Producer, Consumer, KafkaError
import json
import logging
import time
import statistics
from datetime import datetime

class PaymentRouter:
    def __init__(self, kafka_config: Dict[str, Any]):
        self.producer = Producer(kafka_config)
        self.routes = {
            'SWIFT': self.handle_swift,
            'ACH': self.handle_ach,
            'SEPA': self.handle_sepa,
            'WIRE': self.handle_wire,
            'CRYPTO': self.handle_crypto,
            'RTP': self.handle_rtp,
            'CARD': self.handle_card
        }
        self.logger = logging.getLogger(__name__)

    def serialize_payment(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize payment data to ensure JSON compatibility"""
        serialized = {}
        for key, value in payment.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            else:
                serialized[key] = value
        return serialized

    async def route_payment(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Route payment based on type"""
        start_time = datetime.now()
        metrics = {
            'payment_type': payment.get('type', 'UNKNOWN'),
            'router_type': None,
            'kafka_time_ms': 0,
            'total_time_ms': 0
        }
        
        try:
            payment_type = payment.get('type', 'UNKNOWN')
            if payment_type in self.routes:
                # Serialize payment before sending to Kafka
                serialized_payment = self.serialize_payment(payment)
                result = await self.routes[payment_type](serialized_payment)
                metrics['router_type'] = result.get('route', 'unknown')
            else:
                result = await self.handle_unknown_type(payment)
                metrics['router_type'] = 'unknown'
            
            # Wait for Kafka message delivery
            kafka_start = datetime.now()
            self.producer.flush()
            kafka_end = datetime.now()
            
            # Calculate timings
            end_time = datetime.now()
            metrics['kafka_time_ms'] = (kafka_end - kafka_start).total_seconds() * 1000
            metrics['total_time_ms'] = (end_time - start_time).total_seconds() * 1000
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error routing payment: {e}")
            await self.handle_error(payment, str(e))
            raise

    async def handle_swift(self, payment: Dict[str, Any]):
        """Handle SWIFT payment processing"""
        if payment.get('amount', 0) > 1000000:
            return await self.handle_high_value_swift(payment)
        return await self.handle_normal_swift(payment)

    async def handle_high_value_swift(self, payment: Dict[str, Any]):
        """Handle high-value SWIFT payment"""
        self.producer.produce(
            'high-value-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'high-value-swift'}

    async def handle_normal_swift(self, payment: Dict[str, Any]):
        """Handle normal SWIFT payment"""
        self.producer.produce(
            'normal-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'normal-swift'}

    async def handle_ach(self, payment: Dict[str, Any]):
        """Handle ACH payment processing"""
        if payment.get('amount', 0) > 25000:
            return await self.handle_high_value_ach(payment)
        return await self.handle_normal_ach(payment)

    async def handle_high_value_ach(self, payment: Dict[str, Any]):
        """Handle high-value ACH payment"""
        self.producer.produce(
            'high-value-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'high-value-ach'}

    async def handle_normal_ach(self, payment: Dict[str, Any]):
        """Handle normal ACH payment"""
        self.producer.produce(
            'normal-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'normal-ach'}

    async def handle_sepa(self, payment: Dict[str, Any]):
        """Handle SEPA payment processing"""
        if payment.get('amount', 0) > 50000:
            return await self.handle_high_value_sepa(payment)
        return await self.handle_normal_sepa(payment)

    async def handle_high_value_sepa(self, payment: Dict[str, Any]):
        """Handle high-value SEPA payment"""
        self.producer.produce(
            'high-value-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'high-value-sepa'}

    async def handle_normal_sepa(self, payment: Dict[str, Any]):
        """Handle normal SEPA payment"""
        self.producer.produce(
            'normal-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'normal-sepa'}

    async def handle_wire(self, payment: Dict[str, Any]):
        """Handle wire payment processing"""
        if payment.get('amount', 0) > 100000:
            return await self.handle_high_value_wire(payment)
        return await self.handle_normal_wire(payment)

    async def handle_high_value_wire(self, payment: Dict[str, Any]):
        """Handle high-value wire payment"""
        self.producer.produce(
            'high-value-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'high-value-wire'}

    async def handle_normal_wire(self, payment: Dict[str, Any]):
        """Handle normal wire payment"""
        self.producer.produce(
            'normal-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'normal-wire'}

    async def handle_crypto(self, payment: Dict[str, Any]):
        """Handle crypto payment processing"""
        currency = payment.get('currency', '').upper()
        if currency in ['BTC', 'ETH']:
            return await self.handle_major_crypto(payment)
        return await self.handle_alt_crypto(payment)

    async def handle_major_crypto(self, payment: Dict[str, Any]):
        """Handle major cryptocurrency payment"""
        self.producer.produce(
            'major-crypto-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'major-crypto'}

    async def handle_alt_crypto(self, payment: Dict[str, Any]):
        """Handle alternative cryptocurrency payment"""
        self.producer.produce(
            'alt-crypto-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'alt-crypto'}

    async def handle_rtp(self, payment: Dict[str, Any]):
        """Handle RTP (Real-Time Payment) processing"""
        if payment.get('amount', 0) > 100000:
            return {'status': 'rejected', 'route': 'rejected-rtp', 'reason': 'Amount exceeds RTP limit'}
        return await self.handle_normal_rtp(payment)

    async def handle_normal_rtp(self, payment: Dict[str, Any]):
        """Handle normal RTP payment"""
        self.producer.produce(
            'rtp-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'normal-rtp'}

    async def handle_card(self, payment: Dict[str, Any]):
        """Handle card payment processing"""
        self.producer.produce(
            'card-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'processing', 'route': 'card'}

    async def handle_error(self, payment: Dict[str, Any], error: str):
        """Handle payment processing error"""
        self.logger.error(f"Payment processing error: {error}")
        self.producer.produce(
            'error-payments',
            key=payment.get('payment_id', 'unknown'),
            value=json.dumps({
                'payment': self.serialize_payment(payment), 
                'error': error
            })
        )
        return {'status': 'error', 'route': 'error', 'error': error}

    async def handle_unknown_type(self, payment: Dict[str, Any]):
        """Handle unknown payment type"""
        self.producer.produce(
            'unknown-payments',
            key=payment['payment_id'],
            value=json.dumps(payment)
        )
        return {'status': 'unknown', 'route': 'unknown-type'}

class PaymentProcessor:
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        self.router = PaymentRouter(kafka_config)
        self.db_service = DatabaseService(db_config)
        self.performance_metrics = []
        self.logger = logging.getLogger(__name__)

    async def process_batch(self, payments: List[Dict[str, Any]]):
        """Process a batch of payments"""
        for payment in payments:
            try:
                # Route the payment and get routing metrics
                routing_metrics = await self.router.route_payment(payment)
                
                # Store in database and get storage metrics
                db_metrics = await self.db_service.store_payment(payment)
                
                # Combine all metrics
                metrics = {
                    'payment_type': routing_metrics['payment_type'],
                    'router_type': routing_metrics['router_type'],
                    'database': db_metrics['database'],
                    'db_persistence_time_ms': db_metrics['db_persistence_time_ms'],
                    'kafka_time_ms': routing_metrics['kafka_time_ms'],
                    'total_time_ms': routing_metrics['total_time_ms']
                }
                
                self.performance_metrics.append(metrics)
                
            except Exception as e:
                self.logger.error(f"Error processing payment: {e}")
                continue

    def get_performance_metrics(self) -> List[Dict[str, Any]]:
        """Get all collected performance metrics"""
        return self.performance_metrics
