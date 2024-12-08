# app/router.py
import asyncio
from typing import Dict, Any, List
from confluent_kafka import Producer, Consumer, KafkaError
import json
import logging
import time
import statistics

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

    async def route_payment(self, payment: Dict[str, Any]):
        """Route payment based on type"""
        payment_type = payment.get('type')
        handler = self.routes.get(payment_type)
        
        if handler:
            try:
                return await handler(payment)
            except Exception as e:
                self.logger.error(f"Error processing {payment_type} payment: {e}")
                return await self.handle_error(payment, str(e))
        else:
            return await self.handle_unknown_type(payment)

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
        self.producer.produce(
            'failed-payments',
            key=payment['payment_id'],
            value=json.dumps({'payment': payment, 'error': error})
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
        self.db_config = db_config
        self.processing_times = []

    async def process_batch(self, payments: List[Dict[str, Any]]):
        """Process a batch of payments"""
        results = []
        for payment in payments:
            start_time = time.time()
            result = await self.router.route_payment(payment)
            processing_time = time.time() - start_time
            self.processing_times.append(processing_time)
            results.append(result)
        return results

    def get_performance_metrics(self):
        """Calculate performance metrics"""
        return {
            'average_processing_time': statistics.mean(self.processing_times),
            'max_processing_time': max(self.processing_times),
            'min_processing_time': min(self.processing_times),
            'total_payments': len(self.processing_times)
        }
