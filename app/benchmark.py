import os
import sys
import asyncio
import logging
import math
from datetime import datetime
from typing import Dict, List, Optional, Any
import time
import statistics
import numpy as np
from tqdm import tqdm
import random
import argparse
import uuid
from concurrent.futures import ThreadPoolExecutor
from asyncio import gather
from collections import defaultdict

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.database import DatabaseService
from app.services.messaging import KafkaService
from app.router import PaymentRouter
from app.analysis.performance_analysis import PerformanceAnalyzer
from app.analysis.report_generator import ReportGenerator
from app.models.payment import (
    SwiftPayment,
    ACHPayment,
    SEPAPayment,
    WirePayment,
    CryptoPayment,
    RTPPayment,
    CardPayment
)

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class PaymentBenchmark:
    PAYMENT_TYPES = ['SWIFT', 'ACH', 'SEPA', 'WIRE', 'CRYPTO', 'RTP', 'CARD']
        
    def __init__(self, config: Dict[str, str]):
        self.db_service = DatabaseService(config)
        self.kafka_service = KafkaService(config)
        self.router = PaymentRouter({'bootstrap.servers': config['KAFKA_BOOTSTRAP_SERVERS']})
        self.logger = logging.getLogger(__name__)
        self.metrics = defaultdict(list)
        self.performance_data = []  # Store raw performance data
        self.initialize_metrics()

    async def init(self):
        """Initialize services and database schema"""
        try:
            # First initialize the database service
            await self.db_service.connect()  # Changed from init() to connect()
            
            # Then create schema
            await self.initialize_schema()
            
            self.logger.info("Initialization completed successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize: {e}")
            raise

    async def initialize_schema(self):
        """Initialize database schemas"""
        try:
            # Create PostgreSQL schema
            async with self.db_service.pg_pool.acquire() as conn:
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS payments (
                        id SERIAL PRIMARY KEY,
                        payment_id VARCHAR(50) UNIQUE,
                        type VARCHAR(20),
                        amount DECIMAL(15,2),
                        currency VARCHAR(10),
                        status VARCHAR(20),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        data JSONB  
                    );

                    CREATE INDEX IF NOT EXISTS idx_payment_type ON payments(type);
                    CREATE INDEX IF NOT EXISTS idx_payment_status ON payments(status);
                    CREATE INDEX IF NOT EXISTS idx_payment_created ON payments(created_at);
                ''')

            # Initialize MongoDB indexes
            await self.db_service.mongodb.payments.create_index([("payment_id", 1)], unique=True)
            await self.db_service.mongodb.payments.create_index([("type", 1)])
            await self.db_service.mongodb.payments.create_index([("status", 1)])

            self.logger.info("Database schemas initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database schema: {e}")
            raise

    def initialize_metrics(self):
        """Initialize metrics dictionary"""
        payment_types = self.PAYMENT_TYPES
        
        # Initialize lists for each payment type's metrics
        for payment_type in payment_types:
            self.metrics[f"{payment_type}_processing_times"] = []
            self.metrics[f"{payment_type}_db_times"] = []
            self.metrics[f"{payment_type}_kafka_times"] = []
        
        # Initialize error tracking
        self.metrics['errors'] = []
        
        # Initialize processing times list
        self.metrics['processing_times'] = []

    def _generate_payment_data(self, payment_type: str) -> Dict[str, Any]:
        """Generate payment type specific data"""
        base_data = {
            'payment_id': str(uuid.uuid4()),
            'amount': random.uniform(100, 10000),
            'currency': random.choice(['USD', 'EUR', 'GBP']),
            'created_at': datetime.utcnow(),
            'status': 'RECEIVED',
            'metadata': {},
            'type': payment_type
        }
        
        type_specific_data = {
            'SWIFT': {
                'swift_code': f"SWIFT{random.randint(10000, 99999)}",
                'sender_bic': f"BIC{random.randint(10000, 99999)}",
                'receiver_bic': f"BIC{random.randint(10000, 99999)}",
                'correspondent_bank': f"BANK{random.randint(100, 999)}",
                'intermediary_bank': f"BANK{random.randint(100, 999)}",
                'message_type': 'MT103',
                'settlement_method': 'COVR'
            },
            'ACH': {
                'routing_number': f"{random.randint(100000000, 999999999)}",
                'account_number': f"{random.randint(10000000, 99999999)}",
                'sec_code': random.choice(['PPD', 'CCD', 'WEB']),
                'batch_number': random.randint(1, 9999),
                'company_entry_description': 'PAYMENT'
            },
            'SEPA': {
                'iban': f"EU{random.randint(1000000000, 9999999999)}",
                'bic': f"BIC{random.randint(10000, 99999)}",
                'sepa_type': random.choice(['SCT', 'SDD']),
                'mandate_reference': f"MNDT{random.randint(1000, 9999)}",
                'creditor_id': f"CRED{random.randint(1000, 9999)}",
                'batch_booking': random.choice([True, False])
            },
            'WIRE': {
                'bank_code': f"BANK{random.randint(1000, 9999)}",
                'account_number': f"{random.randint(10000000, 99999999)}",
                'routing_number': f"{random.randint(100000000, 999999999)}",
                'bank_name': f"Bank of {random.choice(['America', 'Europe', 'Asia'])}",
                'beneficiary_name': f"Beneficiary {random.randint(1000, 9999)}",
                'reference_number': f"REF{random.randint(10000, 99999)}"
            },
            'CRYPTO': {
                'wallet_address': f"0x{random.randint(1000000000, 9999999999):x}",
                'network': random.choice(['Mainnet', 'Testnet']),
                'gas_fee': random.uniform(0.001, 0.1),
                'confirmation_blocks': random.randint(1, 12),
                'currency': random.choice(['BTC', 'ETH', 'SOL'])
            },
            'RTP': {
                'clearing_system': random.choice(['TCH', 'FedNow']),
                'priority': random.choice(['HIGH', 'NORMAL']),
                'purpose_code': random.choice(['CASH', 'CORT', 'GDDS'])
            },
            'CARD': {
                'card_number': f"{''.join([str(random.randint(0,9)) for _ in range(16)])}",
                'expiry_month': random.randint(1, 12),
                'expiry_year': random.randint(2024, 2030),
                'cvv': f"{random.randint(100, 999)}",
                'card_type': random.choice(['VISA', 'MASTERCARD', 'AMEX']),
                'card_holder': f"Holder {random.randint(1000, 9999)}",
                'billing_address': f"Address {random.randint(1000, 9999)}"
            }
        }
        
        return {**base_data, **type_specific_data[payment_type]}

    def generate_test_payments(self, count: int, selected_types: List[str] = None) -> List[Dict[str, Any]]:
        """Generate test payments"""
        payments = []
        supported_types = self.PAYMENT_TYPES
        payment_types = [t for t in (selected_types or supported_types) if t in supported_types]
        
        if not payment_types:
            raise ValueError(f"No valid payment types selected. Supported types: {supported_types}")
        
        payment_classes = {
            'SWIFT': SwiftPayment,
            'ACH': ACHPayment,
            'SEPA': SEPAPayment,
            'WIRE': WirePayment,
            'CRYPTO': CryptoPayment,
            'RTP': RTPPayment,
            'CARD': CardPayment
        }
        
        for _ in range(count):
            payment_type = random.choice(payment_types)
            try:
                payment_data = self._generate_payment_data(payment_type)
                payment = payment_classes[payment_type](**payment_data)
                payment_dict = payment.model_dump()
                payment_dict['database_type'] = random.choice(['postgres', 'mongodb', 'redis'])
                payments.append(payment_dict)
            except Exception as e:
                self.logger.error(f"Error generating {payment_type} payment: {e}")
                continue
        
        return payments

    async def run_benchmark(self,
                           total_payments: int = 10000,
                           batch_size: int = 100,
                           payment_types: List[str] = None,
                           concurrent_batches: int = 3):
        """Run the benchmark with enhanced monitoring and concurrent processing"""
        start_time = datetime.now()
        
        # Initialize metrics
        self.initialize_metrics()
        
        # Generate all test payments upfront
        self.logger.info(f"Generating {total_payments} test payments...")
        payments = self.generate_test_payments(total_payments, payment_types)
        
        # Process payments in batches
        tasks = []
        for i in range(0, len(payments), batch_size):
            batch = payments[i:i + batch_size]
            tasks.extend([self.process_batch(batch)])
            
            # Process concurrent_batches at a time
            if len(tasks) >= concurrent_batches * batch_size or i + batch_size >= len(payments):
                with tqdm(total=len(tasks), desc=f"Processing batch {i//batch_size + 1}") as pbar:
                    for task in asyncio.as_completed(tasks):
                        try:
                            await task
                            pbar.update(1)
                        except Exception as e:
                            self.logger.error(f"Error in batch processing: {e}")
                tasks = []
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Generate and save metrics report
        metrics_report = self.generate_metrics_report(duration)
        await self.save_metrics_report(metrics_report)
        
        try:
            # Save detailed performance data
            await self.save_performance_data()
        except Exception as e:
            self.logger.error(f"Error saving performance data: {e}")
        
        return metrics_report

    async def process_batch(self, payments: List[Dict[str, Any]]):
        """Process a batch of payments"""
        for payment in payments:
            try:
                start_time = time.time()
                
                # Route payment
                router_start = time.time()
                await self.router.route_payment(payment)
                router_time = time.time() - router_start
                
                # Store in database
                db_start = time.time()
                await self.db_service.store_payment(payment)
                db_time = time.time() - db_start
                
                # Calculate total time
                total_time = time.time() - start_time
                
                # Store performance data
                perf_data = {
                    'payment_type': payment['type'],
                    'database_type': payment['database_type'],
                    'router_type': self.router.__class__.__name__,  
                    'total_time_ms': total_time * 1000,  
                    'persistence_time': db_time * 1000,  
                    'router_processing_time': router_time * 1000  
                }
                self.performance_data.append(perf_data)
                
                # Update metrics
                self.metrics['processing_times'].append(total_time * 1000)
                
            except Exception as e:
                self.logger.error(f"Error processing payment: {e}")

    async def benchmark_payment_storage(self, payment_type: str, count: int = 500):
        """Benchmark payment storage across databases"""
        try:
            payment_times = {
                'postgres': [],
                'mongodb': [],
                'redis': [],
                'firestore': []  
            }
            
            for _ in tqdm(range(count), desc=f"Processing {payment_type} payments"):
                payment = self.generate_payment(payment_type)
                
                # Measure PostgreSQL time
                pg_start = time.time()
                await self.db_service.store_payment_postgres(payment)
                pg_end = time.time()
                payment_times['postgres'].append(pg_end - pg_start)
                
                # Measure MongoDB time
                mongo_start = time.time()
                await self.db_service.store_payment_mongodb(payment)
                mongo_end = time.time()
                payment_times['mongodb'].append(mongo_end - mongo_start)
                
                # Measure Redis time
                redis_start = time.time()
                await self.db_service.store_payment_redis(payment)
                redis_end = time.time()
                payment_times['redis'].append(redis_end - redis_start)
                
                # Measure Firestore time if available
                if self.db_service.firestore_client:
                    firestore_start = time.time()
                    await self.db_service.store_payment_firestore(payment)
                    firestore_end = time.time()
                    payment_times['firestore'].append(firestore_end - firestore_start)
                
            # Update metrics
            for db_name, times in payment_times.items():
                if times:  
                    self.metrics['grid_metrics']['databases'][db_name][payment_type].extend(times)
                    
        except Exception as e:
            self.logger.error(f"Error in benchmark_payment_storage for {payment_type}: {e}")
            raise

    def _get_currency_for_type(self, payment_type: str) -> str:
        """Get appropriate currency for payment type"""
        currency_maps = {
            'SEPA': 'EUR',
            'ACH': 'USD',
            'CRYPTO': random.choice(['BTC', 'ETH', 'USDC', 'USDT']),  
            'RTP': 'USD'
        }
        return currency_maps.get(payment_type, random.choice(['USD', 'EUR', 'GBP', 'CHF', 'JPY']))

    def _generate_realistic_amount(self, payment_type: str) -> float:
        """Generate realistic payment amounts based on type"""
        amount_ranges = {
            'SWIFT': (10000, 10000000),
            'ACH': (100, 50000),
            'WIRE': (5000, 1000000),
            'SEPA': (100, 100000),
            'RTP': (1, 100000),
            'CRYPTO': (0.0001, 10)  
        }
        min_amount, max_amount = amount_ranges.get(payment_type, (1000, 100000))
        if payment_type == 'CRYPTO':
            return round(random.uniform(min_amount, max_amount), 8)  
        return round(random.uniform(min_amount, max_amount), 2)

    def _generate_metadata(self, payment_type: str) -> Dict[str, Any]:
        """Generate relevant metadata for payment type"""
        return {
            'source_system': random.choice(['API', 'WEB', 'MOBILE', 'BATCH']),
            'customer_type': random.choice(['RETAIL', 'CORPORATE', 'INSTITUTIONAL']),
            'processing_priority': random.choice(['HIGH', 'NORMAL', 'LOW']),
            'purpose': random.choice(['COMMERCIAL', 'PERSONAL', 'SALARY', 'INVESTMENT'])
        }


    def _create_typed_payment(self, payment_type: str, base_payment: Dict[str, Any]):
        """Create a specific payment type with appropriate fields"""
        if payment_type == 'SWIFT':
            return SwiftPayment(
                **base_payment,
                swift_code=f'SWIFT{random.randint(1000,9999)}',
                sender_bic=f"BIC{random.randint(1000,9999)}",
                receiver_bic=f"BIC{random.randint(1000,9999)}"
            )
        elif payment_type == 'ACH':
            return ACHPayment(
                **base_payment,
                routing_number=f'{random.randint(100000000,999999999)}',
                account_number=f'{random.randint(10000000,99999999)}',
                sec_code=random.choice(['PPD', 'CCD', 'WEB'])
            )
        elif payment_type == 'WIRE':
            return WirePayment(
                **base_payment,
                routing_number=f'{random.randint(100000000,999999999)}',
                account_number=f'{random.randint(10000000,99999999)}',
                bank_name=f'BANK{random.randint(100,999)}'
            )
        elif payment_type == 'SEPA':
            return SEPAPayment(
                **base_payment,
                iban=f'DE{random.randint(10000000000000000000,99999999999999999999)}',
                bic=f'DEUTDEFF{random.randint(100,999)}',
                sepa_type=random.choice(['SCT', 'SDD'])
            )
        elif payment_type == 'RTP':
            return RTPPayment(
                **base_payment,
                clearing_system=random.choice(['TCH', 'FED']),
                priority='HIGH'
            )
        elif payment_type == 'CRYPTO':
            return CryptoPayment(
                **base_payment,
                network=random.choice(['ETH', 'BTC']),
                wallet_address=f'0x{random.randint(0,9999999999999999):x}',
                gas_fee=round(random.uniform(0.001, 0.1), 6)
            )
        elif payment_type == 'CARD':
            return CardPayment(
                **base_payment,
                card_number=f"{''.join([str(random.randint(0,9)) for _ in range(16)])}",
                expiry_month=random.randint(1, 12),
                expiry_year=random.randint(2024, 2030),
                cvv=f"{random.randint(100, 999)}",
                card_type=random.choice(['VISA', 'MASTERCARD', 'AMEX']),
                card_holder=f"Holder {random.randint(1000, 9999)}",
                billing_address=f"Address {random.randint(1000, 9999)}"
            )
        else:
            raise ValueError(f"Unknown payment type: {payment_type}")

    def calculate_percentile(self, values, p):
        """Calculate percentile value"""
        if not values:
            return 0
        sorted_values = sorted(values)
        k = (len(sorted_values) - 1) * (p/100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_values[int(k)]
        d0 = sorted_values[int(f)] * (c-k)
        d1 = sorted_values[int(c)] * (k-f)
        return d0 + d1

    def calculate_box_plot_stats(self, values):
        """Calculate statistics needed for box plot"""
        if not values:
            return {
                'q1': 0,
                'median': 0,
                'q3': 0,
                'whisker_low': 0,
                'whisker_high': 0,
                'outliers': [],
                'mean': 0
            }
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        
        # Calculate quartiles
        q1_idx = int(n * 0.25)
        median_idx = int(n * 0.5)
        q3_idx = int(n * 0.75)
        
        q1 = sorted_values[q1_idx]
        median = sorted_values[median_idx]
        q3 = sorted_values[q3_idx]
        
        # Calculate IQR and whiskers
        iqr = q3 - q1
        whisker_low = q1 - 1.5 * iqr
        whisker_high = q3 + 1.5 * iqr
        
        # Adjust whiskers to actual data points
        whisker_low = min([x for x in sorted_values if x >= whisker_low], default=q1)
        whisker_high = max([x for x in sorted_values if x <= whisker_high], default=q3)
        
        # Find outliers
        outliers = [x for x in sorted_values if x < whisker_low or x > whisker_high]
        
        # Calculate mean
        mean = sum(values) / len(values)
        
        return {
            'q1': q1,
            'median': median,
            'q3': q3,
            'whisker_low': whisker_low,
            'whisker_high': whisker_high,
            'outliers': outliers,
            'mean': mean
        }

    def generate_metrics_report(self, duration: float) -> Dict[str, Any]:
        """Generate detailed metrics report"""
        report = {
            'summary': {
                'total_duration_seconds': duration,
                'total_payments_processed': len(self.performance_data),
                'errors': len(self.metrics.get('errors', [])),
                'average_processing_time_ms': 0,
            },
            'by_payment_type': {},
            'by_database': defaultdict(lambda: {'count': 0, 'avg_time_ms': 0}),
            'percentiles': {},
            'errors': self.metrics.get('errors', [])
        }
        
        # Calculate metrics by payment type
        payment_types = set(m['payment_type'] for m in self.performance_data)
        for ptype in payment_types:
            type_metrics = [m for m in self.performance_data if m['payment_type'] == ptype]
            processing_times = [m['total_time_ms'] for m in type_metrics]
            db_times = [m['persistence_time'] for m in type_metrics]
            kafka_times = [m['router_processing_time'] for m in type_metrics]
            
            report['by_payment_type'][ptype] = {
                'count': len(type_metrics),
                'avg_processing_time_ms': sum(processing_times) / len(processing_times) if processing_times else 0,
                'avg_db_time_ms': sum(db_times) / len(db_times) if db_times else 0,
                'avg_kafka_time_ms': sum(kafka_times) / len(kafka_times) if kafka_times else 0,
                'percentiles': {
                    '50th': self.calculate_percentile(processing_times, 50),
                    '95th': self.calculate_percentile(processing_times, 95),
                    '99th': self.calculate_percentile(processing_times, 99)
                }
            }
        
        # Calculate metrics by database
        for metric in self.performance_data:
            db = metric['database_type']
            report['by_database'][db]['count'] += 1
            report['by_database'][db]['avg_time_ms'] += metric['persistence_time']
            
        for db in report['by_database']:
            if report['by_database'][db]['count'] > 0:
                report['by_database'][db]['avg_time_ms'] /= report['by_database'][db]['count']
        
        # Calculate overall average processing time
        if self.performance_data:
            total_time = sum(m['total_time_ms'] for m in self.performance_data)
            report['summary']['average_processing_time_ms'] = total_time / len(self.performance_data)
        
        return report

    def format_grid_table(self, metrics: Dict[str, Dict], headers: List[str]) -> str:
        """Format metrics as a grid table"""
        rows = []
        # Add header
        rows.append('| ' + ' | '.join(headers) + ' |')
        rows.append('|' + '|'.join(['---' for _ in headers]) + '|')
        
        # Add data rows
        for route, data in sorted(metrics.items()):
            if 'count' in data:
                row = [
                    route,
                    str(data['count']),
                    f"{data['avg_time']*1000:.2f}",
                    f"{data['median']*1000:.2f}",
                    f"{data['p95']*1000:.2f}",
                    f"{data['percentage']:.1f}"
                ]
            else:
                row = [
                    route,
                    f"{data['avg_time']*1000:.2f}",
                    f"{data['median']*1000:.2f}",
                    f"{data['p95']*1000:.2f}"
                ]
            rows.append('| ' + ' | '.join(row) + ' |')
        
        return '\n'.join(rows)

    def format_database_grid(self, db_metrics: Dict[str, Dict]) -> str:
        """Format database metrics as a grid table"""
        headers = ['Payment Type', 'Database', 'Avg (ms)', 'Median (ms)', 'P95 (ms)']
        rows = []
        rows.append('| ' + ' | '.join(headers) + ' |')
        rows.append('|' + '|'.join(['---' for _ in headers]) + '|')
        
        payment_types = set()
        for db_type in db_metrics.values():
            payment_types.update(db_type.keys())
        
        for payment_type in sorted(payment_types):
            for db_name in ['postgres', 'mongodb', 'redis', 'firestore']:
                if payment_type in db_metrics[db_name]:
                    data = db_metrics[db_name][payment_type]
                    row = [
                        payment_type,
                        db_name,
                        f"{data['avg_time']*1000:.2f}",
                        f"{data['median']*1000:.2f}",
                        f"{data['p95']*1000:.2f}"
                    ]
                    rows.append('| ' + ' | '.join(row) + ' |')
        
        return '\n'.join(rows)

    async def save_metrics_report(self, metrics_report: Dict):
        """Include metrics in the performance report"""
        self.metrics = metrics_report

    async def save_performance_data(self):
        """Save performance data and generate HTML report"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            results_dir = 'benchmark_results'
            
            # Create the results directory if it doesn't exist
            os.makedirs(results_dir, exist_ok=True)
            
            # Initialize report generator
            report_generator = ReportGenerator(self.performance_data, self.metrics)
            
            # Clean up old results first
            report_generator.cleanup_old_results(results_dir, max_files=3)
            
            # Generate new report
            report_path = os.path.join(results_dir, f'report_{timestamp}.html')
            report_generator.generate_html_report(report_path)
            
            self.logger.info(f"Performance report generated at: {report_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving performance data: {e}")
            raise

async def main():
    parser = argparse.ArgumentParser(description='Run payment processing benchmark')
    parser.add_argument('--payments', type=int, default=10000, help='Total number of payments to process')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--types', type=str, help='Comma-separated list of payment types to test')
    parser.add_argument('--concurrent', type=int, default=3, help='Number of concurrent batches to process')
    
    args = parser.parse_args()

    # Load config from environment variables with fallbacks
    config = {
        'POSTGRES_URL': os.getenv('POSTGRES_URL', 'postgresql://benchmark_user:benchmark_pass@localhost:5432/payment_benchmark'),
        'MONGODB_URL': os.getenv('MONGODB_URL', 'mongodb://localhost:27017'),
        'REDIS_URL': os.getenv('REDIS_URL', 'redis://localhost:6379'),
        'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'GOOGLE_CLOUD_PROJECT': os.getenv('GOOGLE_CLOUD_PROJECT')
    }

    benchmark = PaymentBenchmark(config)
    await benchmark.init()
    
    # Clear all databases before running benchmark
    await benchmark.db_service.clear_all_databases()
    
    selected_types = args.types.split(',') if args.types else None
    await benchmark.run_benchmark(
        total_payments=args.payments,
        batch_size=args.batch_size,
        payment_types=selected_types,
        concurrent_batches=args.concurrent
    )
    
    # Save the performance data
    await benchmark.save_performance_data()

    # Clean up resources
    await benchmark.db_service.cleanup()
    await benchmark.kafka_service.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
