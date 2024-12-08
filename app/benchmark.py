import asyncio
import random
from datetime import datetime
from typing import List, Dict, Any
import logging
import os
from tqdm import tqdm
import json
import time
import statistics
import numpy as np
from collections import defaultdict, Counter
import argparse
from concurrent.futures import ThreadPoolExecutor
from asyncio import gather
from services.database import DatabaseService
from services.messaging import KafkaService
from router import PaymentRouter
from models.payment import (
    SwiftPayment, 
    ACHPayment, 
    WirePayment, 
    SEPAPayment, 
    CryptoPayment,
    RTPPayment
)

import math

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class PaymentBenchmark:
        
    def __init__(self, config: Dict[str, str]):
        self.db_service = DatabaseService(config)
        self.kafka_service = KafkaService(config)
        self.router = PaymentRouter({'bootstrap.servers': config['KAFKA_BOOTSTRAP_SERVERS']})
        self.logger = logging.getLogger(__name__)
        self.metrics = defaultdict(list)
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
        """Initialize metrics collection structure"""
        self.metrics.update({
            'processing_times': [],
            'errors': [],
            'by_type': defaultdict(list),
            'by_route': defaultdict(list),
            'db_latencies': {
                'postgres': defaultdict(list),
                'mongodb': defaultdict(list),
                'redis': defaultdict(list)
            },
            'kafka_latencies': defaultdict(list),
            'batch_times': [],
            'memory_usage': [],
            'success_rates': defaultdict(list)
        })

    def generate_test_payments(self, count: int, selected_types: List[str] = None) -> List[Dict[str, Any]]:
        """Generate test payments"""
        payments = []
        payment_types = ['SWIFT', 'ACH', 'WIRE', 'SEPA', 'CRYPTO', 'RTP'] if not selected_types else selected_types

        for i in range(count):
            payment_type = random.choice(payment_types)
            base_payment = {
                'payment_id': f'PAY{i:010d}',
                'amount': self._generate_realistic_amount(payment_type),
                'currency': self._get_currency_for_type(payment_type),
                'created_at': datetime.utcnow().isoformat()  # Already as string
            }

            try:
                payment = self._create_typed_payment(payment_type, base_payment)
                payments.append(payment.model_dump())
                self.metrics['success_rates'][payment_type].append(1)
            except Exception as e:
                self.logger.error(f"Error generating {payment_type} payment: {e}")
                self.metrics['errors'].append({
                    'type': 'payment_generation',
                    'payment_type': payment_type,
                    'error': str(e)
                })
                self.metrics['success_rates'][payment_type].append(0)

        return payments

    # Update the run_benchmark method in the PaymentBenchmark class
    async def run_benchmark(self,
                           total_payments: int = 10000,
                           batch_size: int = 100,
                           payment_types: List[str] = None,
                           concurrent_batches: int = 3):
        """Run the benchmark with enhanced monitoring and concurrent processing"""
        self.logger.info(f"Starting benchmark with {total_payments} payments...")
        start_time = datetime.now()

        try:
            payments = self.generate_test_payments(total_payments, payment_types)
            batches = [payments[i:i + batch_size] for i in range(0, len(payments), batch_size)]

            self.logger.info(f"Processing {len(batches)} batches...")

            for batch_num, batch in enumerate(tqdm(batches)):
                batch_start = time.time()

                # Process payments in batch concurrently
                tasks = []
                for payment in batch:
                    tasks.append(self.process_payment(payment))

                # Wait for all payments in batch to complete
                await asyncio.gather(*tasks)

                batch_time = time.time() - batch_start
                self.metrics['batch_times'].append(batch_time)

                if batch_num % 10 == 0:  # Log progress every 10 batches
                    self.logger.info(f"Completed batch {batch_num + 1}/{len(batches)}")

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Generate and print detailed metrics
            metrics_report = self.generate_metrics_report(duration)
            print("\nDetailed Benchmark Results:")
            print(json.dumps(metrics_report, indent=2))

            # Save metrics to file
            await self.save_metrics_report(metrics_report)

        except Exception as e:
            self.logger.error(f"Benchmark failed: {e}")
            raise

    async def process_payment(self, payment: Dict[str, Any]):
        """Process a single payment with detailed metrics"""
        start_time = time.time()
        
        try:
            # Serialize payment once for both operations
            serialized_payment = self.db_service.serialize_payment(payment)
            
            # Store in databases with timing
            pg_start = time.time()
            await self.db_service.store_payment_postgres(serialized_payment)
            pg_time = time.time() - pg_start
            self.metrics['db_latencies']['postgres'][payment['type']].append(pg_time)

            mongo_start = time.time()
            await self.db_service.store_payment_mongodb(serialized_payment)
            mongo_time = time.time() - mongo_start
            self.metrics['db_latencies']['mongodb'][payment['type']].append(mongo_time)

            redis_start = time.time()
            await self.db_service.store_payment_redis(serialized_payment)
            redis_time = time.time() - redis_start
            self.metrics['db_latencies']['redis'][payment['type']].append(redis_time)

            # Route payment and get routing decision
            route_start = time.time()
            route_result = await self.router.route_payment(serialized_payment)
            route_time = time.time() - route_start
            route_type = route_result.get('route', 'unknown')
            self.metrics['by_route'][route_type].append(route_time)

            # Produce to Kafka with timing
            kafka_start = time.time()
            await self.kafka_service.produce_message(
                'incoming-payments',
                serialized_payment['payment_id'],
                serialized_payment
            )
            kafka_time = time.time() - kafka_start
            self.metrics['kafka_latencies'][route_type].append(kafka_time)

            # Record success
            processing_time = time.time() - start_time
            self.metrics['by_type'][payment['type']].append(processing_time)
            self.metrics['processing_times'].append(processing_time)

        except Exception as e:
            self.logger.error(f"Error processing payment {payment['payment_id']}: {e}")
            self.metrics['errors'].append({
                'payment_id': payment['payment_id'],
                'type': 'processing',
                'error': str(e)
            })

    def _get_currency_for_type(self, payment_type: str) -> str:
        """Get appropriate currency for payment type"""
        currency_maps = {
            'SEPA': 'EUR',
            'ACH': 'USD',
            'CRYPTO': random.choice(['BTC', 'ETH', 'USDC', 'USDT']),  # Added more crypto options
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
            'CRYPTO': (0.0001, 10)  # Small amounts for crypto
        }
        min_amount, max_amount = amount_ranges.get(payment_type, (1000, 100000))
        if payment_type == 'CRYPTO':
            return round(random.uniform(min_amount, max_amount), 8)  # 8 decimal places for crypto
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
                sender_bic=f'BIC{random.randint(1000,9999)}',
                receiver_bic=f'BIC{random.randint(1000,9999)}'
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
        """Generate detailed metrics report with grid view"""
        total_payments = len(self.metrics['processing_times'])
        
        # Calculate grid metrics
        grid_metrics = {
            'routes': {},
            'databases': {
                'postgres': {},
                'mongodb': {},
                'redis': {}
            },
            'kafka': {}
        }

        # Process route metrics
        for route, times in self.metrics['by_route'].items():
            if times:
                stats = self.calculate_box_plot_stats(times)
                grid_metrics['routes'][route] = {
                    'count': len(times),
                    'avg_time': stats['mean'],
                    'median': stats['median'],
                    'p95': self.calculate_percentile(times, 95),
                    'percentage': (len(times) / total_payments * 100),
                    'box_plot': stats
                }

        # Process database metrics
        for db_type, db_metrics in self.metrics['db_latencies'].items():
            for payment_type, times in db_metrics.items():
                if times:
                    key = f"{payment_type}"
                    stats = self.calculate_box_plot_stats(times)
                    grid_metrics['databases'][db_type][key] = {
                        'avg_time': stats['mean'],
                        'median': stats['median'],
                        'p95': self.calculate_percentile(times, 95),
                        'box_plot': stats
                    }

        # Process Kafka metrics
        for route, times in self.metrics['kafka_latencies'].items():
            if times:
                stats = self.calculate_box_plot_stats(times)
                grid_metrics['kafka'][route] = {
                    'avg_time': stats['mean'],
                    'median': stats['median'],
                    'p95': self.calculate_percentile(times, 95),
                    'box_plot': stats
                }
        
        # Generate summary tables
        summary_tables = {
            'routing_grid': self.format_grid_table(grid_metrics['routes'], 
                ['Route', 'Count', 'Avg (ms)', 'Median (ms)', 'P95 (ms)', '%']),
            'database_grid': self.format_database_grid(grid_metrics['databases']),
            'kafka_grid': self.format_grid_table(grid_metrics['kafka'],
                ['Route', 'Avg (ms)', 'Median (ms)', 'P95 (ms)'])
        }

        return {
            'summary': {
                'total_payments': total_payments,
                'total_duration': duration,
                'average_rate': total_payments / duration if duration > 0 else 0,
                'error_count': len(self.metrics['errors'])
            },
            'grid_metrics': grid_metrics,
            'summary_tables': summary_tables,
            'errors': {
                'count': len(self.metrics['errors']),
                'rate': len(self.metrics['errors']) / total_payments if total_payments > 0 else 0,
                'details': self.metrics['errors'][:10]
            }
        }

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
            for db_name in ['postgres', 'mongodb', 'redis']:
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

    async def save_metrics_report(self, metrics_report: Dict[str, Any]):
        """Save metrics report to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp}.json"
        filepath = os.path.join("benchmark_results", filename)
        
        # Create benchmark_results directory if it doesn't exist
        os.makedirs("benchmark_results", exist_ok=True)
        
        # Write current results
        with open(filepath, 'w') as f:
            json.dump(metrics_report, f, indent=2)
        
        # Keep only last 4 benchmark results
        results_dir = "benchmark_results"
        result_files = []
        for f in os.listdir(results_dir):
            if f.startswith("benchmark_results_") and f.endswith(".json"):
                full_path = os.path.join(results_dir, f)
                result_files.append((os.path.getmtime(full_path), full_path))
        
        # Sort by modification time (newest first)
        result_files.sort(reverse=True)
        
        # Remove older files, keeping only the last 4
        for _, file_path in result_files[4:]:
            try:
                os.remove(file_path)
            except OSError as e:
                self.logger.error(f"Error removing old benchmark result {file_path}: {e}")
        
        self.logger.info(f"Metrics saved to {filename}")

async def main():
    parser = argparse.ArgumentParser(description='Run payment processing benchmark')
    parser.add_argument('--payments', type=int, default=10000, help='Total number of payments to process')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--types', type=str, help='Comma-separated list of payment types to test')
    parser.add_argument('--concurrent', type=int, default=3, help='Number of concurrent batches to process')
    
    args = parser.parse_args()

    config = {
        'POSTGRES_URL': 'postgresql://benchmark_user:benchmark_pass@localhost:5432/payment_benchmark',
        'MONGODB_URL': 'mongodb://localhost:27017',
        'REDIS_URL': 'redis://localhost:6379',
        'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092'
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

if __name__ == "__main__":
    asyncio.run(main())
