# Polymorphic Payment Processing System

A high-performance, type-safe payment processing system that handles multiple payment types through a unified interface while maintaining specific business rules and validation for each payment type.

## Features

- **Type-Safe Polymorphic Models**: Built with Pydantic for robust type validation
- **Multi-Payment Support**: Handles various payment types including:
  - SWIFT (International wire transfers)
  - ACH (US domestic transfers)
  - SEPA (European payments)
  - Wire Transfers
  - Cryptocurrency transactions
  - Real-Time Payments (RTP)
  - Card Payments
- **Multi-Database Support**: Flexible storage strategies across different databases
- **Performance Monitoring**: Built-in benchmarking and reporting tools
- **Detailed Analytics**: Comprehensive timing statistics and performance visualization

## Architecture

### Polymorphic Data Model

The system uses a polymorphic data model that:
- Maintains type safety through Pydantic models
- Supports different validation rules per payment type
- Enables efficient routing based on payment characteristics
- Provides flexible storage strategies across multiple databases

### Components

1. **Payment Models** (`app/models/`)
   - Base payment model with common fields
   - Type-specific models with unique validation rules
   - Pydantic models for type safety

2. **Payment Router** (`app/router/`)
   - Smart routing based on payment characteristics
   - Type-specific processing rules
   - Performance optimization

3. **Database Layer** (`app/storage/`)
   - Multi-database support
   - Type-specific storage strategies
   - Efficient persistence handling

4. **Benchmarking Tools** (`app/benchmark.py`)
   - Performance measurement
   - Timing analysis
   - Load testing capabilities

5. **Analysis Tools** (`app/analysis/`)
   - Performance metrics calculation
   - Statistical analysis
   - Report generation

## Getting Started

### Prerequisites

- Python 3.8+
- pip
- Virtual environment (recommended)

### Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd polymorphic-data-model
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -e .
```

### Configuration

1. Set up your database connections in `config.yaml`:
```yaml
databases:
  mongodb:
    uri: "mongodb://localhost:27017"
  postgres:
    host: "localhost"
    port: 5432
```

2. Configure payment types in `payment_config.yaml`:
```yaml
payment_types:
  - name: "SWIFT"
    enabled: true
    validation_rules: ...
```

## Usage

### Running Benchmarks

```python
from app.benchmark import PaymentBenchmark

# Create benchmark instance
benchmark = PaymentBenchmark()

# Run benchmarks
benchmark.run_all()

# Generate report
benchmark.generate_report()
```

### Processing Payments

```python
from app.models import Payment
from app.router import PaymentRouter

# Create a payment
payment = Payment(
    type="SWIFT",
    amount=1000.00,
    currency="USD",
    ...
)

# Process payment
router = PaymentRouter()
result = router.process(payment)
```

## Performance Analysis

The system includes comprehensive performance analysis tools that generate detailed reports including:

1. **Timing Statistics**
   - Database operation times
   - Router processing times
   - Total processing times

2. **Distribution Analysis**
   - Performance by payment type
   - Database timing distribution
   - Router timing distribution

3. **Visualization**
   - Box plots for timing distribution
   - Performance trends
   - Outlier analysis

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Cloud Setup

For Google Cloud setup instructions, see [Google Cloud Setup](docs/google_cloud_setup.md).
