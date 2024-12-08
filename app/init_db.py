import asyncio
import asyncpg
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def init_postgres():
    try:
        conn = await asyncpg.connect(
            'postgresql://benchmark_user:benchmark_pass@postgres:5432/payment_benchmark'
        )
        
        # Create tables
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                payment_id VARCHAR(50) UNIQUE,
                type VARCHAR(20),
                amount DECIMAL(15,2),
                currency VARCHAR(3),
                status VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_payment_type 
            ON payments(type)
        ''')
        
        logger.info("PostgreSQL initialized successfully")
        await conn.close()
    except Exception as e:
        logger.error(f"Error initializing PostgreSQL: {e}")
        raise

async def main():
    logger.info("Starting database initialization...")
    await init_postgres()
    logger.info("Database initialization completed")

if __name__ == "__main__":
    asyncio.run(main())
