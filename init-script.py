# app/init_db.py
import asyncio
import asyncpg
from motor.motor_asyncio import AsyncIOMotorClient

async def init_postgres():
    conn = await asyncpg.connect('postgres://benchmark_user:benchmark_pass@postgres:5432/payment_benchmark')
    
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            payment_id VARCHAR(50),
            type VARCHAR(20),
            amount DECIMAL(15,2),
            status VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_payment_type ON payments(type);
    ''')
    
    await conn.close()

async def init_mongodb():
    client = AsyncIOMotorClient('mongodb://mongodb:27017')
    db = client.payment_benchmark
    
    # Create indexes
    await db.payments.create_index('payment_id', unique=True)
    await db.payments.create_index('type')

async def main():
    await init_postgres()
    await init_mongodb()

if __name__ == '__main__':
    asyncio.run(main())
