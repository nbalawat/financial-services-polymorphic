import asyncpg
import motor.motor_asyncio
import redis.asyncio as aioredis
import logging
import json
from typing import Dict, Any
from datetime import datetime
from bson import ObjectId
from bson import json_util

class DatabaseService:
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.pg_pool = None
        self.mongodb = None
        self.redis_client = None
        self.logger = logging.getLogger('services.database')

    async def connect(self):
        """Initialize database connections"""
        try:
            # PostgreSQL connection pool
            self.logger.info("Initializing PostgreSQL connection...")
            self.pg_pool = await asyncpg.create_pool(
                self.config['POSTGRES_URL'],
                min_size=5,
                max_size=20
            )

            if not self.pg_pool:
                raise Exception("Failed to create PostgreSQL connection pool")

            # MongoDB connection
            self.logger.info("Initializing MongoDB connection...")
            mongo_client = motor.motor_asyncio.AsyncIOMotorClient(self.config['MONGODB_URL'])
            self.mongodb = mongo_client.payment_benchmark

            # Redis connection
            self.logger.info("Initializing Redis connection...")
            self.redis_client = await aioredis.from_url(self.config['REDIS_URL'])

            # Initialize PostgreSQL schema
            await self.init_postgres_schema()

            self.logger.info("All database connections established successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize database connections: {e}")
            raise

    async def init_postgres_schema(self):
        """Initialize PostgreSQL schema"""
        async with self.pg_pool.acquire() as conn:
            await conn.execute('''
                DROP TABLE IF EXISTS payments
            ''')
            await conn.execute('''
                CREATE TABLE payments (
                    payment_id VARCHAR(255) PRIMARY KEY,
                    type VARCHAR(50) NOT NULL,
                    amount DECIMAL(20, 2) NOT NULL,
                    currency VARCHAR(10) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    metadata JSONB,
                    data JSONB
                )
            ''')

    def serialize_payment(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize payment data for storage"""
        serialized = payment.copy()
        
        # Remove MongoDB _id field if present
        if '_id' in serialized:
            del serialized['_id']
            
        # Convert datetime objects to ISO format strings
        if isinstance(serialized.get('created_at'), datetime):
            serialized['created_at'] = serialized['created_at'].isoformat()

        # Handle nested datetime objects and ObjectId
        for key, value in list(serialized.items()):  # Create a list to avoid runtime modification issues
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            elif isinstance(value, ObjectId):
                serialized[key] = str(value)
            elif isinstance(value, dict):
                for k, v in list(value.items()):  # Create a list to avoid runtime modification issues
                    if isinstance(v, datetime):
                        value[k] = v.isoformat()
                    elif isinstance(v, ObjectId):
                        value[k] = str(v)

        return serialized

    async def store_payment_postgres(self, payment: Dict[str, Any]):
        """Store payment in PostgreSQL"""
        # Convert created_at to datetime if it's a string
        created_at = payment.get('created_at', datetime.now())
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            except ValueError:
                created_at = datetime.now()

        query = """
            INSERT INTO payments (payment_id, type, amount, currency, status, created_at, metadata, data)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        await self.pg_pool.execute(
            query,
            payment['payment_id'],
            payment['type'],
            payment['amount'],
            payment['currency'],
            payment.get('status', 'pending'),
            created_at,
            json.dumps(payment.get('metadata', {})),
            json.dumps(payment.get('data', {}))
        )

    async def store_payment_mongodb(self, payment: Dict[str, Any]):
        """Store payment in MongoDB"""
        # Convert payment to BSON-compatible format
        bson_payment = json.loads(json_util.dumps(payment))
        await self.mongodb.payments.insert_one(bson_payment)

    async def store_payment_redis(self, payment: Dict[str, Any]):
        """Store payment in Redis"""
        # Convert ObjectId to string for Redis storage
        redis_payment = json.loads(json_util.dumps(payment))
        key = f"payment:{payment['payment_id']}"
        await self.redis_client.set(key, json.dumps(redis_payment))

    async def store_payment(self, payment: Dict[str, Any]):
        """Store payment in databases"""
        try:
            # Serialize payment data
            serialized_payment = self.serialize_payment(payment)

            # Store in PostgreSQL
            await self.store_payment_postgres(serialized_payment)

            # Store in MongoDB
            await self.store_payment_mongodb(serialized_payment)

            # Cache in Redis
            await self.store_payment_redis(serialized_payment)

        except Exception as e:
            self.logger.error(f"Error storing payment {payment['payment_id']}: {e}")
            raise

    async def clear_all_databases(self):
        """Clear all data from PostgreSQL, MongoDB and Redis"""
        try:
            self.logger.info("Clearing all databases...")
            
            # Clear PostgreSQL
            async with self.pg_pool.acquire() as conn:
                await conn.execute('TRUNCATE TABLE payments')
            
            # Clear MongoDB
            await self.mongodb.payments.delete_many({})
            
            # Clear Redis
            await self.redis_client.flushdb()
            
            self.logger.info("All databases cleared successfully")
        except Exception as e:
            self.logger.error(f"Error clearing databases: {e}")
            raise

    async def cleanup(self):
        """Cleanup database connections"""
        try:
            if self.pg_pool:
                await self.pg_pool.close()
            if self.redis_client:
                await self.redis_client.close()
            self.logger.info("Database connections closed successfully")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
            raise
