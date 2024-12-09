import asyncpg
import motor.motor_asyncio
import redis.asyncio as aioredis
import logging
import json
from typing import Dict, Any
from datetime import datetime
from bson import ObjectId
from bson import json_util
# Commenting out Firestore imports for now
# from google.cloud import firestore
# import google.cloud.firestore_v1.async_client as firestore_async

class DatabaseService:
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.pg_pool = None
        self.mongodb = None
        self.redis_client = None
        # self.firestore_client = None
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

            # Commenting out Firestore initialization
            # self.logger.info("Initializing Firestore connection...")
            # if 'GOOGLE_APPLICATION_CREDENTIALS' in self.config and 'GOOGLE_CLOUD_PROJECT' in self.config:
            #     try:
            #         self.logger.info(f"Using credentials file: {self.config['GOOGLE_APPLICATION_CREDENTIALS']}")
            #         self.logger.info(f"Using project ID: {self.config['GOOGLE_CLOUD_PROJECT']}")
                    
            #         import google.auth
            #         from google.oauth2 import service_account
                    
            #         # Load credentials from the file
            #         credentials = service_account.Credentials.from_service_account_file(
            #             self.config['GOOGLE_APPLICATION_CREDENTIALS']
            #         )
                    
            #         # Initialize Firestore with explicit credentials
            #         self.firestore_client = firestore.AsyncClient(
            #             project=self.config['GOOGLE_CLOUD_PROJECT'],
            #             credentials=credentials
            #         )
                    
            #         # Create payments collection if it doesn't exist
            #         await self.init_firestore_schema()
            #     except Exception as e:
            #         self.logger.error(f"Failed to initialize Firestore: {str(e)}")
            #         self.logger.error(f"Traceback:", exc_info=True)
            #         self.firestore_client = None
            # else:
            #     missing = []
            #     if 'GOOGLE_APPLICATION_CREDENTIALS' not in self.config:
            #         missing.append('GOOGLE_APPLICATION_CREDENTIALS')
            #     if 'GOOGLE_CLOUD_PROJECT' not in self.config:
            #         missing.append('GOOGLE_CLOUD_PROJECT')
            #     self.logger.warning(f"Missing required Firestore configuration: {', '.join(missing)}")

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

    # Commenting out Firestore schema initialization
    # async def init_firestore_schema(self):
    #     """Initialize Firestore collections if they don't exist"""
    #     try:
    #         # Create a test document to ensure collection exists
    #         payments_ref = self.firestore_client.collection('payments')
    #         test_doc = await payments_ref.document('test').get()
    #         if not test_doc.exists:
    #             await payments_ref.document('test').set({'test': True})
    #             await payments_ref.document('test').delete()
    #         self.logger.info("Firestore schema initialized successfully")
    #     except Exception as e:
    #         self.logger.error(f"Failed to initialize Firestore schema: {e}")
    #         raise

    def serialize_payment(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize payment data for storage"""
        serialized = payment.copy()
        
        # Convert datetime objects to ISO format strings
        for key, value in payment.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
        
        # Convert ObjectId to string if present
        if '_id' in serialized and isinstance(serialized['_id'], ObjectId):
            serialized['_id'] = str(serialized['_id'])
            
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

    # Commenting out Firestore payment storage
    # async def store_payment_firestore(self, payment_data: Dict[str, Any]) -> str:
    #     """Store payment data in Firestore"""
    #     try:
    #         # Convert datetime to string for Firestore
    #         payment_data['timestamp'] = payment_data['timestamp'].isoformat()
            
    #         # Add payment to Firestore
    #         doc_ref = self.firestore_client.collection('payments').document()
    #         await doc_ref.set(payment_data)
    #         return doc_ref.id
    #     except Exception as e:
    #         self.logger.error(f"Failed to store payment in Firestore: {e}")
    #         raise

    # Commenting out Firestore payment retrieval
    # async def get_payment_firestore(self, payment_id: str) -> Dict[str, Any]:
    #     """Retrieve payment data from Firestore"""
    #     try:
    #         doc_ref = self.firestore_client.collection('payments').document(payment_id)
    #         doc = await doc_ref.get()
    #         if doc.exists:
    #             data = doc.to_dict()
    #             # Convert timestamp string back to datetime
    #             data['timestamp'] = datetime.fromisoformat(data['timestamp'])
    #             return data
    #         return None
    #     except Exception as e:
    #         self.logger.error(f"Failed to retrieve payment from Firestore: {e}")
    #         raise

    async def store_payment(self, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Store payment in databases"""
        start_time = datetime.now()
        metrics = {
            'database': payment.get('database', 'postgres'),  # default to postgres if not specified
            'db_persistence_time_ms': 0
        }
        
        try:
            # Serialize payment data first
            serialized_payment = self.serialize_payment(payment)
            
            if payment.get('database') == 'mongodb':
                await self.store_payment_mongodb(serialized_payment)
            elif payment.get('database') == 'redis':
                await self.store_payment_redis(serialized_payment)
            else:
                await self.store_payment_postgres(serialized_payment)
                
            end_time = datetime.now()
            metrics['db_persistence_time_ms'] = (end_time - start_time).total_seconds() * 1000
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to store payment: {e}")
            raise

    # Commenting out Firestore clearing
    # async def clear_firestore(self):
    #     """Clear all data from Firestore efficiently using batched deletes"""
    #     if not self.firestore_client:
    #         return
            
    #     try:
    #         self.logger.info("Clearing Firestore database...")
    #         payments_ref = self.firestore_client.collection('payments')
            
    #         # Delete in batches of 500 (Firestore's limit)
    #         while True:
    #             # Get a batch of documents
    #             docs = [doc async for doc in payments_ref.limit(500).stream()]
    #             if not docs:
    #                 break
                    
    #             # Create a new batch
    #             batch = self.firestore_client.batch()
    #             for doc in docs:
    #                 batch.delete(doc.reference)
                    
    #             # Commit the batch
    #             await batch.commit()
            
    #         self.logger.info("Firestore cleared successfully")
    #     except Exception as e:
    #         self.logger.error(f"Error clearing Firestore: {e}")
    #         raise

    async def clear_all_databases(self):
        """Clear all data from PostgreSQL, MongoDB, Redis, and Firestore"""
        try:
            self.logger.info("Clearing all databases...")
            
            # Clear PostgreSQL
            async with self.pg_pool.acquire() as conn:
                await conn.execute('TRUNCATE TABLE payments')
            
            # Clear MongoDB
            await self.mongodb.payments.delete_many({})
            
            # Clear Redis
            await self.redis_client.flushdb()
            
            # Commenting out Firestore clearing
            # await self.clear_firestore()

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
