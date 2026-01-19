"""PostgreSQL database connection using Peewee ORM"""
import os
import logging
from peewee import Model, PostgresqlDatabase

logger = logging.getLogger(__name__)

# Database configuration from environment variables
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/newsglobe')

# Use PostgreSQL database
database = PostgresqlDatabase(DATABASE_URL)


class BaseModel(Model):
    """Base model for all database models"""
    class Meta:
        database = database


def close_database():
    """Close database connection"""
    if not database.is_closed():
        database.close()
        logger.info("Database connection closed")


def initialize_database():
    """Create all tables if they don't exist"""
    from .models import NormalizedItem, Cluster

    logger.info("Initializing PostgreSQL database...")
    with database:
        database.create_tables([NormalizedItem, Cluster], safe=True)
    logger.info("Database tables created successfully")
