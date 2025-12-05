#!/usr/bin/env python3
"""
Shared Utilities for Movie Re-ranking System
============================================

Common database operations, configuration management, and utilities
used across all Python scripts in the re-ranking system.

This module provides:
- DatabaseConnection class for reusable database operations
- ConfigManager for environment variable management
- Common print utilities with consistent formatting
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from typing import Dict, Any, Optional, List, Union
from contextlib import contextmanager

# Load environment variables from .env file
load_dotenv()


class ConfigManager:
    """Environment and configuration management for the re-ranking system"""

    @staticmethod
    def get_db_config() -> Dict[str, str]:
        """Get database configuration from environment variables

        Returns:
            Dict with database connection parameters
        """
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "5433")),
            "database": os.getenv("DB_NAME", "postgres"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD") or os.getenv("PGPASSWORD")
        }

    @staticmethod
    def validate_config(config: Dict[str, str]) -> bool:
        """Validate database configuration

        Args:
            config: Database configuration dictionary

        Returns:
            bool: True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        if not config.get("password"):
            print_error("❌ Database password not configured!")
            print_info("💡 Set DB_PASSWORD or PGPASSWORD environment variable")
            print_info("📝 Example: export DB_PASSWORD='your-password-here'")
            return False

        required_fields = ["host", "port", "database", "user", "password"]
        for field in required_fields:
            if not config.get(field):
                print_error(f"❌ Missing required configuration: {field}")
                return False

        return True


class DatabaseConnection:
    """Reusable database connection and transaction management"""

    def __init__(self, config: Optional[Dict[str, str]] = None):
        """Initialize database connection with configuration

        Args:
            config: Database configuration dictionary. If None, loads from environment.
        """
        self.config = config or ConfigManager.get_db_config()
        self.conn: Optional[psycopg2.extensions.connection] = None

        # Validate configuration
        if not ConfigManager.validate_config(self.config):
            raise ValueError("Invalid database configuration")

    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.config)
            self.conn.autocommit = False
            print_success("✅ Connected to PostgreSQL database")
        except Exception as e:
            print_error(f"❌ Database connection failed: {e}")
            raise

    def close(self) -> None:
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print_info("🔌 Database connection closed")

    def execute_batch(self, query: str, data: List[Any], template: Optional[str] = None,
                      page_size: int = 1000) -> None:
        """Execute batch query using psycopg2 execute_values

        Args:
            query: SQL query with %s placeholder
            data: List of data tuples
            template: Optional template for execute_values
            page_size: Page size for batch operations
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")

        try:
            with self.conn.cursor() as cursor:
                execute_values(
                    cursor,
                    query,
                    data,
                    template=template,
                    page_size=page_size
                )
        except Exception as e:
            print_error(f"❌ Batch execution failed: {e}")
            raise

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """Execute a single query and return results

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            List of result tuples
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")

        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            print_error(f"❌ Query execution failed: {e}")
            raise

    def commit(self) -> None:
        """Commit current transaction"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
        self.conn.commit()

    def rollback(self) -> None:
        """Rollback current transaction"""
        if not self.conn:
            raise RuntimeError("Database connection not established")
        self.conn.rollback()

    def __enter__(self) -> 'DatabaseConnection':
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        if exc_type:
            self.rollback()
        self.close()


class PrintUtils:
    """Consistent print utilities with emoji formatting"""

    @staticmethod
    def success(message: str) -> None:
        """Print success message with green emoji"""
        print(f"✅ {message}")

    @staticmethod
    def error(message: str) -> None:
        """Print error message with red emoji"""
        print(f"❌ {message}")

    @staticmethod
    def info(message: str) -> None:
        """Print info message with blue emoji"""
        print(f"ℹ️  {message}")

    @staticmethod
    def warning(message: str) -> None:
        """Print warning message with yellow emoji"""
        print(f"⚠️  {message}")

    @staticmethod
    def progress(message: str) -> None:
        """Print progress message with rocket emoji"""
        print(f"🚀 {message}")

    @staticmethod
    def data(message: str) -> None:
        """Print data-related message with folder emoji"""
        print(f"📁 {message}")

    @staticmethod
    def database(message: str) -> None:
        """Print database-related message with database emoji"""
        print(f"🗄️  {message}")


# Export convenient aliases
print_success = PrintUtils.success
print_error = PrintUtils.error
print_info = PrintUtils.info
print_warning = PrintUtils.warning
print_progress = PrintUtils.progress
print_data = PrintUtils.data
print_database = PrintUtils.database


def get_db_connection() -> DatabaseConnection:
    """Convenient function to get database connection with environment config

    Returns:
        DatabaseConnection instance with environment configuration
    """
    return DatabaseConnection()


@contextmanager
def db_transaction(config: Optional[Dict[str, str]] = None):
    """Context manager for database transactions with automatic rollback on error

    Args:
        config: Optional database configuration. Uses environment if None.

    Yields:
        DatabaseConnection instance
    """
    conn = DatabaseConnection(config)
    try:
        conn.connect()
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()