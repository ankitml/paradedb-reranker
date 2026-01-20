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
import re
import csv
import json
import logging
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from typing import Dict, Any, Optional, List, Union, Tuple, Iterator
from contextlib import contextmanager
from pathlib import Path

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
            "password": os.getenv("DB_PASSWORD", ""),
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
        required_fields = ["host", "port", "database", "user"]
        for field in required_fields:
            if not config.get(field):
                print_error(f"❌ Missing required configuration: {field}")
                return False

        return True

    @staticmethod
    def get_openrouter_config() -> Dict[str, str]:
        """Get OpenRouter API configuration from environment variables

        Returns:
            Dict with OpenRouter configuration
        """
        return {
            "api_key": os.getenv("OPENROUTER_API_KEY"),
            "base_url": "https://openrouter.ai/api/v1",
            "model": "sentence-transformers/all-minilm-l12-v2"
        }

    @staticmethod
    def get_batch_size(config_key: str, default: int = 1000) -> int:
        """Get batch size from environment variable with fallback

        Args:
            config_key: Environment variable key
            default: Default batch size

        Returns:
            Batch size integer
        """
        return int(os.getenv(config_key, str(default)))

    @staticmethod
    def validate_openrouter_config() -> bool:
        """Validate OpenRouter API configuration

        Returns:
            bool: True if configuration is valid
        """
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            print_error("❌ OpenRouter API key not configured!")
            print_info("💡 Set OPENROUTER_API_KEY environment variable")
            print_info("📝 Example: export OPENROUTER_API_KEY='your-key-here'")
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
        except Exception as e:
            print_error(f"❌ Database connection failed: {e}")
            raise

    def close(self) -> None:
        """Close database connection"""
        if self.conn:
            self.conn.close()

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

    def execute_update(self, query: str, params: Optional[tuple] = None, commit: bool = True) -> int:
        """Execute an UPDATE/INSERT/DELETE query and return affected row count

        Args:
            query: SQL query to execute
            params: Optional query parameters
            commit: Whether to auto-commit the transaction (default: True)

        Returns:
            Number of affected rows
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")

        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                affected_rows = cursor.rowcount

                if commit:
                    self.conn.commit()

                return affected_rows
        except Exception as e:
            print_error(f"❌ Update execution failed: {e}")
            if commit:
                self.conn.rollback()
            raise

    def execute_no_response(self, query: str, params: Optional[tuple] = None) -> None:
        """Execute a SQL query without expecting results (CREATE, DROP, etc.)

        Args:
            query: SQL query to execute
            params: Optional query parameters
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")

        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
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


class MovieDataUtils:
    """Utilities for processing movie data"""

    @staticmethod
    def extract_year_from_title(title: str) -> Tuple[str, Optional[int]]:
        """Extract year from movie title and clean title

        Args:
            title: Movie title possibly containing year in parentheses

        Returns:
            Tuple of (clean_title, year) where year may be None
        """
        year_match = re.search(r'\((\d{4})\)$', title)
        if year_match:
            year = int(year_match.group(1))
            clean_title = title[:year_match.start()].rstrip()
            return clean_title, year
        return title, None

    @staticmethod
    def parse_genres(genres_str: str) -> List[str]:
        """Parse pipe-separated genres into list

        Args:
            genres_str: Pipe-separated genre string

        Returns:
            List of genre strings
        """
        if genres_str == "(no genres listed)":
            return []
        return [genre.strip() for genre in genres_str.split("|") if genre.strip()]

    @staticmethod
    def format_movie_text(title: str, year: Optional[int] = None, genres: List[str] = None) -> str:
        """Format movie data for embedding generation

        Args:
            title: Movie title
            year: Optional year
            genres: Optional list of genres

        Returns:
            Formatted text string for embedding
        """
        parts = [title]
        if year:
            parts.append(str(year))
        if genres:
            parts.append(' '.join(genres))
        return ' '.join(parts)


class FileUtils:
    """Utilities for file processing and validation"""

    @staticmethod
    def validate_file_exists(file_path: Path, description: str = "File") -> None:
        """Validate that a file exists, raise exception if not

        Args:
            file_path: Path to file to validate
            description: Description for error message

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if not file_path.exists():
            raise FileNotFoundError(f"{description} not found: {file_path}")

    @staticmethod
    def count_csv_rows(file_path: Path) -> int:
        """Count rows in CSV file (excluding header)

        Args:
            file_path: Path to CSV file

        Returns:
            Number of data rows
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            return sum(1 for _ in file) - 1  # Subtract header row

    @staticmethod
    def batch_csv_reader(file_path: Path, batch_size: int) -> Iterator[List[Dict[str, Any]]]:
        """Read CSV file in batches for memory efficiency

        Args:
            file_path: Path to CSV file
            batch_size: Number of rows per batch

        Yields:
            List of dictionaries representing CSV rows
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            batch = []

            for row in reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            if batch:  # Yield remaining rows
                yield batch

    @staticmethod
    def load_json_embeddings(csv_path: Path, movie_id_col: str = 'movie_id',
                           embedding_col: str = 'movie_embedding') -> List[Dict[str, Any]]:
        """Load embeddings from CSV with JSON-encoded vectors

        Args:
            csv_path: Path to CSV file
            movie_id_col: Name of movie ID column
            embedding_col: Name of embedding column

        Returns:
            List of dictionaries with movie_id and embedding_vector
        """
        embeddings = []

        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                movie_id = int(row[movie_id_col])
                # Parse JSON string back to list of floats
                embedding_vector = json.loads(row[embedding_col])

                embeddings.append({
                    'movie_id': movie_id,
                    'content_embedding': embedding_vector
                })

        return embeddings


class LoggingUtils:
    """Utilities for logging configuration"""

    @staticmethod
    def setup_logging(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
        """Configure logging for the ingestion process

        Args:
            name: Logger name
            level: Logging level

        Returns:
            Configured logger instance
        """
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(name)


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
