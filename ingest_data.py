#!/usr/bin/env python3
"""
MovieLens Data Ingestion Script
================================

Scalable Python script for ingesting MovieLens data into PostgreSQL using:
- psycopg2 with COPY command for bulk performance
- Modern PostgreSQL MERGE/ON CONFLICT syntax for duplicate handling
- Configurable batch sizes for memory efficiency
- Progress tracking for large datasets

Compatible with Python 3.8+
"""

import os
import sys
import csv
import argparse
import logging
from pathlib import Path
from typing import Iterator, List, Dict, Any
from datetime import datetime
import re
from tqdm import tqdm

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


class MovieLensIngester:
    """High-performance MovieLens data ingestion with PostgreSQL COPY and MERGE capabilities"""

    def __init__(self, db_config: Dict[str, str], batch_size: int = 10000):
        self.db_config = db_config
        self.batch_size = batch_size
        self.logger = self._setup_logging()
        self.conn = None

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the ingestion process"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            self.logger.info("✅ Connected to PostgreSQL database")
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            raise

    def close(self) -> None:
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("🔌 Database connection closed")

    def extract_year_from_title(self, title: str) -> tuple:
        """Extract year from movie title and clean title"""
        year_match = re.search(r'\((\d{4})\)$', title)
        if year_match:
            year = int(year_match.group(1))
            clean_title = title[:year_match.start()].rstrip()
            return clean_title, year
        return title, None

    def parse_genres(self, genres_str: str) -> List[str]:
        """Parse pipe-separated genres into PostgreSQL array format"""
        if genres_str == "(no genres listed)":
            return []
        return [genre.strip() for genre in genres_str.split("|") if genre.strip()]

    def batch_csv_reader(self, file_path: Path, batch_size: int = None) -> Iterator[List[Dict[str, Any]]]:
        """Read CSV file in batches for memory efficiency"""
        if batch_size is None:
            batch_size = self.batch_size

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

    def ingest_movies(self, movies_csv: Path, links_csv: Path = None) -> None:
        """Ingest movies with optional external ID mapping"""
        self.logger.info(f"🎬 Starting movie ingestion from {movies_csv}")

        # Load links data if available for external ID mapping
        links_data = {}
        if links_csv and links_csv.exists():
            self.logger.info(f"🔗 Loading external IDs from {links_csv}")
            with open(links_csv, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    movie_id = int(row['movieId'])
                    links_data[movie_id] = {
                        'imdb_id': f"tt{row['imdbId']}" if row.get('imdbId') else None,
                        'tmdb_id': int(row['tmdbId']) if row.get('tmdbId') else None
                    }

        # Count total rows for progress bar
        total_rows = sum(1 for _ in open(movies_csv, 'r', encoding='utf-8')) - 1

        with tqdm(total=total_rows, desc="🎬 Ingesting movies", unit="movies") as pbar:
            for batch in self.batch_csv_reader(movies_csv):
                self._ingest_movies_batch(batch, links_data)
                pbar.update(len(batch))

    def _ingest_movies_batch(self, batch: List[Dict[str, Any]], links_data: Dict[int, Dict]) -> None:
        """Ingest a batch of movies using COPY + ON CONFLICT for upserts"""
        try:
            with self.conn.cursor() as cursor:
                # Prepare batch data
                movie_data = []
                for row in batch:
                    movie_id = int(row['movieId'])
                    title, year = self.extract_year_from_title(row['title'])
                    genres = self.parse_genres(row['genres'])

                    # Add external IDs if available
                    link_info = links_data.get(movie_id, {})

                    movie_data.append((
                        movie_id,           # movie_id
                        title,              # title
                        year,               # year
                        genres,             # genres array
                        link_info.get('imdb_id'),  # imdb_id
                        link_info.get('tmdb_id')   # tmdb_id
                    ))

                # Use PostgreSQL COPY with temporary table + MERGE approach for best performance
                create_temp_table = """
                CREATE TEMP TABLE temp_movies (
                    movie_id INTEGER,
                    title VARCHAR(500),
                    year SMALLINT,
                    genres TEXT[],
                    imdb_id VARCHAR(20),
                    tmdb_id INTEGER
                ) ON COMMIT DROP;
                """

                cursor.execute(create_temp_table)

                # Copy batch to temp table
                execute_values(
                    cursor,
                    """
                    INSERT INTO temp_movies (movie_id, title, year, genres, imdb_id, tmdb_id)
                    VALUES %s
                    """,
                    movie_data,
                    template=None,
                    page_size=len(movie_data)
                )

                # Merge using modern PostgreSQL ON CONFLICT syntax
                merge_query = """
                INSERT INTO movies (movie_id, title, year, genres, imdb_id, tmdb_id)
                SELECT movie_id, title, year, genres, imdb_id, tmdb_id FROM temp_movies
                ON CONFLICT (movie_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    year = EXCLUDED.year,
                    genres = EXCLUDED.genres,
                    imdb_id = EXCLUDED.imdb_id,
                    tmdb_id = EXCLUDED.tmdb_id,
                    created_at = movies.created_at  -- Preserve original created_at
                """

                cursor.execute(merge_query)
                self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"❌ Movie batch ingestion failed: {e}")
            raise

    def ingest_users(self, ratings_csv: Path) -> None:
        """Extract and ingest unique users from ratings data"""
        self.logger.info(f"👥 Extracting users from {ratings_csv}")

        # First pass: collect unique user IDs
        unique_users = set()
        with open(ratings_csv, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                unique_users.add(int(row['userId']))

        self.logger.info(f"📊 Found {len(unique_users)} unique users")

        # Batch insert users
        users_list = list(unique_users)
        total_users = len(users_list)

        with tqdm(total=total_users, desc="👥 Ingesting users", unit="users") as pbar:
            for i in range(0, total_users, self.batch_size):
                batch = users_list[i:i + self.batch_size]
                self._ingest_users_batch(batch)
                pbar.update(len(batch))

    def _ingest_users_batch(self, user_ids: List[int]) -> None:
        """Ingest a batch of users"""
        try:
            with self.conn.cursor() as cursor:
                user_data = [(user_id,) for user_id in user_ids]

                execute_values(
                    cursor,
                    """
                    INSERT INTO users (user_id)
                    VALUES %s
                    ON CONFLICT (user_id) DO NOTHING
                    """,
                    user_data,
                    template=None,
                    page_size=len(user_data)
                )

                self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"❌ User batch ingestion failed: {e}")
            raise

    def ingest_ratings(self, ratings_csv: Path) -> None:
        """Ingest ratings with timestamp conversion"""
        self.logger.info(f"⭐ Starting rating ingestion from {ratings_csv}")

        # Count total rows for progress bar
        total_rows = sum(1 for _ in open(ratings_csv, 'r', encoding='utf-8')) - 1

        with tqdm(total=total_rows, desc="⭐ Ingesting ratings", unit="ratings") as pbar:
            for batch in self.batch_csv_reader(ratings_csv):
                self._ingest_ratings_batch(batch)
                pbar.update(len(batch))

    def _ingest_ratings_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Ingest a batch of ratings with timestamp conversion"""
        try:
            with self.conn.cursor() as cursor:
                rating_data = []
                for row in batch:
                    rating_timestamp = datetime.fromtimestamp(int(row['timestamp']))

                    rating_data.append((
                        int(row['userId']),
                        int(row['movieId']),
                        float(row['rating']),
                        rating_timestamp
                    ))

                execute_values(
                    cursor,
                    """
                    INSERT INTO ratings (user_id, movie_id, rating, rating_timestamp)
                    VALUES %s
                    ON CONFLICT (user_id, movie_id) DO UPDATE SET
                        rating = EXCLUDED.rating,
                        rating_timestamp = EXCLUDED.rating_timestamp,
                        created_at = ratings.created_at  -- Preserve original created_at
                    """,
                    rating_data,
                    template=None,
                    page_size=len(rating_data)
                )

                self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"❌ Rating batch ingestion failed: {e}")
            raise

    def ingest_tags(self, tags_csv: Path) -> None:
        """Ingest tags with timestamp conversion"""
        self.logger.info(f"🏷️  Starting tag ingestion from {tags_csv}")

        # Count total rows for progress bar
        total_rows = sum(1 for _ in open(tags_csv, 'r', encoding='utf-8')) - 1

        with tqdm(total=total_rows, desc="🏷️  Ingesting tags", unit="tags") as pbar:
            for batch in self.batch_csv_reader(tags_csv):
                self._ingest_tags_batch(batch)
                pbar.update(len(batch))

    def _ingest_tags_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Ingest a batch of tags with timestamp conversion"""
        try:
            with self.conn.cursor() as cursor:
                tag_data = []
                for row in batch:
                    tag_timestamp = datetime.fromtimestamp(int(row['timestamp']))

                    tag_data.append((
                        int(row['userId']),
                        int(row['movieId']),
                        row['tag'].strip(),
                        tag_timestamp
                    ))

                execute_values(
                    cursor,
                    """
                    INSERT INTO tags (user_id, movie_id, tag, tag_timestamp)
                    VALUES %s
                    ON CONFLICT (user_id, movie_id, tag) DO UPDATE SET
                        tag_timestamp = EXCLUDED.tag_timestamp,
                        created_at = tags.created_at  -- Preserve original created_at
                    """,
                    tag_data,
                    template=None,
                    page_size=len(tag_data)
                )

                self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"❌ Tag batch ingestion failed: {e}")
            raise

    def ingest_all(self, data_dir: Path) -> None:
        """Complete ingestion pipeline for all MovieLens data"""
        try:
            self.connect()

            # Define file paths
            movies_csv = data_dir / "movies.csv"
            links_csv = data_dir / "links.csv"
            ratings_csv = data_dir / "ratings.csv"
            tags_csv = data_dir / "tags.csv"

            # Validate required files exist
            required_files = [movies_csv, ratings_csv]
            for file_path in required_files:
                if not file_path.exists():
                    raise FileNotFoundError(f"Required file not found: {file_path}")

            # Ingestion order matters for foreign key constraints
            self.ingest_movies(movies_csv, links_csv)
            self.ingest_users(ratings_csv)
            self.ingest_ratings(ratings_csv)

            if tags_csv.exists():
                self.ingest_tags(tags_csv)
            else:
                self.logger.info("🏷️  No tags.csv file found, skipping tags ingestion")

            self.logger.info("🎉 All data ingestion completed successfully!")

        except Exception as e:
            self.logger.error(f"❌ Ingestion pipeline failed: {e}")
            raise
        finally:
            self.close()


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Ingest MovieLens data into PostgreSQL")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("sample_data"),
        help="Directory containing MovieLens CSV files"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Number of rows per batch (default: 10000)"
    )
    parser.add_argument(
        "--db-host",
        default="localhost",
        help="PostgreSQL host (default: localhost)"
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=5432,
        help="PostgreSQL port (default: 5432)"
    )
    parser.add_argument(
        "--db-name",
        default="postgres",
        help="PostgreSQL database name (default: postgres)"
    )
    parser.add_argument(
        "--db-user",
        default="postgres",
        help="PostgreSQL user (default: postgres)"
    )
    parser.add_argument(
        "--db-password",
        help="PostgreSQL password (can also set PGPASSWORD env var)"
    )

    args = parser.parse_args()

    # Database configuration
    db_config = {
        "host": args.db_host,
        "port": args.db_port,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password or os.getenv("PGPASSWORD")
    }

    if not db_config["password"]:
        print("❌ Database password required. Set PGPASSWORD environment variable or use --db-password")
        sys.exit(1)

    # Create ingester and run pipeline
    ingester = MovieLensIngester(db_config, args.batch_size)
    ingester.ingest_all(args.data_dir)


if __name__ == "__main__":
    main()