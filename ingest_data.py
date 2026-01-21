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
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from tqdm import tqdm

from utils import (
    DatabaseConnection,
    ConfigManager,
    MovieDataUtils,
    FileUtils,
    LoggingUtils,
    print_success,
    print_error,
    print_info,
    print_warning,
    print_progress,
    print_data
)


class MovieLensIngester:
    """High-performance MovieLens data ingestion with PostgreSQL COPY and MERGE capabilities"""

    def __init__(self, db_config: Dict[str, str] = None, batch_size: int = 10000):
        self.db_config = db_config or ConfigManager.get_db_config()
        self.batch_size = batch_size
        self.logger = LoggingUtils.setup_logging()
        self.db = DatabaseConnection(self.db_config)

    def connect(self) -> None:
        """Establish database connection"""
        self.db.connect()

    def close(self) -> None:
        """Close database connection"""
        self.db.close()

    def ingest_movies(self, movies_csv: Path, links_csv: Path = None) -> None:
        """Ingest movies with optional external ID mapping"""
        self.logger.info(f"🎬 Starting movie ingestion from {movies_csv}")

        # Validate input file
        FileUtils.validate_file_exists(movies_csv, "Movies CSV file")

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
        total_rows = FileUtils.count_csv_rows(movies_csv)

        with tqdm(total=total_rows, desc="🎬 Ingesting movies", unit="movies") as pbar:
            for batch in FileUtils.batch_csv_reader(movies_csv, self.batch_size):
                self._ingest_movies_batch(batch, links_data)
                pbar.update(len(batch))

    def _ingest_movies_batch(self, batch: List[Dict[str, Any]], links_data: Dict[int, Dict]) -> None:
        """Ingest a batch of movies using COPY + ON CONFLICT for upserts"""
        try:
            # Prepare batch data
            movie_data = []
            for row in batch:
                movie_id = int(row['movieId'])
                title, year = MovieDataUtils.extract_year_from_title(row['title'])
                genres = MovieDataUtils.parse_genres(row['genres'])

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

            self.db.execute_no_response(create_temp_table)

            # Copy batch to temp table
            self.db.execute_batch(
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

            self.db.execute_update(merge_query, commit=False)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.error(f"❌ Movie batch ingestion failed: {e}")
            raise

    def ingest_users(self, ratings_csv: Path) -> None:
        """Extract and ingest unique users from ratings data"""
        self.logger.info(f"👥 Extracting users from {ratings_csv}")

        # Validate input file
        FileUtils.validate_file_exists(ratings_csv, "Ratings CSV file")

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
            user_data = [(user_id,) for user_id in user_ids]

            self.db.execute_batch(
                """
                INSERT INTO users (user_id)
                VALUES %s
                ON CONFLICT (user_id) DO NOTHING
                """,
                user_data,
                template=None,
                page_size=len(user_data)
            )

            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.error(f"❌ User batch ingestion failed: {e}")
            raise

    def ingest_ratings(self, ratings_csv: Path) -> None:
        """Ingest ratings with timestamp conversion"""
        self.logger.info(f"⭐ Starting rating ingestion from {ratings_csv}")

        # Count total rows for progress bar
        total_rows = FileUtils.count_csv_rows(ratings_csv)

        with tqdm(total=total_rows, desc="⭐ Ingesting ratings", unit="ratings") as pbar:
            for batch in FileUtils.batch_csv_reader(ratings_csv, self.batch_size):
                self._ingest_ratings_batch(batch)
                pbar.update(len(batch))

    def _ingest_ratings_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Ingest a batch of ratings with timestamp conversion"""
        try:
            rating_data = []
            for row in batch:
                rating_timestamp = datetime.fromtimestamp(int(row['timestamp']))

                rating_data.append((
                    int(row['userId']),
                    int(row['movieId']),
                    float(row['rating']),
                    rating_timestamp
                ))

            self.db.execute_batch(
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

            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.error(f"❌ Rating batch ingestion failed: {e}")
            raise

    def ingest_tags(self, tags_csv: Path) -> None:
        """Ingest tags with timestamp conversion"""
        self.logger.info(f"🏷️  Starting tag ingestion from {tags_csv}")

        # Count total rows for progress bar
        total_rows = FileUtils.count_csv_rows(tags_csv)

        with tqdm(total=total_rows, desc="🏷️  Ingesting tags", unit="tags") as pbar:
            for batch in FileUtils.batch_csv_reader(tags_csv, self.batch_size):
                self._ingest_tags_batch(batch)
                pbar.update(len(batch))

    def _ingest_tags_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Ingest a batch of tags with timestamp conversion"""
        try:
            tag_data = []
            for row in batch:
                tag_timestamp = datetime.fromtimestamp(int(row['timestamp']))

                tag_data.append((
                    int(row['userId']),
                    int(row['movieId']),
                    row['tag'].strip(),
                    tag_timestamp
                ))

            self.db.execute_batch(
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

            self.db.commit()

        except Exception as e:
            self.db.rollback()
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
                FileUtils.validate_file_exists(file_path, f"Required file")

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
        help="PostgreSQL host (default: from env or localhost)"
    )
    parser.add_argument(
        "--db-port",
        type=int,
        help="PostgreSQL port (default: from env or 5433)"
    )
    parser.add_argument(
        "--db-name",
        help="PostgreSQL database name (default: from env or postgres)"
    )
    parser.add_argument(
        "--db-user",
        help="PostgreSQL user (default: from env or postgres)"
    )
    parser.add_argument(
        "--db-password",
        help="PostgreSQL password (can also set PGPASSWORD env var)"
    )

    args = parser.parse_args()

    # Database configuration with overrides from command line
    db_config = ConfigManager.get_db_config()
    if args.db_host:
        db_config["host"] = args.db_host
    if args.db_port:
        db_config["port"] = args.db_port
    if args.db_name:
        db_config["database"] = args.db_name
    if args.db_user:
        db_config["user"] = args.db_user
    if args.db_password:
        db_config["password"] = args.db_password

    # Validate configuration
    if not ConfigManager.validate_config(db_config):
        sys.exit(1)

    # Create ingester and run pipeline
    ingester = MovieLensIngester(db_config, args.batch_size)
    ingester.ingest_all(args.data_dir)


if __name__ == "__main__":
    main()