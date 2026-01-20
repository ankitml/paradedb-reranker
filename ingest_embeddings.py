#!/usr/bin/env python3
"""
Movie Embeddings Ingestion Script
=================================

Uploads generated embeddings from CSV to PostgreSQL movies table.
Updates the content_embedding column with 384-dimensional vectors.

Requirements:
- pip install psycopg2-binary tqdm python-dotenv
- PostgreSQL database with pgvector extension installed
- Generated embeddings.csv from generate_embedding.py
"""

import os
import sys
import argparse
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict, Any

from utils import (
    DatabaseConnection,
    ConfigManager,
    FileUtils,
    print_success,
    print_error,
    print_info,
    print_warning,
    print_progress,
    print_data
)


class EmbeddingIngester:
    """Ingest movie embeddings into PostgreSQL database"""

    def __init__(self, db_config: Dict[str, str] = None, batch_size: int = 1000):
        self.db_config = db_config or ConfigManager.get_db_config()
        self.batch_size = batch_size
        self.db = DatabaseConnection(self.db_config)

    def connect(self) -> None:
        """Establish database connection"""
        self.db.connect()

    def close(self) -> None:
        """Close database connection"""
        self.db.close()

    def load_embeddings_csv(self, csv_path: Path) -> List[Dict[str, Any]]:
        """Load embeddings from CSV file"""
        return FileUtils.load_json_embeddings(csv_path, 'movie_id', 'movie_embedding')

    def ingest_embeddings(self, embeddings: List[Dict[str, Any]]) -> None:
        """Ingest embeddings into movies table using batch processing"""

        print_data(f"📦 Ingesting {len(embeddings)} embeddings...")
        print_data(f"🎯 Batch size: {self.batch_size}")
        print()

        total_processed = 0
        total_failed = 0

        with tqdm(total=len(embeddings), desc="🚀 Uploading embeddings", unit="embeddings") as pbar:
            for i in range(0, len(embeddings), self.batch_size):
                batch = embeddings[i:i + self.batch_size]

                try:
                    # Prepare batch data
                    update_data = [
                        (
                            item['movie_id'],
                            item['content_embedding']
                        )
                        for item in batch
                    ]

                    # Create temporary table for batch updates
                    self.db.execute_no_response("""
                        CREATE TEMP TABLE temp_embeddings (
                            movie_id INTEGER,
                            content_embedding vector(384)
                        ) ON COMMIT DROP;
                    """)

                    # Insert batch data into temp table
                    self.db.execute_batch(
                        """
                        INSERT INTO temp_embeddings (movie_id, content_embedding)
                        VALUES %s
                        """,
                        update_data,
                        template=None,
                        page_size=len(update_data)
                    )

                    # Update movies table from temp table
                    self.db.execute_update("""
                        UPDATE movies m
                        SET content_embedding = t.content_embedding
                        FROM temp_embeddings t
                        WHERE m.movie_id = t.movie_id
                    """, commit=False)

                    self.db.commit()

                    batch_processed = len(batch)
                    total_processed += batch_processed
                    pbar.update(batch_processed)

                except Exception as e:
                    self.db.rollback()
                    print_error(f"❌ Batch {i//self.batch_size + 1} failed: {e}")
                    total_failed += len(batch)
                    pbar.update(len(batch))

        print()
        print_data("📊 Ingestion Summary:")
        print_success(f"✅ Successfully processed: {total_processed}")
        if total_failed > 0:
            print_error(f"❌ Failed: {total_failed}")
        success_rate = (total_processed / len(embeddings)) * 100
        print_success(f"📈 Success rate: {success_rate:.1f}%")

    def verify_ingestion(self, expected_count: int) -> None:
        """Verify that embeddings were successfully ingested"""
        try:
            # Count movies with embeddings
            results = self.db.execute_query("""
                SELECT COUNT(*) FROM movies WHERE content_embedding IS NOT NULL
            """)
            embedded_count = results[0][0]

            # Check for any NULL embeddings
            results = self.db.execute_query("""
                SELECT COUNT(*) FROM movies WHERE content_embedding IS NULL
            """)
            null_count = results[0][0]

            print()
            print_data("🔍 Verification Results:")
            print_success(f"📊 Movies with embeddings: {embedded_count}")
            if null_count > 0:
                print_warning(f"⚠️  Movies without embeddings: {null_count}")
            print_data(f"🎯 Expected: {expected_count}")

            if embedded_count == expected_count:
                print_success("✅ All embeddings successfully ingested!")
            else:
                print_warning("⚠️  Some embeddings may be missing")

        except Exception as e:
            print_error(f"❌ Verification failed: {e}")

    def get_sample_embeddings(self, limit: int = 3) -> None:
        """Show sample of ingested embeddings for verification"""
        try:
            results = self.db.execute_query("""
                SELECT movie_id, title,
                       array_length(content_embedding::real[], 1) as embedding_dim
                FROM movies
                WHERE content_embedding IS NOT NULL
                ORDER BY movie_id
                LIMIT %s
            """, (limit,))

            print()
            print_data("📋 Sample Embeddings:")
            for movie_id, title, dim in results:
                print_success(f"  🎬 {title} (ID: {movie_id}) - {dim} dimensions")

        except Exception as e:
            print_error(f"❌ Sample query failed: {e}")


def main():
    """Main execution function"""

    parser = argparse.ArgumentParser(description="Ingest movie embeddings into PostgreSQL")
    parser.add_argument(
        "--csv-file",
        type=Path,
        default=Path("data/embeddings.csv"),
        help="Path to embeddings CSV file (default: data/embeddings.csv)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for ingestion (default: 1000)"
    )
    args = parser.parse_args()

    embeddings_csv = args.csv_file
    batch_size = args.batch_size

    # Validate files and configuration
    FileUtils.validate_file_exists(embeddings_csv, "Embeddings CSV file")

    db_config = ConfigManager.get_db_config()
    if not ConfigManager.validate_config(db_config):
        sys.exit(1)

    print_data("📦 Movie Embeddings Ingestion")
    print("=" * 40)
    print_data(f"📁 Input: {embeddings_csv}")
    print_data(f"🗄️  Database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print()

    # Create ingester
    ingester = EmbeddingIngester(batch_size=batch_size)

    try:
        # Connect to database
        ingester.connect()

        # Load embeddings from CSV
        print_progress("📖 Loading embeddings from CSV...")
        embeddings = ingester.load_embeddings_csv(embeddings_csv)
        print_success(f"✅ Loaded {len(embeddings)} embeddings")
        print()

        # Show sample
        print_data("🔍 Sample data:")
        for i, item in enumerate(embeddings[:3]):
            print_data(f"  🎬 Movie {item['movie_id']}: {len(item['content_embedding'])} dimensions")
        print()

        # Ingest embeddings
        ingester.ingest_embeddings(embeddings)

        # Verify ingestion
        ingester.verify_ingestion(len(embeddings))

        # Show sample results
        ingester.get_sample_embeddings()

    except KeyboardInterrupt:
        print_warning("\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        ingester.close()


if __name__ == "__main__":
    main()