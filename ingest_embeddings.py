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
import csv
import json
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables from .env file
load_dotenv()


class EmbeddingIngester:
    """Ingest movie embeddings into PostgreSQL database"""

    def __init__(self, db_config: Dict[str, str], batch_size: int = 1000):
        self.db_config = db_config
        self.batch_size = batch_size
        self.conn = None

    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            print("✅ Connected to PostgreSQL database")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise

    def close(self) -> None:
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("🔌 Database connection closed")

    def load_embeddings_csv(self, csv_path: Path) -> List[Dict[str, Any]]:
        """Load embeddings from CSV file"""
        embeddings = []

        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                movie_id = int(row['movie_id'])
                # Parse JSON string back to list of floats
                embedding_vector = json.loads(row['movie_embedding'])

                embeddings.append({
                    'movie_id': movie_id,
                    'content_embedding': embedding_vector
                })

        return embeddings

    def ingest_embeddings(self, embeddings: List[Dict[str, Any]]) -> None:
        """Ingest embeddings into movies table using batch processing"""

        print(f"📦 Ingesting {len(embeddings)} embeddings...")
        print(f"🎯 Batch size: {self.batch_size}")
        print()

        total_processed = 0
        total_failed = 0

        with tqdm(total=len(embeddings), desc="🚀 Uploading embeddings", unit="embeddings") as pbar:
            for i in range(0, len(embeddings), self.batch_size):
                batch = embeddings[i:i + self.batch_size]

                try:
                    with self.conn.cursor() as cursor:
                        # Prepare batch data
                        update_data = [
                            (
                                item['movie_id'],
                                item['content_embedding']
                            )
                            for item in batch
                        ]

                        # Create temporary table for batch updates
                        cursor.execute("""
                            CREATE TEMP TABLE temp_embeddings (
                                movie_id INTEGER,
                                content_embedding vector(384)
                            ) ON COMMIT DROP;
                        """)

                        # Insert batch data into temp table
                        execute_values(
                            cursor,
                            """
                            INSERT INTO temp_embeddings (movie_id, content_embedding)
                            VALUES %s
                            """,
                            update_data,
                            template=None,
                            page_size=len(update_data)
                        )

                        # Update movies table from temp table
                        cursor.execute("""
                            UPDATE movies m
                            SET content_embedding = t.content_embedding
                            FROM temp_embeddings t
                            WHERE m.movie_id = t.movie_id
                        """)

                        self.conn.commit()

                        batch_processed = len(batch)
                        total_processed += batch_processed
                        pbar.update(batch_processed)

                except Exception as e:
                    self.conn.rollback()
                    print(f"❌ Batch {i//self.batch_size + 1} failed: {e}")
                    total_failed += len(batch)
                    pbar.update(len(batch))

        print(f"\n📊 Ingestion Summary:")
        print(f"✅ Successfully processed: {total_processed}")
        print(f"❌ Failed: {total_failed}")
        print(f"📈 Success rate: {(total_processed / len(embeddings)) * 100:.1f}%")

    def verify_ingestion(self, expected_count: int) -> None:
        """Verify that embeddings were successfully ingested"""
        try:
            with self.conn.cursor() as cursor:
                # Count movies with embeddings
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM movies
                    WHERE content_embedding IS NOT NULL
                """)
                embedded_count = cursor.fetchone()[0]

                # Check for any NULL embeddings
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM movies
                    WHERE content_embedding IS NULL
                """)
                null_count = cursor.fetchone()[0]

                print(f"\n🔍 Verification Results:")
                print(f"📊 Movies with embeddings: {embedded_count}")
                print(f"⚠️  Movies without embeddings: {null_count}")
                print(f"🎯 Expected: {expected_count}")

                if embedded_count == expected_count:
                    print("✅ All embeddings successfully ingested!")
                else:
                    print("⚠️  Some embeddings may be missing")

        except Exception as e:
            print(f"❌ Verification failed: {e}")

    def get_sample_embeddings(self, limit: int = 3) -> None:
        """Show sample of ingested embeddings for verification"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT movie_id, title,
                           array_length(content_embedding::real[], 1) as embedding_dim
                    FROM movies
                    WHERE content_embedding IS NOT NULL
                    ORDER BY movie_id
                    LIMIT %s
                """, (limit,))

                results = cursor.fetchall()

                print(f"\n📋 Sample Embeddings:")
                for movie_id, title, dim in results:
                    print(f"  🎬 {title} (ID: {movie_id}) - {dim} dimensions")

        except Exception as e:
            print(f"❌ Sample query failed: {e}")


def main():
    """Main execution function"""

    # File paths
    script_dir = Path(__file__).parent
    sample_data_dir = script_dir / "sample_data"
    embeddings_csv = sample_data_dir / "embeddings.csv"

    # Check if embeddings file exists
    if not embeddings_csv.exists():
        print(f"❌ Embeddings file not found: {embeddings_csv}")
        print("💡 Run generate_embedding.py first to create embeddings.csv")
        sys.exit(1)

    # Database configuration (hardcoded except port and password)
    db_config = {
        "host": "localhost",
        "port": int(os.getenv("DB_PORT", "5433")),
        "database": "postgres",
        "user": "postgres",
        "password": os.getenv("DB_PASSWORD")
    }

    if not db_config["password"]:
        print("❌ Database password not configured!")
        print("💡 Set DB_PASSWORD or PGPASSWORD environment variable")
        print("📝 Example: export DB_PASSWORD='your-password-here'")
        sys.exit(1)

    # Configuration
    batch_size = int(os.getenv("EMBEDDING_BATCH_SIZE", "1000"))

    print("📦 Movie Embeddings Ingestion")
    print("=" * 40)
    print(f"📁 Input: {embeddings_csv}")
    print(f"🗄️  Database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print()

    # Create ingester
    ingester = EmbeddingIngester(db_config, batch_size)

    try:
        # Connect to database
        ingester.connect()

        # Load embeddings from CSV
        print("📖 Loading embeddings from CSV...")
        embeddings = ingester.load_embeddings_csv(embeddings_csv)
        print(f"✅ Loaded {len(embeddings)} embeddings")
        print()

        # Show sample
        print("🔍 Sample data:")
        for i, item in enumerate(embeddings[:3]):
            print(f"  🎬 Movie {item['movie_id']}: {len(item['content_embedding'])} dimensions")
        print()

        # Ingest embeddings
        ingester.ingest_embeddings(embeddings)

        # Verify ingestion
        ingester.verify_ingestion(len(embeddings))

        # Show sample results
        ingester.get_sample_embeddings()

    except KeyboardInterrupt:
        print("\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        ingester.close()


if __name__ == "__main__":
    main()