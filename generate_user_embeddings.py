#!/usr/bin/env python3
"""
User Preference Embeddings Generator
===================================

Computes user preference embeddings using collaborative filtering approach with pure SQL:
- Positive signals: User ratings >= 4.0 (likes)
- Negative signals: User ratings < 3.0 (dislikes)
- Formula: user_embedding = weighted_avg(likes) - weighted_avg(dislikes)

Requirements:
- pip install psycopg2-binary tqdm
- PostgreSQL database with pgvector extension and movie embeddings
- MovieLens ratings data already loaded in ratings table
"""

import os
import sys
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict, Any, Optional

from utils import (
    DatabaseConnection,
    ConfigManager,
    print_success,
    print_error,
    print_info,
    print_warning,
    print_progress,
    print_data
)


class UserEmbeddingGenerator:
    """Generate user preference embeddings using collaborative filtering approach"""

    def __init__(self, db_config: Dict[str, str] = None):
        self.db_config = db_config or ConfigManager.get_db_config()
        self.db = None

    def setup_database(self) -> None:
        """Initialize database connection"""
        try:
            self.db = DatabaseConnection(self.db_config)
            self.db.connect()
        except Exception as e:
            print_error(f"Database setup failed: {e}")
            raise

    def generate_user_embedding(self, user_id: int) -> bool:
        """Generate embedding for a single user using pure SQL

        Args:
            user_id: User ID to generate embedding for

        Returns:
            bool: True if embedding was generated, False if no data
        """
        try:
            # First check if user has any ratings with movie embeddings
            check_result = self.db.execute_query("""
                SELECT COUNT(*) as rating_count
                FROM ratings r
                JOIN movies m ON r.movie_id = m.movie_id
                WHERE r.user_id = %s
                  AND m.content_embedding IS NOT NULL
                  AND (r.rating >= 4.0 OR r.rating < 3.0)
            """, (user_id,))

            rating_count = check_result[0][0]
            if rating_count == 0:
                print_warning(f"User {user_id} has no qualifying ratings - skipping")
                return False

            # Compute user embedding in SQL and update directly
            affected_rows = self.db.execute_update("""
                WITH user_weights AS (
                    SELECT
                        CASE
                            WHEN r.rating >= 4.0 THEN (r.rating - 3.5)
                            ELSE -(3.5 - r.rating)
                        END as weight,
                        m.content_embedding
                    FROM ratings r
                    JOIN movies m ON r.movie_id = m.movie_id
                    WHERE r.user_id = %s
                      AND m.content_embedding IS NOT NULL
                      AND (r.rating >= 4.0 OR r.rating < 3.0)
                ),
                weighted_sum AS (
                    SELECT SUM(
                        ARRAY(
                            SELECT elem * weight
                            FROM unnest(content_embedding::float4[]) AS elem
                        )::vector(384)
                    ) as sum_weighted_embeddings,
                    SUM(weight) as total_weight
                    FROM user_weights
                ),
                final_embedding AS (
                    SELECT ARRAY(
                        SELECT elem / total_weight
                        FROM unnest(sum_weighted_embeddings::float4[]) AS elem
                    )::vector(384) as embedding
                    FROM weighted_sum
                )
                UPDATE users
                SET embedding = (SELECT embedding FROM final_embedding),
                    updated_at = NOW()
                WHERE user_id = %s
            """, (user_id, user_id))

            # Verify that exactly 1 row was updated
            if affected_rows != 1:
                print_warning(f"Expected to update 1 row for user {user_id}, but updated {affected_rows} rows")

            self.db.commit()
            return True

        except Exception as e:
            print_error(f"Failed to generate embedding for user {user_id}: {e}")
            self.db.rollback()
            return False

    def generate_all_user_embeddings(self) -> None:
        """Generate embeddings for all users using simple loop approach"""
        try:
            # Get all users
            users = self.db.execute_query("SELECT user_id FROM users ORDER BY user_id")
            user_ids = [row[0] for row in users]

            print_progress(f"Generating embeddings for {len(user_ids)} users...")
            print_data("Using simple loop with pure SQL operations")

            total_processed = 0
            total_failed = 0

            with tqdm(total=len(user_ids), desc="🚀 Processing users", unit="users") as pbar:
                for user_id in user_ids:
                    if self.generate_user_embedding(user_id):
                        total_processed += 1
                    else:
                        total_failed += 1
                    pbar.update(1)

            # Final summary
            self._print_summary(total_processed, total_failed, len(user_ids))

        except Exception as e:
            print_error(f"User embedding generation failed: {e}")
            raise

    def _print_summary(self, processed: int, failed: int, total: int) -> None:
        """Print generation summary"""
        print()
        print_data("📊 User Embedding Generation Summary:")
        print_success(f"✅ Successfully processed: {processed}")
        if failed > 0:
            print_error(f"❌ Failed: {failed}")
        else:
            print_success(f"✅ All users processed successfully!")

        success_rate = (processed / total) * 100 if total > 0 else 0
        print_data(f"📈 Success rate: {success_rate:.1f}%")

    def verify_test_users(self) -> None:
        """Verify that our test users (10001, 10002, 20001, 20002) have embeddings"""
        test_user_ids = [10001, 10002, 20001, 20002]

        print()
        print_data("🔍 Verifying Test User Embeddings:")

        for user_id in test_user_ids:
            try:
                result = self.db.execute_query("""
                    SELECT embedding, updated_at
                    FROM users
                    WHERE user_id = %s AND embedding IS NOT NULL
                """, (user_id,))

                if result:
                    embedding, updated_at = result[0]
                    print_success(f"  👤 User {user_id}: ✅ Has embedding")
                else:
                    print_error(f"  👤 User {user_id}: ❌ No embedding found")

            except Exception as e:
                print_error(f"  👤 User {user_id}: ❌ Verification failed: {e}")

    def verify_statistics(self) -> None:
        """Show overall statistics about the embedding generation"""
        try:
            stats = self.db.execute_query("""
                SELECT
                    COUNT(*) as total_users,
                    COUNT(embedding) as users_with_embeddings,
                    COUNT(*) - COUNT(embedding) as users_without_embeddings
                FROM users
            """)[0]

            total_users, with_embeddings, without_embeddings = stats

            print()
            print_data("📈 Overall Statistics:")
            print_success(f"  Total users: {total_users}")
            print_success(f"  Users with embeddings: {with_embeddings}")
            if without_embeddings > 0:
                print_warning(f"  Users without embeddings: {without_embeddings}")

            coverage = (with_embeddings / total_users) * 100 if total_users > 0 else 0
            print_success(f"  Coverage: {coverage:.1f}%")

        except Exception as e:
            print_error(f"Statistics verification failed: {e}")

    def close(self) -> None:
        """Close database connection"""
        if self.db:
            self.db.close()


def main():
    """Main execution function"""

    # Validate database configuration
    db_config = ConfigManager.get_db_config()
    if not ConfigManager.validate_config(db_config):
        sys.exit(1)

    print_progress("🎯 User Preference Embeddings Generator (Simple Loop)")
    print("=" * 60)
    print_data(f"🗄️  Database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print_success("🚀 Using simple loop with pure SQL operations")
    print()

    # Create generator
    generator = UserEmbeddingGenerator()

    try:
        # Setup database connection and verify schema
        generator.setup_database()

        # Generate embeddings for all users
        generator.generate_all_user_embeddings()

        # Verify results
        generator.verify_statistics()
        generator.verify_test_users()

        print_success("🎉 User embedding generation completed successfully!")

    except KeyboardInterrupt:
        print_warning("\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        generator.close()


if __name__ == "__main__":
    main()
