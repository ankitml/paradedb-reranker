#!/usr/bin/env python3
"""
Pure SQL User Embedding Generator
=================================

Generates all user embeddings in a single SQL operation without transferring
vectors between Python and PostgreSQL. This approach avoids psycopg2 vector
serialization issues entirely.

The regularization fixes edge cases like users with only negative ratings.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

from utils import (
    DatabaseConnection,
    ConfigManager,
    print_success,
    print_error,
    print_info,
    print_warning,
    print_data,
    print_progress
)


class PureSQLEmbeddingGenerator:
    """Generate user embeddings using pure SQL operations"""

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

    def generate_all_embeddings_pure_sql(self) -> None:
        """Generate all user embeddings using a single massive SQL operation"""
        try:
            print_info("🚀 Generating all user embeddings using pure SQL...")
            print_data("This will process all users in one database operation")

            # Single SQL operation that handles all users
            affected_rows = self.db.execute_update("""
                UPDATE users u
                SET embedding = (
                    WITH user_weights AS (
                        SELECT
                            CASE
                                WHEN r.rating >= 4.0 THEN (r.rating - 3.5)
                                ELSE -(3.5 - r.rating) * 0.8  -- Reduce negative weight by 20%
                            END + 0.1 as weight,  -- Add small positive bias to prevent pure negative
                            m.content_embedding
                        FROM ratings r
                        JOIN movies m ON r.movie_id = m.movie_id
                        WHERE r.user_id = u.user_id
                          AND m.content_embedding IS NOT NULL
                          AND (r.rating >= 4.0 OR r.rating < 3.0)
                    ),
                    weighted_sum AS (
                        SELECT
                            SUM(
                                ARRAY(
                                    SELECT elem * weight
                                    FROM unnest(content_embedding::float4[]) AS elem
                                )::vector(384)
                            ) as sum_weighted_embeddings,
                            SUM(weight) as total_weight
                        FROM user_weights
                        WHERE weight != 0  -- Avoid zero weights
                    ),
                    final_embedding AS (
                        SELECT CASE
                            WHEN total_weight = 0 OR total_weight IS NULL THEN NULL
                            ELSE ARRAY(
                                SELECT elem / total_weight
                                FROM unnest(sum_weighted_embeddings::float4[]) AS elem
                            )::vector(384)
                        END as embedding
                        FROM weighted_sum
                    )
                    SELECT embedding FROM final_embedding
                ),
                updated_at = NOW()
                WHERE EXISTS (
                    SELECT 1 FROM ratings r
                    JOIN movies m ON r.movie_id = m.movie_id
                    WHERE r.user_id = u.user_id
                      AND m.content_embedding IS NOT NULL
                      AND (r.rating >= 4.0 OR r.rating < 3.0)
                )
            """)

            print_success(f"✅ Updated embeddings for {affected_rows} users")

        except Exception as e:
            print_error(f"Pure SQL embedding generation failed: {e}")
            self.db.rollback()
            raise

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
            print_data("📈 Pure SQL Embedding Generation Results:")
            print_success(f"  Total users: {total_users}")
            print_success(f"  Users with embeddings: {with_embeddings}")
            if without_embeddings > 0:
                print_warning(f"  Users without embeddings: {without_embeddings}")

            coverage = (with_embeddings / total_users) * 100 if total_users > 0 else 0
            print_success(f"  Coverage: {coverage:.1f}%")

        except Exception as e:
            print_error(f"Statistics verification failed: {e}")

    def verify_test_users(self) -> None:
        """Verify that our test users (10001, 10002, 20001, 20002) have embeddings"""
        test_user_ids = [10001, 10002, 20001, 20002]

        print()
        print_data("🔍 Verifying Test User Embeddings:")

        for user_id in test_user_ids:
            try:
                result = self.db.execute_query("""
                    SELECT embedding IS NOT NULL as has_embedding, updated_at
                    FROM users
                    WHERE user_id = %s
                """, (user_id,))

                if result:
                    has_embedding, updated_at = result[0]
                    if has_embedding:
                        print_success(f"  👤 User {user_id}: ✅ Has embedding")
                    else:
                        print_error(f"  👤 User {user_id}: ❌ No embedding found")
                else:
                    print_error(f"  👤 User {user_id}: ❌ User not found")

            except Exception as e:
                print_error(f"  👤 User {user_id}: ❌ Verification failed: {e}")

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

    print_progress("🎯 Pure SQL User Embedding Generator")
    print("=" * 60)
    print_data(f"🗄️  Database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print_success("🚀 Single SQL operation - no Python-PostgreSQL data transfer")
    print()

    # Create generator
    generator = PureSQLEmbeddingGenerator()

    try:
        # Setup database connection
        generator.setup_database()

        # Generate all embeddings in one SQL operation
        generator.generate_all_embeddings_pure_sql()

        # Verify results
        generator.verify_statistics()
        generator.verify_test_users()

        print_success("🎉 Pure SQL user embedding generation completed successfully!")

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