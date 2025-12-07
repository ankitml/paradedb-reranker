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
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, List

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

    def generate_user_embedding_sql(self, user_id: int) -> str:
        """Generate SQL for a single user embedding update"""
        return f"""
            UPDATE users u
            SET embedding = (
                WITH preference_vectors AS (
                    SELECT
                        CASE
                            WHEN r.rating >= 4.0 THEN
                                -- Positive: Add weighted movie embedding
                                ARRAY(
                                    SELECT elem * (r.rating - 3.0)
                                    FROM unnest(m.content_embedding::float4[]) AS elem
                                )::vector(384)
                            ELSE
                                -- Negative: Subtract weighted movie embedding
                                ARRAY(
                                    SELECT elem * -(3.0 - r.rating)
                                    FROM unnest(m.content_embedding::float4[]) AS elem
                                )::vector(384)
                        END as weighted_vector
                    FROM ratings r
                    JOIN movies m ON r.movie_id = m.movie_id
                    WHERE r.user_id = {user_id}
                      AND m.content_embedding IS NOT NULL
                      AND (r.rating >= 4.0 OR r.rating < 3.0)
                ),
                combined_vector AS (
                    SELECT SUM(weighted_vector) as user_vector
                    FROM preference_vectors
                )
                SELECT user_vector FROM combined_vector
            ),
            updated_at = NOW()
            WHERE u.user_id = {user_id}
              AND EXISTS (
                SELECT 1 FROM ratings r
                JOIN movies m ON r.movie_id = m.movie_id
                WHERE r.user_id = {user_id}
                  AND m.content_embedding IS NOT NULL
                  AND (r.rating >= 4.0 OR r.rating < 3.0)
            )
        """

    def generate_embeddings_pure_sql(self, user_ids: Optional[List[int]] = None) -> None:
        """Generate user embeddings using directional vectors"""
        try:
            if user_ids:
                print_info(f"🚀 Generating FIXED embeddings for {len(user_ids)} users...")
                print_data(f"Target users: {user_ids}")
                print_data("✨ Using vector addition/subtraction")
            else:
                # Get all users that need embeddings
                all_users_result = self.db.execute_query("""
                    SELECT user_id FROM users
                    WHERE EXISTS (
                        SELECT 1 FROM ratings r
                        JOIN movies m ON r.movie_id = m.movie_id
                        WHERE r.user_id = users.user_id
                          AND m.content_embedding IS NOT NULL
                          AND (r.rating >= 4.0 OR r.rating < 3.0)
                    )
                """)
                user_ids = [row[0] for row in all_users_result]
                print_info(f"🚀 Generating FIXED embeddings for ALL {len(user_ids)} users...")
                print_data("✨ Using vector addition/subtraction")

            # Process each user with the same SQL
            total_affected = 0
            for user_id in user_ids:
                user_sql = self.generate_user_embedding_sql(user_id)
                affected_rows = self.db.execute_update(user_sql)
                total_affected += affected_rows
  
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

    def verify_test_users(self, user_ids: Optional[List[int]] = None) -> None:
        """Verify that specified users have embeddings"""
        if user_ids is None:
            user_ids = [10001, 10002, 20001, 20002]  # Default test users

        print()
        print_data(f"🔍 Verifying User Embeddings:")

        for user_id in user_ids:
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

    parser = argparse.ArgumentParser(
        description="Pure SQL User Embedding Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate embeddings for all users
    python generate_user_embeddings.py

    # Generate embeddings for specific users
    python generate_user_embeddings.py --user-ids 10001 10002 20001

    # Generate for a single user
    python generate_user_embeddings.py --user-ids 10001
        """
    )

    parser.add_argument(
        "--user-ids", "-u",
        type=int,
        nargs="*",
        help="Specific user IDs to generate embeddings for (default: all users)"
    )

    args = parser.parse_args()

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

        # Generate embeddings for specified users or all users
        generator.generate_embeddings_pure_sql(args.user_ids)

        # Verify results
        generator.verify_statistics()
        generator.verify_test_users(args.user_ids)

        if args.user_ids:
            print_success(f"🎉 Embedding generation completed for {len(args.user_ids)} users!")
        else:
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