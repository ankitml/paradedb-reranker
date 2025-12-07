#!/usr/bin/env python3
"""
Personalized Movie Search CLI
=============================

Command-line interface for testing personalized movie recommendations.
Compares three scoring approaches:
1. BM25 Only ( ParadeDB full-text search)
2. 100% Rerank (Pure vector similarity re-ranking)
3. 50/50 Hybrid (Linear combination of BM25 + similarity)

Usage:
    python search_cli.py --query "lord" --user-id 10001 [--show-scores]
"""

import argparse
import sys
from typing import List, Dict, Any, Optional
from prettytable import PrettyTable
from utils import (
    DatabaseConnection,
    ConfigManager,
    print_success,
    print_error,
    print_info,
    print_warning
)


class PersonalizedSearchEngine:
    """Engine for personalized movie search with multiple scoring approaches"""

    def __init__(self):
        self.db_config = ConfigManager.get_db_config()
        self.db = None

    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.db = DatabaseConnection(self.db_config)
            self.db.connect()
        except Exception as e:
            print_error(f"Database connection failed: {e}")
            raise

    def validate_user(self, user_id: int) -> bool:
        """Check if user exists and has embedding"""
        try:
            result = self.db.execute_query("""
                SELECT user_id, embedding IS NOT NULL as has_embedding
                FROM users
                WHERE user_id = %s
            """, (user_id,))

            if not result:
                print_error(f"User {user_id} not found in database")
                return False

            user_id_found, has_embedding = result[0]
            if not has_embedding:
                print_error(f"User {user_id} exists but has no embedding")
                print_info("Run generate_user_embeddings.py first to create user embeddings")
                return False

            return True

        except Exception as e:
            print_error(f"Error validating user {user_id}: {e}")
            return False

  
    
    def unified_search(self, query: str, user_id: int, bm25_weight: float,
                     similarity_weight: float, limit: int = 10) -> List[Dict[str, Any]]:
        """Unified search using single SQL query with parameterized weights

        Executes the complete search pipeline in one query:
        1. first_pass_retrieval - BM25 candidate generation
        2. normalization - BM25 score normalization
        3. personalized_ranker - Vector similarity calculation
        4. joint_ranker - Final weighted combination

        Args:
            query: Search query string
            user_id: User ID for personalization
            bm25_weight: Weight for BM25 scores (0.0 to 1.0)
            similarity_weight: Weight for similarity scores (0.0 to 1.0)
            limit: Number of results to return

        Returns:
            List of movies with all scores calculated
        """
        try:
            results = self.db.execute_query("""
                WITH first_pass_retrieval AS (
                    SELECT
                        movie_id, title, year, genres,
                        paradedb.score(movie_id) as bm25_score
                    FROM movies
                    WHERE movies @@@ %s
                    ORDER BY paradedb.score(movie_id) DESC, movie_id ASC
                    LIMIT %s
                ),
                normalization AS (
                    SELECT
                        *,
                        (bm25_score - MIN(bm25_score) OVER()) /
                            NULLIF(MAX(bm25_score) OVER() - MIN(bm25_score) OVER(), 0) as normalized_bm25
                    FROM first_pass_retrieval
                ),
                personalized_ranker AS (
                    SELECT
                        n.*,
                        (1 - (u.embedding <=> m.content_embedding)) as cosine_similarity,
                        (1 - (u.embedding <=> m.content_embedding) + 1) / 2 as normalized_similarity
                    FROM normalization n
                    JOIN movies m ON n.movie_id = m.movie_id
                    CROSS JOIN users u WHERE u.user_id = %s
                ),
                joint_ranker AS (
                    SELECT
                        movie_id, title, year, genres,
                        normalized_bm25,
                        normalized_similarity,
                        (%s * normalized_bm25 + %s * normalized_similarity) as combined_score
                    FROM personalized_ranker
                )
                SELECT * FROM joint_ranker
                ORDER BY combined_score DESC
            """, (query, limit, user_id, bm25_weight, similarity_weight))

            # Convert to list of dictionaries
            return [
                {
                    'movie_id': row[0],
                    'title': row[1],
                    'year': row[2],
                    'genres': row[3],
                    'normalized_bm25_score': float(row[4]),
                    'similarity_score': float(row[5]),
                    'combined_score': float(row[6])
                }
                for row in results
            ]

        except Exception as e:
            print_error(f"Unified search failed: {e}")
            raise

    def search(self, query: str, user_id: int, show_scores: bool = False) -> None:
        """Main search method using unified SQL approach with three weight combinations"""

        print(f"\n🔍 Personalized Movie Search")
        print(f"   Query: '{query}'")
        print(f"   User ID: {user_id}")
        print(f"   Show Scores: {show_scores}")
        print("=" * 80)

        # Validate user exists and has embedding
        if not self.validate_user(user_id):
            return

        print_info("🚀 Executing unified search with three weight combinations...")

        # Generate results for all three approaches using the unified query
        bm25_only = self.unified_search(query, user_id, bm25_weight=1.0, similarity_weight=0.0)
        rerank_only = self.unified_search(query, user_id, bm25_weight=0.0, similarity_weight=1.0)
        hybrid_50_50 = self.unified_search(query, user_id, bm25_weight=0.5, similarity_weight=0.5)

        print_info(f"   Found {len(bm25_only)} movies across all approaches")

        # Display results in three columns
        self.display_results(bm25_only, rerank_only, hybrid_50_50, show_scores)

    def display_results(self, bm25_results: List[Dict], rerank_results: List[Dict],
                       hybrid_results: List[Dict], show_scores: bool = False) -> None:
        """Display results in three column format"""

        print("\n🎬 Search Results Comparison")
        print("=" * 80)

        # Create three separate tables for better formatting
        table_bm25 = PrettyTable()
        table_rerank = PrettyTable()
        table_hybrid = PrettyTable()

        # Configure columns
        if show_scores:
            table_bm25.field_names = ["Rank", "Movie", "Year", "Score"]
            table_rerank.field_names = ["Rank", "Movie", "Year", "Score"]
            table_hybrid.field_names = ["Rank", "Movie", "Year", "Score"]
        else:
            table_bm25.field_names = ["Rank", "Movie", "Year"]
            table_rerank.field_names = ["Rank", "Movie", "Year"]
            table_hybrid.field_names = ["Rank", "Movie", "Year"]

        # Set alignment
        table_bm25.align["Movie"] = "l"
        table_rerank.align["Movie"] = "l"
        table_hybrid.align["Movie"] = "l"

        # Add data to tables
        for i in range(10):
            # BM25 Only
            if i < len(bm25_results):
                movie = bm25_results[i]
                title = f"{movie['title'][:40]}{'...' if len(movie['title']) > 40 else ''}"
                if show_scores:
                    table_bm25.add_row([
                        i+1, title, movie['year'],
                        f"{movie['normalized_bm25_score']:.3f}"
                    ])
                else:
                    table_bm25.add_row([i+1, title, movie['year']])
            else:
                if show_scores:
                    table_bm25.add_row([i+1, "", "", ""])
                else:
                    table_bm25.add_row([i+1, "", ""])

            # Rerank Only
            if i < len(rerank_results):
                movie = rerank_results[i]
                title = f"{movie['title'][:40]}{'...' if len(movie['title']) > 40 else ''}"
                if show_scores:
                    table_rerank.add_row([
                        i+1, title, movie['year'],
                        f"{movie['similarity_score']:.3f}"
                    ])
                else:
                    table_rerank.add_row([i+1, title, movie['year']])
            else:
                if show_scores:
                    table_rerank.add_row([i+1, "", "", ""])
                else:
                    table_rerank.add_row([i+1, "", ""])

            # Hybrid 50/50
            if i < len(hybrid_results):
                movie = hybrid_results[i]
                title = f"{movie['title'][:40]}{'...' if len(movie['title']) > 40 else ''}"
                if show_scores:
                    table_hybrid.add_row([
                        i+1, title, movie['year'],
                        f"{movie['combined_score']:.3f}"
                    ])
                else:
                    table_hybrid.add_row([i+1, title, movie['year']])
            else:
                if show_scores:
                    table_hybrid.add_row([i+1, "", "", ""])
                else:
                    table_hybrid.add_row([i+1, "", ""])

        # Display tables side by side
        print("📊 BM25 Only (0%)".center(25) + "   " +
              "🎯 100% Rerank".center(25) + "   " +
              "⚖️  50/50 Hybrid".center(25))
        print("-" * 80)

        bm25_lines = str(table_bm25).split('\n')
        rerank_lines = str(table_rerank).split('\n')
        hybrid_lines = str(table_hybrid).split('\n')

        for i in range(max(len(bm25_lines), len(rerank_lines), len(hybrid_lines))):
            bm25_line = bm25_lines[i] if i < len(bm25_lines) else " " * 25
            rerank_line = rerank_lines[i] if i < len(rerank_lines) else " " * 25
            hybrid_line = hybrid_lines[i] if i < len(hybrid_lines) else " " * 25

            # Pad lines to consistent width
            bm25_line = bm25_line.ljust(25)
            rerank_line = rerank_line.ljust(25)
            hybrid_line = hybrid_line.ljust(25)

            print(f"{bm25_line}   {rerank_line}   {hybrid_line}")

    def close(self) -> None:
        """Close database connection"""
        if self.db:
            self.db.close()


def main():
    """Main CLI entry point"""

    parser = argparse.ArgumentParser(
        description="Personalized Movie Search CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python search_cli.py --query "lord" --user-id 10001
    python search_cli.py --query "king" --user-id 10002 --show-scores
    python search_cli.py --query "ring" --user-id 20001 --show-scores

Test Users:
  - 10001: Fantasy Lover (likes fantasy movies)
  - 10002: Fantasy Hater (dislikes fantasy movies)
  - 20001: Extreme Fantasy Lover (very strong preference)
  - 20002: Extreme Fantasy Hater (very strong dislike)
        """
    )

    parser.add_argument(
        "--query", "-q",
        required=True,
        help="Search query for movies (e.g., 'lord', 'king', 'ring')"
    )

    parser.add_argument(
        "--user-id", "-u",
        type=int,
        required=True,
        help="User ID for personalized recommendations"
    )

    parser.add_argument(
        "--show-scores", "-s",
        action="store_true",
        help="Show similarity and BM25 scores in results"
    )

    args = parser.parse_args()

    # Validate database configuration
    db_config = ConfigManager.get_db_config()
    if not ConfigManager.validate_config(db_config):
        sys.exit(1)

    # Create and run search engine
    search_engine = PersonalizedSearchEngine()

    try:
        search_engine.connect()
        search_engine.search(args.query, args.user_id, args.show_scores)

    except KeyboardInterrupt:
        print_warning("\n⚠️  Search interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"❌ Search failed: {e}")
        sys.exit(1)
    finally:
        search_engine.close()


if __name__ == "__main__":
    main()