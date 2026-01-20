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
import os
from typing import List, Dict, Any, Optional
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

        Executes complete search pipeline in one query:
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
        # Prefix query with title: for better BM25 search
        formatted_query = f"title:{query}"
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
                        CASE
                            WHEN MAX(bm25_score) OVER() = MIN(bm25_score) OVER() THEN 0.5
                            ELSE (bm25_score - MIN(bm25_score) OVER()) /
                                 (MAX(bm25_score) OVER() - MIN(bm25_score) OVER())
                        END as normalized_bm25
                    FROM first_pass_retrieval
                ),
                personalized_ranker AS (
                    SELECT
                        n.*,
                        (1 - (u.embedding <=> m.content_embedding)) as cosine_similarity
                    FROM normalization n
                    JOIN movies m ON n.movie_id = m.movie_id
                    CROSS JOIN users u WHERE u.user_id = %s
                ),
                joint_ranker AS (
                    SELECT
                        movie_id, title, year, genres,
                        normalized_bm25,
                        cosine_similarity,
                        (%s * normalized_bm25 + %s * cosine_similarity) as combined_score
                    FROM personalized_ranker
                )
                SELECT * FROM joint_ranker
                ORDER BY combined_score DESC
            """, (formatted_query, limit, user_id, bm25_weight, similarity_weight))

            # Convert to list of dictionaries
            return [
                {
                    'movie_id': row[0],
                    'title': row[1],
                    'year': row[2],
                    'genres': row[3],
                    'normalized_bm25_score': float(row[4]),
                    'cosine_similarity': float(row[5]),
                    'combined_score': float(row[6])
                }
                for row in results
            ]

        except Exception as e:
            print_error(f"Unified search failed: {e}")
            raise

    def search(self, query: str, user_id: int, show_scores: bool = False, partial_weight: float = 50.0) -> None:
        """Main search method using unified SQL approach with three weight combinations"""

        # Validate user exists and has embedding
        if not self.validate_user(user_id):
            return

        # Convert percentage to decimal
        partial_decimal = partial_weight / 100.0

        # Generate results for all three approaches using the unified query
        bm25_only = self.unified_search(query, user_id, bm25_weight=1.0, similarity_weight=0.0)
        partial = self.unified_search(query, user_id, bm25_weight=1.0-partial_decimal, similarity_weight=partial_decimal)
        rerank_only = self.unified_search(query, user_id, bm25_weight=0.0, similarity_weight=1.0)

        # Display results in three columns
        self.display_results(bm25_only, partial, rerank_only, show_scores, partial_weight)

    def display_results(self, bm25_results: List[Dict], hybrid_results: List[Dict],
                       rerank_results: List[Dict], show_scores: bool = False, partial_weight: float = 50.0) -> None:
        """Display results using full terminal width"""

        # Get terminal width
        try:
            terminal_width = os.get_terminal_size().columns
        except:
            terminal_width = 120  # fallback width

        # Calculate column widths
        separator_width = 3  # " | "
        total_content_width = terminal_width - (separator_width * 2)
        col_width = total_content_width // 3

        # Headers
        bm25_header = f"BM25 (0%)"
        partial_header = f"Partial ({partial_weight:.0f}%)"
        rerank_header = "Rerank (100%)"

        # Adjust title length based on available space
        max_title_length = col_width - 15  # Space for "10. " and scores

        # Print headers
        header_line = f"{bm25_header:<{col_width}} | {partial_header:<{col_width}} | {rerank_header:<{col_width}}"
        print(header_line)
        print("-" * terminal_width)

        # Print rows
        for i in range(10):
            # BM25 Only
            if i < len(bm25_results):
                movie = bm25_results[i]
                title = self._truncate_title(movie['title'], max_title_length)
                if show_scores:
                    bm25_col = f"{i+1:2d}. {title} ({movie['normalized_bm25_score']:.3f})"
                else:
                    bm25_col = f"{i+1:2d}. {title}"
            else:
                bm25_col = ""

            # Partial
            if i < len(hybrid_results):
                movie = hybrid_results[i]
                title = self._truncate_title(movie['title'], max_title_length)
                if show_scores:
                    partial_col = f"{i+1:2d}. {title} ({movie['combined_score']:.3f})"
                else:
                    partial_col = f"{i+1:2d}. {title}"
            else:
                partial_col = ""

            # Rerank Only
            if i < len(rerank_results):
                movie = rerank_results[i]
                title = self._truncate_title(movie['title'], max_title_length)
                if show_scores:
                    rerank_col = f"{i+1:2d}. {title} ({movie['cosine_similarity']:.3f})"
                else:
                    rerank_col = f"{i+1:2d}. {title}"
            else:
                rerank_col = ""

            # Print row with proper truncation
            bm25_col = bm25_col[:col_width]
            partial_col = partial_col[:col_width]
            rerank_col = rerank_col[:col_width]

            print(f"{bm25_col:<{col_width}} | {partial_col:<{col_width}} | {rerank_col:<{col_width}}")

        print("-" * terminal_width)

    def _truncate_title(self, title: str, max_length: int) -> str:
        """Truncate title to max_length with ellipsis if needed"""
        if len(title) <= max_length:
            return title
        return title[:max_length-3] + "..."

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
    python search_cli.py --query "ring" --user-id 20001 --show-scores --partial-weight 25
    python search_cli.py --query "dragon" --user-id 20002 --partial-weight 75

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

    parser.add_argument(
        "--partial-weight", "-p",
        type=float,
        default=50.0,
        help="Weight for partial personalization (0-100, default: 50)"
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
        search_engine.search(args.query, args.user_id, args.show_scores, args.partial_weight)

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