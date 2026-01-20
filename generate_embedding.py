#!/usr/bin/env python3
"""
Movie Embeddings Generator
==========================

Generates embeddings for movies using OpenRouter's all-MiniLM-L12-v2 model.
Reads movies.csv and outputs embeddings.csv with movie_id and embedding vectors.

Requirements:
- pip install requests tqdm python-dotenv
- OpenRouter API key with available credits
"""

import os
import sys
import csv
import requests
import json
import argparse
from pathlib import Path
from tqdm import tqdm
import time
from typing import List, Dict, Any

from utils import (
    ConfigManager,
    MovieDataUtils,
    FileUtils,
    print_success,
    print_error,
    print_info,
    print_warning,
    print_progress,
    print_data
)


class MovieEmbeddingGenerator:
    """Generate movie embeddings using OpenRouter API with batch processing"""

    def __init__(self, api_key: str = None, batch_size: int = 100):
        config = ConfigManager.get_openrouter_config()
        self.api_key = api_key or config["api_key"]
        self.batch_size = batch_size
        self.base_url = config["base_url"]
        self.model = config["model"]

    def load_movies(self, movies_csv: Path) -> List[Dict[str, Any]]:
        """Load movies from CSV file"""
        FileUtils.validate_file_exists(movies_csv, "Movies CSV file")

        movies = []
        with open(movies_csv, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                movie_id = int(row['movieId'])
                title, year = MovieDataUtils.extract_year_from_title(row['title'])
                genres = MovieDataUtils.parse_genres(row['genres'])

                movies.append({
                    'movie_id': movie_id,
                    'title': title,
                    'year': year,
                    'genres': genres
                })

        return movies

    def format_movie_text(self, movie: Dict[str, Any]) -> str:
        """Format movie data for embedding generation"""
        return MovieDataUtils.format_movie_text(
            movie['title'],
            movie['year'],
            movie['genres']
        )

    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a batch of texts using OpenRouter API"""

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://paradedb.com",
            "X-Title": "Movie Recommendation System"
        }

        payload = {
            "model": self.model,
            "input": texts
        }

        try:
            response = requests.post(
                f"{self.base_url}/embeddings",
                headers=headers,
                json=payload,
                timeout=60
            )

            if response.status_code == 200:
                data = response.json()
                # Extract embeddings from response
                embeddings = [item['embedding'] for item in data['data']]
                return embeddings
            else:
                print(f"❌ API Error: {response.status_code}")
                print(f"Response: {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            return None

    def generate_all_embeddings(self, movies: List[Dict[str, Any]], output_path: Path) -> None:
        """Generate embeddings for all movies and save to CSV"""

        print(f"🎬 Generating embeddings for {len(movies)} movies...")
        print(f"📦 Batch size: {self.batch_size}")
        print(f"🤖 Model: {self.model}")
        print()

        # Prepare output data
        movie_embeddings = []

        # Process in batches
        for i in tqdm(range(0, len(movies), self.batch_size), desc="🚀 Generating embeddings"):
            batch = movies[i:i + self.batch_size]

            # Format texts for this batch
            texts = [self.format_movie_text(movie) for movie in batch]
            movie_ids = [movie['movie_id'] for movie in batch]

            # Generate embeddings
            embeddings = self.generate_embeddings_batch(texts)

            if embeddings:
                # Combine movie IDs with embeddings
                for movie_id, embedding in zip(movie_ids, embeddings):
                    # Convert embedding list to string for CSV
                    embedding_str = json.dumps(embedding)
                    movie_embeddings.append({
                        'movie_id': movie_id,
                        'movie_embedding': embedding_str
                    })

                # Rate limiting - wait a bit between batches
                time.sleep(0.5)
            else:
                print(f"⚠️  Failed to generate embeddings for batch {i//self.batch_size + 1}")

        # Save to CSV
        if movie_embeddings:
            self.save_embeddings_csv(movie_embeddings, output_path)
            print(f"✅ Successfully generated embeddings for {len(movie_embeddings)} movies!")
            print(f"📁 Saved to: {output_path}")
        else:
            print("❌ No embeddings were generated!")

    def save_embeddings_csv(self, embeddings: List[Dict], output_path: Path) -> None:
        """Save embeddings to CSV file"""

        with open(output_path, 'w', encoding='utf-8', newline='') as file:
            fieldnames = ['movie_id', 'movie_embedding']
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(embeddings)


def main():
    """Main execution function"""

    parser = argparse.ArgumentParser(description="Generate movie embeddings using OpenRouter")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory containing movies.csv (default: data)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output CSV path for embeddings (default: <data-dir>/embeddings.csv)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Batch size for API calls (default: from EMBEDDING_BATCH_SIZE or 100)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of movies to process (default: all)"
    )
    args = parser.parse_args()

    # File paths
    movies_csv = args.data_dir / "movies.csv"
    output_csv = args.output or (args.data_dir / "embeddings.csv")

    # Validate OpenRouter configuration
    if not ConfigManager.validate_openrouter_config():
        sys.exit(1)

    # Configuration from environment or command line
    batch_size = args.batch_size or ConfigManager.get_batch_size("EMBEDDING_BATCH_SIZE", 100)

    print("🎬 Movie Embedding Generator")
    print("=" * 40)
    print(f"📁 Input: {movies_csv}")
    print(f"💾 Output: {output_csv}")
    print(f"📦 Batch size: {batch_size}")
    if args.limit:
        print(f"🎯 Limit: {args.limit} movies")
    print()

    # Create generator
    generator = MovieEmbeddingGenerator(batch_size=batch_size)

    try:
        # Load movies
        print("📖 Loading movies from CSV...")
        movies = generator.load_movies(movies_csv)
        print(f"✅ Loaded {len(movies)} movies")

        # Apply limit if specified
        if args.limit:
            movies = movies[:args.limit]
            print(f"🎯 Limited to {len(movies)} movies")

        print()

        # Show sample of formatted texts
        print("📝 Sample formatted texts:")
        for i, movie in enumerate(movies[:3]):
            text = generator.format_movie_text(movie)
            print(f"  {movie['title']} -> \"{text}\"")
        print()

        # Generate embeddings
        generator.generate_all_embeddings(movies, output_csv)

    except KeyboardInterrupt:
        print_warning("\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
