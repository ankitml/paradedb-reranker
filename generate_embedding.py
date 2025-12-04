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
from pathlib import Path
from tqdm import tqdm
import time
from typing import List, Tuple, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class MovieEmbeddingGenerator:
    """Generate movie embeddings using OpenRouter API with batch processing"""

    def __init__(self, api_key: str, batch_size: int = 100):
        self.api_key = api_key
        self.batch_size = batch_size
        self.base_url = "https://openrouter.ai/api/v1"
        self.model = "sentence-transformers/all-minilm-l12-v2"

    def load_movies(self, movies_csv: Path) -> List[Dict[str, Any]]:
        """Load movies from CSV file"""
        movies = []

        with open(movies_csv, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Extract year from title if not present as separate field
                title = row['title']
                year = None
                genres = row['genres'].split('|') if row['genres'] != '(no genres listed)' else []

                # Try to extract year from title (e.g., "Toy Story (1995)")
                import re
                year_match = re.search(r'\((\d{4})\)$', title)
                if year_match:
                    year = int(year_match.group(1))
                    # Clean title by removing year
                    title = title[:year_match.start()].rstrip()

                movie_id = int(row['movieId'])

                movies.append({
                    'movie_id': movie_id,
                    'title': title,
                    'year': year,
                    'genres': genres
                })

        return movies

    def format_movie_text(self, movie: Dict[str, Any]) -> str:
        """Format movie data for embedding generation"""
        title = movie['title']
        year = movie['year'] if movie['year'] else ''
        genres = ' '.join(movie['genres']) if movie['genres'] else ''

        # Combine all fields: title + year + genres
        parts = [title]
        if year:
            parts.append(str(year))
        if genres:
            parts.append(genres)

        return ' '.join(parts)

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

    # File paths
    script_dir = Path(__file__).parent
    sample_data_dir = script_dir / "sample_data"
    movies_csv = sample_data_dir / "movies.csv"
    output_csv = sample_data_dir / "embeddings.csv"

    # Check if input file exists
    if not movies_csv.exists():
        print(f"❌ Input file not found: {movies_csv}")
        sys.exit(1)

    # Get API key from environment (loaded from .env)
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("❌ OPENROUTER_API_KEY environment variable not set!")
        print("💡 Create a .env file based on .env.example and add your API key")
        print("📝 Example: cp .env.example .env")
        print("🔑 Then edit .env and add: OPENROUTER_API_KEY=your-key-here")
        sys.exit(1)

    # Configuration from environment or default
    batch_size = int(os.getenv("EMBEDDING_BATCH_SIZE", "100"))

    print("🎬 Movie Embedding Generator")
    print("=" * 40)
    print(f"📁 Input: {movies_csv}")
    print(f"💾 Output: {output_csv}")
    print()

    # Create generator
    generator = MovieEmbeddingGenerator(api_key, batch_size)

    try:
        # Load movies
        print("📖 Loading movies from CSV...")
        movies = generator.load_movies(movies_csv)
        print(f"✅ Loaded {len(movies)} movies")
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
        print("\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
