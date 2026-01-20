# Data Summary

## Overview
This dataset is a subset of the MovieLens dataset, augmented with embeddings and links to external databases (TMDB/IMDB). It serves as the foundation for the search and recommendation demos.

## File Statistics

| File Name | Row Count | Description |
| :--- | :--- | :--- |
| **movies.csv** | 9,743 | Core movie metadata (Title, Year, Genres). |
| **links.csv** | 9,743 | IDs mapping to external systems (IMDB, TMDB). Essential for data enrichment. |
| **embeddings.csv** | 9,743 | Vector embeddings (384-dim) for movie content. |
| **ratings.csv** | 100,837 | User ratings (0.5 - 5.0) from ~600 users. |
| **tags.csv** | 3,684 | Free-text tags applied by users to movies. |

## Schemas

### movies.csv
*   `movieId` (Int): Primary Key
*   `title` (String): Title including Release Year
*   `genres` (String): Pipe-separated list (e.g., "Action|Adventure")

### ratings.csv
*   `userId` (Int): User ID
*   `movieId` (Int): Foreign Key to movies
*   `rating` (Float): 0.5 to 5.0
*   `timestamp` (Int): Unix timestamp

### links.csv
*   `movieId` (Int): Primary Key
*   `imdbId` (String): IMDB Identifier
*   `tmdbId` (Int): The Movie Database Identifier (Key for API enrichment)
