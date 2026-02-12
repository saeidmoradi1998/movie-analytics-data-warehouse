import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/raw/imdb")
PROCESSED_PATH = Path("data/processed")

PROCESSED_PATH.mkdir(exist_ok=True)


def transform_imdb():
    movies = pd.read_csv(RAW_PATH / "imdb_movies.csv")
    ratings = pd.read_csv(RAW_PATH / "imdb_ratings.csv")

    df = movies.merge(ratings, on="imdb_id", how="left")

    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")

    df = df.dropna(subset=["genres"])

    df["genres"] = df["genres"].str.split(",")
    df = df.explode("genres")

    output_file = PROCESSED_PATH / "imdb_cleaned.csv"
    df.to_csv(output_file, index=False)

    print(f"Saved cleaned IMDB data to {output_file}")


def main():
    transform_imdb()


if __name__ == "__main__":
    main()
