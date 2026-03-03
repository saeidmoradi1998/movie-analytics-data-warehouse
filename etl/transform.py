import pandas as pd
from pathlib import Path
from logger_config import setup_logger

logger = setup_logger()

RAW_PATH = Path(__file__).parent.parent / "data" / "raw" / "imdb"
PROCESSED_PATH = Path(__file__).parent.parent / "data" / "processed"

PROCESSED_PATH.mkdir(exist_ok=True)


def transform_imdb():
    logger.info("Starting transformation process...")
    movies = pd.read_csv(RAW_PATH / "imdb_movies.csv")
    ratings = pd.read_csv(RAW_PATH / "imdb_ratings.csv")
    logger.info(f"Movies rows: {len(movies)}")
    logger.info(f"Ratings rows: {len(ratings)}")

    df = movies.merge(ratings, on="imdb_id", how="left")
    logger.info(f"Rows after merge: {len(df)}")

    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")

    initial_rows = len(df)
    df = df.dropna(subset=["genres"])
    df = df[df["genres"] != "\\N"]
    logger.info(f"Dropped {initial_rows - len(df)} rows due to missing genres")

    df["genres"] = df["genres"].str.split(",")
    df = df.explode("genres")
    logger.info(f"Rows after exploding genres: {len(df)}")

    output_file = PROCESSED_PATH / "imdb_cleaned.csv"
    df.to_csv(output_file, index=False)
    logger.info(f"Saved cleaned IMDB data to {output_file}")


def main():
    logger.info("Starting full transformation process...")

    transform_imdb()

    logger.info("Transformation process completed successfully.")


if __name__ == "__main__":
    main()
