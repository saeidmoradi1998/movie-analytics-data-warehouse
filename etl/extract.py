import pandas as pd
from pathlib import Path


RAW_IMDB_PATH = Path("data/raw/imdb")


def extract_imdb_basics():
    input_file = RAW_IMDB_PATH / "title.basics.tsv"
    output_file = RAW_IMDB_PATH / "imdb_movies.csv"

    df = pd.read_csv(input_file, sep="\t", low_memory=False)

    df = df[[
        "tconst",
        "primaryTitle",
        "startYear",
        "genres"
    ]]

    df.columns = [
        "imdb_id",
        "title",
        "release_year",
        "genres"
    ]

    df.to_csv(output_file, index=False)
    print(f"Saved IMDB basics to {output_file}")


def extract_imdb_ratings():
    input_file = RAW_IMDB_PATH / "title.ratings.tsv"
    output_file = RAW_IMDB_PATH / "imdb_ratings.csv"

    df = pd.read_csv(input_file, sep="\t", low_memory=False)

    df = df[[
        "tconst",
        "averageRating",
        "numVotes"
    ]]

    df.columns = [
        "imdb_id",
        "rating",
        "vote_count"
    ]

    df.to_csv(output_file, index=False)
    print(f"Saved IMDB ratings to {output_file}")


def extract_tmdb_data():
    # Will be implemented in the next step
    pass


def main():
    extract_imdb_basics()
    extract_imdb_ratings()


if __name__ == "__main__":
    main()
