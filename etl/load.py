import os
import psycopg2
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# Path to processed data
DATA_PATH = Path("data/processed/imdb_cleaned.csv")


def get_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(**DB_CONFIG)


def load_dim_movie():
    # Read cleaned IMDB data
    df = pd.read_csv(DATA_PATH)

    # Keep only columns needed for dim_movie
    df = df[["imdb_id", "title", "release_year"]]

    # Remove duplicate movies
    df = df.drop_duplicates(subset=["imdb_id"])

    # ⏩ LIMIT rows for faster loading (TEMPORARY)
    df = df.head(5000)

    # Convert release_year to numeric (invalid values -> NaN)
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")

    # Set unrealistic years to NaN
    df.loc[
        (df["release_year"] < 1888) | (df["release_year"] > 2100),
        "release_year"
    ] = pd.NA

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        year = row["release_year"]

        # Convert pandas / numpy values to Python-native types
        if pd.isna(year):
            year = None
        else:
            year = int(year)

        cur.execute(
            """
            INSERT INTO dim_movie (imdb_id, title, release_year)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (row["imdb_id"], row["title"], year)
        )

    conn.commit()
    cur.close()
    conn.close()

    print("✅ dim_movie loaded successfully (5000 rows)")


def load_dim_genre():
    df = pd.read_csv(DATA_PATH)

    # فقط ستون genre
    df = df[["genres"]]

    # حذف NULL
    df = df.dropna()

    # unique genres
    df = df.drop_duplicates()

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO dim_genre (genre_name)
            VALUES (%s)
            ON CONFLICT DO NOTHING
            """,
            (row["genres"],)
        )

    conn.commit()
    cur.close()
    conn.close()

    print("✅ dim_genre loaded successfully")

def load_dim_date():
    df = pd.read_csv(DATA_PATH)

    # فقط سال‌ها
    years = pd.to_numeric(df["release_year"], errors="coerce")
    years = years.dropna().astype(int)
    years = years.unique()

    conn = get_connection()
    cur = conn.cursor()

    for y in years:
        year = int(y)  # 🔑 numpy.int64 -> int
        date_id = int(f"{year}0101")
        decade = int((year // 10) * 10)

        cur.execute(
            """
            INSERT INTO dim_date (date_id, year, month, day, decade)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (date_id, year, 1, 1, decade)
        )

    conn.commit()
    cur.close()
    conn.close()

    print("✅ dim_date loaded successfully")

def load_fact_movie_performance():
    df = pd.read_csv(DATA_PATH)

    # ستون‌های لازم
    df = df[["imdb_id", "release_year", "rating", "vote_count"]]

    # تبدیل به numeric
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce")
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")

    # حذف ردیف‌های ناقص
    df = df.dropna(subset=["imdb_id", "rating", "vote_count", "release_year"])
    
    # ⏩ محدودسازی برای سرعت (مهم)
    df = df.head(3000)

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        year = int(row["release_year"])
        date_id = int(f"{year}0101")

        # گرفتن movie_id
        cur.execute(
            "SELECT movie_id FROM dim_movie WHERE imdb_id = %s",
            (row["imdb_id"],)
        )
        result = cur.fetchone()

        if not result:
            continue

        movie_id = result[0]

        cur.execute(
            """
            INSERT INTO fact_movie_performance
                (movie_id, date_id, rating, vote_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                movie_id,
                date_id,
                float(row["rating"]),
                int(row["vote_count"])
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    print("✅ fact_movie_performance loaded successfully")


if __name__ == "__main__":
    # load_dim_movie()
    # load_dim_genre()
    # load_dim_date()
    load_fact_movie_performance()
