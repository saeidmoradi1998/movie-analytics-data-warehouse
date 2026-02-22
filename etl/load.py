import os
import psycopg2
import pandas as pd
from logger_config import setup_logger

logger = setup_logger()

from pathlib import Path
from dotenv import load_dotenv
from psycopg2.extras import execute_values



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
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established.")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


def load_dim_movie():
    
    logger.info("Starting load for dim_movie...")
    
    # Read cleaned IMDB data
    df = pd.read_csv(DATA_PATH)

    # Keep only columns needed for dim_movie
    df = df[["imdb_id", "title", "release_year"]]

    # Remove duplicate movies
    df = df.drop_duplicates(subset=["imdb_id"])
    logger.info(f"Unique movies to insert: {len(df)}")

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
    logger.info("dim_movie committed to database.")
    cur.close()
    conn.close()
    
    logger.info("dim_movie load completed successfully.")


def load_dim_genre():
    
    logger.info("Starting load for dim_genre...")
    
    df = pd.read_csv(DATA_PATH)


    df = df[["genres"]]

    df = df.dropna()
    

    df = df[df["genres"] != "\\N"]


    df = df.drop_duplicates()
    logger.info(f"Unique genres to insert: {len(df)}")

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
    logger.info("dim_genre committed to database.")
    cur.close()
    conn.close()

    logger.info("dim_genre load completed successfully.")

def load_dim_date():
    
    logger.info("Starting load for dim_date...")
    
    df = pd.read_csv(DATA_PATH)

    years = pd.to_numeric(df["release_year"], errors="coerce")
    years = years.dropna().astype(int).unique()
    logger.info(f"Unique years found for dim_date: {len(years)}")

    conn = get_connection()
    cur = conn.cursor()

    records = []

    for y in years:
        year = int(y)
        date_id = year          
        decade = (year // 10) * 10

        records.append((date_id, year, 1, 1, decade))

    insert_query = """
        INSERT INTO dim_date (date_id, year, month, day, decade)
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    execute_values(cur, insert_query, records)

    conn.commit()
    logger.info("dim_date committed to database.")
    cur.close()
    conn.close()

    logger.info("dim_date load completed successfully")



def load_bridge_movie_genre():
    
    logger.info("Starting load for bridge_movie_genre...")
    
    df = pd.read_csv(DATA_PATH)


    df = df.head(5000)

    df = df[["imdb_id", "genres"]]
    df = df.dropna()
    logger.info(f"Rows prepared for bridge table: {len(df)}")


    df = df[df["genres"] != "\\N"]

    conn = get_connection()
    cur = conn.cursor()

    # 🔥 Load movie_ids into dictionary
    cur.execute("SELECT movie_id, imdb_id FROM dim_movie")
    movie_map = {imdb_id: movie_id for movie_id, imdb_id in cur.fetchall()}

    # 🔥 Load genre_ids into dictionary
    cur.execute("SELECT genre_id, genre_name FROM dim_genre")
    genre_map = {genre_name: genre_id for genre_id, genre_name in cur.fetchall()}

    records = []

    for _, row in df.iterrows():
        movie_id = movie_map.get(row["imdb_id"])

        if not movie_id:
            continue

        genres = row["genres"].split(",")

        for g in genres:
            g = g.strip()   
            genre_id = genre_map.get(g)

            if genre_id:
                records.append((movie_id, genre_id))

    logger.info(f"Total bridge records prepared: {len(records)}")

    insert_query = """
        INSERT INTO bridge_movie_genre (movie_id, genre_id)
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    if records:   
        execute_values(cur, insert_query, records)

    conn.commit()
    logger.info("bridge_movie_genre committed to database.")
    cur.close()
    conn.close()

    logger.info("bridge_movie_genre load completed successfully")





def load_fact_movie_performance():
    
    logger.info("Starting load for fact_movie_performance...")
    
    df = pd.read_csv(DATA_PATH)

    df = df[["imdb_id", "release_year", "rating", "vote_count"]]

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce")
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")

    df = df.dropna(subset=["imdb_id", "rating", "vote_count", "release_year"])
    
    logger.info(f"Fact rows after cleaning: {len(df)}")


    df = df.head(3000)

    conn = get_connection()
    cur = conn.cursor()


    cur.execute("SELECT movie_id, imdb_id FROM dim_movie")
    movie_map = {imdb_id: movie_id for movie_id, imdb_id in cur.fetchall()}

    records = []

    for _, row in df.iterrows():
        movie_id = movie_map.get(row["imdb_id"])
        if not movie_id:
            continue

        year = int(row["release_year"])
        date_id = year

        records.append((
            movie_id,
            date_id,
            float(row["rating"]),
            int(row["vote_count"])
        ))
    
    logger.info(f"Total fact records prepared: {len(records)}")

    insert_query = """
        INSERT INTO fact_movie_performance
        (movie_id, date_id, rating, vote_count)
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    execute_values(cur, insert_query, records)

    conn.commit()
    logger.info("fact_movie_performance committed to database.")
    cur.close()
    conn.close()

    logger.info("fact_movie_performance load completed successfully")



if __name__ == "__main__":
    logger.info("Starting full ETL load process...")
    load_dim_movie()
    load_dim_genre()
    load_dim_date()
    load_bridge_movie_genre()
    load_fact_movie_performance()
    logger.info("Full ETL load process completed successfully.")