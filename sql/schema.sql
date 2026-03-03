-- 1. Dimension tables

CREATE TABLE IF NOT EXISTS dim_movie (
    movie_id SERIAL PRIMARY KEY,
    imdb_id VARCHAR(20) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    release_year INT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    decade INT
);

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_id SERIAL PRIMARY KEY,
    genre_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_language (
    language_id SERIAL PRIMARY KEY,
    language_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS bridge_movie_genre (
    movie_id INT REFERENCES dim_movie(movie_id),
    genre_id INT REFERENCES dim_genre(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);


-- 2. Fact table

CREATE TABLE IF NOT EXISTS fact_movie_performance (
    fact_id SERIAL PRIMARY KEY,

    movie_id INT REFERENCES dim_movie(movie_id),
    date_id INT REFERENCES dim_date(date_id),
    genre_id INT REFERENCES dim_genre(genre_id),
    country_id INT REFERENCES dim_country(country_id),
    language_id INT REFERENCES dim_language(language_id),

    rating NUMERIC(3,1),
    vote_count INT,
    popularity NUMERIC,
    revenue BIGINT,
    budget BIGINT,
    runtime INT,

    UNIQUE (movie_id, date_id)
);
