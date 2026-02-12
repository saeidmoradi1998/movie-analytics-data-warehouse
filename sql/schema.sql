-- 1. Dimension tables

CREATE TABLE dim_movie (
    movie_id SERIAL PRIMARY KEY,
    imdb_id VARCHAR(20),
    title TEXT NOT NULL,
    release_year INT
);

CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    decade INT
);

CREATE TABLE dim_genre (
    genre_id SERIAL PRIMARY KEY,
    genre_name TEXT UNIQUE
);

CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name TEXT UNIQUE
);

CREATE TABLE dim_language (
    language_id SERIAL PRIMARY KEY,
    language_name TEXT UNIQUE
);


-- 2. Fact table

CREATE TABLE fact_movie_performance (
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
    runtime INT
);
