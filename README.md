# 🎬 Movie Analytics Data Warehouse

A professional end-to-end Data Engineering project implementing a **Star Schema Data Warehouse** using IMDB datasets and Python ETL.

---

## 🚀 Project Overview

This project demonstrates how to design and build a scalable Movie Analytics Data Warehouse using:

- Python (Pandas, psycopg2)
- PostgreSQL
- SQL
- Star Schema Modeling
- Git

### Workflow

1. Extract cleaned IMDB CSV data  
2. Transform data using Pandas  
3. Load data into PostgreSQL using bulk insert  
4. Run analytical SQL queries  

---


## 📁 Project Structure
```
movie-analytics-data-warehouse/
│
├── data/
│ ├── raw/
│ └── processed/
│
├── etl/
│ ├── extract.py
│ ├── transform.py
│ └── load.py
│
├── sql/
│ ├── schema.sql
│ └── example_queries.sql
│
├── assets/
├── docs/
├── notebooks/
│
├── README.md
└── requirements.txt
```
---

## 🏗️ Data Model – Star Schema

The warehouse follows a **Star Schema** design:

- `fact_movie_performance` → central fact table  
- `dim_movie` → movie attributes  
- `dim_date` → time dimension  
- `dim_genre` → genre dimension  
- `bridge_movie_genre` → handles many-to-many relationships  

### Why a Bridge Table?

Movies can belong to multiple genres.  
To correctly model this many-to-many relationship, a bridge table was implemented.

---

## 🧠 Design Decisions

- Star Schema chosen for analytical performance
- Bridge table implemented for multi-genre movies
- Foreign key constraints enforced for data integrity
- Bulk inserts used for ETL performance optimization
- Incremental loading tested before full dataset scaling

---


## 🗂️ Schema Structure

### dim_movie
- movie_id (PK)
- imdb_id
- title
- release_year
- runtime

### dim_date
- date_id (PK)
- year
- month
- day
- decade

### dim_genre
- genre_id (PK)
- genre_name

### bridge_movie_genre
- movie_id (FK)
- genre_id (FK)

### fact_movie_performance
- movie_id (FK)
- date_id (FK)
- rating
- vote_count

---

## 📊 Sample Analytical Queries

### Average Rating by Year

```sql
SELECT d.year,
       ROUND(AVG(f.rating),2) AS avg_rating
FROM fact_movie_performance f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;
```

### Top Genres by Average Rating

```sql
SELECT g.genre_name,
       ROUND(AVG(f.rating),2) AS avg_rating
FROM fact_movie_performance f
JOIN bridge_movie_genre b ON f.movie_id = b.movie_id
JOIN dim_genre g ON b.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC;
```
---

## ⚡ Query Performance Testing

Example:

```sql
EXPLAIN ANALYZE
SELECT *
FROM fact_movie_performance
WHERE rating > 8;
```


---

## ⚙️ ETL Highlights

- Data cleaning with Pandas  
- Null handling & numeric conversion  
- Multi-genre support (split & mapping)  
- Bulk insert using `execute_values()`  
- Foreign key integrity validation  

---

## 🛠️ Installation

```bash
git clone https://github.com/saeidmoradi1998/movie-analytics-data-warehouse.git
cd movie-analytics-data-warehouse
pip install -r requirements.txt
```

---

## ▶️ Run ETL

```bash
python etl/load.py
```

---

## 🎯 Key Concepts Demonstrated

- Star Schema modeling  
- Fact & Dimension design  
- Many-to-many relationship handling  
- Bulk insert optimization  
- Analytical SQL queries  
- Data validation & integrity checks  

---

## 📌 Current Scope

The ETL process has been validated on a development subset of records.  
The schema design supports scalable full-dataset loading.

---


## 📈 Data Validation Checks

After loading the data into PostgreSQL, several validation checks were performed:

- Verified foreign key integrity between fact and dimension tables
- Checked for orphan records in bridge and fact tables
- Validated rating range (min/max)
- Validated vote_count range
- Compared dimension vs fact row counts
- Verified multi-genre relationships in bridge table

These checks ensure data integrity and correctness of the warehouse design.

---