"""
Tests for data preparation logic in etl/load.py
- Year validation (unrealistic years → NaN)
- Duplicate removal for dim_movie
- Genre cleaning (NaN and \\N filtering)
- Fact table record preparation
- dim_date decade calculation
"""

import sys
import pandas as pd
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "etl"))


# ────────────────────────────────────────────────────────────────────────────
# Helpers — replicate the preparation logic from load.py
# (unit tests validate the logic independently from DB calls)
# ────────────────────────────────────────────────────────────────────────────

def prepare_dim_movie(df: pd.DataFrame) -> pd.DataFrame:
    """Replicates the data-prep part of load_dim_movie."""
    df = df[["imdb_id", "title", "release_year"]].copy()
    df = df.drop_duplicates(subset=["imdb_id"])
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
    df.loc[
        (df["release_year"] < 1888) | (df["release_year"] > 2100),
        "release_year"
    ] = pd.NA
    return df


def prepare_dim_genre(df: pd.DataFrame) -> pd.DataFrame:
    """Replicates the data-prep part of load_dim_genre."""
    df = df[["genres"]].copy()
    df = df.dropna()
    df = df[df["genres"] != "\\N"]
    df = df.drop_duplicates()
    return df


def prepare_dim_date(df: pd.DataFrame):
    """Replicates the dim_date record-building logic."""
    years = pd.to_numeric(df["release_year"], errors="coerce")
    years = years.dropna().astype(int).unique()
    records = []
    for y in years:
        year = int(y)
        date_id = year
        decade = (year // 10) * 10
        records.append((date_id, year, 1, 1, decade))
    return records


def prepare_fact(df: pd.DataFrame) -> pd.DataFrame:
    """Replicates the data-prep part of load_fact_movie_performance."""
    df = df[["imdb_id", "release_year", "rating", "vote_count"]].copy()
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce")
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
    df = df.dropna(subset=["imdb_id", "rating", "vote_count", "release_year"])
    df = df.drop_duplicates(subset=["imdb_id", "release_year"])
    return df


# ────────────────────────────────────────────────────────────────────────────
# dim_movie preparation
# ────────────────────────────────────────────────────────────────────────────

class TestDimMoviePrep:

    def test_duplicate_imdb_id_removed(self):
        """Rows with the same imdb_id must be deduplicated."""
        df = pd.DataFrame({
            "imdb_id":      ["tt001", "tt001", "tt002"],
            "title":        ["A", "A", "B"],
            "release_year": [2000, 2000, 2005],
        })
        result = prepare_dim_movie(df)
        assert len(result) == 2
        assert list(result["imdb_id"]) == ["tt001", "tt002"]

    def test_year_before_1888_becomes_nan(self):
        """Year before 1888 (before cinema existed) must become NaN."""
        df = pd.DataFrame({
            "imdb_id": ["tt001"], "title": ["Old"], "release_year": [1800]
        })
        result = prepare_dim_movie(df)
        assert pd.isna(result.iloc[0]["release_year"])

    def test_year_after_2100_becomes_nan(self):
        """Unrealistically far future year must become NaN."""
        df = pd.DataFrame({
            "imdb_id": ["tt001"], "title": ["Future"], "release_year": [2200]
        })
        result = prepare_dim_movie(df)
        assert pd.isna(result.iloc[0]["release_year"])

    def test_valid_year_preserved(self):
        """Normal year like 1994 must be preserved."""
        df = pd.DataFrame({
            "imdb_id": ["tt001"], "title": ["Pulp Fiction"], "release_year": [1994]
        })
        result = prepare_dim_movie(df)
        assert result.iloc[0]["release_year"] == 1994.0

    def test_boundary_year_1888_is_valid(self):
        """Year 1888 (oldest known film) must NOT be nullified."""
        df = pd.DataFrame({
            "imdb_id": ["tt001"], "title": ["First"], "release_year": [1888]
        })
        result = prepare_dim_movie(df)
        assert result.iloc[0]["release_year"] == 1888.0

    def test_string_year_converted(self):
        """String year '2001' must be converted to numeric 2001."""
        df = pd.DataFrame({
            "imdb_id": ["tt001"], "title": ["Space"], "release_year": ["2001"]
        })
        result = prepare_dim_movie(df)
        assert result.iloc[0]["release_year"] == 2001.0


# ────────────────────────────────────────────────────────────────────────────
# dim_genre preparation
# ────────────────────────────────────────────────────────────────────────────

class TestDimGenrePrep:

    def test_nan_genres_removed(self):
        """Rows with NaN genre must be dropped."""
        df = pd.DataFrame({"genres": ["Action", None, "Drama"]})
        result = prepare_dim_genre(df)
        assert len(result) == 2
        assert None not in result["genres"].tolist()

    def test_backslash_n_genres_removed(self):
        """Rows with '\\N' genre (IMDB null marker) must be dropped."""
        df = pd.DataFrame({"genres": ["Action", "\\N", "Drama"]})
        result = prepare_dim_genre(df)
        assert "\\N" not in result["genres"].tolist()
        assert len(result) == 2

    def test_duplicate_genres_removed(self):
        """Same genre appearing multiple times must be deduplicated."""
        df = pd.DataFrame({"genres": ["Action", "Action", "Drama"]})
        result = prepare_dim_genre(df)
        assert len(result) == 2
        assert list(result["genres"]) == ["Action", "Drama"]


# ────────────────────────────────────────────────────────────────────────────
# dim_date preparation
# ────────────────────────────────────────────────────────────────────────────

class TestDimDatePrep:

    def test_decade_calculated_correctly(self):
        """Year 1994 → decade 1990, year 2001 → decade 2000."""
        df = pd.DataFrame({"release_year": [1994, 2001, 1987]})
        records = prepare_dim_date(df)
        decade_map = {r[1]: r[4] for r in records}
        assert decade_map[1994] == 1990
        assert decade_map[2001] == 2000
        assert decade_map[1987] == 1980

    def test_date_id_equals_year(self):
        """date_id must equal year value."""
        df = pd.DataFrame({"release_year": [2005]})
        records = prepare_dim_date(df)
        assert records[0][0] == 2005  # date_id
        assert records[0][1] == 2005  # year

    def test_unique_years_only(self):
        """Duplicate years must produce only one record each."""
        df = pd.DataFrame({"release_year": [2000, 2000, 2001]})
        records = prepare_dim_date(df)
        years = [r[1] for r in records]
        assert len(years) == len(set(years))

    def test_nan_years_excluded(self):
        """NaN years must not produce any dim_date record."""
        df = pd.DataFrame({"release_year": [2000, None, float("nan")]})
        records = prepare_dim_date(df)
        years = [r[1] for r in records]
        assert 2000 in years
        assert len(years) == 1


# ────────────────────────────────────────────────────────────────────────────
# fact_movie_performance preparation
# ────────────────────────────────────────────────────────────────────────────

class TestFactPrep:

    def _base_df(self):
        return pd.DataFrame({
            "imdb_id":      ["tt001", "tt002", "tt003", "tt004"],
            "release_year": [1994,    2001,    None,    2005],
            "rating":       [9.2,     7.5,     8.0,     "bad"],
            "vote_count":   [10000,   500,     200,     300],
            "genres":       ["Drama", "Comedy","Action","Horror"],
        })

    def test_row_with_nan_year_dropped(self):
        """Rows with missing release_year must be dropped from fact."""
        result = prepare_fact(self._base_df())
        assert "tt003" not in result["imdb_id"].values

    def test_row_with_invalid_rating_dropped(self):
        """Rows with non-numeric rating ('bad') must be dropped."""
        result = prepare_fact(self._base_df())
        assert "tt004" not in result["imdb_id"].values

    def test_valid_rows_kept(self):
        """Rows with all valid fields must be preserved."""
        result = prepare_fact(self._base_df())
        assert "tt001" in result["imdb_id"].values
        assert "tt002" in result["imdb_id"].values

    def test_duplicate_imdb_year_deduplicated(self):
        """Same (imdb_id, release_year) must produce only one fact row."""
        df = pd.DataFrame({
            "imdb_id":      ["tt001", "tt001"],
            "release_year": [2000,    2000],
            "rating":       [7.0,     7.5],
            "vote_count":   [100,     150],
        })
        result = prepare_fact(df)
        assert len(result) == 1
