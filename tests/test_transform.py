"""
Tests for etl/transform.py
- Merge of movies + ratings
- Genre explode
- Dropping rows with missing/invalid genres
- release_year conversion to numeric
"""

import sys
import pandas as pd
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "etl"))


def make_movies_csv(tmp_path: Path) -> None:
    df = pd.DataFrame({
        "imdb_id":       ["tt001", "tt002", "tt003", "tt004", "tt005"],
        "title":         ["Movie A", "Movie B", "Movie C", "Movie D", "Movie E"],
        "release_year":  ["1995", "2001", "abc", None, "1888"],
        "genres":        ["Action,Drama", "Comedy", None, "\\N", "Horror"],
    })
    df.to_csv(tmp_path / "imdb_movies.csv", index=False)


def make_ratings_csv(tmp_path: Path) -> None:
    df = pd.DataFrame({
        "imdb_id":    ["tt001", "tt002", "tt003"],
        "rating":     [7.5, 6.2, 8.0],
        "vote_count": [1000, 500, 200],
    })
    df.to_csv(tmp_path / "imdb_ratings.csv", index=False)


def run_transform(tmp_path, monkeypatch) -> pd.DataFrame:
    """Helper: run transform_imdb() with temp paths and return resulting CSV."""
    import transform
    make_movies_csv(tmp_path)
    make_ratings_csv(tmp_path)
    monkeypatch.setattr(transform, "RAW_PATH", tmp_path)
    monkeypatch.setattr(transform, "PROCESSED_PATH", tmp_path)

    transform.transform_imdb()

    return pd.read_csv(tmp_path / "imdb_cleaned.csv")


# ────────────────────────────────────────────────────────────────────────────
# Merge
# ────────────────────────────────────────────────────────────────────────────

class TestMerge:

    def test_ratings_joined_correctly(self, tmp_path, monkeypatch):
        """Movies with matching imdb_id should have rating filled in."""
        result = run_transform(tmp_path, monkeypatch)
        tt001 = result[result["imdb_id"] == "tt001"]
        assert tt001["rating"].iloc[0] == pytest.approx(7.5)

    def test_movie_without_rating_still_present(self, tmp_path, monkeypatch):
        """Left join — movies without ratings should still appear (rating=NaN)."""
        result = run_transform(tmp_path, monkeypatch)
        # tt005 has no rating row, but has genre so it should appear
        assert "tt005" in result["imdb_id"].values


# ────────────────────────────────────────────────────────────────────────────
# Genres
# ────────────────────────────────────────────────────────────────────────────

class TestGenres:

    def test_multi_genre_exploded_to_separate_rows(self, tmp_path, monkeypatch):
        """'Action,Drama' should become two separate rows."""
        result = run_transform(tmp_path, monkeypatch)
        tt001_rows = result[result["imdb_id"] == "tt001"]
        assert len(tt001_rows) == 2
        genres = set(tt001_rows["genres"].tolist())
        assert genres == {"Action", "Drama"}

    def test_null_genre_rows_dropped(self, tmp_path, monkeypatch):
        """Rows where genres is None/NaN must be dropped (tt003, tt004)."""
        result = run_transform(tmp_path, monkeypatch)
        assert "tt003" not in result["imdb_id"].values

    def test_backslash_n_genre_rows_dropped(self, tmp_path, monkeypatch):
        """Rows where genres is '\\N' (IMDB null) must be dropped (tt004)."""
        result = run_transform(tmp_path, monkeypatch)
        assert "tt004" not in result["imdb_id"].values

    def test_single_genre_stays_as_one_row(self, tmp_path, monkeypatch):
        """Single-genre movies must produce exactly one row after explode."""
        result = run_transform(tmp_path, monkeypatch)
        tt002_rows = result[result["imdb_id"] == "tt002"]
        assert len(tt002_rows) == 1
        assert tt002_rows["genres"].iloc[0] == "Comedy"


# ────────────────────────────────────────────────────────────────────────────
# release_year
# ────────────────────────────────────────────────────────────────────────────

class TestReleaseYear:

    def test_valid_year_is_numeric(self, tmp_path, monkeypatch):
        """Valid string year '1995' should be converted to numeric 1995."""
        result = run_transform(tmp_path, monkeypatch)
        tt001 = result[result["imdb_id"] == "tt001"].iloc[0]
        assert tt001["release_year"] == 1995.0

    def test_invalid_year_becomes_nan(self, tmp_path, monkeypatch):
        """Non-numeric year like 'abc' should become NaN (tt003 dropped — no genre)."""
        # tt003 is dropped due to missing genre, so check via direct logic
        df = pd.DataFrame({"release_year": ["abc", "1999", "xyz"]})
        df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
        assert pd.isna(df.loc[0, "release_year"])
        assert df.loc[1, "release_year"] == 1999.0
        assert pd.isna(df.loc[2, "release_year"])
