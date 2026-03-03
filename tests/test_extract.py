"""
Tests for etl/extract.py
- Column selection and renaming from TSV files
- Output CSV is saved with correct structure
"""

import sys
import pandas as pd
import pytest
from pathlib import Path

# Add etl/ to path so we can import the modules
sys.path.insert(0, str(Path(__file__).parent.parent / "etl"))


def make_basics_tsv(tmp_path: Path) -> Path:
    """Create a minimal title.basics.tsv for testing."""
    content = (
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n"
        "tt0000001\tshort\tCarmencita\tCarmencita\t0\t1894\t\\N\t1\tDocumentary,Short\n"
        "tt0000002\tshort\tLe clown\tLe clown\t0\t1892\t\\N\t5\tComedy\n"
        "tt0000003\tshort\tMissing Year\tMissing Year\t0\t\\N\t\\N\t3\tDrama\n"
    )
    f = tmp_path / "title.basics.tsv"
    f.write_text(content, encoding="utf-8")
    return tmp_path


def make_ratings_tsv(tmp_path: Path) -> Path:
    """Create a minimal title.ratings.tsv for testing."""
    content = (
        "tconst\taverageRating\tnumVotes\n"
        "tt0000001\t5.7\t1853\n"
        "tt0000002\t6.1\t240\n"
    )
    f = tmp_path / "title.ratings.tsv"
    f.write_text(content, encoding="utf-8")
    return tmp_path


# ────────────────────────────────────────────────────────────────────────────
# extract_imdb_basics
# ────────────────────────────────────────────────────────────────────────────

class TestExtractImdbBasics:

    def test_output_columns_correct(self, tmp_path, monkeypatch):
        """Output CSV must have exactly: imdb_id, title, release_year, genres."""
        import extract
        make_basics_tsv(tmp_path)
        monkeypatch.setattr(extract, "RAW_IMDB_PATH", tmp_path)

        extract.extract_imdb_basics()

        result = pd.read_csv(tmp_path / "imdb_movies.csv")
        assert list(result.columns) == ["imdb_id", "title", "release_year", "genres"]

    def test_row_count_matches_input(self, tmp_path, monkeypatch):
        """Row count in output should match input TSV (3 rows)."""
        import extract
        make_basics_tsv(tmp_path)
        monkeypatch.setattr(extract, "RAW_IMDB_PATH", tmp_path)

        extract.extract_imdb_basics()

        result = pd.read_csv(tmp_path / "imdb_movies.csv")
        assert len(result) == 3

    def test_imdb_id_values_correct(self, tmp_path, monkeypatch):
        """imdb_id column must contain original tconst values."""
        import extract
        make_basics_tsv(tmp_path)
        monkeypatch.setattr(extract, "RAW_IMDB_PATH", tmp_path)

        extract.extract_imdb_basics()

        result = pd.read_csv(tmp_path / "imdb_movies.csv")
        assert list(result["imdb_id"]) == ["tt0000001", "tt0000002", "tt0000003"]

    def test_no_extra_columns(self, tmp_path, monkeypatch):
        """Output must NOT contain columns like titleType, isAdult, etc."""
        import extract
        make_basics_tsv(tmp_path)
        monkeypatch.setattr(extract, "RAW_IMDB_PATH", tmp_path)

        extract.extract_imdb_basics()

        result = pd.read_csv(tmp_path / "imdb_movies.csv")
        assert "titleType" not in result.columns
        assert "isAdult" not in result.columns


# ────────────────────────────────────────────────────────────────────────────
# extract_imdb_ratings
# ────────────────────────────────────────────────────────────────────────────

class TestExtractImdbRatings:

    def test_output_columns_correct(self, tmp_path, monkeypatch):
        """Output CSV must have exactly: imdb_id, rating, vote_count."""
        import extract
        make_ratings_tsv(tmp_path)
        monkeypatch.setattr(extract, "RAW_IMDB_PATH", tmp_path)

        extract.extract_imdb_ratings()

        result = pd.read_csv(tmp_path / "imdb_ratings.csv")
        assert list(result.columns) == ["imdb_id", "rating", "vote_count"]

    def test_row_count_matches_input(self, tmp_path, monkeypatch):
        """Row count in output should match input TSV (2 rows)."""
        import extract
        make_ratings_tsv(tmp_path)
        monkeypatch.setattr(extract, "RAW_IMDB_PATH", tmp_path)

        extract.extract_imdb_ratings()

        result = pd.read_csv(tmp_path / "imdb_ratings.csv")
        assert len(result) == 2

    def test_rating_values_correct(self, tmp_path, monkeypatch):
        """Rating values must be preserved correctly."""
        import extract
        make_ratings_tsv(tmp_path)
        monkeypatch.setattr(extract, "RAW_IMDB_PATH", tmp_path)

        extract.extract_imdb_ratings()

        result = pd.read_csv(tmp_path / "imdb_ratings.csv")
        assert result.loc[0, "rating"] == pytest.approx(5.7)
        assert result.loc[1, "vote_count"] == 240
