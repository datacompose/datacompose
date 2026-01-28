"""Tests for fuzzy matching primitives."""

import pytest
from pyspark.sql import functions as F

from datacompose.transformers.analytics.fuzzy_matching.pyspark.pyspark_primitives import (
    fuzzy,
)


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for fuzzy matching tests."""
    data = [
        ("john", "jon"),
        ("smith", "smyth"),
        ("hello", "hello"),
        ("abc", "xyz"),
        ("", ""),
        (None, "test"),
    ]
    return spark.createDataFrame(data, ["col_a", "col_b"])


@pytest.fixture
def token_df(spark):
    """Create a sample DataFrame for token-based tests."""
    data = [
        ("john smith", "smith john"),
        ("acme corp inc", "acme inc"),
        ("hello world", "hello world"),
        ("a b c", "x y z"),
    ]
    return spark.createDataFrame(data, ["col_a", "col_b"])


@pytest.mark.unit
class TestLevenshtein:
    """Test Levenshtein distance functions."""

    def test_levenshtein_basic(self, sample_df):
        """Test basic Levenshtein distance calculation."""
        result = sample_df.select(
            fuzzy.levenshtein(F.col("col_a"), F.col("col_b")).alias("distance")
        ).collect()

        assert result[0]["distance"] == 1  # john -> jon
        assert result[1]["distance"] == 1  # smith -> smyth
        assert result[2]["distance"] == 0  # hello -> hello (identical)
        assert result[3]["distance"] == 3  # abc -> xyz

    def test_levenshtein_normalized(self, sample_df):
        """Test normalized Levenshtein similarity."""
        result = (
            sample_df.filter(F.col("col_a").isNotNull())
            .select(
                F.col("col_a"),
                F.col("col_b"),
                fuzzy.levenshtein_normalized(F.col("col_a"), F.col("col_b")).alias(
                    "similarity"
                ),
            )
            .collect()
        )

        # john (4) vs jon (3) -> distance 1, max_len 4, similarity = 1 - 1/4 = 0.75
        assert abs(result[0]["similarity"] - 0.75) < 0.01

        # hello vs hello -> identical = 1.0
        assert result[2]["similarity"] == 1.0

    def test_levenshtein_normalized_empty_strings(self, spark):
        """Test normalized Levenshtein with empty strings."""
        df = spark.createDataFrame([("", "")], ["a", "b"])
        result = df.select(
            fuzzy.levenshtein_normalized(F.col("a"), F.col("b")).alias("sim")
        ).collect()

        # Empty strings should return 1.0 (identical)
        assert result[0]["sim"] == 1.0

    def test_levenshtein_threshold(self, sample_df):
        """Test Levenshtein threshold matching."""
        result = (
            sample_df.filter(F.col("col_a").isNotNull())
            .select(
                F.col("col_a"),
                F.col("col_b"),
                fuzzy.levenshtein_threshold(
                    F.col("col_a"), F.col("col_b"), threshold=0.7
                ).alias("is_match"),
            )
            .collect()
        )

        # john/jon (0.75) >= 0.7 -> True
        assert result[0]["is_match"] is True

        # hello/hello (1.0) >= 0.7 -> True
        assert result[2]["is_match"] is True

        # abc/xyz (0.0) >= 0.7 -> False
        assert result[3]["is_match"] is False

    def test_levenshtein_threshold_configured(self, sample_df):
        """Test pre-configured Levenshtein threshold."""
        strict_match = fuzzy.levenshtein_threshold(threshold=0.9)

        result = (
            sample_df.filter(F.col("col_a").isNotNull())
            .select(
                fuzzy.levenshtein_threshold(
                    F.col("col_a"), F.col("col_b"), threshold=0.9
                ).alias("is_match")
            )
            .collect()
        )

        # john/jon (0.75) >= 0.9 -> False
        assert result[0]["is_match"] is False

        # hello/hello (1.0) >= 0.9 -> True
        assert result[2]["is_match"] is True


@pytest.mark.unit
class TestSoundex:
    """Test Soundex phonetic functions."""

    def test_soundex_basic(self, spark):
        """Test basic Soundex encoding."""
        df = spark.createDataFrame([("Robert",), ("Rupert",)], ["name"])
        result = df.select(
            F.col("name"), fuzzy.soundex(F.col("name")).alias("code")
        ).collect()

        # Robert and Rupert should have same Soundex code (R163)
        assert result[0]["code"] == result[1]["code"]

    def test_soundex_match(self, spark):
        """Test Soundex matching between two columns."""
        df = spark.createDataFrame(
            [
                ("Smith", "Smyth"),
                ("Robert", "Richard"),
                ("Robert", "Rupert"),
            ],
            ["name_a", "name_b"],
        )

        result = df.select(
            F.col("name_a"),
            F.col("name_b"),
            fuzzy.soundex_match(F.col("name_a"), F.col("name_b")).alias("sounds_alike"),
        ).collect()

        # Smith/Smyth sound alike (S530)
        assert result[0]["sounds_alike"] is True

        # Robert/Richard don't sound alike (R163 vs R263)
        assert result[1]["sounds_alike"] is False

        # Robert/Rupert sound alike (R163)
        assert result[2]["sounds_alike"] is True


@pytest.mark.unit
class TestTokenBased:
    """Test token-based similarity functions."""

    def test_jaccard_similarity(self, token_df):
        """Test Jaccard similarity calculation."""
        result = token_df.select(
            F.col("col_a"),
            F.col("col_b"),
            fuzzy.jaccard_similarity(F.col("col_a"), F.col("col_b")).alias("jaccard"),
        ).collect()

        # "john smith" vs "smith john" -> same tokens, order different = 1.0
        assert result[0]["jaccard"] == 1.0

        # "hello world" vs "hello world" -> identical = 1.0
        assert result[2]["jaccard"] == 1.0

        # "a b c" vs "x y z" -> no overlap = 0.0
        assert result[3]["jaccard"] == 0.0

    def test_jaccard_similarity_partial_overlap(self, spark):
        """Test Jaccard with partial token overlap."""
        df = spark.createDataFrame(
            [
                ("a b c", "a b d"),  # 2 shared out of 4 unique = 0.5
            ],
            ["col_a", "col_b"],
        )

        result = df.select(
            fuzzy.jaccard_similarity(F.col("col_a"), F.col("col_b")).alias("jaccard")
        ).collect()

        assert result[0]["jaccard"] == 0.5

    def test_token_overlap(self, token_df):
        """Test token overlap count."""
        result = token_df.select(
            F.col("col_a"),
            F.col("col_b"),
            fuzzy.token_overlap(F.col("col_a"), F.col("col_b")).alias("overlap"),
        ).collect()

        # "john smith" vs "smith john" -> 2 shared tokens
        assert result[0]["overlap"] == 2

        # "a b c" vs "x y z" -> 0 shared tokens
        assert result[3]["overlap"] == 0


@pytest.mark.unit
class TestUtilityFunctions:
    """Test utility matching functions."""

    def test_exact_match_case_insensitive(self, spark):
        """Test exact match with case insensitivity."""
        df = spark.createDataFrame(
            [
                ("Hello", "hello"),
                ("World", "WORLD"),
                ("Test", "Different"),
            ],
            ["a", "b"],
        )

        result = df.select(
            fuzzy.exact_match(F.col("a"), F.col("b")).alias("match")
        ).collect()

        assert result[0]["match"] is True
        assert result[1]["match"] is True
        assert result[2]["match"] is False

    def test_exact_match_case_sensitive(self, spark):
        """Test exact match with case sensitivity."""
        df = spark.createDataFrame(
            [
                ("Hello", "hello"),
                ("Hello", "Hello"),
            ],
            ["a", "b"],
        )

        result = df.select(
            fuzzy.exact_match(F.col("a"), F.col("b"), ignore_case=False).alias("match")
        ).collect()

        assert result[0]["match"] is False  # Different case
        assert result[1]["match"] is True  # Exact match

    def test_contains_match(self, spark):
        """Test contains match."""
        df = spark.createDataFrame(
            [
                ("hello world", "world"),
                ("test", "testing"),
                ("abc", "xyz"),
            ],
            ["a", "b"],
        )

        result = df.select(
            fuzzy.contains_match(F.col("a"), F.col("b")).alias("contains")
        ).collect()

        assert result[0]["contains"] is True  # "world" in "hello world"
        assert result[1]["contains"] is True  # "test" in "testing"
        assert result[2]["contains"] is False  # no containment

    def test_prefix_match(self, spark):
        """Test prefix matching."""
        df = spark.createDataFrame(
            [
                ("Johnson", "Johnston"),
                ("Smith", "Smyth"),
                ("Abc", "Xyz"),
            ],
            ["a", "b"],
        )

        result = df.select(
            fuzzy.prefix_match(F.col("a"), F.col("b"), length=3).alias("prefix_match")
        ).collect()

        # "joh" == "joh"
        assert result[0]["prefix_match"] is True

        # "smi" == "smy" -> False
        assert result[1]["prefix_match"] is False

        # "abc" != "xyz"
        assert result[2]["prefix_match"] is False


@pytest.mark.unit
class TestConfiguredPrimitives:
    """Test pre-configured primitive usage."""

    def test_configured_levenshtein_threshold(self, spark):
        """Test using pre-configured Levenshtein threshold."""
        df = spark.createDataFrame(
            [
                ("hello", "hallo"),
                ("abc", "xyz"),
            ],
            ["a", "b"],
        )

        # Create configured matcher
        strict = fuzzy.levenshtein_threshold(threshold=0.9)
        lenient = fuzzy.levenshtein_threshold(threshold=0.5)

        strict_result = df.select(
            strict(F.col("a"), F.col("b")).alias("match")
        ).collect()
        lenient_result = df.select(
            lenient(F.col("a"), F.col("b")).alias("match")
        ).collect()

        # hello/hallo similarity ~0.8, fails strict but passes lenient
        assert strict_result[0]["match"] is False
        assert lenient_result[0]["match"] is True

    def test_configured_jaccard_with_delimiter(self, spark):
        """Test Jaccard with custom delimiter."""
        df = spark.createDataFrame(
            [
                ("a,b,c", "a,b,d"),
            ],
            ["col_a", "col_b"],
        )

        comma_jaccard = fuzzy.jaccard_similarity(delimiter=",")
        result = df.select(
            comma_jaccard(F.col("col_a"), F.col("col_b")).alias("jaccard")
        ).collect()

        # 2 shared (a,b) out of 4 unique (a,b,c,d) = 0.5
        assert result[0]["jaccard"] == 0.5


@pytest.mark.unit
class TestNgramSimilarity:
    """Test N-gram similarity functions."""

    def test_ngram_similarity_identical(self, spark):
        """Test N-gram similarity with identical strings."""
        df = spark.createDataFrame([("hello", "hello")], ["a", "b"])
        result = df.select(
            fuzzy.ngram_similarity(F.col("a"), F.col("b")).alias("sim")
        ).collect()
        assert result[0]["sim"] == 1.0

    def test_ngram_similarity_different(self, spark):
        """Test N-gram similarity with different strings."""
        df = spark.createDataFrame([("abc", "xyz")], ["a", "b"])
        result = df.select(
            fuzzy.ngram_similarity(F.col("a"), F.col("b")).alias("sim")
        ).collect()
        assert result[0]["sim"] == 0.0

    def test_ngram_similarity_partial(self, spark):
        """Test N-gram similarity with partial overlap."""
        df = spark.createDataFrame([("hello", "hallo")], ["a", "b"])
        result = df.select(
            fuzzy.ngram_similarity(F.col("a"), F.col("b"), n=2).alias("sim")
        ).collect()
        # Should have some overlap but not perfect
        assert 0.0 < result[0]["sim"] < 1.0

    def test_ngram_distance(self, spark):
        """Test N-gram distance calculation."""
        df = spark.createDataFrame([("hello", "hello")], ["a", "b"])
        result = df.select(
            fuzzy.ngram_distance(F.col("a"), F.col("b")).alias("dist")
        ).collect()
        assert result[0]["dist"] == 0.0


@pytest.mark.unit
class TestCosineSimilarity:
    """Test Cosine similarity function."""

    def test_cosine_similarity_identical(self, spark):
        """Test Cosine similarity with identical strings."""
        df = spark.createDataFrame([("hello world", "hello world")], ["a", "b"])
        result = df.select(
            fuzzy.cosine_similarity(F.col("a"), F.col("b")).alias("sim")
        ).collect()
        # Use tolerance for floating point comparison
        assert abs(result[0]["sim"] - 1.0) < 0.0001

    def test_cosine_similarity_no_overlap(self, spark):
        """Test Cosine similarity with no common tokens."""
        df = spark.createDataFrame([("hello world", "foo bar")], ["a", "b"])
        result = df.select(
            fuzzy.cosine_similarity(F.col("a"), F.col("b")).alias("sim")
        ).collect()
        assert result[0]["sim"] == 0.0

    def test_cosine_similarity_partial(self, spark):
        """Test Cosine similarity with partial overlap."""
        df = spark.createDataFrame([("hello world", "hello there")], ["a", "b"])
        result = df.select(
            fuzzy.cosine_similarity(F.col("a"), F.col("b")).alias("sim")
        ).collect()
        # Should have some similarity due to "hello"
        assert 0.0 < result[0]["sim"] < 1.0


