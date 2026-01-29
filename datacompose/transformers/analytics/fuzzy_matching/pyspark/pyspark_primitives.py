"""
Fuzzy matching primitives for PySpark.

Provides string similarity and comparison functions for row-wise operations
that compare two or more columns.

Preview Output:
+----------+----------+------------+----------------+---------------+
|name_a    |name_b    |levenshtein |levenshtein_norm|soundex_match  |
+----------+----------+------------+----------------+---------------+
|john      |jon       |1           |0.75            |true           |
|smith     |smyth     |1           |0.80            |true           |
|acme corp |acme inc  |4           |0.56            |false          |
|robert    |bob       |5           |0.17            |false          |
+----------+----------+------------+----------------+---------------+

Usage Example:
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transformers.pyspark.fuzzy_matching import fuzzy

# Initialize Spark
spark = SparkSession.builder.appName("FuzzyMatching").getOrCreate()

# Create sample data
data = [
    ("john", "jon"),
    ("smith", "smyth"),
    ("acme corp", "acme inc"),
]
df = spark.createDataFrame(data, ["name_a", "name_b"])

# Compare strings
result_df = df.select(
    F.col("name_a"),
    F.col("name_b"),
    fuzzy.levenshtein(F.col("name_a"), F.col("name_b")).alias("distance"),
    fuzzy.levenshtein_normalized(F.col("name_a"), F.col("name_b")).alias("similarity"),
    fuzzy.soundex_match(F.col("name_a"), F.col("name_b")).alias("soundex_match")
)

# Filter to similar matches
similar = result_df.filter(F.col("similarity") >= 0.8)

Installation:
datacompose add fuzzy_matching
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column
    from pyspark.sql import functions as F
else:
    try:
        from pyspark.sql import Column
        from pyspark.sql import functions as F
    except ImportError:
        pass

try:
    from utils.primitives import PrimitiveRegistry  # type: ignore
except ImportError:
    from datacompose.operators.primitives import PrimitiveRegistry

fuzzy = PrimitiveRegistry("fuzzy")


# =============================================================================
# Distance Functions
# =============================================================================


@fuzzy.register()
def levenshtein(col1: "Column", col2: "Column") -> "Column":
    """Calculate Levenshtein edit distance between two strings.

    The Levenshtein distance is the minimum number of single-character edits
    (insertions, deletions, substitutions) required to transform one string
    into another.

    Args:
        col1: First string column
        col2: Second string column

    Returns:
        Column with integer edit distance (0 = identical)

    Example:
        >>> df.withColumn("dist", fuzzy.levenshtein(F.col("a"), F.col("b")))
    """
    return F.levenshtein(col1, col2)


@fuzzy.register()
def levenshtein_normalized(col1: "Column", col2: "Column") -> "Column":
    """Calculate normalized Levenshtein similarity (0.0 to 1.0).

    Returns a similarity score where 1.0 means identical strings and
    0.0 means completely different. Calculated as:
    1 - (levenshtein_distance / max(len(str1), len(str2)))

    Args:
        col1: First string column
        col2: Second string column

    Returns:
        Column with float similarity score between 0.0 and 1.0

    Example:
        >>> df.withColumn("sim", fuzzy.levenshtein_normalized(F.col("a"), F.col("b")))
    """
    distance = F.levenshtein(col1, col2)
    max_len = F.greatest(F.length(col1), F.length(col2))
    return F.when(max_len == 0, F.lit(1.0)).otherwise(F.lit(1.0) - (distance / max_len))


@fuzzy.register()
def levenshtein_threshold(
    col1: "Column", col2: "Column", threshold: float = 0.8
) -> "Column":
    """Check if normalized Levenshtein similarity meets threshold.

    Args:
        col1: First string column
        col2: Second string column
        threshold: Minimum similarity score (default 0.8)

    Returns:
        Column with boolean indicating if similarity >= threshold

    Example:
        >>> df.withColumn("is_match", fuzzy.levenshtein_threshold(F.col("a"), F.col("b"), threshold=0.9))
    """
    distance = F.levenshtein(col1, col2)
    max_len = F.greatest(F.length(col1), F.length(col2))
    similarity = F.when(max_len == 0, F.lit(1.0)).otherwise(
        F.lit(1.0) - (distance / max_len)
    )
    return similarity >= F.lit(threshold)


# =============================================================================
# Phonetic Functions
# =============================================================================


@fuzzy.register()
def soundex(col: "Column") -> "Column":
    """Calculate Soundex phonetic encoding of a string.

    Soundex encodes a string into a letter followed by three digits,
    representing how the word sounds in English.

    Args:
        col: String column to encode

    Returns:
        Column with Soundex code (e.g., "Robert" -> "R163")

    Example:
        >>> df.withColumn("code", fuzzy.soundex(F.col("name")))
    """
    return F.soundex(col)


@fuzzy.register()
def soundex_match(col1: "Column", col2: "Column") -> "Column":
    """Check if two strings have the same Soundex encoding.

    Useful for matching names that sound alike but are spelled differently
    (e.g., "Smith" and "Smyth").

    Args:
        col1: First string column
        col2: Second string column

    Returns:
        Column with boolean indicating if Soundex codes match

    Example:
        >>> df.withColumn("sounds_alike", fuzzy.soundex_match(F.col("a"), F.col("b")))
    """
    return F.soundex(col1) == F.soundex(col2)


# =============================================================================
# Token-based Functions
# =============================================================================


@fuzzy.register()
def jaccard_similarity(
    col1: "Column", col2: "Column", delimiter: str = " "
) -> "Column":
    """Calculate Jaccard similarity between tokenized strings.

    Splits both strings into tokens and calculates:
    |intersection| / |union|

    Useful for comparing multi-word strings where word order doesn't matter.

    Args:
        col1: First string column
        col2: Second string column
        delimiter: Token delimiter (default: space)

    Returns:
        Column with float similarity score between 0.0 and 1.0

    Example:
        >>> df.withColumn("sim", fuzzy.jaccard_similarity(F.col("a"), F.col("b")))
    """
    tokens1 = F.split(F.lower(col1), delimiter)
    tokens2 = F.split(F.lower(col2), delimiter)
    intersection = F.size(F.array_intersect(tokens1, tokens2))
    union = F.size(F.array_union(tokens1, tokens2))
    return F.when(union == 0, F.lit(1.0)).otherwise(intersection / union)


@fuzzy.register()
def token_overlap(col1: "Column", col2: "Column", delimiter: str = " ") -> "Column":
    """Count number of overlapping tokens between two strings.

    Args:
        col1: First string column
        col2: Second string column
        delimiter: Token delimiter (default: space)

    Returns:
        Column with integer count of shared tokens

    Example:
        >>> df.withColumn("overlap", fuzzy.token_overlap(F.col("a"), F.col("b")))
    """
    tokens1 = F.split(F.lower(col1), delimiter)
    tokens2 = F.split(F.lower(col2), delimiter)
    return F.size(F.array_intersect(tokens1, tokens2))


# =============================================================================
# Utility Functions
# =============================================================================


@fuzzy.register()
def exact_match(col1: "Column", col2: "Column", ignore_case: bool = True) -> "Column":
    """Check if two strings match exactly.

    Args:
        col1: First string column
        col2: Second string column
        ignore_case: If True, comparison is case-insensitive (default: True)

    Returns:
        Column with boolean indicating exact match

    Example:
        >>> df.withColumn("match", fuzzy.exact_match(F.col("a"), F.col("b")))
    """
    if ignore_case:
        return F.lower(col1) == F.lower(col2)
    return col1 == col2


@fuzzy.register()
def contains_match(
    col1: "Column", col2: "Column", ignore_case: bool = True
) -> "Column":
    """Check if one string contains the other.

    Returns True if col1 contains col2 OR col2 contains col1.

    Args:
        col1: First string column
        col2: Second string column
        ignore_case: If True, comparison is case-insensitive (default: True)

    Returns:
        Column with boolean indicating containment

    Example:
        >>> df.withColumn("contains", fuzzy.contains_match(F.col("a"), F.col("b")))
    """
    if ignore_case:
        c1, c2 = F.lower(col1), F.lower(col2)
    else:
        c1, c2 = col1, col2
    return F.contains(c1, c2) | F.contains(c2, c1)


@fuzzy.register()
def prefix_match(col1: "Column", col2: "Column", length: int = 3) -> "Column":
    """Check if two strings share the same prefix.

    Args:
        col1: First string column
        col2: Second string column
        length: Number of characters to compare (default: 3)

    Returns:
        Column with boolean indicating prefix match

    Example:
        >>> df.withColumn("same_prefix", fuzzy.prefix_match(F.col("a"), F.col("b"), length=4))
    """
    return F.left(F.lower(col1), F.lit(length)) == F.left(F.lower(col2), F.lit(length))


# =============================================================================
# N-gram Functions
# =============================================================================


@fuzzy.register()
def ngram_similarity(col1: "Column", col2: "Column", n: int = 2) -> "Column":
    """Calculate n-gram (character-level) similarity between two strings.

    Breaks strings into overlapping character sequences of length n,
    then calculates Jaccard similarity on the n-gram sets.

    Good for catching typos and character-level variations.

    Args:
        col1: First string column
        col2: Second string column
        n: Size of n-grams (default: 2 for bigrams)

    Returns:
        Column with float similarity score between 0.0 and 1.0

    Example:
        >>> df.withColumn("sim", fuzzy.ngram_similarity(F.col("a"), F.col("b"), n=2))
    """

    # Generate n-grams using transform to create array of substrings
    # For a string of length L, we get L-n+1 n-grams
    def make_ngrams(col: "Column", n: int) -> "Column":
        # Pad the string to handle short strings
        padded = F.lower(col)
        length = F.length(padded)
        # Generate indices from 0 to length-n
        indices = F.sequence(F.lit(0), F.greatest(length - F.lit(n), F.lit(0)))
        # Extract substring at each index
        return F.transform(indices, lambda i: F.substring(padded, i + 1, n))

    ngrams1 = make_ngrams(col1, n)
    ngrams2 = make_ngrams(col2, n)

    intersection = F.size(F.array_intersect(ngrams1, ngrams2))
    union = F.size(F.array_union(ngrams1, ngrams2))

    return F.when(union == 0, F.lit(1.0)).otherwise(intersection / union)


@fuzzy.register()
def ngram_distance(col1: "Column", col2: "Column", n: int = 2) -> "Column":
    """Calculate n-gram distance (1 - similarity) between two strings.

    Args:
        col1: First string column
        col2: Second string column
        n: Size of n-grams (default: 2 for bigrams)

    Returns:
        Column with float distance between 0.0 and 1.0

    Example:
        >>> df.withColumn("dist", fuzzy.ngram_distance(F.col("a"), F.col("b")))
    """

    def make_ngrams(col: "Column", n: int) -> "Column":
        padded = F.lower(col)
        length = F.length(padded)
        indices = F.sequence(F.lit(0), F.greatest(length - F.lit(n), F.lit(0)))
        return F.transform(indices, lambda i: F.substring(padded, i + 1, n))

    ngrams1 = make_ngrams(col1, n)
    ngrams2 = make_ngrams(col2, n)

    intersection = F.size(F.array_intersect(ngrams1, ngrams2))
    union = F.size(F.array_union(ngrams1, ngrams2))

    similarity = F.when(union == 0, F.lit(1.0)).otherwise(intersection / union)
    return F.lit(1.0) - similarity


# =============================================================================
# Cosine Similarity
# =============================================================================


@fuzzy.register()
def cosine_similarity(col1: "Column", col2: "Column", delimiter: str = " ") -> "Column":
    """Calculate cosine similarity between tokenized strings.

    Treats each string as a bag of words and computes cosine similarity
    based on term frequency. Good for comparing longer text.

    Args:
        col1: First string column
        col2: Second string column
        delimiter: Token delimiter (default: space)

    Returns:
        Column with float similarity score between 0.0 and 1.0

    Example:
        >>> df.withColumn("sim", fuzzy.cosine_similarity(F.col("a"), F.col("b")))
    """
    # Tokenize
    tokens1 = F.split(F.lower(col1), delimiter)
    tokens2 = F.split(F.lower(col2), delimiter)

    # Get all unique tokens
    all_tokens = F.array_union(tokens1, tokens2)

    # Calculate dot product and magnitudes
    # dot_product = sum(tf1[t] * tf2[t] for t in all_tokens)
    # magnitude1 = sqrt(sum(tf1[t]^2 for t in all_tokens))
    # magnitude2 = sqrt(sum(tf2[t]^2 for t in all_tokens))

    dot_product = F.aggregate(
        all_tokens,
        F.lit(0.0),
        lambda acc, token: acc
        + (
            F.size(F.filter(tokens1, lambda t: t == token)).cast("double")
            * F.size(F.filter(tokens2, lambda t: t == token)).cast("double")
        ),
    )

    magnitude1 = F.sqrt(
        F.aggregate(
            all_tokens,
            F.lit(0.0),
            lambda acc, token: acc
            + F.pow(F.size(F.filter(tokens1, lambda t: t == token)).cast("double"), 2),
        )
    )

    magnitude2 = F.sqrt(
        F.aggregate(
            all_tokens,
            F.lit(0.0),
            lambda acc, token: acc
            + F.pow(F.size(F.filter(tokens2, lambda t: t == token)).cast("double"), 2),
        )
    )

    denominator = magnitude1 * magnitude2

    return F.when(denominator == 0, F.lit(0.0)).otherwise(dot_product / denominator)
