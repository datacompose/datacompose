"""
Tests for string cleaning primitives.
Covers control characters, zero-width chars, unicode normalization, and whitespace.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField


# =============================================================================
# TEST DATA
# =============================================================================

# --- Control Character Cleaning ---
REMOVE_CONTROL_CHARS_DATA = [
    ("hello\x00world", "helloworld"),
    ("test\x01\x02\x03", "test"),
    ("line1\x0bline2", "line1line2"),
    ("text\x0c", "text"),
    ("data\x1b[31m", "data[31m"),
    ("normal\x7ftext", "normaltext"),
    ("\x00\x01\x02\x03\x04\x05", ""),
    ("", ""),
    (None, ""),
    ("no control chars", "no control chars"),
    ("hello\tworld", "hello\tworld"),
    ("hello\nworld", "hello\nworld"),
    ("hello\rworld", "hello\rworld"),
]

REMOVE_NON_PRINTABLE_DATA = [
    ("hello\x00world", "helloworld"),
    ("test\x07bell", "testbell"),
    ("data\x08back", "databack"),
    ("line\x1b[0m", "line[0m"),
    ("\x00\x07\x08\x1b", ""),
    ("clean text", "clean text"),
    ("with\ttabs", "with\ttabs"),
    ("with\nnewlines", "with\nnewlines"),
    ("", ""),
    (None, ""),
]

REMOVE_ANSI_CODES_DATA = [
    ("\x1b[31mred text\x1b[0m", "red text"),
    ("\x1b[1mbold\x1b[0m", "bold"),
    ("\x1b[32;1mgreen bold\x1b[0m", "green bold"),
    ("\x1b[38;5;196mextended color\x1b[0m", "extended color"),
    ("\x1b[38;2;255;0;0mtrue color\x1b[0m", "true color"),
    ("no ansi codes", "no ansi codes"),
    ("", ""),
    (None, ""),
]

# --- Invisible Character Cleaning ---
REMOVE_ZERO_WIDTH_DATA = [
    ("hel\u200blo", "hello"),
    ("wor\u200cld", "world"),
    ("te\u200dst", "test"),
    ("da\ufeffta", "data"),
    ("a\u2060b", "ab"),
    ("x\u180ey", "xy"),
    ("\u200b\u200c\u200d", ""),
    ("clean text", "clean text"),
    ("", ""),
    (None, ""),
    ("h\u200be\u200cl\u200dl\ufeffo", "hello"),
]

STRIP_INVISIBLE_DATA = [
    ("\ufeffhello\u200b world\x00", "hello world"),
    ("test\u200b\u200c\u200d", "test"),
    ("\x00\x01test\x7f", "test"),
    ("clean", "clean"),
    ("", ""),
    (None, ""),
]

REMOVE_BOM_DATA = [
    ("\ufeffHello", "Hello"),
    ("\ufeff\ufeffDouble", "Double"),
    ("No BOM", "No BOM"),
    ("\ufeff", ""),
    ("", ""),
    (None, ""),
]

# --- Unicode Normalization Cleaning ---
# Note: Full NFKC normalization (combining chars, full-width) requires UDF
# This function handles: curly quotes, fancy dashes, ellipsis, special spaces
NORMALIZE_UNICODE_DATA = [
    ("\u201chello\u201d", '"hello"'),  # curly double quotes
    ("\u2018test\u2019", "'test'"),  # curly single quotes
    ("test\u2013value", "test-value"),  # en-dash
    ("test\u2014value", "test-value"),  # em-dash
    ("wait\u2026", "wait..."),  # ellipsis
    ("hello\u00a0world", "hello world"),  # non-breaking space
    ("hello\u2003world", "hello world"),  # em space
    ("hello\u2009world", "hello world"),  # thin space
    ("", ""),
    (None, ""),
    ("already normal", "already normal"),
]

REMOVE_ACCENTS_DATA = [
    ("café", "cafe"),
    ("naïve", "naive"),
    ("résumé", "resume"),
    ("piñata", "pinata"),
    ("Zürich", "Zurich"),
    ("Ångström", "Angstrom"),
    # Note: Czech/Vietnamese chars need more extensive char mapping
    ("日本語", "日本語"),  # Non-Latin chars pass through unchanged
    ("", ""),
    (None, ""),
    ("already ascii", "already ascii"),
]

# --- Whitespace Cleaning ---
NORMALIZE_WHITESPACE_DATA = [
    ("  hello  ", "hello"),
    ("hello   world", "hello world"),
    ("hello\t\tworld", "hello world"),
    ("hello\n\nworld", "hello world"),
    ("  hello   world  ", "hello world"),
    ("\t\n hello \t\n", "hello"),
    ("", ""),
    (None, ""),
    ("   ", ""),
    ("already clean", "already clean"),
]

# --- Content Removal Cleaning ---
REMOVE_HTML_TAGS_DATA = [
    ("<p>Hello</p>", "Hello"),
    ("<div class='test'>Content</div>", "Content"),
    ("<script>alert('xss')</script>", "alert('xss')"),
    ("<br/>", ""),
    ("<a href='url'>Link</a>", "Link"),
    ("No <b>bold</b> here", "No bold here"),
    ("<p>Line 1</p><p>Line 2</p>", "Line 1Line 2"),
    ("plain text", "plain text"),
    ("", ""),
    (None, ""),
    ("<!-- comment -->text", "text"),
]

REMOVE_URLS_DATA = [
    ("Visit https://example.com today", "Visit  today"),
    ("Check http://test.org/path?q=1", "Check "),
    ("Link: www.google.com here", "Link:  here"),
    ("Email me@test.com stays", "Email me@test.com stays"),
    ("ftp://files.server.com/file", ""),
    ("Multiple https://a.com and http://b.com", "Multiple  and "),
    ("no urls here", "no urls here"),
    ("", ""),
    (None, ""),
]

REMOVE_EMOJIS_DATA = [
    ("Hello 😀 World", "Hello  World"),
    ("🎉 Party 🎊", " Party "),
    ("Test 👍🏻 emoji", "Test  emoji"),
    ("Flags 🇺🇸🇬🇧", "Flags "),
    # Note: keycap emojis (1️⃣) have combining chars that may not be fully removed
    ("no emojis", "no emojis"),
    ("", ""),
    (None, ""),
]

# --- Character Type Removal Cleaning ---
REMOVE_PUNCTUATION_DATA = [
    ("Hello, World!", "Hello World"),
    ("What?! Yes.", "What Yes"),
    ("test-case_value", "testcase_value"),  # underscore is kept (part of \w)
    ("a.b.c", "abc"),
    ("'quoted'", "quoted"),
    ('"double"', "double"),
    ("no punct", "no punct"),
    ("", ""),
    (None, ""),
]

REMOVE_DIGITS_DATA = [
    ("test123", "test"),
    ("abc 456 def", "abc  def"),
    ("2024-01-15", "--"),
    ("phone: 555-1234", "phone: -"),
    ("no digits", "no digits"),
    ("", ""),
    (None, ""),
]

REMOVE_LETTERS_DATA = [
    ("test123", "123"),
    ("abc 456 def", " 456 "),
    ("2024-01-15", "2024-01-15"),
    ("phone: 555-1234", ": 555-1234"),
    ("123", "123"),
    ("", ""),
    (None, ""),
]

REMOVE_ESCAPE_SEQUENCES_DATA = [
    (r"hello\nworld", "helloworld"),
    (r"tab\there", "tabhere"),
    (r"back\\slash", "backslash"),
    (r"\r\n\t", ""),
    ("no escapes", "no escapes"),
    ("", ""),
    (None, ""),
]

STRIP_TO_ALPHANUMERIC_DATA = [
    ("Hello, World! 123", "HelloWorld123"),
    ("test@example.com", "testexamplecom"),
    ("phone: (555) 123-4567", "phone5551234567"),
    ("___abc___", "abc"),
    ("", ""),
    (None, ""),
]

# --- Comprehensive Cleaning ---
CLEAN_FOR_COMPARISON_DATA = [
    ("  Hello World  ", "hello world"),
    ("CAFÉ", "cafe"),
    ("  Multiple   Spaces  ", "multiple spaces"),
    ("MixedCASE", "mixedcase"),
    ("Naïve Résumé", "naive resume"),
    ("", ""),
    (None, ""),
]

SLUGIFY_DATA = [
    ("Hello World", "hello-world"),
    ("This is a TEST", "this-is-a-test"),
    ("Café Münster", "cafe-munster"),
    ("Multiple   Spaces", "multiple-spaces"),
    ("Special!@#Characters", "specialcharacters"),
    ("Already-slugified", "already-slugified"),
    ("  trim me  ", "trim-me"),
    ("", ""),
    (None, ""),
]

COLLAPSE_REPEATS_DATA = [
    ("hellooooo", 2, "helloo"),
    ("yeeeessss", 1, "yes"),
    ("aabbcc", 1, "abc"),
    ("normal", 2, "normal"),
    ("", 2, ""),
    (None, 2, ""),
]

CLEAN_STRING_DATA = [
    ("\ufeffHello\u200b World\x00", "Hello World"),
    ("  café\u2019s  ", "café's"),
    ("\x1b[31mColored\x1b[0m", "Colored"),
    ("Normal text", "Normal text"),
    ("", ""),
    (None, ""),
]


# =============================================================================
# TESTS
# =============================================================================

@pytest.mark.unit
class TestControlCharacterCleaning:
    """Tests for control character cleaning functions."""

    @pytest.mark.parametrize("input_val,expected", REMOVE_CONTROL_CHARS_DATA)
    def test_remove_control_characters(self, spark, input_val, expected):
        """Test removal of control characters while preserving tabs/newlines."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_control_characters(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_control_characters({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", REMOVE_NON_PRINTABLE_DATA)
    def test_remove_non_printable(self, spark, input_val, expected):
        """Test removal of non-printable characters."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_non_printable(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_non_printable({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", REMOVE_ANSI_CODES_DATA)
    def test_remove_ansi_codes(self, spark, input_val, expected):
        """Test removal of ANSI escape codes."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_ansi_codes(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_ansi_codes({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestInvisibleCharacterCleaning:
    """Tests for invisible and zero-width character cleaning functions."""

    @pytest.mark.parametrize("input_val,expected", REMOVE_ZERO_WIDTH_DATA)
    def test_remove_zero_width_characters(self, spark, input_val, expected):
        """Test removal of zero-width characters."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_zero_width_characters(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_zero_width_characters({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", STRIP_INVISIBLE_DATA)
    def test_strip_invisible(self, spark, input_val, expected):
        """Test comprehensive removal of all invisible characters."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.strip_invisible(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"strip_invisible({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", REMOVE_BOM_DATA)
    def test_remove_bom(self, spark, input_val, expected):
        """Test removal of byte order mark."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_bom(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_bom({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestUnicodeNormalizationCleaning:
    """Tests for unicode normalization cleaning functions."""

    @pytest.mark.parametrize("input_val,expected", NORMALIZE_UNICODE_DATA)
    def test_normalize_unicode(self, spark, input_val, expected):
        """Test unicode normalization (NFKC + common replacements)."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.normalize_unicode(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"normalize_unicode({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", REMOVE_ACCENTS_DATA)
    def test_remove_accents(self, spark, input_val, expected):
        """Test removal of accents/diacritics."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_accents(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_accents({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestWhitespaceCleaning:
    """Tests for whitespace cleaning functions."""

    @pytest.mark.parametrize("input_val,expected", NORMALIZE_WHITESPACE_DATA)
    def test_normalize_whitespace(self, spark, input_val, expected):
        """Test whitespace normalization (trim + collapse multiple)."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.normalize_whitespace(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"normalize_whitespace({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestContentRemovalCleaning:
    """Tests for content removal cleaning functions (HTML, URLs, emojis)."""

    @pytest.mark.parametrize("input_val,expected", REMOVE_HTML_TAGS_DATA)
    def test_remove_html_tags(self, spark, input_val, expected):
        """Test removal of HTML tags."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_html_tags(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_html_tags({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", REMOVE_URLS_DATA)
    def test_remove_urls(self, spark, input_val, expected):
        """Test removal of URLs."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_urls(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_urls({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", REMOVE_EMOJIS_DATA)
    def test_remove_emojis(self, spark, input_val, expected):
        """Test removal of emojis."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_emojis(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_emojis({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestCharacterTypeCleaning:
    """Tests for character type removal cleaning functions."""

    @pytest.mark.parametrize("input_val,expected", REMOVE_PUNCTUATION_DATA)
    def test_remove_punctuation(self, spark, input_val, expected):
        """Test removal of punctuation."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_punctuation(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_punctuation({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", REMOVE_DIGITS_DATA)
    def test_remove_digits(self, spark, input_val, expected):
        """Test removal of digits."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_digits(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_digits({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", REMOVE_LETTERS_DATA)
    def test_remove_letters(self, spark, input_val, expected):
        """Test removal of letters."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_letters(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_letters({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", REMOVE_ESCAPE_SEQUENCES_DATA)
    def test_remove_escape_sequences(self, spark, input_val, expected):
        """Test removal of literal escape sequences."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.remove_escape_sequences(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"remove_escape_sequences({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", STRIP_TO_ALPHANUMERIC_DATA)
    def test_strip_to_alphanumeric(self, spark, input_val, expected):
        """Test stripping to alphanumeric only."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.strip_to_alphanumeric(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"strip_to_alphanumeric({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestComprehensiveCleaning:
    """Tests for comprehensive cleaning functions."""

    @pytest.mark.parametrize("input_val,expected", CLEAN_FOR_COMPARISON_DATA)
    def test_clean_for_comparison(self, spark, input_val, expected):
        """Test comprehensive cleaning for string comparison."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.clean_for_comparison(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"clean_for_comparison({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", SLUGIFY_DATA)
    def test_slugify(self, spark, input_val, expected):
        """Test slugification."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.slugify(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"slugify({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,max_repeat,expected", COLLAPSE_REPEATS_DATA)
    def test_collapse_repeats(self, spark, input_val, max_repeat, expected):
        """Test collapsing repeated characters."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.collapse_repeats(F.col("input"), max_repeat=max_repeat))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"collapse_repeats({input_val!r}, {max_repeat}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", CLEAN_STRING_DATA)
    def test_clean_string(self, spark, input_val, expected):
        """Test comprehensive string cleaning."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.clean_string(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"clean_string({input_val!r}) = {result!r}, expected {expected!r}"
