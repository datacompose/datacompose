"""
Tests for string validation primitives.
Covers hexadecimal, base64, control characters, and unicode detection.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField


# =============================================================================
# TEST DATA
# =============================================================================

# --- Hex ---
VALID_HEX_DATA = [
    ("0x1a2b3c", True),
    ("0X1A2B3C", True),
    ("1a2b3c", True),
    ("1A2B3C", True),
    ("deadbeef", True),
    ("DEADBEEF", True),
    ("00", True),
    ("ff", True),
    ("0000", True),
    ("ffff", True),
    ("0xaabbccdd", True),
    ("a1b2c3d4e5f6", True),
]

INVALID_HEX_DATA = [
    ("0xGHIJ", False),
    ("xyz123", False),
    ("12345g", False),
    ("hello", False),
    ("0x", False),
    ("", False),
    (None, False),
    ("   ", False),
    ("12.34", False),
    ("0x12.34", False),
]

# --- Base64 ---
VALID_BASE64_DATA = [
    ("SGVsbG8=", True),
    ("SGVsbG8gV29ybGQ=", True),
    ("dGVzdA==", True),
    ("YWJj", True),
    ("YWI=", True),
    ("YQ==", True),
    ("", True),
    ("MTIzNDU2Nzg5MA==", True),
]

INVALID_BASE64_DATA = [
    ("!!!!", False),
    ("SGVs bG8=", False),
    ("====", False),
    ("SGVsbG8===", False),
    (None, False),
]

# --- URL Encoding ---
HAS_URL_ENCODING_DATA = [
    ("Hello%20World", True),
    ("test%2B", True),
    ("%3Cscript%3E", True),
    ("caf%C3%A9", True),
    ("%E2%9C%93", True),
    ("no encoding", False),
    ("100% done", False),
    ("", False),
    (None, False),
    ("%", False),
    ("%2", False),
    ("%ZZ", False),
]

IS_VALID_URL_ENCODED_DATA = [
    ("Hello%20World", True),
    ("test%2Bvalue", True),
    ("%3Cscript%3E", True),
    ("caf%C3%A9", True),
    ("no encoding needed", True),
    ("", True),
    (None, False),
    ("%ZZ", False),
    ("%2", False),
    ("%%20", False),
]

# --- HTML Entities ---
HAS_HTML_ENTITIES_DATA = [
    ("&amp;", True),
    ("&lt;", True),
    ("&gt;", True),
    ("&quot;", True),
    ("&#60;", True),
    ("&#x3C;", True),
    ("Tom &amp; Jerry", True),
    ("&copy;", True),
    ("no entities", False),
    ("just & symbol", False),
    ("", False),
    (None, False),
    ("&;", False),
    ("&invalid", False),
]

# --- Control Characters ---
HAS_CONTROL_CHARS_DATA = [
    ("hello\x00world", True),
    ("test\x1b[0m", True),
    ("normal text", False),
    ("with\ttab", False),
    ("with\nnewline", False),
    ("", False),
    (None, False),
    ("\x00", True),
    ("\x7f", True),
    ("\x01\x02\x03", True),
]

# --- Zero-Width Characters ---
HAS_ZERO_WIDTH_DATA = [
    ("hel\u200blo", True),
    ("wor\u200cld", True),
    ("te\u200dst", True),
    ("da\ufeffta", True),
    ("a\u2060b", True),
    ("normal", False),
    ("\u200b", True),
    ("", False),
    (None, False),
]

# --- Non-Printable ---
HAS_NON_PRINTABLE_DATA = [
    ("hello\x00world", True),
    ("test\x07", True),
    ("data\x08", True),
    ("\x1b[0m", True),
    ("normal text", False),
    ("with\ttabs", False),
    ("with\nnewlines", False),
    ("", False),
    (None, False),
]

# --- Non-ASCII ---
HAS_NON_ASCII_DATA = [
    ("café", True),
    ("naïve", True),
    ("hello", False),
    ("123", False),
    ("Hello World!", False),
    ("日本語", True),
    ("emoji 😀", True),
    ("", False),
    (None, False),
]

# --- Escape Sequences ---
HAS_ESCAPE_SEQUENCES_DATA = [
    (r"hello\nworld", True),
    (r"test\ttab", True),
    ("real\nnewline", False),
    (r"\\backslash", True),
    ("no escapes", False),
    ("", False),
    (None, False),
    (r"\x41", True),
    (r"\u0041", True),
]

# --- ANSI Codes ---
HAS_ANSI_CODES_DATA = [
    ("\x1b[31mred\x1b[0m", True),
    ("\x1b[1mbold\x1b[0m", True),
    ("\x1b[32;1m", True),
    ("\x1b[38;5;196m", True),
    ("\x1b[38;2;255;0;0m", True),
    ("\x1b[H\x1b[2J", True),
    ("no ansi", False),
    ("", False),
    (None, False),
    ("\x1b", False),
]

# --- Accents ---
HAS_ACCENTS_DATA = [
    ("café", True),
    ("naïve", True),
    ("résumé", True),
    ("piñata", True),
    ("Zürich", True),
    ("Ångström", True),
    ("hello", False),
    ("123", False),
    ("", False),
    (None, False),
    ("日本語", False),
]

# --- Unicode Issues ---
HAS_UNICODE_ISSUES_DATA = [
    ("\u201chello\u201d", True),
    ("\u2018test\u2019", True),
    ("test\u2013value", True),
    ("test\u2014value", True),
    ("wait\u2026", True),
    ("ＡＢＣ", True),
    ("１２３", True),
    ("hello\u00a0world", True),
    ("cafe\u0301", True),
    ("normal text", False),
    ('"straight quotes"', False),
    ("", False),
    (None, False),
]

# --- Whitespace Issues ---
HAS_WHITESPACE_ISSUES_DATA = [
    ("  leading", True),
    ("trailing  ", True),
    ("multiple   spaces", True),
    ("tabs\t\there", True),
    ("newlines\n\nhere", True),
    ("\u00a0non-breaking", True),
    ("\u2003em space", True),
    ("clean", False),
    ("one space", False),
    ("", False),
    (None, False),
]


# =============================================================================
# TESTS
# =============================================================================

@pytest.mark.unit
class TestHexValidation:
    """Tests for hexadecimal validation functions."""

    @pytest.mark.parametrize("input_val,expected", VALID_HEX_DATA + INVALID_HEX_DATA)
    def test_is_valid_hex(self, spark, input_val, expected):
        """Test hexadecimal string validation."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.is_valid_hex(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"is_valid_hex({input_val!r}) = {result}, expected {expected}"


@pytest.mark.unit
class TestBase64Validation:
    """Tests for base64 validation functions."""

    @pytest.mark.parametrize("input_val,expected", VALID_BASE64_DATA + INVALID_BASE64_DATA)
    def test_is_valid_base64(self, spark, input_val, expected):
        """Test base64 string validation."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.is_valid_base64(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"is_valid_base64({input_val!r}) = {result}, expected {expected}"


@pytest.mark.unit
class TestUrlEncodingValidation:
    """Tests for URL encoding validation and detection functions."""

    @pytest.mark.parametrize("input_val,expected", HAS_URL_ENCODING_DATA)
    def test_has_url_encoding(self, spark, input_val, expected):
        """Test URL encoding detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_url_encoding(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_url_encoding({input_val!r}) = {result}, expected {expected}"

    @pytest.mark.parametrize("input_val,expected", IS_VALID_URL_ENCODED_DATA)
    def test_is_valid_url_encoded(self, spark, input_val, expected):
        """Test URL encoded string validation."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.is_valid_url_encoded(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"is_valid_url_encoded({input_val!r}) = {result}, expected {expected}"


@pytest.mark.unit
class TestHtmlEntitiesValidation:
    """Tests for HTML entity detection functions."""

    @pytest.mark.parametrize("input_val,expected", HAS_HTML_ENTITIES_DATA)
    def test_has_html_entities(self, spark, input_val, expected):
        """Test HTML entity detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_html_entities(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_html_entities({input_val!r}) = {result}, expected {expected}"


@pytest.mark.unit
class TestControlCharacterValidation:
    """Tests for control character and non-printable detection functions."""

    @pytest.mark.parametrize("input_val,expected", HAS_CONTROL_CHARS_DATA)
    def test_has_control_characters(self, spark, input_val, expected):
        """Test control character detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_control_characters(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_control_characters({input_val!r}) = {result}, expected {expected}"

    @pytest.mark.parametrize("input_val,expected", HAS_NON_PRINTABLE_DATA)
    def test_has_non_printable(self, spark, input_val, expected):
        """Test non-printable character detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_non_printable(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_non_printable({input_val!r}) = {result}, expected {expected}"

    @pytest.mark.parametrize("input_val,expected", HAS_ANSI_CODES_DATA)
    def test_has_ansi_codes(self, spark, input_val, expected):
        """Test ANSI escape code detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_ansi_codes(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_ansi_codes({input_val!r}) = {result}, expected {expected}"


@pytest.mark.unit
class TestInvisibleCharacterValidation:
    """Tests for invisible and zero-width character detection functions."""

    @pytest.mark.parametrize("input_val,expected", HAS_ZERO_WIDTH_DATA)
    def test_has_zero_width_characters(self, spark, input_val, expected):
        """Test zero-width character detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_zero_width_characters(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_zero_width_characters({input_val!r}) = {result}, expected {expected}"


@pytest.mark.unit
class TestUnicodeValidation:
    """Tests for unicode-related validation functions."""

    @pytest.mark.parametrize("input_val,expected", HAS_NON_ASCII_DATA)
    def test_has_non_ascii(self, spark, input_val, expected):
        """Test non-ASCII character detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_non_ascii(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_non_ascii({input_val!r}) = {result}, expected {expected}"

    @pytest.mark.parametrize("input_val,expected", HAS_ACCENTS_DATA)
    def test_has_accents(self, spark, input_val, expected):
        """Test accent/diacritic detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_accents(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_accents({input_val!r}) = {result}, expected {expected}"

    @pytest.mark.parametrize("input_val,expected", HAS_UNICODE_ISSUES_DATA)
    def test_has_unicode_issues(self, spark, input_val, expected):
        """Test unicode normalization issue detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_unicode_issues(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_unicode_issues({input_val!r}) = {result}, expected {expected}"


@pytest.mark.unit
class TestEscapeSequenceValidation:
    """Tests for escape sequence detection functions."""

    @pytest.mark.parametrize("input_val,expected", HAS_ESCAPE_SEQUENCES_DATA)
    def test_has_escape_sequences(self, spark, input_val, expected):
        """Test escape sequence detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_escape_sequences(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_escape_sequences({input_val!r}) = {result}, expected {expected}"


@pytest.mark.unit
class TestWhitespaceValidation:
    """Tests for whitespace issue detection functions."""

    @pytest.mark.parametrize("input_val,expected", HAS_WHITESPACE_ISSUES_DATA)
    def test_has_whitespace_issues(self, spark, input_val, expected):
        """Test whitespace issue detection."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.has_whitespace_issues(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"has_whitespace_issues({input_val!r}) = {result}, expected {expected}"
