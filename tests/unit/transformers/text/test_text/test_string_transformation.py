"""
Tests for string transformation primitives.
Covers hex conversion, base64, URL encoding, HTML entities, and escape sequences.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField


# =============================================================================
# TEST DATA
# =============================================================================

# --- Hex Transformations ---
HEX_TO_TEXT_DATA = [
    ("48656c6c6f", "Hello"),
    ("0x48656c6c6f", "Hello"),
    ("48 65 6c 6c 6f", "Hello"),
    ("576f726c64", "World"),
    ("48656c6c6f20576f726c64", "Hello World"),
    ("746573742031323334", "test 1234"),
    ("0a", "\n"),
    ("0d0a", "\r\n"),
    ("09", "\t"),
    ("20", " "),
    ("", ""),
    (None, ""),
]

TEXT_TO_HEX_DATA = [
    ("Hello", "48656c6c6f"),
    ("World", "576f726c64"),
    ("Hello World", "48656c6c6f20576f726c64"),
    ("test 1234", "746573742031323334"),
    ("\n", "0a"),
    ("\t", "09"),
    (" ", "20"),
    ("", ""),
    (None, ""),
]

CLEAN_HEX_DATA = [
    ("0x1a2b3c", "1a2b3c"),
    ("0X1A2B3C", "1a2b3c"),
    ("1A2B3C", "1a2b3c"),
    ("12 34 56", "123456"),
    ("12:34:56", "123456"),
    ("12-34-56", "123456"),
    ("0x 1a 2b", "1a2b"),
    ("DEADBEEF", "deadbeef"),
    ("", ""),
    (None, ""),
]

EXTRACT_HEX_DATA = [
    ("Color: #ff0000", "ff0000"),
    ("0xDEADBEEF in memory", "deadbeef"),
    ("MAC: 00:11:22:33:44:55", "001122334455"),
    ("No hex here", ""),
    ("Mixed 0xAB and #CD", "ab"),
    ("", ""),
    (None, ""),
]

# --- Base64 Transformations ---
BASE64_DECODE_DATA = [
    ("SGVsbG8=", "Hello"),
    ("SGVsbG8gV29ybGQ=", "Hello World"),
    ("dGVzdA==", "test"),
    ("YWJj", "abc"),
    ("YWI=", "ab"),
    ("YQ==", "a"),
    ("", ""),
    ("MTIzNDU2Nzg5MA==", "1234567890"),
    (None, ""),
]

BASE64_ENCODE_DATA = [
    ("Hello", "SGVsbG8="),
    ("Hello World", "SGVsbG8gV29ybGQ="),
    ("test", "dGVzdA=="),
    ("abc", "YWJj"),
    ("", ""),
    (None, ""),
]

CLEAN_BASE64_DATA = [
    ("SGVs bG8=", "SGVsbG8="),
    ("SGVs\nbG8=", "SGVsbG8="),
    ("SGVsbG8", "SGVsbG8="),
    ("SGVsbG8==", "SGVsbG8="),
    ("  SGVsbG8=  ", "SGVsbG8="),
    ("", ""),
    (None, ""),
]

EXTRACT_BASE64_DATA = [
    ("data:image/png;base64,SGVsbG8=", "SGVsbG8="),
    ("The token is: SGVsbG8gV29ybGQ=", "SGVsbG8gV29ybGQ="),
    ("No base64 here", ""),
    ("", ""),
    (None, ""),
]

# --- URL Encoding Transformations ---
URL_DECODE_DATA = [
    ("Hello%20World", "Hello World"),
    ("test%2Bvalue", "test+value"),
    ("a%26b%3Dc", "a&b=c"),
    ("100%25", "100%"),
    ("%3Cscript%3E", "<script>"),
    ("caf%C3%A9", "café"),
    ("%E2%9C%93", "✓"),
    ("hello%0Aworld", "hello\nworld"),
    ("path%2Fto%2Ffile", "path/to/file"),
    ("", ""),
    (None, ""),
    ("no+encoding+needed", "no encoding needed"),
]

URL_ENCODE_DATA = [
    ("Hello World", "Hello%20World"),
    ("test+value", "test%2Bvalue"),
    ("a&b=c", "a%26b%3Dc"),
    ("100%", "100%25"),
    ("<script>", "%3Cscript%3E"),
    ("", ""),
    (None, ""),
]

# --- HTML Entity Transformations ---
HTML_DECODE_DATA = [
    ("&amp;", "&"),
    ("&lt;", "<"),
    ("&gt;", ">"),
    ("&quot;", '"'),
    ("&apos;", "'"),
    ("&#60;", "<"),
    ("&#x3C;", "<"),
    ("&#x3c;", "<"),
    ("&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;", '<script>alert("xss")</script>'),
    ("Tom &amp; Jerry", "Tom & Jerry"),
    ("5 &gt; 3 &amp;&amp; 3 &lt; 5", "5 > 3 && 3 < 5"),
    ("&copy;", "©"),
    ("&reg;", "®"),
    ("&trade;", "™"),
    ("", ""),
    (None, ""),
    ("no entities here", "no entities here"),
]

HTML_ENCODE_DATA = [
    ("&", "&amp;"),
    ("<", "&lt;"),
    (">", "&gt;"),
    ('"', "&quot;"),
    ("<script>", "&lt;script&gt;"),
    ("Tom & Jerry", "Tom &amp; Jerry"),
    ("", ""),
    (None, ""),
]

# --- Escape Sequence Transformations ---
UNESCAPE_DATA = [
    (r"hello\nworld", "hello\nworld"),
    (r"tab\there", "tab\there"),
    (r"back\\slash", "back\\slash"),
    (r"quote\"here", 'quote"here'),
    (r"single\'quote", "single'quote"),
    (r"\r\n", "\r\n"),
    (r"hex\x41", "hexA"),
    (r"unicode\u0041", "unicodeA"),
    ("already\nunescaped", "already\nunescaped"),
    ("", ""),
    (None, ""),
]

ESCAPE_DATA = [
    ("hello\nworld", r"hello\nworld"),
    ("tab\there", r"tab\there"),
    ("back\\slash", r"back\\slash"),
    ('quote"here', r'quote\"here'),
    ("\r\n", r"\r\n"),
    ("", ""),
    (None, ""),
]

# --- Line Ending Transformations ---
NORMALIZE_LINE_ENDINGS_DATA = [
    ("hello\r\nworld", "hello\nworld"),
    ("hello\rworld", "hello\nworld"),
    ("hello\nworld", "hello\nworld"),
    ("line1\r\nline2\rline3\n", "line1\nline2\nline3\n"),
    ("no newlines", "no newlines"),
    ("", ""),
    (None, ""),
]

# --- Unicode/ASCII Transformations ---
TO_ASCII_DATA = [
    ("café", "cafe"),
    ("naïve", "naive"),
    ("résumé", "resume"),
    ("Zürich", "Zurich"),
    ("\u201chello\u201d", '"hello"'),
    ("test\u2014value", "test-value"),
    ("日本語", "???"),
    ("", ""),
    (None, ""),
]

# Note: to_codepoints/from_codepoints have limited functionality without UDFs
# They only handle the first character and basic ASCII/BMP characters
TO_CODEPOINTS_DATA = [
    ("A", "U+41"),  # Basic ASCII
    ("", ""),
    (None, ""),
]

FROM_CODEPOINTS_DATA = [
    # Note: from_codepoints only handles single codepoints in BMP range
    ("", ""),
    (None, ""),
]

# --- String Manipulation Transformations ---
REVERSE_STRING_DATA = [
    ("hello", "olleh"),
    ("Hello World", "dlroW olleH"),
    ("12345", "54321"),
    ("a", "a"),
    ("", ""),
    (None, ""),
    ("café", "éfac"),
]

TRUNCATE_DATA = [
    ("Hello World", 8, "Hello..."),  # 8 chars = 5 + "..."
    ("Hello World", 11, "Hello World"),  # exactly 11 chars, no truncation
    ("Hi", 10, "Hi"),  # shorter than max, no truncation
    ("", 5, ""),
    (None, 5, ""),
]

PAD_LEFT_DATA = [
    ("42", 5, "0", "00042"),
    ("hello", 10, " ", "     hello"),
    ("abc", 3, "x", "abc"),
    ("", 3, "0", "000"),
    (None, 3, "0", ""),
]

PAD_RIGHT_DATA = [
    ("42", 5, "0", "42000"),
    ("hello", 10, " ", "hello     "),
    ("abc", 3, "x", "abc"),
    ("", 3, "0", "000"),
    (None, 3, "0", ""),
]


# =============================================================================
# TESTS
# =============================================================================

@pytest.mark.unit
class TestHexTransformations:
    """Tests for hexadecimal transformation functions."""

    @pytest.mark.parametrize("input_val,expected", HEX_TO_TEXT_DATA)
    def test_hex_to_text(self, spark, input_val, expected):
        """Test hex string to text conversion."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.hex_to_text(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"hex_to_text({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", TEXT_TO_HEX_DATA)
    def test_text_to_hex(self, spark, input_val, expected):
        """Test text to hex string conversion."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.text_to_hex(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"text_to_hex({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", CLEAN_HEX_DATA)
    def test_clean_hex(self, spark, input_val, expected):
        """Test hex string cleaning."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.clean_hex(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"clean_hex({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", EXTRACT_HEX_DATA)
    def test_extract_hex(self, spark, input_val, expected):
        """Test hex extraction from mixed content."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.extract_hex(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"extract_hex({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestBase64Transformations:
    """Tests for base64 transformation functions."""

    @pytest.mark.parametrize("input_val,expected", BASE64_DECODE_DATA)
    def test_decode_base64(self, spark, input_val, expected):
        """Test base64 decoding."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.decode_base64(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"decode_base64({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", BASE64_ENCODE_DATA)
    def test_encode_base64(self, spark, input_val, expected):
        """Test base64 encoding."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.encode_base64(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"encode_base64({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", CLEAN_BASE64_DATA)
    def test_clean_base64(self, spark, input_val, expected):
        """Test base64 string cleaning."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.clean_base64(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"clean_base64({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", EXTRACT_BASE64_DATA)
    def test_extract_base64(self, spark, input_val, expected):
        """Test base64 extraction from mixed content."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.extract_base64(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"extract_base64({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestUrlEncodingTransformations:
    """Tests for URL encoding transformation functions."""

    @pytest.mark.parametrize("input_val,expected", URL_DECODE_DATA)
    def test_decode_url(self, spark, input_val, expected):
        """Test URL decoding."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.decode_url(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"decode_url({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", URL_ENCODE_DATA)
    def test_encode_url(self, spark, input_val, expected):
        """Test URL encoding."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.encode_url(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"encode_url({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestHtmlEntityTransformations:
    """Tests for HTML entity transformation functions."""

    @pytest.mark.parametrize("input_val,expected", HTML_DECODE_DATA)
    def test_decode_html_entities(self, spark, input_val, expected):
        """Test HTML entity decoding."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.decode_html_entities(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"decode_html_entities({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", HTML_ENCODE_DATA)
    def test_encode_html_entities(self, spark, input_val, expected):
        """Test HTML entity encoding."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.encode_html_entities(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"encode_html_entities({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestEscapeSequenceTransformations:
    """Tests for escape sequence transformation functions."""

    @pytest.mark.parametrize("input_val,expected", UNESCAPE_DATA)
    def test_unescape_string(self, spark, input_val, expected):
        """Test unescaping literal escape sequences."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.unescape_string(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"unescape_string({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", ESCAPE_DATA)
    def test_escape_string(self, spark, input_val, expected):
        """Test escaping to literal escape sequences."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.escape_string(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"escape_string({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestLineEndingTransformations:
    """Tests for line ending transformation functions."""

    @pytest.mark.parametrize("input_val,expected", NORMALIZE_LINE_ENDINGS_DATA)
    def test_normalize_line_endings(self, spark, input_val, expected):
        """Test line ending normalization."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.normalize_line_endings(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"normalize_line_endings({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestUnicodeTransformations:
    """Tests for unicode transformation functions."""

    @pytest.mark.parametrize("input_val,expected", TO_ASCII_DATA)
    def test_to_ascii(self, spark, input_val, expected):
        """Test non-ASCII to ASCII transliteration."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.to_ascii(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"to_ascii({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", TO_CODEPOINTS_DATA)
    def test_to_codepoints(self, spark, input_val, expected):
        """Test unicode to codepoint conversion."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.to_codepoints(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"to_codepoints({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,expected", FROM_CODEPOINTS_DATA)
    def test_from_codepoints(self, spark, input_val, expected):
        """Test codepoint to unicode conversion."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.from_codepoints(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"from_codepoints({input_val!r}) = {result!r}, expected {expected!r}"


@pytest.mark.unit
class TestStringManipulationTransformations:
    """Tests for string manipulation transformation functions."""

    @pytest.mark.parametrize("input_val,expected", REVERSE_STRING_DATA)
    def test_reverse_string(self, spark, input_val, expected):
        """Test string reversal."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.reverse_string(F.col("input")))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"reverse_string({input_val!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,max_len,expected", TRUNCATE_DATA)
    def test_truncate(self, spark, input_val, max_len, expected):
        """Test string truncation."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.truncate(F.col("input"), max_length=max_len))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"truncate({input_val!r}, {max_len}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,width,char,expected", PAD_LEFT_DATA)
    def test_pad_left(self, spark, input_val, width, char, expected):
        """Test left padding."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.pad_left(F.col("input"), width=width, pad_char=char))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"pad_left({input_val!r}, {width}, {char!r}) = {result!r}, expected {expected!r}"

    @pytest.mark.parametrize("input_val,width,char,expected", PAD_RIGHT_DATA)
    def test_pad_right(self, spark, input_val, width, char, expected):
        """Test right padding."""
        from datacompose.transformers.text.text.pyspark.pyspark_primitives import text

        schema = StructType([StructField("input", StringType(), True)])
        df = spark.createDataFrame([(input_val,)], schema)
        result_df = df.withColumn("result", text.pad_right(F.col("input"), width=width, pad_char=char))
        result = result_df.collect()[0]["result"]
        assert result == expected, f"pad_right({input_val!r}, {width}, {char!r}) = {result!r}, expected {expected!r}"
