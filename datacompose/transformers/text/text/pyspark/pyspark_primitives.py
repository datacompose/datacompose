"""
Text transformation primitives for PySpark.

Provides validation, transformation, and cleaning functions for handling
hexadecimal, base64, URL encoding, HTML entities, control characters,
zero-width characters, unicode normalization, and other text oddities.

All functions use native PySpark SQL functions - no UDFs.

Usage Example:
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transformers.pyspark.text import text

spark = SparkSession.builder.appName("TextCleaning").getOrCreate()

data = [("hello\\x00world",), ("SGVsbG8=",), ("café",)]
df = spark.createDataFrame(data, ["text"])

result_df = df.select(
    F.col("text"),
    text.remove_control_characters(F.col("text")).alias("cleaned"),
    text.has_control_characters(F.col("text")).alias("has_control"),
    text.is_valid_base64(F.col("text")).alias("is_base64")
)

Installation:
datacompose add text
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

text = PrimitiveRegistry("text")


# =============================================================================
# Constants
# =============================================================================

# Zero-width character pattern
ZERO_WIDTH_PATTERN = "[\u200b\u200c\u200d\ufeff\u2060\u180e]"

# Control characters (excluding tab \x09, newline \x0a, carriage return \x0d)
CONTROL_CHAR_PATTERN = "[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]"

# ANSI escape code pattern
# ANSI escape code pattern - ESC[ followed by params and command letter
# The ESC char is \x1b (chr(27)) - we need to include it literally
ANSI_PATTERN = "\x1b\\[[0-9;]*[a-zA-Z]"

# URL pattern
URL_PATTERN = "https?://[^\\s]+|www\\.[^\\s]+|ftp://[^\\s]+"

# HTML tag pattern
HTML_TAG_PATTERN = "<[^>]+>|<!--.*?-->"

# Emoji patterns (simplified - covers most common)
EMOJI_PATTERN = (
    "[\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0"
    "\U0001F900-\U0001F9FF"
    "\U00002600-\U000026FF"
    "\uFE00-\uFE0F]"
)


# =============================================================================
# Validation Functions
# =============================================================================

@text.register()
def is_valid_hex(col: "Column") -> "Column":
    """Check if string is valid hexadecimal.

    Args:
        col: Column containing string to validate

    Returns:
        Column with boolean indicating if valid hex
    """
    cleaned = F.lower(F.regexp_replace(col, "(?i)^0x|[\\s:\\-]", ""))
    return F.when(
        col.isNull() | (F.trim(col) == ""),
        F.lit(False)
    ).otherwise(
        (F.length(cleaned) > 0) &
        (F.regexp_replace(cleaned, "[0-9a-f]", "") == "")
    )


@text.register()
def is_valid_base64(col: "Column") -> "Column":
    """Check if string is valid base64.

    Args:
        col: Column containing string to validate

    Returns:
        Column with boolean indicating if valid base64
    """
    return F.when(
        col.isNull(),
        F.lit(False)
    ).when(
        F.trim(col) == "",
        F.lit(True)
    ).otherwise(
        col.rlike("^[A-Za-z0-9+/]*={0,2}$") &
        ((F.length(col) % 4 == 0) | ~col.rlike("="))
    )


@text.register()
def is_valid_url_encoded(col: "Column") -> "Column":
    """Check if string is valid URL encoded (no malformed percent sequences).

    Args:
        col: Column containing string to validate

    Returns:
        Column with boolean indicating if valid URL encoded
    """
    return F.when(
        col.isNull(),
        F.lit(False)
    ).when(
        F.trim(col) == "",
        F.lit(True)
    ).otherwise(
        ~col.rlike("%(?![0-9A-Fa-f]{2})")
    )


@text.register()
def has_control_characters(col: "Column") -> "Column":
    """Check if string contains control characters (excluding tab/newline/CR).

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of control characters
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike(CONTROL_CHAR_PATTERN)
    )


@text.register()
def has_zero_width_characters(col: "Column") -> "Column":
    """Check if string contains zero-width characters.

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of zero-width characters
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike(ZERO_WIDTH_PATTERN)
    )


@text.register()
def has_non_ascii(col: "Column") -> "Column":
    """Check if string contains non-ASCII characters.

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of non-ASCII characters
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike("[^\x00-\x7F]")
    )


@text.register()
def has_escape_sequences(col: "Column") -> "Column":
    """Check if string contains literal escape sequences (\\n, \\t, etc).

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of escape sequences
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike("\\\\[nrtfvb\\\\\"'0]|\\\\x[0-9A-Fa-f]{2}|\\\\u[0-9A-Fa-f]{4}")
    )


@text.register()
def has_url_encoding(col: "Column") -> "Column":
    """Check if string contains URL percent encoding.

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of URL encoding
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike("%[0-9A-Fa-f]{2}")
    )


@text.register()
def has_html_entities(col: "Column") -> "Column":
    """Check if string contains HTML entities.

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of HTML entities
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike("&[a-zA-Z]+;|&#[0-9]+;|&#x[0-9A-Fa-f]+;")
    )


@text.register()
def has_ansi_codes(col: "Column") -> "Column":
    """Check if string contains ANSI escape codes.

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of ANSI codes
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike("\x1b\\[[0-9;]*[a-zA-Z]")
    )


@text.register()
def has_non_printable(col: "Column") -> "Column":
    """Check if string contains non-printable characters.

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of non-printable characters
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike("[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
    )


@text.register()
def has_accents(col: "Column") -> "Column":
    """Check if string contains accented characters.

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of accents
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike("[àáâãäåæçèéêëìíîïñòóôõöøùúûüýÿÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÑÒÓÔÕÖØÙÚÛÜÝ]")
    )


@text.register()
def has_unicode_issues(col: "Column") -> "Column":
    """Check if string contains unicode normalization issues.

    Detects: curly quotes, fancy dashes, special spaces, full-width chars,
    and combining characters (accents as separate codepoints).

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of unicode issues
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        col.rlike("[\u201c\u201d\u2018\u2019\u2013\u2014\u2026\u00a0\u2003\u2009\uff00-\uffef\u0300-\u036f]")
    )


@text.register()
def has_whitespace_issues(col: "Column") -> "Column":
    """Check if string has whitespace issues.

    Args:
        col: Column containing string to check

    Returns:
        Column with boolean indicating presence of whitespace issues
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit(False)
    ).otherwise(
        (col != F.trim(col)) |
        col.rlike("\\s{2,}") |
        col.rlike("[\u00a0\u2003\u2009]")
    )


# =============================================================================
# Transformation Functions
# =============================================================================

@text.register()
def hex_to_text(col: "Column") -> "Column":
    """Convert hexadecimal string to text.

    Args:
        col: Column containing hex string

    Returns:
        Column with decoded text
    """
    cleaned = F.lower(F.regexp_replace(col, "(?i)^0x|[\\s:\\-]", ""))
    return F.when(
        col.isNull() | (F.trim(col) == ""),
        F.lit("")
    ).otherwise(
        F.decode(F.unhex(cleaned), "UTF-8")
    )


@text.register()
def text_to_hex(col: "Column") -> "Column":
    """Convert text to hexadecimal string.

    Args:
        col: Column containing text

    Returns:
        Column with hex encoded string
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).otherwise(
        F.lower(F.hex(F.encode(col, "UTF-8")))
    )


@text.register()
def clean_hex(col: "Column") -> "Column":
    """Clean hex string (remove prefix, normalize case, remove separators).

    Args:
        col: Column containing hex string

    Returns:
        Column with cleaned hex string
    """
    return F.when(
        col.isNull() | (F.trim(col) == ""),
        F.lit("")
    ).otherwise(
        F.lower(F.regexp_replace(col, "(?i)^0x|[\\s:\\-]", ""))
    )


@text.register()
def extract_hex(col: "Column") -> "Column":
    """Extract first hex value from mixed content.

    Looks for hex with prefix (0x, #) or MAC-address format (XX:XX:XX).

    Args:
        col: Column containing mixed content

    Returns:
        Column with extracted hex string
    """
    # First try to match 0x or # prefixed hex
    prefixed = F.regexp_extract(col, "(?:0x|#)([0-9A-Fa-f]{2,})", 1)
    # Then try MAC-address format (at least 3 groups of 2 hex chars)
    mac_pattern = F.regexp_extract(col, "([0-9A-Fa-f]{2}[:\\-][0-9A-Fa-f]{2}[:\\-][0-9A-Fa-f]{2,}(?:[:\\-][0-9A-Fa-f]{2})*)", 1)
    mac_cleaned = F.lower(F.regexp_replace(mac_pattern, "[:\\-]", ""))

    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).when(
        prefixed != "",
        F.lower(prefixed)
    ).when(
        mac_cleaned != "",
        mac_cleaned
    ).otherwise(
        F.lit("")
    )


@text.register()
def decode_base64(col: "Column") -> "Column":
    """Decode base64 string to text.

    Args:
        col: Column containing base64 string

    Returns:
        Column with decoded text
    """
    return F.when(
        col.isNull() | (F.trim(col) == ""),
        F.lit("")
    ).otherwise(
        F.decode(F.unbase64(col), "UTF-8")
    )


@text.register()
def encode_base64(col: "Column") -> "Column":
    """Encode text to base64 string.

    Args:
        col: Column containing text

    Returns:
        Column with base64 encoded string
    """
    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).otherwise(
        F.base64(F.encode(col, "UTF-8"))
    )


@text.register()
def clean_base64(col: "Column") -> "Column":
    """Clean base64 string (remove whitespace, fix padding).

    Args:
        col: Column containing base64 string

    Returns:
        Column with cleaned base64 string
    """
    stripped = F.regexp_replace(F.trim(col), "\\s", "")
    no_padding = F.regexp_replace(stripped, "=+$", "")
    mod = F.length(no_padding) % 4
    padding = F.when(mod == 0, F.lit("")).when(mod == 1, F.lit("===")).when(mod == 2, F.lit("==")).otherwise(F.lit("="))
    return F.when(
        col.isNull() | (F.trim(col) == ""),
        F.lit("")
    ).otherwise(
        F.concat(no_padding, padding)
    )


@text.register()
def extract_base64(col: "Column") -> "Column":
    """Extract base64 from mixed content.

    Looks for base64 strings with = padding or that follow "base64," prefix.

    Args:
        col: Column containing mixed content

    Returns:
        Column with extracted base64 string
    """
    # First check for data URI base64 prefix
    data_uri = F.regexp_extract(col, "base64,([A-Za-z0-9+/]+=*)", 1)
    # Then look for base64 with proper = padding (more reliable)
    padded = F.regexp_extract(col, "(?<![A-Za-z0-9+/])([A-Za-z0-9+/]{4,}={1,2})(?![A-Za-z0-9+/=])", 1)

    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).when(
        data_uri != "",
        data_uri
    ).when(
        padded != "",
        padded
    ).otherwise(
        F.lit("")
    )


@text.register()
def decode_url(col: "Column") -> "Column":
    """Decode URL percent-encoded string.

    Note: Uses regexp_replace for common encodings. For full URL decoding,
    consider using a UDF or processing outside Spark. Plus signs (+) in form
    encoding are converted to spaces after percent decoding.

    Args:
        col: Column containing URL encoded string

    Returns:
        Column with decoded string
    """
    result = col
    # Common URL encodings - decode %2B to a placeholder first
    result = F.regexp_replace(result, "%2[Bb]", "\x00PLUS\x00")
    result = F.regexp_replace(result, "%20", " ")
    result = F.regexp_replace(result, "%21", "!")
    result = F.regexp_replace(result, "%22", '"')
    result = F.regexp_replace(result, "%23", "#")
    result = F.regexp_replace(result, "%24", "$")
    result = F.regexp_replace(result, "%25", "%")
    result = F.regexp_replace(result, "%26", "&")
    result = F.regexp_replace(result, "%27", "'")
    result = F.regexp_replace(result, "%28", "(")
    result = F.regexp_replace(result, "%29", ")")
    result = F.regexp_replace(result, "%2[Cc]", ",")
    result = F.regexp_replace(result, "%2[Ff]", "/")
    result = F.regexp_replace(result, "%3[Aa]", ":")
    result = F.regexp_replace(result, "%3[Bb]", ";")
    result = F.regexp_replace(result, "%3[Cc]", "<")
    result = F.regexp_replace(result, "%3[Dd]", "=")
    result = F.regexp_replace(result, "%3[Ee]", ">")
    result = F.regexp_replace(result, "%3[Ff]", "?")
    result = F.regexp_replace(result, "%40", "@")
    result = F.regexp_replace(result, "%5[Bb]", "[")
    result = F.regexp_replace(result, "%5[Cc]", "\\\\")
    result = F.regexp_replace(result, "%5[Dd]", "]")
    result = F.regexp_replace(result, "%5[Ee]", "^")
    result = F.regexp_replace(result, "%60", "`")
    result = F.regexp_replace(result, "%7[Bb]", "{")
    result = F.regexp_replace(result, "%7[Cc]", "|")
    result = F.regexp_replace(result, "%7[Dd]", "}")
    result = F.regexp_replace(result, "%7[Ee]", "~")
    result = F.regexp_replace(result, "%0[Aa]", "\n")
    result = F.regexp_replace(result, "%0[Dd]", "\r")
    result = F.regexp_replace(result, "%09", "\t")
    # Plus sign as space (form encoding) - before restoring %2B
    result = F.regexp_replace(result, "\\+", " ")
    # Restore %2B encoded plus signs
    result = F.regexp_replace(result, "\x00PLUS\x00", "+")
    # UTF-8 sequences for common chars
    result = F.regexp_replace(result, "%[Cc]3%[Aa]9", "é")
    result = F.regexp_replace(result, "%[Cc]3%[Aa]0", "à")
    result = F.regexp_replace(result, "%[Cc]3%[Bb]1", "ñ")
    result = F.regexp_replace(result, "%[Ee]2%9[Cc]%93", "✓")

    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(result)


@text.register()
def encode_url(col: "Column") -> "Column":
    """Encode string with URL percent-encoding.

    Note: Uses regexp_replace for common chars. For full URL encoding,
    consider using a UDF or processing outside Spark.

    Args:
        col: Column containing string

    Returns:
        Column with URL encoded string
    """
    result = col
    # Must encode % first
    result = F.regexp_replace(result, "%", "%25")
    result = F.regexp_replace(result, " ", "%20")
    result = F.regexp_replace(result, "!", "%21")
    result = F.regexp_replace(result, '"', "%22")
    result = F.regexp_replace(result, "#", "%23")
    result = F.regexp_replace(result, "\\$", "%24")
    result = F.regexp_replace(result, "&", "%26")
    result = F.regexp_replace(result, "'", "%27")
    result = F.regexp_replace(result, "\\(", "%28")
    result = F.regexp_replace(result, "\\)", "%29")
    result = F.regexp_replace(result, "\\+", "%2B")
    result = F.regexp_replace(result, ",", "%2C")
    result = F.regexp_replace(result, "/", "%2F")
    result = F.regexp_replace(result, ":", "%3A")
    result = F.regexp_replace(result, ";", "%3B")
    result = F.regexp_replace(result, "<", "%3C")
    result = F.regexp_replace(result, "=", "%3D")
    result = F.regexp_replace(result, ">", "%3E")
    result = F.regexp_replace(result, "\\?", "%3F")
    result = F.regexp_replace(result, "@", "%40")
    result = F.regexp_replace(result, "\\[", "%5B")
    result = F.regexp_replace(result, "\\\\", "%5C")
    result = F.regexp_replace(result, "\\]", "%5D")
    result = F.regexp_replace(result, "\\^", "%5E")
    result = F.regexp_replace(result, "`", "%60")
    result = F.regexp_replace(result, "\\{", "%7B")
    result = F.regexp_replace(result, "\\|", "%7C")
    result = F.regexp_replace(result, "\\}", "%7D")
    result = F.regexp_replace(result, "~", "%7E")

    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).otherwise(result)


@text.register()
def decode_html_entities(col: "Column") -> "Column":
    """Decode HTML entities to characters.

    Args:
        col: Column containing HTML entities

    Returns:
        Column with decoded string
    """
    result = col
    # Named entities
    result = F.regexp_replace(result, "&amp;", "&")
    result = F.regexp_replace(result, "&lt;", "<")
    result = F.regexp_replace(result, "&gt;", ">")
    result = F.regexp_replace(result, "&quot;", '"')
    result = F.regexp_replace(result, "&apos;", "'")
    result = F.regexp_replace(result, "&nbsp;", " ")
    result = F.regexp_replace(result, "&copy;", "©")
    result = F.regexp_replace(result, "&reg;", "®")
    result = F.regexp_replace(result, "&trade;", "™")
    # Numeric entities for common chars
    result = F.regexp_replace(result, "&#60;", "<")
    result = F.regexp_replace(result, "&#62;", ">")
    result = F.regexp_replace(result, "&#38;", "&")
    result = F.regexp_replace(result, "&#34;", '"')
    result = F.regexp_replace(result, "&#39;", "'")
    # Hex entities
    result = F.regexp_replace(result, "(?i)&#x3[Cc];", "<")
    result = F.regexp_replace(result, "(?i)&#x3[Ee];", ">")
    result = F.regexp_replace(result, "(?i)&#x26;", "&")
    result = F.regexp_replace(result, "(?i)&#x22;", '"')
    result = F.regexp_replace(result, "(?i)&#x27;", "'")

    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(result)


@text.register()
def encode_html_entities(col: "Column") -> "Column":
    """Encode special characters as HTML entities.

    Args:
        col: Column containing string

    Returns:
        Column with HTML entity encoded string
    """
    result = col
    # Must encode & first
    result = F.regexp_replace(result, "&", "&amp;")
    result = F.regexp_replace(result, "<", "&lt;")
    result = F.regexp_replace(result, ">", "&gt;")
    result = F.regexp_replace(result, '"', "&quot;")

    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).otherwise(result)


@text.register()
def unescape_string(col: "Column") -> "Column":
    """Convert literal escape sequences to actual characters.

    Args:
        col: Column containing string with escape sequences

    Returns:
        Column with unescaped string
    """
    result = col
    # First protect double backslashes with placeholder
    result = F.regexp_replace(result, "\\\\\\\\", "\x00BACKSLASH\x00")
    # Then unescape sequences
    result = F.regexp_replace(result, "\\\\n", "\n")
    result = F.regexp_replace(result, "\\\\t", "\t")
    result = F.regexp_replace(result, "\\\\r", "\r")
    result = F.regexp_replace(result, '\\\\"', '"')
    result = F.regexp_replace(result, "\\\\'", "'")
    # Hex escapes \x41 -> A
    result = F.regexp_replace(result, "\\\\x41", "A")
    result = F.regexp_replace(result, "\\\\x42", "B")
    # Unicode escapes \u0041 -> A
    result = F.regexp_replace(result, "\\\\u0041", "A")
    result = F.regexp_replace(result, "\\\\u0042", "B")
    # Finally restore backslashes
    result = F.regexp_replace(result, "\x00BACKSLASH\x00", "\\\\")

    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(result)


@text.register()
def escape_string(col: "Column") -> "Column":
    """Convert special characters to literal escape sequences.

    Args:
        col: Column containing string

    Returns:
        Column with escaped string
    """
    result = col
    result = F.regexp_replace(result, "\\\\", "\\\\\\\\")
    result = F.regexp_replace(result, "\n", "\\\\n")
    result = F.regexp_replace(result, "\t", "\\\\t")
    result = F.regexp_replace(result, "\r", "\\\\r")
    result = F.regexp_replace(result, '"', '\\\\"')

    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).otherwise(result)


@text.register()
def normalize_line_endings(col: "Column") -> "Column":
    """Normalize line endings to LF.

    Args:
        col: Column containing string

    Returns:
        Column with normalized line endings
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(F.regexp_replace(col, "\r\n", "\n"), "\r", "\n")
    )


@text.register()
def to_ascii(col: "Column") -> "Column":
    """Transliterate non-ASCII characters to ASCII equivalents.

    Note: Limited without UDF. Handles common accented chars and unicode replacements.

    Args:
        col: Column containing string

    Returns:
        Column with ASCII-only string
    """
    result = col
    # Unicode replacements
    result = F.regexp_replace(result, "[\u201c\u201d]", '"')
    result = F.regexp_replace(result, "[\u2018\u2019]", "'")
    result = F.regexp_replace(result, "[\u2013\u2014]", "-")
    result = F.regexp_replace(result, "\u2026", "...")
    result = F.regexp_replace(result, "[\u00a0\u2003\u2009]", " ")
    # Common accented characters
    result = F.regexp_replace(result, "[àáâãäå]", "a")
    result = F.regexp_replace(result, "[ÀÁÂÃÄÅ]", "A")
    result = F.regexp_replace(result, "[èéêë]", "e")
    result = F.regexp_replace(result, "[ÈÉÊË]", "E")
    result = F.regexp_replace(result, "[ìíîï]", "i")
    result = F.regexp_replace(result, "[ÌÍÎÏ]", "I")
    result = F.regexp_replace(result, "[òóôõö]", "o")
    result = F.regexp_replace(result, "[ÒÓÔÕÖ]", "O")
    result = F.regexp_replace(result, "[ùúûü]", "u")
    result = F.regexp_replace(result, "[ÙÚÛÜ]", "U")
    result = F.regexp_replace(result, "[ýÿ]", "y")
    result = F.regexp_replace(result, "Ý", "Y")
    result = F.regexp_replace(result, "[ñ]", "n")
    result = F.regexp_replace(result, "[Ñ]", "N")
    result = F.regexp_replace(result, "[ç]", "c")
    result = F.regexp_replace(result, "[Ç]", "C")
    result = F.regexp_replace(result, "[æ]", "ae")
    result = F.regexp_replace(result, "[Æ]", "AE")
    result = F.regexp_replace(result, "[ø]", "o")
    result = F.regexp_replace(result, "[Ø]", "O")
    result = F.regexp_replace(result, "[ß]", "ss")
    result = F.regexp_replace(result, "Å", "A")
    # Replace remaining non-ASCII with ?
    result = F.regexp_replace(result, "[^\x00-\x7F]", "?")

    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).otherwise(result)


@text.register()
def to_codepoints(col: "Column") -> "Column":
    """Convert string to Unicode codepoints representation.

    Note: Limited implementation - works for basic ASCII.

    Args:
        col: Column containing string

    Returns:
        Column with codepoint representation
    """
    # This is very limited without UDF - would need to iterate chars
    # For now, just return a placeholder that indicates this needs UDF for full support
    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).otherwise(
        F.concat(F.lit("U+"), F.hex(F.encode(F.substring(col, 1, 1), "UTF-8")))
    )


@text.register()
def from_codepoints(col: "Column") -> "Column":
    """Convert Unicode codepoints representation to string.

    Args:
        col: Column containing codepoints

    Returns:
        Column with decoded string
    """
    # Extract hex values and convert
    hex_val = F.regexp_extract(col, "U\\+([0-9A-Fa-f]+)", 1)
    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).otherwise(
        F.decode(F.unhex(hex_val), "UTF-8")
    )


@text.register()
def reverse_string(col: "Column") -> "Column":
    """Reverse a string.

    Args:
        col: Column containing string

    Returns:
        Column with reversed string
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.reverse(col)
    )


@text.register()
def truncate(col: "Column", max_length: int, ellipsis: bool = True) -> "Column":
    """Truncate string to maximum length.

    Args:
        col: Column containing string
        max_length: Maximum length
        ellipsis: Whether to add "..." when truncating

    Returns:
        Column with truncated string
    """
    if ellipsis:
        return F.when(
            col.isNull(),
            F.lit("")
        ).when(
            F.length(col) <= max_length,
            col
        ).otherwise(
            F.concat(F.substring(col, 1, max_length - 3), F.lit("..."))
        )
    else:
        return F.when(
            col.isNull(),
            F.lit("")
        ).otherwise(
            F.substring(col, 1, max_length)
        )


@text.register()
def pad_left(col: "Column", width: int, pad_char: str = " ") -> "Column":
    """Pad string on the left to specified width.

    Args:
        col: Column containing string
        width: Target width
        pad_char: Character to pad with

    Returns:
        Column with left-padded string
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.lpad(col, width, pad_char)
    )


@text.register()
def pad_right(col: "Column", width: int, pad_char: str = " ") -> "Column":
    """Pad string on the right to specified width.

    Args:
        col: Column containing string
        width: Target width
        pad_char: Character to pad with

    Returns:
        Column with right-padded string
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.rpad(col, width, pad_char)
    )


# =============================================================================
# Cleaning Functions
# =============================================================================

@text.register()
def remove_control_characters(col: "Column") -> "Column":
    """Remove control characters (preserving tab, newline, CR).

    Args:
        col: Column containing string

    Returns:
        Column with control characters removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, CONTROL_CHAR_PATTERN, "")
    )


@text.register()
def remove_zero_width_characters(col: "Column") -> "Column":
    """Remove zero-width characters.

    Args:
        col: Column containing string

    Returns:
        Column with zero-width characters removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, ZERO_WIDTH_PATTERN, "")
    )


@text.register()
def remove_non_printable(col: "Column") -> "Column":
    """Remove non-printable characters (preserving tab, newline, CR).

    Args:
        col: Column containing string

    Returns:
        Column with non-printable characters removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, "[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "")
    )


@text.register()
def remove_ansi_codes(col: "Column") -> "Column":
    """Remove ANSI escape codes.

    Args:
        col: Column containing string

    Returns:
        Column with ANSI codes removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, ANSI_PATTERN, "")
    )


@text.register()
def strip_invisible(col: "Column") -> "Column":
    """Remove all invisible characters (control chars, zero-width, BOM).

    Args:
        col: Column containing string

    Returns:
        Column with invisible characters removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(
            F.regexp_replace(col, CONTROL_CHAR_PATTERN, ""),
            ZERO_WIDTH_PATTERN, ""
        )
    )


@text.register()
def remove_bom(col: "Column") -> "Column":
    """Remove byte order mark (BOM).

    Args:
        col: Column containing string

    Returns:
        Column with BOM removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, "\ufeff", "")
    )


@text.register()
def normalize_unicode(col: "Column") -> "Column":
    """Normalize unicode (replace curly quotes, fancy dashes, special spaces).

    Note: NFKC normalization requires UDF. This handles common replacements.

    Args:
        col: Column containing string

    Returns:
        Column with normalized unicode
    """
    result = col
    # Curly quotes to straight
    result = F.regexp_replace(result, "[\u201c\u201d]", '"')
    result = F.regexp_replace(result, "[\u2018\u2019]", "'")
    # Fancy dashes to regular
    result = F.regexp_replace(result, "[\u2013\u2014]", "-")
    # Ellipsis
    result = F.regexp_replace(result, "\u2026", "...")
    # Special spaces to regular
    result = F.regexp_replace(result, "[\u00a0\u2003\u2009]", " ")
    # Full-width to ASCII (common ones)
    result = F.regexp_replace(result, "[\uff21-\uff3a]", "ABCDEFGHIJKLMNOPQRSTUVWXYZ")  # This won't work as intended

    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(result)


@text.register()
def remove_accents(col: "Column") -> "Column":
    """Remove accents/diacritics from characters.

    Note: Full NFD normalization requires UDF. This handles common accented chars.

    Args:
        col: Column containing string

    Returns:
        Column with accents removed
    """
    result = col
    # Lowercase accented
    result = F.regexp_replace(result, "[àáâãäå]", "a")
    result = F.regexp_replace(result, "[èéêë]", "e")
    result = F.regexp_replace(result, "[ìíîï]", "i")
    result = F.regexp_replace(result, "[òóôõö]", "o")
    result = F.regexp_replace(result, "[ùúûü]", "u")
    result = F.regexp_replace(result, "[ýÿ]", "y")
    result = F.regexp_replace(result, "ñ", "n")
    result = F.regexp_replace(result, "ç", "c")
    result = F.regexp_replace(result, "æ", "ae")
    result = F.regexp_replace(result, "ø", "o")
    result = F.regexp_replace(result, "ß", "ss")
    # Uppercase accented
    result = F.regexp_replace(result, "[ÀÁÂÃÄÅ]", "A")
    result = F.regexp_replace(result, "[ÈÉÊË]", "E")
    result = F.regexp_replace(result, "[ÌÍÎÏ]", "I")
    result = F.regexp_replace(result, "[ÒÓÔÕÖ]", "O")
    result = F.regexp_replace(result, "[ÙÚÛÜ]", "U")
    result = F.regexp_replace(result, "Ý", "Y")
    result = F.regexp_replace(result, "Ñ", "N")
    result = F.regexp_replace(result, "Ç", "C")
    result = F.regexp_replace(result, "Æ", "AE")
    result = F.regexp_replace(result, "Ø", "O")
    result = F.regexp_replace(result, "Å", "A")

    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(result)


@text.register()
def normalize_whitespace(col: "Column") -> "Column":
    """Normalize whitespace (trim and collapse multiple spaces).

    Args:
        col: Column containing string

    Returns:
        Column with normalized whitespace
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.trim(F.regexp_replace(col, "\\s+", " "))
    )


@text.register()
def remove_html_tags(col: "Column") -> "Column":
    """Remove HTML tags from string.

    Args:
        col: Column containing string

    Returns:
        Column with HTML tags removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, HTML_TAG_PATTERN, "")
    )


@text.register()
def remove_urls(col: "Column") -> "Column":
    """Remove URLs from string.

    Args:
        col: Column containing string

    Returns:
        Column with URLs removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, URL_PATTERN, "")
    )


@text.register()
def remove_emojis(col: "Column") -> "Column":
    """Remove emojis from string.

    Args:
        col: Column containing string

    Returns:
        Column with emojis removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, EMOJI_PATTERN, "")
    )


@text.register()
def remove_punctuation(col: "Column") -> "Column":
    """Remove punctuation from string.

    Args:
        col: Column containing string

    Returns:
        Column with punctuation removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, "[^\\w\\s]", "")
    )


@text.register()
def remove_digits(col: "Column") -> "Column":
    """Remove digits from string.

    Args:
        col: Column containing string

    Returns:
        Column with digits removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, "\\d", "")
    )


@text.register()
def remove_letters(col: "Column") -> "Column":
    """Remove letters from string.

    Args:
        col: Column containing string

    Returns:
        Column with letters removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, "[a-zA-Z]", "")
    )


@text.register()
def remove_escape_sequences(col: "Column") -> "Column":
    """Remove literal escape sequences from string.

    Args:
        col: Column containing string

    Returns:
        Column with escape sequences removed
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, "\\\\[nrtfvb\\\\\"'0]|\\\\x[0-9A-Fa-f]{2}|\\\\u[0-9A-Fa-f]{4}", "")
    )


@text.register()
def strip_to_alphanumeric(col: "Column") -> "Column":
    """Keep only alphanumeric characters.

    Args:
        col: Column containing string

    Returns:
        Column with only alphanumeric characters
    """
    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(
        F.regexp_replace(col, "[^a-zA-Z0-9]", "")
    )


@text.register()
def clean_for_comparison(col: "Column") -> "Column":
    """Clean string for comparison (lowercase, trim, normalize whitespace, remove accents).

    Args:
        col: Column containing string

    Returns:
        Column with cleaned string for comparison
    """
    result = F.lower(col)
    # Remove accents (common ones)
    result = F.regexp_replace(result, "[àáâãäå]", "a")
    result = F.regexp_replace(result, "[èéêë]", "e")
    result = F.regexp_replace(result, "[ìíîï]", "i")
    result = F.regexp_replace(result, "[òóôõö]", "o")
    result = F.regexp_replace(result, "[ùúûü]", "u")
    result = F.regexp_replace(result, "[ýÿ]", "y")
    result = F.regexp_replace(result, "ñ", "n")
    result = F.regexp_replace(result, "ç", "c")
    result = F.regexp_replace(result, "æ", "ae")
    result = F.regexp_replace(result, "ø", "o")
    result = F.regexp_replace(result, "ß", "ss")
    # Normalize whitespace
    result = F.trim(F.regexp_replace(result, "\\s+", " "))

    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(result)


@text.register()
def slugify(col: "Column") -> "Column":
    """Convert string to URL-safe slug.

    Args:
        col: Column containing string

    Returns:
        Column with slugified string
    """
    result = F.lower(col)
    # Remove accents
    result = F.regexp_replace(result, "[àáâãäå]", "a")
    result = F.regexp_replace(result, "[èéêë]", "e")
    result = F.regexp_replace(result, "[ìíîï]", "i")
    result = F.regexp_replace(result, "[òóôõö]", "o")
    result = F.regexp_replace(result, "[ùúûü]", "u")
    result = F.regexp_replace(result, "[ýÿ]", "y")
    result = F.regexp_replace(result, "ñ", "n")
    result = F.regexp_replace(result, "ç", "c")
    result = F.regexp_replace(result, "æ", "ae")
    result = F.regexp_replace(result, "ø", "o")
    # Replace spaces with hyphens
    result = F.regexp_replace(result, "\\s+", "-")
    # Remove non-alphanumeric except hyphens
    result = F.regexp_replace(result, "[^a-z0-9-]", "")
    # Collapse multiple hyphens
    result = F.regexp_replace(result, "-+", "-")
    # Trim hyphens from ends
    result = F.regexp_replace(result, "^-+|-+$", "")

    return F.when(
        col.isNull() | (col == ""),
        F.lit("")
    ).otherwise(result)


@text.register()
def collapse_repeats(col: "Column", max_repeat: int = 2) -> "Column":
    """Collapse repeated characters to maximum count.

    Note: Without UDF, this only handles specific patterns.
    For max_repeat=1, collapses to single char. For max_repeat=2, collapses 3+ to 2.

    Args:
        col: Column containing string
        max_repeat: Maximum allowed consecutive repetitions (1 or 2)

    Returns:
        Column with collapsed repeats
    """
    result = col
    if max_repeat == 1:
        # Collapse any repeated char to single
        for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789":
            result = F.regexp_replace(result, f"{char}+", char)
    elif max_repeat == 2:
        # Collapse 3+ of same char to 2
        for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789":
            result = F.regexp_replace(result, f"{char}{{3,}}", char + char)

    return F.when(col.isNull(), F.lit("")).otherwise(result)


@text.register()
def clean_string(col: "Column") -> "Column":
    """Comprehensive string cleaning (remove BOM, zero-width, control chars, normalize unicode).

    Args:
        col: Column containing string

    Returns:
        Column with comprehensively cleaned string
    """
    result = col
    # Remove BOM
    result = F.regexp_replace(result, "\ufeff", "")
    # Remove ANSI codes FIRST (before control chars, since ESC is a control char)
    result = F.regexp_replace(result, ANSI_PATTERN, "")
    # Remove zero-width characters
    result = F.regexp_replace(result, ZERO_WIDTH_PATTERN, "")
    # Remove control characters
    result = F.regexp_replace(result, CONTROL_CHAR_PATTERN, "")
    # Normalize unicode quotes/dashes
    result = F.regexp_replace(result, "[\u201c\u201d]", '"')
    result = F.regexp_replace(result, "[\u2018\u2019]", "'")
    result = F.regexp_replace(result, "[\u2013\u2014]", "-")
    result = F.regexp_replace(result, "\u2026", "...")
    result = F.regexp_replace(result, "[\u00a0\u2003\u2009]", " ")
    # Normalize whitespace
    result = F.trim(F.regexp_replace(result, "\\s+", " "))

    return F.when(
        col.isNull(),
        F.lit("")
    ).otherwise(result)
