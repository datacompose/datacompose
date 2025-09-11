"""
Test real-world use cases for conditional pipelines.
"""

import pytest
from pyspark.sql import functions as f

from datacompose.operators.primitives import PrimitiveRegistry


@pytest.fixture
def diverse_test_data(spark):
    """Create diverse test dataset for conditional testing"""
    data = [
        ("A", 10, "small", 1, "short"),           # category A - short text
        ("A", 20, "medium", 2, "simple"),         # category A  
        ("A", 30, "large", 3, "s_text"),          # category A
        ("A", 40, "xlarge", 4, "special!@#text"), # category A
        ("B", 50, "small", 5, None),              # category B - NULL text for null test
        ("B", 60, "medium", 6, "medium_text"),    # category B - ends with 'text'
        ("B", 70, "large", 7, "  SPACES  "),        # category B - needs cleaning (spaces)
        ("C", 80, "small", 8, "UPPERCASE"),       # category C - all caps
        ("C", 90, "medium", 9, "simple_text"),    # category C
        ("D", 100, "large", 10, ""),              # empty text
        ("E", 110, "medium", 11, "12345"),        # numeric text
        ("F", 120, "small", 12, "mixed123ABC"),   # mixed text
        ("G", 130, "large", 13, "ALPHA"),         # alpha text
        (None, 140, "unknown", 14, None)          # NULL category for testing
    ]
    return spark.createDataFrame(data, ["category", "value", "size", "id", "text"])


@pytest.mark.unit
class TestRealWorldScenarios:
    """Test real-world use cases for conditional pipelines"""

    def test_data_quality_pipeline(self, diverse_test_data):
        """Test data quality check -> clean or reject pipeline"""
        ns = PrimitiveRegistry("quality")

        @ns.register()
        def is_valid_text(col):
            # Not null, not empty, reasonable length
            return col.isNotNull() & (f.length(col) > 0) & (f.length(col) <= 100)

        @ns.register()
        def needs_cleaning(col):
            # Has leading/trailing spaces or mixed case
            return (col != f.trim(col)) | (col != f.lower(col))

        @ns.register()
        def clean(col):
            return f.trim(f.lower(col))

        @ns.register()
        def mark_invalid(col):
            return f.lit("INVALID_DATA")

        @ns.register()
        def mark_clean(col):
            return f.concat(f.lit("CLEAN:"), col)

        @ns.compose(ns=ns, debug=True)
        def quality_pipeline():
            if ns.is_valid_text():
                if ns.needs_cleaning():
                    ns.clean()
                    ns.mark_clean()
                else:
                    ns.mark_clean()
            else:
                ns.mark_invalid()

        result = diverse_test_data.withColumn(
            "quality_checked", quality_pipeline(f.col("text"))
        )

        collected = result.collect()

        # Check invalid data handling
        null_row = [r for r in collected if r["id"] == 5][0]
        assert null_row["quality_checked"] == "INVALID_DATA"

        empty_row = [r for r in collected if r["text"] == ""][0]
        assert empty_row["quality_checked"] == "INVALID_DATA"

        # Check cleaning
        spaces_row = [r for r in collected if r["id"] == 7][0]
        assert spaces_row["quality_checked"] == "CLEAN:spaces"

    def test_routing_pipeline(self, diverse_test_data):
        """Test routing data to different transforms based on type"""
        ns = PrimitiveRegistry("router")

        @ns.register()
        def is_numeric_id(col):
            return col.rlike("^[0-9]+$")

        @ns.register()
        def is_alpha_id(col):
            return col.rlike("^[a-zA-Z]+$")

        @ns.register()
        def is_mixed_id(col):
            return col.rlike("^[a-zA-Z0-9]+$")

        @ns.register()
        def process_numeric(col):
            # Pad with zeros
            return f.lpad(col, 10, "0")

        @ns.register()
        def process_alpha(col):
            # Convert to uppercase code
            return f.upper(f.concat(f.lit("ID_"), col))

        @ns.register()
        def process_mixed(col):
            # Hash the value
            return f.md5(col)

        @ns.register()
        def process_special(col):
            # Base64 encode
            return f.base64(col)

        @ns.compose(ns=ns, debug=True)
        def route_by_type():
            if ns.is_numeric_id():
                ns.process_numeric()
            else:
                if ns.is_alpha_id():
                    ns.process_alpha()
                else:
                    if ns.is_mixed_id():
                        ns.process_mixed()
                    else:
                        ns.process_special()

        result = diverse_test_data.withColumn("routed", route_by_type(f.col("text")))

        collected = result.collect()

        # Check routing
        numeric_row = [r for r in collected if r["text"] == "12345"][0]
        assert numeric_row["routed"] == "0000012345"

        mixed_row = [r for r in collected if r["text"] == "mixed123ABC"][0]
        assert len(mixed_row["routed"]) == 32  # MD5 hash length

    def test_validation_pipeline(self, spark):
        """Test validate -> process or quarantine pipeline"""
        ns = PrimitiveRegistry("validate")

        @ns.register()
        def is_valid_email(col):
            return col.rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

        @ns.register()
        def is_blacklisted(col):
            blacklist_domains = ["spam.com", "fake.org"]
            conditions = [col.endswith(domain) for domain in blacklist_domains]
            return conditions[0] | conditions[1]  # OR all conditions

        @ns.register()
        def normalize_email(col):
            return f.lower(f.trim(col))

        @ns.register()
        def quarantine(col):
            return f.concat(f.lit("QUARANTINE:"), col)

        @ns.register()
        def approve(col):
            return f.concat(f.lit("APPROVED:"), col)

        @ns.compose(ns=ns, debug=True)  # pyright: ignore
        def email_validator():
            if ns.is_valid_email():
                ns.normalize_email()
                if ns.is_blacklisted():
                    ns.quarantine()
                else:
                    ns.approve()
            else:
                ns.quarantine()

        data = [
            ("user@example.com",),
            ("invalid-email",),
            ("spammer@spam.com",),
            ("User@EXAMPLE.COM",),
            ("fake@fake.org",),
        ]
        df = spark.createDataFrame(data, ["email"])

        result = df.withColumn("validated", email_validator(f.col("email")))
        collected = result.collect()

        # Check validation results
        assert collected[0]["validated"] == "APPROVED:user@example.com"
        assert collected[1]["validated"] == "QUARANTINE:invalid-email"
        assert collected[2]["validated"] == "QUARANTINE:spammer@spam.com"
        assert collected[3]["validated"] == "APPROVED:user@example.com"
        assert collected[4]["validated"] == "QUARANTINE:fake@fake.org"

    def test_phone_number_processing_pipeline(self, spark):
        """Test phone number validation and formatting pipeline"""
        ns = PrimitiveRegistry("phone")

        @ns.register()
        def is_valid_nanp(col):
            # North American Numbering Plan validation
            # Allow 555 for testing purposes (normally reserved for fictional numbers)
            cleaned = f.regexp_replace(col, r"[^0-9]", "")
            return cleaned.rlike(r"^[2-9]\d{9}$") | cleaned.rlike(
                r"^1[2-9]\d{9}$"
            )

        @ns.register()
        def is_toll_free(col):
            cleaned = f.regexp_replace(col, r"[^0-9]", "")
            return cleaned.rlike(r"^1?(800|888|877|866|855|844|833)\d{7}$")

        @ns.register()
        def has_letters(col):
            return col.rlike(r"[A-Za-z]")

        @ns.register()
        def convert_letters_to_numbers(col):
            # Convert phone letters to numbers (2=ABC, 3=DEF, etc.)
            result = col
            letter_map = {
                "A": "2",
                "B": "2",
                "C": "2",
                "D": "3",
                "E": "3",
                "F": "3",
                "G": "4",
                "H": "4",
                "I": "4",
                "J": "5",
                "K": "5",
                "L": "5",
                "M": "6",
                "N": "6",
                "O": "6",
                "P": "7",
                "Q": "7",
                "R": "7",
                "S": "7",
                "T": "8",
                "U": "8",
                "V": "8",
                "W": "9",
                "X": "9",
                "Y": "9",
                "Z": "9",
            }
            for letter, number in letter_map.items():
                result = f.regexp_replace(result, letter, number)
                result = f.regexp_replace(result, letter.lower(), number)
            return result

        @ns.register()
        def format_nanp_paren(col):
            # Format as (XXX) XXX-XXXX
            cleaned = f.regexp_replace(col, r"[^0-9]", "")
            # Remove leading 1 if present
            cleaned = f.when(
                cleaned.startswith("1") & (f.length(cleaned) == 11),
                f.substring(cleaned, 2, 10),
            ).otherwise(cleaned)
            return f.concat(
                f.lit("("),
                f.substring(cleaned, 1, 3),
                f.lit(") "),
                f.substring(cleaned, 4, 3),
                f.lit("-"),
                f.substring(cleaned, 7, 4),
            )

        @ns.register()
        def format_e164(col):
            # Format as E.164: +1XXXXXXXXXX
            cleaned = f.regexp_replace(col, r"[^0-9]", "")
            # Add +1 if not present
            return f.when(
                cleaned.startswith("1"), f.concat(f.lit("+"), cleaned)
            ).otherwise(f.concat(f.lit("+1"), cleaned))

        @ns.register()
        def mask_phone(col):
            return f.lit("XXX-XXX-XXXX")

        @ns.compose(ns=ns, debug=True)
        def process_phone_number():
            """Process phone numbers with conditional logic."""
            if ns.has_letters():
                ns.convert_letters_to_numbers()

            if ns.is_toll_free():
                # Toll-free numbers get E.164 format
                ns.format_e164()
            else:
                if ns.is_valid_nanp():
                    # Valid NANP number - format with parentheses
                    ns.format_nanp_paren()
                else:
                    # Invalid - return masked
                    ns.mask_phone()

        # Test with sample data
        data = [
            ("(555) 123-4567",),
            ("1-800-FLOWERS",),
            ("invalid-phone",),
            ("1-888-555-1234",),
            ("212-555-1234",),
            ("1800GOFEDEX",),
        ]
        df = spark.createDataFrame(data, ["phone"])

        result = df.withColumn("formatted", process_phone_number(f.col("phone")))
        collected = result.collect()
        
        # Verify each phone number is processed correctly
        # The pipeline should:
        # 1. Convert letters to numbers (1-800-FLOWERS -> digits)
        # 2. Detect toll-free numbers and format as E.164
        # 3. Format valid NANP numbers with parentheses
        # 4. Mask invalid phone numbers
        
        assert collected[0]["formatted"] == "(555) 123-4567"  # Valid NANP (555 allowed for testing)
        assert collected[1]["formatted"] == "+18003569377"  # 1-800-FLOWERS -> toll-free E.164
        assert collected[2]["formatted"] == "XXX-XXX-XXXX"  # Invalid format -> masked
        assert collected[3]["formatted"] == "+18885551234"  # Toll-free -> E.164
        assert collected[4]["formatted"] == "(212) 555-1234"  # Valid NANP -> parentheses format
        assert collected[5]["formatted"] == "+18004633339"  # 1800GOFEDEX -> toll-free E.164
