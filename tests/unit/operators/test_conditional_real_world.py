"""
Test real-world use cases for conditional pipelines.
"""

import pytest
from pyspark.sql import functions as f

from datacompose.operators.primitives import PrimitiveRegistry


@pytest.mark.unit
class TestRealWorldScenarios:
    """Test real-world use cases for conditional pipelines"""

    def test_data_quality_pipeline(self, diverse_test_data):
        """Test data quality check -> clean or reject pipeline"""
        ns = PrimitiveRegistry("quality")

        @ns.register(is_conditional=True)
        def is_valid_text(col):
            # Not null, not empty, reasonable length
            return col.isNotNull() & (f.length(col) > 0) & (f.length(col) <= 100)

        @ns.register(is_conditional=True)
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

        @ns.register(is_conditional=True)
        def is_numeric_id(col):
            return col.rlike("^[0-9]+$")

        @ns.register(is_conditional=True)
        def is_alpha_id(col):
            return col.rlike("^[a-zA-Z]+$")

        @ns.register(is_conditional=True)
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

        @ns.register(is_conditional=True)
        def is_valid_email(col):
            return col.rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

        @ns.register(is_conditional=True)
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
