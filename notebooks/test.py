# %%

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("DatacomposeTest")
    .getOrCreate()
)

# Test it
df = spark.range(10)
df.show()

# %%

from pyspark.sql import functions as F

from transformers.pyspark.phone_numbers import phone_numbers


# Example 1: Basic compose with if-then for phone validation and formatting
@phone_numbers.compose(debug=True)
def process_phone_number():
    """Process phone numbers with conditional logic."""
    if phone_numbers.is_valid_nanp():
        # Valid NANP number - clean and format
        phone_numbers.convert_letters_to_numbers()
        phone_numbers.format_nanp_paren()
    elif phone_numbers.is_valid_international():
        # Valid international - format differently
        phone_numbers.format_international()
    else:
        # Invalid - return masked
        phone_numbers.mask_phone()


# Example 2: Toll-free number handling
@phone_numbers.compose(debug=True)
def handle_business_phone():
    """Handle business phone numbers with special logic for toll-free."""
    if phone_numbers.is_toll_free():
        # Toll-free numbers get E.164 format
        phone_numbers.convert_letters_to_numbers()
        phone_numbers.format_e164()
    elif phone_numbers.is_premium_rate():
        # Premium rate numbers get masked
        phone_numbers.mask_phone()
    else:
        # Standard numbers get standard format
        phone_numbers.standardize_phone()


# Example 3: Data quality check with cleaning
@phone_numbers.compose(debug=True)
def validate_and_clean():
    """Validate and clean phone numbers."""
    if phone_numbers.is_valid_phone():
        # Valid phone - clean and standardize
        phone_numbers.convert_letters_to_numbers()
        phone_numbers.standardize_phone()
    else:
        # Invalid - clean what we can
        phone_numbers.clean_phone()


# Test with sample data
data = [
    ("(555) 123-4567",),
    ("1-800-FLOWERS",),
    ("invalid-phone",),
    ("+44 20 7946 0958",),
    ("900-555-1234",),
]
test_df = spark.createDataFrame(data, ["phone"])
test_df.show()


# %%
# Apply the composed functions
result_df = test_df.select(
    F.col("phone"),
    process_phone_number(F.col("phone")).alias("processed"),
    handle_business_phone(F.col("phone")).alias("business_format"),
    phone_numbers.get_phone_numbers_type(F.col("phone")).alias("type"),
)

# %%
result_df.show(truncate=False)

# %%
