# Custom Transformations

This guide shows how to create your own custom transformation modules that work just like the built-in `clean_emails`, `clean_addresses`, and `clean_phone_numbers` transformers.

## Creating a Custom Transformer: Social Media Data

Let's create a complete `clean_social_media` transformer for handling social media usernames, URLs, and hashtags.

### Step 1: Define Your Transformer Structure

```python
# File: datacompose/transformers/text/clean_social_media/__init__.py

from datacompose.transformers.discovery import register_transformer

# Register your transformer so it can be discovered
@register_transformer
def clean_social_media():
    return {
        "name": "clean_social_media",
        "description": "Social media data cleaning and validation",
        "version": "1.0.0",
        "primitives": "social_media_primitives.py"
    }
```

### Step 2: Create the Primitives Module

```python
# File: datacompose/transformers/text/clean_social_media/pyspark/pyspark_primitives.py

from datacompose.operators.primitives import PrimitiveRegistry
from pyspark.sql import functions as F
import re

# Create the registry
social = PrimitiveRegistry("social")

# ============================================
# VALIDATION PRIMITIVES
# ============================================

@social.register()
def is_valid_twitter_handle(col):
    """Check if valid Twitter/X handle"""
    # Must start with @, 1-15 chars, alphanumeric and underscore only
    return col.rlike(r'^@[A-Za-z0-9_]{1,15}$')

@social.register()
def is_valid_instagram_handle(col):
    """Check if valid Instagram handle"""
    # Can include periods, 1-30 chars
    return col.rlike(r'^@?[A-Za-z0-9_.]{1,30}$')

@social.register()
def is_valid_linkedin_url(col):
    """Check if valid LinkedIn profile URL"""
    return col.rlike(r'^https?://(www\.)?linkedin\.com/in/[\w-]+/?$')

@social.register()
def is_valid_hashtag(col):
    """Check if valid hashtag format"""
    # Must start with #, no spaces, alphanumeric
    return col.rlike(r'^#[A-Za-z0-9_]+$')

@social.register()
def is_valid_youtube_url(col):
    """Check if valid YouTube URL"""
    patterns = [
        r'youtube\.com/watch\?v=[\w-]+',
        r'youtu\.be/[\w-]+',
        r'youtube\.com/channel/[\w-]+',
        r'youtube\.com/@[\w-]+'
    ]
    is_valid = F.lit(False)
    for pattern in patterns:
        is_valid = is_valid | col.rlike(pattern)
    return is_valid

@social.register()
def is_verified_handle(col):
    """Check if handle appears to be verified (contains checkmark)"""
    return col.contains("✓") | col.contains("✔") | col.contains("☑")

# ============================================
# EXTRACTION PRIMITIVES
# ============================================

@social.register()
def extract_platform(col):
    """Extract social media platform from URL or handle"""
    return F.when(col.contains("twitter.com") | col.contains("x.com"), "Twitter")\
        .when(col.contains("instagram.com"), "Instagram")\
        .when(col.contains("linkedin.com"), "LinkedIn")\
        .when(col.contains("facebook.com") | col.contains("fb.com"), "Facebook")\
        .when(col.contains("tiktok.com"), "TikTok")\
        .when(col.contains("youtube.com") | col.contains("youtu.be"), "YouTube")\
        .when(col.contains("reddit.com"), "Reddit")\
        .when(col.contains("pinterest.com"), "Pinterest")\
        .otherwise("Unknown")

@social.register()
def extract_username(col):
    """Extract username from social media URL or handle"""
    # Remove @ symbol and domain parts
    username = F.regexp_extract(col, r'@?([A-Za-z0-9_.]+)', 1)
    
    # For URLs, extract from path
    url_username = F.regexp_extract(col, r'/(?:in/|@|user/|u/)([A-Za-z0-9_.-]+)', 1)
    
    return F.coalesce(url_username, username)

@social.register()
def extract_hashtags(col):
    """Extract all hashtags from text"""
    # Returns array of hashtags
    return F.split(
        F.regexp_replace(col, r'[^#\w\s]', ' '),
        r'\s+'
    ).filter(lambda x: x.startswith('#'))

@social.register()
def extract_mentions(col):
    """Extract all @mentions from text"""
    # Returns array of mentions
    return F.split(
        F.regexp_replace(col, r'[^@\w\s]', ' '),
        r'\s+'
    ).filter(lambda x: x.startswith('@'))

@social.register()
def extract_video_id(col):
    """Extract video ID from YouTube URL"""
    patterns = [
        (r'youtube\.com/watch\?v=([\w-]+)', 1),
        (r'youtu\.be/([\w-]+)', 1),
        (r'youtube\.com/embed/([\w-]+)', 1)
    ]
    
    for pattern, group in patterns:
        vid_id = F.regexp_extract(col, pattern, group)
        if vid_id.isNotNull():
            return vid_id
    return None

@social.register()
def extract_profile_id(col):
    """Extract profile ID from various social platforms"""
    # Facebook numeric ID
    fb_id = F.regexp_extract(col, r'facebook\.com/.*?([0-9]{10,})', 1)
    
    # LinkedIn profile ID
    li_id = F.regexp_extract(col, r'linkedin\.com/in/([\w-]+)', 1)
    
    # Twitter/X ID from URL
    tw_id = F.regexp_extract(col, r'(?:twitter|x)\.com/([A-Za-z0-9_]+)', 1)
    
    return F.coalesce(fb_id, li_id, tw_id)

# ============================================
# STANDARDIZATION PRIMITIVES
# ============================================

@social.register()
def standardize_handle(col):
    """Standardize social media handles"""
    # Lowercase, ensure @ prefix
    handle = F.lower(F.trim(col))
    return F.when(
        ~handle.startswith("@"),
        F.concat(F.lit("@"), handle)
    ).otherwise(handle)

@social.register()
def standardize_hashtag(col):
    """Standardize hashtags"""
    # Lowercase, ensure # prefix, remove spaces
    tag = F.lower(F.regexp_replace(col, r'\s+', ''))
    return F.when(
        ~tag.startswith("#"),
        F.concat(F.lit("#"), tag)
    ).otherwise(tag)

@social.register()
def normalize_platform_url(col, platform):
    """Normalize URL to standard platform format"""
    username = extract_username(col)
    
    platform_templates = {
        "Twitter": "https://twitter.com/{}",
        "Instagram": "https://instagram.com/{}",
        "LinkedIn": "https://linkedin.com/in/{}",
        "Facebook": "https://facebook.com/{}",
        "TikTok": "https://tiktok.com/@{}",
        "YouTube": "https://youtube.com/@{}"
    }
    
    template = platform_templates.get(platform, "https://example.com/{}")
    return F.format_string(template, username)

@social.register()
def clean_bio_text(col):
    """Clean social media bio text"""
    # Remove excessive emojis, clean whitespace
    cleaned = F.regexp_replace(col, r'[\U0001F600-\U0001F6FF]{3,}', '')  # Limit emojis
    cleaned = F.regexp_replace(cleaned, r'\s+', ' ')  # Normalize whitespace
    cleaned = F.trim(cleaned)
    return cleaned

# ============================================
# FILTERING PRIMITIVES
# ============================================

@social.register()
def filter_active_accounts(col, min_followers=10):
    """Filter for likely active accounts"""
    # Simple heuristic - would need actual follower data
    return F.when(
        is_valid_twitter_handle(col) | 
        is_valid_instagram_handle(col) |
        is_valid_linkedin_url(col),
        col
    ).otherwise(None)

@social.register()
def filter_business_accounts(col):
    """Filter for business/brand accounts"""
    business_indicators = [
        "official", "brand", "company", "corp", 
        "inc", "llc", "ltd", "business"
    ]
    
    is_business = F.lit(False)
    lower_col = F.lower(col)
    for indicator in business_indicators:
        is_business = is_business | lower_col.contains(indicator)
    
    return F.when(is_business, col).otherwise(None)

# ============================================
# ANALYSIS PRIMITIVES
# ============================================

@social.register()
def calculate_engagement_potential(col):
    """Estimate engagement potential based on platform"""
    platform = extract_platform(col)
    
    # Simplified scoring based on platform
    return F.when(platform == "TikTok", 0.9)\
        .when(platform == "Instagram", 0.8)\
        .when(platform == "Twitter", 0.7)\
        .when(platform == "YouTube", 0.7)\
        .when(platform == "LinkedIn", 0.6)\
        .when(platform == "Facebook", 0.5)\
        .otherwise(0.3)

@social.register()
def categorize_content_type(col):
    """Categorize based on hashtags and mentions"""
    hashtags = extract_hashtags(col)
    
    # Check for common content categories
    return F.when(F.array_contains(hashtags, "#news"), "news")\
        .when(F.array_contains(hashtags, "#tech"), "technology")\
        .when(F.array_contains(hashtags, "#fashion"), "fashion")\
        .when(F.array_contains(hashtags, "#food"), "food")\
        .when(F.array_contains(hashtags, "#travel"), "travel")\
        .when(F.array_contains(hashtags, "#fitness"), "fitness")\
        .otherwise("general")

# ============================================
# COMPOSED PIPELINES
# ============================================

@social.compose(social=social)
def process_social_handle():
    """Complete social media handle processing"""
    # Standardize first
    social.standardize_handle()
    
    # Validate based on platform
    platform = social.extract_platform()
    
    if platform == "Twitter":
        if social.is_valid_twitter_handle():
            return social.standardize_handle()
    elif platform == "Instagram":
        if social.is_valid_instagram_handle():
            return social.standardize_handle()
    
    return None

@social.compose(social=social)
def extract_social_metadata():
    """Extract all metadata from social content"""
    return F.struct(
        social.extract_platform().alias("platform"),
        social.extract_username().alias("username"),
        social.extract_hashtags().alias("hashtags"),
        social.extract_mentions().alias("mentions"),
        social.calculate_engagement_potential().alias("engagement_score"),
        social.categorize_content_type().alias("content_type")
    )

@social.compose(social=social)
def social_account_quality_check():
    """Check quality of social media account"""
    # Validate format
    is_valid = (
        social.is_valid_twitter_handle() |
        social.is_valid_instagram_handle() |
        social.is_valid_linkedin_url()
    )
    
    if not is_valid:
        return F.lit(0.0)
    
    # Check if verified
    if social.is_verified_handle():
        return F.lit(1.0)
    
    # Check if business
    if social.filter_business_accounts():
        return F.lit(0.8)
    
    # Active account
    if social.filter_active_accounts():
        return F.lit(0.6)
    
    return F.lit(0.3)

# Export the registry
__all__ = ['social']
```

### Step 3: Create a Generator for Your Transformer

```python
# File: datacompose/generators/pyspark/social_generator.py

from datacompose.generators.base import BaseGenerator

class SocialMediaGenerator(BaseGenerator):
    """Generator for social media transformation primitives"""
    
    def __init__(self):
        super().__init__()
        self.name = "social_media"
        self.version = "1.0.0"
    
    def generate(self, output_path):
        """Generate the social media primitives module"""
        # Copy primitives file to output
        source = "datacompose/transformers/text/clean_social_media/pyspark/pyspark_primitives.py"
        dest = f"{output_path}/clean_social_media/social_primitives.py"
        
        # Copy with dependencies
        self.copy_with_dependencies(source, dest)
        
        print(f"Generated social media primitives at {dest}")
```

## Creating a Custom Transformer: Product Data

Let's create another example - a `clean_product_data` transformer for e-commerce data.

```python
# File: datacompose/transformers/text/clean_product_data/pyspark/pyspark_primitives.py

from datacompose.operators.primitives import PrimitiveRegistry
from pyspark.sql import functions as F

# Create the registry
products = PrimitiveRegistry("products")

# ============================================
# VALIDATION PRIMITIVES
# ============================================

@products.register()
def is_valid_sku(col):
    """Validate SKU format"""
    # Common SKU patterns: alphanumeric with dashes
    return col.rlike(r'^[A-Z0-9]{2,}-[A-Z0-9]{2,}(-[A-Z0-9]+)*$')

@products.register()
def is_valid_upc(col):
    """Validate UPC barcode"""
    # UPC-A is 12 digits
    digits = F.regexp_replace(col, r'[^0-9]', '')
    return F.length(digits) == 12

@products.register()
def is_valid_ean(col):
    """Validate EAN barcode"""
    # EAN-13 is 13 digits
    digits = F.regexp_replace(col, r'[^0-9]', '')
    return F.length(digits) == 13

@products.register()
def is_valid_price(col):
    """Check if price is valid"""
    # Extract numeric value
    price_value = F.regexp_extract(col, r'[\d.]+', 0).cast('float')
    return (price_value > 0) & (price_value < 1000000)

# ============================================
# EXTRACTION PRIMITIVES
# ============================================

@products.register()
def extract_brand(col):
    """Extract brand name from product title"""
    # Common brand position patterns
    # Usually first word or before certain keywords
    brand = F.split(col, ' ').getItem(0)
    
    # Clean up
    brand = F.regexp_replace(brand, r'[^A-Za-z0-9]', '')
    return F.upper(brand)

@products.register()
def extract_model_number(col):
    """Extract model number from product description"""
    patterns = [
        r'Model[:#\s]+([A-Z0-9-]+)',
        r'Model Number[:#\s]+([A-Z0-9-]+)',
        r'Item[:#\s]+([A-Z0-9-]+)',
        r'#([A-Z0-9]{4,})'
    ]
    
    for pattern in patterns:
        model = F.regexp_extract(col, pattern, 1)
        if model.isNotNull():
            return model
    return None

@products.register()
def extract_size(col):
    """Extract size information"""
    # Common size patterns
    patterns = [
        r'(\d+(?:\.\d+)?)\s*(?:oz|lb|kg|g|mg)',  # Weight
        r'(\d+(?:\.\d+)?)\s*(?:ml|l|gal|fl oz)',  # Volume
        r'(\d+(?:\.\d+)?)\s*(?:in|cm|mm|ft|m)',  # Length
        r'(?:Small|Medium|Large|XL|XXL|XXXL)',    # Clothing
        r'(\d+)\s*(?:count|pack|piece)'           # Quantity
    ]
    
    for pattern in patterns:
        size = F.regexp_extract(col, pattern, 0)
        if size.isNotNull():
            return size
    return None

@products.register()
def extract_color(col):
    """Extract color from product description"""
    colors = [
        "Black", "White", "Red", "Blue", "Green", "Yellow",
        "Orange", "Purple", "Pink", "Brown", "Gray", "Grey",
        "Silver", "Gold", "Navy", "Beige", "Ivory"
    ]
    
    lower_col = F.lower(col)
    for color in colors:
        if lower_col.contains(color.lower()):
            return color
    return None

@products.register()
def extract_price_value(col):
    """Extract numeric price value"""
    # Remove currency symbols and extract number
    price = F.regexp_extract(col, r'[\$£€¥]?\s*(\d+(?:\.\d{2})?)', 1)
    return F.cast(price, 'decimal(10,2)')

@products.register()
def extract_currency(col):
    """Extract currency from price"""
    return F.when(col.contains('$'), 'USD')\
        .when(col.contains('£'), 'GBP')\
        .when(col.contains('€'), 'EUR')\
        .when(col.contains('¥'), 'JPY')\
        .otherwise('USD')  # Default

# ============================================
# STANDARDIZATION PRIMITIVES
# ============================================

@products.register()
def standardize_sku(col):
    """Standardize SKU format"""
    # Uppercase, remove spaces
    sku = F.upper(F.regexp_replace(col, r'\s+', ''))
    # Add dashes if missing
    if F.length(sku) > 8 and not sku.contains('-'):
        # Insert dash every 4 characters
        sku = F.concat(
            F.substring(sku, 1, 4),
            F.lit('-'),
            F.substring(sku, 5, 4),
            F.lit('-'),
            F.substring(sku, 9, 100)
        )
    return sku

@products.register()
def standardize_product_title(col):
    """Standardize product title"""
    # Proper capitalization, remove extra spaces
    title = F.initcap(F.regexp_replace(col, r'\s+', ' '))
    # Remove special characters
    title = F.regexp_replace(title, r'[™®©]', '')
    return F.trim(title)

@products.register()
def normalize_category(col):
    """Normalize product category"""
    category_map = {
        r'electronic': 'Electronics',
        r'cloth': 'Clothing',
        r'book': 'Books',
        r'home.*garden': 'Home & Garden',
        r'sport': 'Sports',
        r'toy': 'Toys',
        r'food': 'Food & Beverage'
    }
    
    lower_col = F.lower(col)
    normalized = col
    
    for pattern, category in category_map.items():
        normalized = F.when(
            lower_col.rlike(pattern),
            category
        ).otherwise(normalized)
    
    return normalized

# ============================================
# ANALYSIS PRIMITIVES
# ============================================

@products.register()
def calculate_price_tier(col):
    """Categorize products by price tier"""
    price = extract_price_value(col)
    
    return F.when(price < 10, "Budget")\
        .when(price < 50, "Standard")\
        .when(price < 200, "Premium")\
        .when(price < 1000, "Luxury")\
        .otherwise("Ultra-Luxury")

@products.register()
def estimate_margin_category(col):
    """Estimate margin category based on product type"""
    # Simplified margin estimation
    category = normalize_category(col)
    
    return F.when(category == "Electronics", "Low")\
        .when(category == "Clothing", "High")\
        .when(category == "Books", "Medium")\
        .when(category == "Food & Beverage", "Low")\
        .when(category == "Toys", "High")\
        .otherwise("Medium")

@products.register(is_conditional=True)
def is_seasonal_product(col):
    """Check if product appears seasonal"""
    seasonal_keywords = [
        "Christmas", "Halloween", "Easter", "Valentine",
        "Summer", "Winter", "Spring", "Fall", "Holiday",
        "Back to School", "Black Friday"
    ]
    
    is_seasonal = F.lit(False)
    lower_col = F.lower(col)
    for keyword in seasonal_keywords:
        is_seasonal = is_seasonal | lower_col.contains(keyword.lower())
    
    return is_seasonal

# ============================================
# COMPOSED PIPELINES
# ============================================

@products.compose(products=products)
def process_product_listing():
    """Complete product listing processing"""
    # Standardize title
    products.standardize_product_title()
    
    # Extract components
    brand = products.extract_brand()
    model = products.extract_model_number()
    size = products.extract_size()
    color = products.extract_color()
    
    # Validate identifiers
    if products.is_valid_sku():
        products.standardize_sku()
    
    # Extract pricing
    price = products.extract_price_value()
    currency = products.extract_currency()
    tier = products.calculate_price_tier()
    
    return F.struct(
        F.lit(brand).alias("brand"),
        F.lit(model).alias("model"),
        F.lit(size).alias("size"),
        F.lit(color).alias("color"),
        F.lit(price).alias("price"),
        F.lit(currency).alias("currency"),
        F.lit(tier).alias("price_tier")
    )

@products.compose(products=products)
def inventory_optimization_analysis():
    """Analyze product for inventory optimization"""
    # Check if seasonal
    if products.is_seasonal_product():
        inventory_strategy = "Seasonal Stock"
    else:
        # Check price tier
        tier = products.calculate_price_tier()
        if tier in ["Budget", "Standard"]:
            inventory_strategy = "High Turnover"
        else:
            inventory_strategy = "Low Turnover"
    
    # Get margin category
    margin = products.estimate_margin_category()
    
    return F.struct(
        F.lit(inventory_strategy).alias("strategy"),
        F.lit(margin).alias("margin_category"),
        products.is_seasonal_product().alias("is_seasonal")
    )

# Export the registry
__all__ = ['products']
```

## Using Your Custom Transformers

Once you've created your custom transformers, you can use them just like the built-in ones:

```python
from pyspark.sql import SparkSession
from build.clean_social_media.social_primitives import social
from build.clean_product_data.product_primitives import products

# Initialize Spark
spark = SparkSession.builder.appName("CustomTransformers").getOrCreate()

# Load data
social_df = spark.read.csv("social_media_data.csv", header=True)
product_df = spark.read.csv("product_catalog.csv", header=True)

# Process social media data
social_df = social_df.withColumn(
    "handle_clean",
    social.standardize_handle(F.col("twitter_handle"))
).withColumn(
    "is_verified",
    social.is_verified_handle(F.col("handle_clean"))
).withColumn(
    "platform",
    social.extract_platform(F.col("profile_url"))
).withColumn(
    "engagement_score",
    social.calculate_engagement_potential(F.col("profile_url"))
).withColumn(
    "metadata",
    extract_social_metadata(F.col("post_content"))
)

# Process product data
product_df = product_df.withColumn(
    "product_info",
    process_product_listing(F.col("product_description"))
).withColumn(
    "inventory_strategy",
    inventory_optimization_analysis(F.col("product_description"))
).withColumn(
    "sku_clean",
    products.standardize_sku(F.col("sku"))
).withColumn(
    "price_tier",
    products.calculate_price_tier(F.col("price"))
)

# Show results
social_df.select(
    "handle_clean",
    "platform",
    "engagement_score",
    "metadata.hashtags",
    "metadata.content_type"
).show()

product_df.select(
    "sku_clean",
    "product_info.brand",
    "product_info.price_tier",
    "inventory_strategy.strategy"
).show()
```

## Best Practices for Custom Transformers

1. **Follow Naming Conventions**
   - Use descriptive names: `clean_[data_type]`
   - Keep primitive names clear and specific

2. **Group Related Functions**
   - Validation functions: `is_valid_*`
   - Extraction functions: `extract_*`
   - Standardization functions: `standardize_*`, `normalize_*`

3. **Use Type Hints and Documentation**
   ```python
   @social.register()
   def extract_username(col: Column) -> Column:
       """
       Extract username from social media URL or handle.
       
       Args:
           col: Column containing social media URL or handle
           
       Returns:
           Column with extracted username
       """
   ```

4. **Create Composed Pipelines**
   - Combine multiple primitives into logical workflows
   - Return structured data when appropriate

5. **Test Your Transformers**
   ```python
   def test_social_media_primitives():
       test_data = [
           ("@johndoe", "@johndoe", True),
           ("twitter.com/janedoe", "janedoe", True),
           ("not_a_handle", None, False)
       ]
       
       df = spark.createDataFrame(test_data, ["input", "expected", "is_valid"])
       
       result = df.withColumn(
           "extracted",
           social.extract_username(F.col("input"))
       ).withColumn(
           "validated",
           social.is_valid_twitter_handle(F.col("input"))
       )
       
       assert result.filter(F.col("extracted") != F.col("expected")).count() == 0
   ```

## Next Steps

- Create transformers for your specific domain data
- Share transformers with your team
- Build a library of reusable transformations
- Integrate with your data pipeline