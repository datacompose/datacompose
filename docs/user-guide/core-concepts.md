# Core Concepts

Understanding these core concepts will help you make the most of Datacompose.

## PrimitiveRegistry

The `PrimitiveRegistry` is the foundation of Datacompose. It's a container that organizes related transformation functions.

### Creating a Registry

```python
from datacompose.operators.primitives import PrimitiveRegistry

# Create a registry for text operations
text = PrimitiveRegistry("text")

# Create a registry for date operations
dates = PrimitiveRegistry("dates")
```

### Registering Functions

Functions become primitives when registered:

```python
@text.register()
def lowercase(col):
    """Convert text to lowercase"""
    return F.lower(col)

@text.register()
def remove_whitespace(col):
    """Remove all whitespace characters"""
    return F.regexp_replace(col, r'\s+', '')
```

### Using Primitives

Once registered, primitives can be used directly:

```python
df = df.withColumn("clean_text", text.lowercase(F.col("input")))
df = df.withColumn("no_spaces", text.remove_whitespace(F.col("input")))
```

## SmartPrimitive

`SmartPrimitive` enables partial application - the ability to pre-configure functions with default parameters.

### Basic Usage

```python
@text.register()
def replace_chars(col, old_chars, new_char=''):
    """Replace specified characters"""
    pattern = f"[{old_chars}]"
    return F.regexp_replace(col, pattern, new_char)

# Direct usage
df = df.withColumn("clean", text.replace_chars(F.col("input"), "@#$", "_"))

# Partial application
remove_special = text.replace_chars(old_chars="@#$%")
df = df.withColumn("clean", remove_special(F.col("input")))
```

### Benefits of Partial Application

1. **Reusability** - Create configured versions for common use cases
2. **Consistency** - Ensure the same configuration across your pipeline
3. **Readability** - Named configurations are self-documenting

## Pipeline Composition

Composition allows you to build complex transformations from simple primitives.

### Basic Composition

```python
@text.compose(text=text)
def clean_text_pipeline():
    """Standard text cleaning pipeline"""
    text.trim()
    text.lowercase()
    text.remove_whitespace()

# Apply the pipeline
df = df.withColumn("cleaned", clean_text_pipeline(F.col("input")))
```

### Multi-Registry Composition

Combine primitives from different registries:

```python
@validation.compose(emails=emails, phones=phones)
def validate_contact():
    """Validate contact information"""
    emails.standardize_email()
    phones.standardize_phone()
    
    # Both must be valid
    if emails.is_valid_email():
        if phones.is_valid_phone():
            return True
    return False
```

## Conditional Logic

Datacompose supports conditional transformations using Spark's `when/otherwise` constructs.

### Conditional Primitives

```python
@text.register(is_conditional=True)
def is_empty(col):
    """Check if text is empty or null"""
    return F.coalesce(F.length(col), F.lit(0)) == 0

@text.register(is_conditional=True)
def contains_digits(col):
    """Check if text contains digits"""
    return col.rlike(r'\d+')
```

### Conditional Pipelines

```python
@text.compose(text=text)
def smart_clean():
    """Clean text based on content"""
    if text.is_empty():
        # Return default value for empty strings
        return F.lit("N/A")
    elif text.contains_digits():
        # Special handling for text with numbers
        text.remove_special_chars()
        text.uppercase()
    else:
        # Standard text cleaning
        text.lowercase()
        text.trim()
```

## Code Generation

Understanding how Datacompose generates code helps you customize effectively.

### Generation Process

1. **Template Processing** - Jinja2 templates define the structure
2. **Primitive Injection** - Your primitives are embedded in the generated code
3. **Dependency Bundling** - Required utilities are included
4. **Optimization** - Code is optimized for Spark execution

### Generated Structure

```
build/pyspark/clean_emails/
├── email_primitives.py      # Your primitives
└── utils/
    └── primitives.py        # Core framework (embedded)
```

### Customization Points

You can customize at several levels:

1. **Primitive Level** - Modify individual functions
2. **Pipeline Level** - Create custom compositions
3. **Registry Level** - Add new primitives to existing registries
4. **Framework Level** - Extend the core primitive system

## Type Safety

Datacompose maintains Spark's type safety throughout transformations.

### Column Types

All primitives work with Spark column expressions:

```python
# Correct - passing column expression
df = df.withColumn("clean", text.lowercase(F.col("input")))

# Also correct - column from another operation
df = df.withColumn("clean", text.lowercase(F.upper(F.col("input"))))

# Incorrect - passing string value
# df = df.withColumn("clean", text.lowercase("input"))  # ❌ Won't work
```

### Return Types

Primitives return column expressions that can be chained:

```python
# Chain transformations
df = df.withColumn(
    "result",
    text.remove_whitespace(
        text.lowercase(
            text.trim(F.col("input"))
        )
    )
)
```

## Best Practices

### 1. Registry Organization

Group related transformations:

```python
# Good - focused registries
emails = PrimitiveRegistry("emails")
phones = PrimitiveRegistry("phones")
addresses = PrimitiveRegistry("addresses")

# Bad - mixed concerns
misc = PrimitiveRegistry("misc")  # Too generic
```

### 2. Primitive Naming

Use clear, descriptive names:

```python
# Good
@text.register()
def remove_html_tags(col):
    pass

# Bad
@text.register()
def clean(col):  # Too vague
    pass
```

### 3. Documentation

Always document your primitives:

```python
@text.register()
def normalize_whitespace(col):
    """
    Normalize whitespace in text.
    
    - Replaces multiple spaces with single space
    - Removes leading/trailing whitespace
    - Converts tabs and newlines to spaces
    
    Args:
        col: Spark column containing text
    
    Returns:
        Column with normalized whitespace
    """
    return F.regexp_replace(F.trim(col), r'\s+', ' ')
```

### 4. Testing

Test your primitives thoroughly:

```python
def test_normalize_whitespace():
    df = spark.createDataFrame([
        ("  hello   world  ",),
        ("tab\there",),
        ("new\nline",),
    ], ["text"])
    
    result = df.withColumn(
        "normalized",
        text.normalize_whitespace(F.col("text"))
    ).collect()
    
    assert result[0]["normalized"] == "hello world"
    assert result[1]["normalized"] == "tab here"
    assert result[2]["normalized"] == "new line"
```

## Next Steps

- Explore available [Primitives](primitives.md)
- Learn to build [Pipelines](pipelines.md)
- Understand [Configuration](configuration.md) options