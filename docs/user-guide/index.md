# User Guide

This comprehensive guide covers all aspects of using Datacompose for your data transformation needs.

## Overview

Datacompose follows a unique philosophy: instead of being a traditional library, it's a code generator that creates data transformation primitives that become part of YOUR codebase. This approach gives you complete control, transparency, and flexibility.

## Guide Sections

<div class="grid cards" markdown>

-   :material-foundation:{ .lg .middle } **[Core Concepts](core-concepts.md)**

    ---

    Understand the fundamental building blocks of Datacompose

-   :material-puzzle-outline:{ .lg .middle } **[Primitives](primitives.md)**

    ---

    Learn how to use and create transformation primitives

-   :material-pipe:{ .lg .middle } **[Pipelines](pipelines.md)**

    ---

    Build complex data transformation pipelines

-   :material-cog:{ .lg .middle } **[Configuration](configuration.md)**

    ---

    Configure Datacompose for your project needs

</div>

## Key Concepts to Master

### 1. Code Generation Philosophy

Datacompose generates code rather than providing a runtime library. This means:

- **No version conflicts** - The generated code has no external dependencies
- **Full transparency** - You can read and understand every line of code
- **Easy customization** - Modify the generated code to fit your exact needs
- **Better performance** - No abstraction overhead

### 2. Composable Primitives

Primitives are the building blocks of data transformations:

```python
# Simple primitive usage
df = df.withColumn("clean_email", emails.standardize_email(F.col("email")))

# Composed primitives
@emails.compose(emails=emails)
def validate_and_clean():
    emails.standardize_email()
    if emails.is_valid_email():
        emails.extract_domain()
```

### 3. Smart Partial Application

Configure primitives with default parameters for reuse:

```python
# Create a configured primitive
remove_special = text.remove_chars(chars="@#$%")

# Use it multiple times
df = df.withColumn("clean_1", remove_special(F.col("field1")))
df = df.withColumn("clean_2", remove_special(F.col("field2")))
```

## Common Workflows

### Data Cleaning Pipeline

A typical data cleaning workflow with Datacompose:

1. **Generate primitives** for your data types
2. **Import** the generated primitives
3. **Apply** transformations to your DataFrame
4. **Validate** the results
5. **Customize** as needed

### ETL Process Integration

Integrate Datacompose into your ETL pipelines:

```python
# Extract
df = spark.read.parquet("raw_data/")

# Transform with Datacompose
df = df.withColumn("email_clean", emails.standardize_email(F.col("email")))
df = df.withColumn("phone_clean", phones.format_nanp(F.col("phone")))
df = df.filter(emails.is_valid_email(F.col("email_clean")))

# Load
df.write.parquet("clean_data/")
```

## Best Practices

!!! tip "Best Practices"
    1. **Generate once, customize freely** - Don't regenerate primitives after customization
    2. **Version control generated code** - Treat it as part of your codebase
    3. **Create project-specific primitives** - Build your own registry for domain-specific transformations
    4. **Test your pipelines** - The generated code is testable like any other code
    5. **Document customizations** - Add comments when you modify generated code

## Performance Tips

- **Minimize column operations** - Combine multiple transformations when possible
- **Use conditional logic wisely** - Spark's `when/otherwise` is vectorized
- **Cache intermediate results** - For complex pipelines with multiple branches
- **Partition appropriately** - Ensure good data distribution for parallel processing

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Import errors | Ensure you've run `datacompose add` for the required primitives |
| Type mismatches | Check that columns exist and have expected types |
| Performance issues | Review pipeline composition and consider caching |
| Customization conflicts | Keep customizations in separate functions |

## Next Steps

- Dive deep into [Core Concepts](core-concepts.md)
- Explore available [Primitives](primitives.md)
- Learn to build [Pipelines](pipelines.md)
- Master [Configuration](configuration.md) options