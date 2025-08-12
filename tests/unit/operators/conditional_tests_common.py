"""
Common documentation and imports for conditional compilation tests.

DESIGN DECISION - Single Column Constraint:
-------------------------------------------
The conditional compilation framework is designed to work with single-column transformations.
This is an intentional design choice to keep the framework focused and maintainable.

For multi-column logic:
1. Pre-compute the logic into a column before the pipeline
2. Use closure pattern to capture external values (see test_conditional_with_dynamic_thresholds)
3. Reference other columns by name with f.col() if column names are known

Example:
    # Pre-compute multi-column logic
    df = df.withColumn("is_priority", (f.col("value") > 50) & (f.col("flag") == True))

    # Then use in pipeline
    @ns.register(is_conditional=True)
    def is_priority(col):
        return f.col("is_priority")

This keeps the framework simple while still being powerful for most use cases.
Future versions may add row-wise operations for more complex multi-column logic.
"""

