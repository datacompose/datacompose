---
title: Emails
description: Data cleaning transformers for emails
---

# Emails

Clean and validate email addresses in your data pipelines.

## Available Implementations

Choose your data processing framework:

- [Apache Spark - Distributed data processing](./emails/pyspark)

## Quick Start

```python
from datacompose.transformers.text.emails import emails

# Use the @emails.compose() decorator to chain operations
@emails.compose()
def clean_data(df):
    return df
```
