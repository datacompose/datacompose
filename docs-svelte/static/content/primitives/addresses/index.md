---
title: Address Primitives
description: Address parsing, validation, and standardization tools
generated: true
---

# Address Primitives

Address parsing, validation, and standardization tools

## Import

```python
from datacompose import clean_addresses
```

## Available Methods

### Pyspark Primitives

#### `add_custom_state()`

Add a custom state or region to the state mappings.

**Parameters:** full_name, abbreviation

#### `add_custom_city()`

Add a custom city name to improve city extraction.

**Parameters:** city_name

#### `remove_custom_state()`

Remove a custom state from the mappings.

**Parameters:** identifier

#### `remove_custom_city()`

Remove a custom city from the set.

**Parameters:** city_name

#### `extract_street_number()`

Extract street/house number from address.

**Parameters:** col

#### `extract_street_prefix()`

Extract directional prefix from street address.

**Parameters:** col

#### `extract_street_name()`

Extract street name from address.

**Parameters:** col

#### `extract_street_suffix()`

Extract street type/suffix from address.

**Parameters:** col

#### `extract_full_street()`

Extract complete street address (number + prefix + name + suffix).

**Parameters:** col

#### `standardize_street_prefix()`

Standardize street directional prefixes to abbreviated form.

**Parameters:** col, custom_mappings

#### `standardize_street_suffix()`

Standardize street type/suffix to USPS abbreviated form.

**Parameters:** col, custom_mappings

#### `extract_apartment_number()`

Extract apartment/unit number from address.

**Parameters:** col

#### `extract_floor()`

Extract floor number from address.

**Parameters:** col

#### `extract_building()`

Extract building name or identifier from address.

**Parameters:** col

#### `extract_unit_type()`

Extract the type of unit (Apt, Suite, Unit, etc.) from address.

**Parameters:** col

#### `standardize_unit_type()`

Standardize unit type to common abbreviations.

**Parameters:** col, custom_mappings

#### `extract_secondary_address()`

Extract complete secondary address information (unit type + number).

**Parameters:** col

#### `has_apartment()`

Check if address contains apartment/unit information.

**Parameters:** col

#### `remove_secondary_address()`

Remove apartment/suite/unit information from address.

**Parameters:** col

#### `format_secondary_address()`

Format unit type and number into standard secondary address.

**Parameters:** unit_type, unit_number

#### `extract_zip_code()`

Extract US ZIP code (5-digit or ZIP+4 format) from text.

**Parameters:** col

#### `validate_zip_code()`

Validate if a ZIP code is in correct US format.

**Parameters:** col

#### `is_valid_zip_code()`

Alias for validate_zip_code for consistency.

**Parameters:** col

#### `standardize_zip_code()`

Standardize ZIP code format.

**Parameters:** col

#### `get_zip_code_type()`

Determine the type of ZIP code.

**Parameters:** col

#### `split_zip_code()`

Split ZIP+4 code into base and extension components.

**Parameters:** col

#### `extract_city()`

Extract city name from US address text.

**Parameters:** col, custom_cities

#### `extract_state()`

Extract and standardize state to 2-letter abbreviation.

**Parameters:** col, custom_states

#### `validate_city()`

Validate if a city name appears valid.

**Parameters:** col, known_cities, min_length, max_length

#### `validate_state()`

Validate if state code is a valid US state abbreviation.

**Parameters:** col

#### `standardize_city()`

Standardize city name formatting.

**Parameters:** col, custom_mappings

#### `standardize_state()`

Convert state to standard 2-letter format.

**Parameters:** col

#### `get_state_name()`

Convert state abbreviation to full name.

**Parameters:** col

#### `extract_country()`

Extract country from address.

**Parameters:** col

#### `has_country()`

Check if address contains country information.

**Parameters:** col

#### `remove_country()`

Remove country from address.

**Parameters:** col

#### `standardize_country()`

Standardize country name to consistent format.

**Parameters:** col, custom_mappings

#### `extract_po_box()`

Extract PO Box number from address.

**Parameters:** col

#### `has_po_box()`

Check if address contains PO Box.

**Parameters:** col

#### `is_po_box_only()`

Check if address is ONLY a PO Box (no street address).

**Parameters:** col

#### `remove_po_box()`

Remove PO Box from address.

**Parameters:** col

#### `standardize_po_box()`

Standardize PO Box format to consistent representation.

**Parameters:** col

#### `extract_private_mailbox()`

Extract private mailbox (PMB) number from address.

**Parameters:** col



## Example Pipeline

```python
from datacompose import clean_addresses

@clean_addresses.compose()
def process_pipeline(df):
    return (df
        .add_custom_state(column_name)
        .add_custom_city(column_name)
        .remove_custom_state(column_name)
    )

# Apply to your DataFrame
result_df = process_pipeline(spark_df)
```

## Using the @compose Decorator

The `@compose` decorator allows you to chain multiple operations efficiently:

```python
@clean_addresses.compose()
def my_pipeline(df):
    return df.add_custom_state("column_name")
```

## Custom Primitives

You can extend this library with custom primitives:

```python
from pyspark.sql import functions as F
from datacompose import clean_addresses

@clean_addresses.register_primitive
def custom_transformation(df, column_name):
    """Your custom transformation logic"""
    return df.withColumn(
        f"{column_name}_transformed",
        F.col(column_name)  # Your logic here
    )
```

---
*This documentation was automatically generated from the source code.*


## Detailed Method Documentation

For detailed documentation of each method, see:

- [PySpark Methods →](./addresses/pyspark/) - All 43 available PySpark methods

