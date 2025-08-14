---
title: Address Primitives - PySpark Methods
description: All available PySpark methods for Address Primitives
---

# Address Primitives - PySpark Methods

## Available Methods (43 total)


### Core Methods

- [`add_custom_city()`](/primitives/addresses/pyspark/add_custom_city) - Add a custom city name to improve city extraction
- [`add_custom_state()`](/primitives/addresses/pyspark/add_custom_state) - Add a custom state or region to the state mappings
- [`extract_apartment_number()`](/primitives/addresses/pyspark/extract_apartment_number) - Extract apartment/unit number from address
- [`extract_building()`](/primitives/addresses/pyspark/extract_building) - Extract building name or identifier from address
- [`extract_city()`](/primitives/addresses/pyspark/extract_city) - Extract city name from US address text
- [`extract_country()`](/primitives/addresses/pyspark/extract_country) - Extract country from address
- [`extract_floor()`](/primitives/addresses/pyspark/extract_floor) - Extract floor number from address
- [`extract_full_street()`](/primitives/addresses/pyspark/extract_full_street) - Extract complete street address (number + prefix + name + suffix)
- [`extract_po_box()`](/primitives/addresses/pyspark/extract_po_box) - Extract PO Box number from address
- [`extract_private_mailbox()`](/primitives/addresses/pyspark/extract_private_mailbox) - Extract private mailbox (PMB) number from address
- [`extract_secondary_address()`](/primitives/addresses/pyspark/extract_secondary_address) - Extract complete secondary address information (unit type + number)
- [`extract_state()`](/primitives/addresses/pyspark/extract_state) - Extract and standardize state to 2-letter abbreviation
- [`extract_street_name()`](/primitives/addresses/pyspark/extract_street_name) - Extract street name from address
- [`extract_street_number()`](/primitives/addresses/pyspark/extract_street_number) - Extract street/house number from address
- [`extract_street_prefix()`](/primitives/addresses/pyspark/extract_street_prefix) - Extract directional prefix from street address
- [`extract_street_suffix()`](/primitives/addresses/pyspark/extract_street_suffix) - Extract street type/suffix from address
- [`extract_unit_type()`](/primitives/addresses/pyspark/extract_unit_type) - Extract the type of unit (Apt, Suite, Unit, etc
- [`extract_zip_code()`](/primitives/addresses/pyspark/extract_zip_code) - Extract US ZIP code (5-digit or ZIP+4 format) from text
- [`format_secondary_address()`](/primitives/addresses/pyspark/format_secondary_address) - Format unit type and number into standard secondary address
- [`get_state_name()`](/primitives/addresses/pyspark/get_state_name) - Convert state abbreviation to full name
- [`get_zip_code_type()`](/primitives/addresses/pyspark/get_zip_code_type) - Determine the type of ZIP code
- [`has_apartment()`](/primitives/addresses/pyspark/has_apartment) - Check if address contains apartment/unit information
- [`has_country()`](/primitives/addresses/pyspark/has_country) - Check if address contains country information
- [`has_po_box()`](/primitives/addresses/pyspark/has_po_box) - Check if address contains PO Box
- [`is_po_box_only()`](/primitives/addresses/pyspark/is_po_box_only) - Check if address is ONLY a PO Box (no street address)
- [`is_valid_zip_code()`](/primitives/addresses/pyspark/is_valid_zip_code) - Alias for validate_zip_code for consistency
- [`remove_country()`](/primitives/addresses/pyspark/remove_country) - Remove country from address
- [`remove_custom_city()`](/primitives/addresses/pyspark/remove_custom_city) - Remove a custom city from the set
- [`remove_custom_state()`](/primitives/addresses/pyspark/remove_custom_state) - Remove a custom state from the mappings
- [`remove_po_box()`](/primitives/addresses/pyspark/remove_po_box) - Remove PO Box from address
- [`remove_secondary_address()`](/primitives/addresses/pyspark/remove_secondary_address) - Remove apartment/suite/unit information from address
- [`split_zip_code()`](/primitives/addresses/pyspark/split_zip_code) - Split ZIP+4 code into base and extension components
- [`standardize_city()`](/primitives/addresses/pyspark/standardize_city) - Standardize city name formatting
- [`standardize_country()`](/primitives/addresses/pyspark/standardize_country) - Standardize country name to consistent format
- [`standardize_po_box()`](/primitives/addresses/pyspark/standardize_po_box) - Standardize PO Box format to consistent representation
- [`standardize_state()`](/primitives/addresses/pyspark/standardize_state) - Convert state to standard 2-letter format
- [`standardize_street_prefix()`](/primitives/addresses/pyspark/standardize_street_prefix) - Standardize street directional prefixes to abbreviated form
- [`standardize_street_suffix()`](/primitives/addresses/pyspark/standardize_street_suffix) - Standardize street type/suffix to USPS abbreviated form
- [`standardize_unit_type()`](/primitives/addresses/pyspark/standardize_unit_type) - Standardize unit type to common abbreviations
- [`standardize_zip_code()`](/primitives/addresses/pyspark/standardize_zip_code) - Standardize ZIP code format
- [`validate_city()`](/primitives/addresses/pyspark/validate_city) - Validate if a city name appears valid
- [`validate_state()`](/primitives/addresses/pyspark/validate_state) - Validate if state code is a valid US state abbreviation
- [`validate_zip_code()`](/primitives/addresses/pyspark/validate_zip_code) - Validate if a ZIP code is in correct US format
