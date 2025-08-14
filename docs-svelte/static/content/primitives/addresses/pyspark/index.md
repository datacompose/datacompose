---
title: Address Primitives - PySpark Methods
description: All available PySpark methods for Address Primitives
---

# Address Primitives - PySpark Methods

## Available Methods (43 total)


### Core Methods

- [`add_custom_city()`](./add_custom_city.md) - Add a custom city name to improve city extraction
- [`add_custom_state()`](./add_custom_state.md) - Add a custom state or region to the state mappings
- [`extract_apartment_number()`](./extract_apartment_number.md) - Extract apartment/unit number from address
- [`extract_building()`](./extract_building.md) - Extract building name or identifier from address
- [`extract_city()`](./extract_city.md) - Extract city name from US address text
- [`extract_country()`](./extract_country.md) - Extract country from address
- [`extract_floor()`](./extract_floor.md) - Extract floor number from address
- [`extract_full_street()`](./extract_full_street.md) - Extract complete street address (number + prefix + name + suffix)
- [`extract_po_box()`](./extract_po_box.md) - Extract PO Box number from address
- [`extract_private_mailbox()`](./extract_private_mailbox.md) - Extract private mailbox (PMB) number from address
- [`extract_secondary_address()`](./extract_secondary_address.md) - Extract complete secondary address information (unit type + number)
- [`extract_state()`](./extract_state.md) - Extract and standardize state to 2-letter abbreviation
- [`extract_street_name()`](./extract_street_name.md) - Extract street name from address
- [`extract_street_number()`](./extract_street_number.md) - Extract street/house number from address
- [`extract_street_prefix()`](./extract_street_prefix.md) - Extract directional prefix from street address
- [`extract_street_suffix()`](./extract_street_suffix.md) - Extract street type/suffix from address
- [`extract_unit_type()`](./extract_unit_type.md) - Extract the type of unit (Apt, Suite, Unit, etc
- [`extract_zip_code()`](./extract_zip_code.md) - Extract US ZIP code (5-digit or ZIP+4 format) from text
- [`format_secondary_address()`](./format_secondary_address.md) - Format unit type and number into standard secondary address
- [`get_state_name()`](./get_state_name.md) - Convert state abbreviation to full name
- [`get_zip_code_type()`](./get_zip_code_type.md) - Determine the type of ZIP code
- [`has_apartment()`](./has_apartment.md) - Check if address contains apartment/unit information
- [`has_country()`](./has_country.md) - Check if address contains country information
- [`has_po_box()`](./has_po_box.md) - Check if address contains PO Box
- [`is_po_box_only()`](./is_po_box_only.md) - Check if address is ONLY a PO Box (no street address)
- [`is_valid_zip_code()`](./is_valid_zip_code.md) - Alias for validate_zip_code for consistency
- [`remove_country()`](./remove_country.md) - Remove country from address
- [`remove_custom_city()`](./remove_custom_city.md) - Remove a custom city from the set
- [`remove_custom_state()`](./remove_custom_state.md) - Remove a custom state from the mappings
- [`remove_po_box()`](./remove_po_box.md) - Remove PO Box from address
- [`remove_secondary_address()`](./remove_secondary_address.md) - Remove apartment/suite/unit information from address
- [`split_zip_code()`](./split_zip_code.md) - Split ZIP+4 code into base and extension components
- [`standardize_city()`](./standardize_city.md) - Standardize city name formatting
- [`standardize_country()`](./standardize_country.md) - Standardize country name to consistent format
- [`standardize_po_box()`](./standardize_po_box.md) - Standardize PO Box format to consistent representation
- [`standardize_state()`](./standardize_state.md) - Convert state to standard 2-letter format
- [`standardize_street_prefix()`](./standardize_street_prefix.md) - Standardize street directional prefixes to abbreviated form
- [`standardize_street_suffix()`](./standardize_street_suffix.md) - Standardize street type/suffix to USPS abbreviated form
- [`standardize_unit_type()`](./standardize_unit_type.md) - Standardize unit type to common abbreviations
- [`standardize_zip_code()`](./standardize_zip_code.md) - Standardize ZIP code format
- [`validate_city()`](./validate_city.md) - Validate if a city name appears valid
- [`validate_state()`](./validate_state.md) - Validate if state code is a valid US state abbreviation
- [`validate_zip_code()`](./validate_zip_code.md) - Validate if a ZIP code is in correct US format
