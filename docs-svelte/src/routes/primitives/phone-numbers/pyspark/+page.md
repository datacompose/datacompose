---
title: Phone Number Primitives - PySpark Methods
description: All available PySpark methods for Phone Number Primitives
---

# Phone Number Primitives - PySpark Methods

## Available Methods (36 total)


### Core Methods

- [`add_country_code()`](/primitives/phone-numbers/pyspark/add_country_code) - Add country code "1" if not present (for NANP numbers)
- [`clean_phone()`](/primitives/phone-numbers/pyspark/clean_phone) - Clean and validate phone number, returning null for invalid numbers
- [`convert_letters_to_numbers()`](/primitives/phone-numbers/pyspark/convert_letters_to_numbers) - Convert phone letters to numbers (e
- [`extract_all_phones_from_text()`](/primitives/phone-numbers/pyspark/extract_all_phones_from_text) - Extract all phone numbers from text as an array
- [`extract_area_code()`](/primitives/phone-numbers/pyspark/extract_area_code) - Extract area code from NANP phone number
- [`extract_country_code()`](/primitives/phone-numbers/pyspark/extract_country_code) - Extract country code from phone number
- [`extract_digits()`](/primitives/phone-numbers/pyspark/extract_digits) - Extract only digits from phone number string
- [`extract_exchange()`](/primitives/phone-numbers/pyspark/extract_exchange) - Extract exchange (first 3 digits of local number) from NANP phone number
- [`extract_extension()`](/primitives/phone-numbers/pyspark/extract_extension) - Extract extension from phone number if present
- [`extract_local_number()`](/primitives/phone-numbers/pyspark/extract_local_number) - Extract local number (exchange + subscriber) from NANP phone number
- [`extract_phone_from_text()`](/primitives/phone-numbers/pyspark/extract_phone_from_text) - Extract first phone number from text using regex patterns
- [`extract_subscriber()`](/primitives/phone-numbers/pyspark/extract_subscriber) - Extract subscriber number (last 4 digits) from NANP phone number
- [`filter_nanp_phones()`](/primitives/phone-numbers/pyspark/filter_nanp_phones) - Return phone number only if valid NANP, otherwise return null
- [`filter_toll_free_phones()`](/primitives/phone-numbers/pyspark/filter_toll_free_phones) - Return phone number only if toll-free, otherwise return null
- [`filter_valid_phones()`](/primitives/phone-numbers/pyspark/filter_valid_phones) - Return phone number only if valid, otherwise return null
- [`format_e164()`](/primitives/phone-numbers/pyspark/format_e164) - Format phone number in E
- [`format_international()`](/primitives/phone-numbers/pyspark/format_international) - Format international phone number with country code
- [`format_nanp()`](/primitives/phone-numbers/pyspark/format_nanp) - Format NANP phone number in standard hyphen format (XXX-XXX-XXXX)
- [`format_nanp_dot()`](/primitives/phone-numbers/pyspark/format_nanp_dot) - Format NANP phone number with dots (XXX
- [`format_nanp_paren()`](/primitives/phone-numbers/pyspark/format_nanp_paren) - Format NANP phone number with parentheses ((XXX) XXX-XXXX)
- [`format_nanp_space()`](/primitives/phone-numbers/pyspark/format_nanp_space) - Format NANP phone number with spaces (XXX XXX XXXX)
- [`get_phone_type()`](/primitives/phone-numbers/pyspark/get_phone_type) - Get phone number type (toll-free, premium, standard, international)
- [`get_region_from_area_code()`](/primitives/phone-numbers/pyspark/get_region_from_area_code) - Get geographic region from area code (simplified - would need lookup table)
- [`has_extension()`](/primitives/phone-numbers/pyspark/has_extension) - Check if phone number has an extension
- [`is_premium_rate()`](/primitives/phone-numbers/pyspark/is_premium_rate) - Check if phone number is premium rate (900)
- [`is_toll_free()`](/primitives/phone-numbers/pyspark/is_toll_free) - Check if phone number is toll-free (800, 888, 877, 866, 855, 844, 833)
- [`is_valid_international()`](/primitives/phone-numbers/pyspark/is_valid_international) - Check if phone number could be valid international format
- [`is_valid_nanp()`](/primitives/phone-numbers/pyspark/is_valid_nanp) - Check if phone number is valid NANP format (North American Numbering Plan)
- [`is_valid_phone()`](/primitives/phone-numbers/pyspark/is_valid_phone) - Check if phone number is valid (NANP or international)
- [`mask_phone()`](/primitives/phone-numbers/pyspark/mask_phone) - Mask phone number for privacy keeping last 4 digits (e
- [`normalize_separators()`](/primitives/phone-numbers/pyspark/normalize_separators) - Normalize various separator styles to hyphens
- [`remove_extension()`](/primitives/phone-numbers/pyspark/remove_extension) - Remove extension from phone number
- [`remove_non_digits()`](/primitives/phone-numbers/pyspark/remove_non_digits) - Remove all non-digit characters from phone number
- [`standardize_phone()`](/primitives/phone-numbers/pyspark/standardize_phone) - Standardize phone number with cleaning and NANP formatting
- [`standardize_phone_digits()`](/primitives/phone-numbers/pyspark/standardize_phone_digits) - Standardize phone number and return digits only
- [`standardize_phone_e164()`](/primitives/phone-numbers/pyspark/standardize_phone_e164) - Standardize phone number with cleaning and E
