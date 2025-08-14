---
title: Phone Number Primitives - PySpark Methods
description: All available PySpark methods for Phone Number Primitives
---

# Phone Number Primitives - PySpark Methods

## Available Methods (36 total)


### Core Methods

- [`add_country_code()`](./add_country_code.md) - Add country code "1" if not present (for NANP numbers)
- [`clean_phone()`](./clean_phone.md) - Clean and validate phone number, returning null for invalid numbers
- [`convert_letters_to_numbers()`](./convert_letters_to_numbers.md) - Convert phone letters to numbers (e
- [`extract_all_phones_from_text()`](./extract_all_phones_from_text.md) - Extract all phone numbers from text as an array
- [`extract_area_code()`](./extract_area_code.md) - Extract area code from NANP phone number
- [`extract_country_code()`](./extract_country_code.md) - Extract country code from phone number
- [`extract_digits()`](./extract_digits.md) - Extract only digits from phone number string
- [`extract_exchange()`](./extract_exchange.md) - Extract exchange (first 3 digits of local number) from NANP phone number
- [`extract_extension()`](./extract_extension.md) - Extract extension from phone number if present
- [`extract_local_number()`](./extract_local_number.md) - Extract local number (exchange + subscriber) from NANP phone number
- [`extract_phone_from_text()`](./extract_phone_from_text.md) - Extract first phone number from text using regex patterns
- [`extract_subscriber()`](./extract_subscriber.md) - Extract subscriber number (last 4 digits) from NANP phone number
- [`filter_nanp_phones()`](./filter_nanp_phones.md) - Return phone number only if valid NANP, otherwise return null
- [`filter_toll_free_phones()`](./filter_toll_free_phones.md) - Return phone number only if toll-free, otherwise return null
- [`filter_valid_phones()`](./filter_valid_phones.md) - Return phone number only if valid, otherwise return null
- [`format_e164()`](./format_e164.md) - Format phone number in E
- [`format_international()`](./format_international.md) - Format international phone number with country code
- [`format_nanp()`](./format_nanp.md) - Format NANP phone number in standard hyphen format (XXX-XXX-XXXX)
- [`format_nanp_dot()`](./format_nanp_dot.md) - Format NANP phone number with dots (XXX
- [`format_nanp_paren()`](./format_nanp_paren.md) - Format NANP phone number with parentheses ((XXX) XXX-XXXX)
- [`format_nanp_space()`](./format_nanp_space.md) - Format NANP phone number with spaces (XXX XXX XXXX)
- [`get_phone_type()`](./get_phone_type.md) - Get phone number type (toll-free, premium, standard, international)
- [`get_region_from_area_code()`](./get_region_from_area_code.md) - Get geographic region from area code (simplified - would need lookup table)
- [`has_extension()`](./has_extension.md) - Check if phone number has an extension
- [`is_premium_rate()`](./is_premium_rate.md) - Check if phone number is premium rate (900)
- [`is_toll_free()`](./is_toll_free.md) - Check if phone number is toll-free (800, 888, 877, 866, 855, 844, 833)
- [`is_valid_international()`](./is_valid_international.md) - Check if phone number could be valid international format
- [`is_valid_nanp()`](./is_valid_nanp.md) - Check if phone number is valid NANP format (North American Numbering Plan)
- [`is_valid_phone()`](./is_valid_phone.md) - Check if phone number is valid (NANP or international)
- [`mask_phone()`](./mask_phone.md) - Mask phone number for privacy keeping last 4 digits (e
- [`normalize_separators()`](./normalize_separators.md) - Normalize various separator styles to hyphens
- [`remove_extension()`](./remove_extension.md) - Remove extension from phone number
- [`remove_non_digits()`](./remove_non_digits.md) - Remove all non-digit characters from phone number
- [`standardize_phone()`](./standardize_phone.md) - Standardize phone number with cleaning and NANP formatting
- [`standardize_phone_digits()`](./standardize_phone_digits.md) - Standardize phone number and return digits only
- [`standardize_phone_e164()`](./standardize_phone_e164.md) - Standardize phone number with cleaning and E
