---
title: Email Primitives - PySpark Methods
description: All available PySpark methods for Email Primitives
---

# Email Primitives - PySpark Methods

## Available Methods (27 total)


### Core Methods

- [`extract_all_emails()`](/primitives/emails/pyspark/extract_all_emails) - Extract all email addresses from text as an array
- [`extract_domain()`](/primitives/emails/pyspark/extract_domain) - Extract domain from email address
- [`extract_domain_name()`](/primitives/emails/pyspark/extract_domain_name) - Extract domain name without TLD from email address
- [`extract_email()`](/primitives/emails/pyspark/extract_email) - Extract first valid email address from text
- [`extract_name_from_email()`](/primitives/emails/pyspark/extract_name_from_email) - Attempt to extract person's name from email username
- [`extract_tld()`](/primitives/emails/pyspark/extract_tld) - Extract top-level domain from email address
- [`extract_username()`](/primitives/emails/pyspark/extract_username) - Extract username (local part) from email address
- [`filter_corporate_emails()`](/primitives/emails/pyspark/filter_corporate_emails) - Return email only if corporate, otherwise return null
- [`filter_non_disposable_emails()`](/primitives/emails/pyspark/filter_non_disposable_emails) - Return email only if not disposable, otherwise return null
- [`filter_valid_emails()`](/primitives/emails/pyspark/filter_valid_emails) - Return email only if valid, otherwise return null
- [`fix_common_typos()`](/primitives/emails/pyspark/fix_common_typos) - Fix common domain typos in email addresses
- [`get_canonical_email()`](/primitives/emails/pyspark/get_canonical_email) - Get canonical form of email address for deduplication
- [`get_email_provider()`](/primitives/emails/pyspark/get_email_provider) - Get email provider name from domain
- [`has_plus_addressing()`](/primitives/emails/pyspark/has_plus_addressing) - Check if email uses plus addressing (e
- [`is_corporate_email()`](/primitives/emails/pyspark/is_corporate_email) - Check if email appears to be from a corporate domain (not free email provider)
- [`is_disposable_email()`](/primitives/emails/pyspark/is_disposable_email) - Check if email is from a disposable email service
- [`is_valid_domain()`](/primitives/emails/pyspark/is_valid_domain) - Check if email domain part is valid
- [`is_valid_email()`](/primitives/emails/pyspark/is_valid_email) - Check if email address has valid format
- [`is_valid_username()`](/primitives/emails/pyspark/is_valid_username) - Check if email username part is valid
- [`lowercase_domain()`](/primitives/emails/pyspark/lowercase_domain) - Convert only domain part to lowercase, preserve username case
- [`lowercase_email()`](/primitives/emails/pyspark/lowercase_email) - Convert entire email address to lowercase
- [`mask_email()`](/primitives/emails/pyspark/mask_email) - Mask email address for privacy (e
- [`normalize_gmail()`](/primitives/emails/pyspark/normalize_gmail) - Normalize Gmail addresses (remove dots, plus addressing, lowercase)
- [`remove_dots_from_gmail()`](/primitives/emails/pyspark/remove_dots_from_gmail) - Remove dots from Gmail addresses (Gmail ignores dots in usernames)
- [`remove_plus_addressing()`](/primitives/emails/pyspark/remove_plus_addressing) - Remove plus addressing from email (e
- [`remove_whitespace()`](/primitives/emails/pyspark/remove_whitespace) - Remove all whitespace from email address
- [`standardize_email()`](/primitives/emails/pyspark/standardize_email) - Apply standard email cleaning and normalization
