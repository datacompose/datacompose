---
title: Email Primitives - PySpark Methods
description: All available PySpark methods for Email Primitives
---

# Email Primitives - PySpark Methods

## Available Methods (27 total)


### Core Methods

- [`extract_all_emails()`](./extract_all_emails.md) - Extract all email addresses from text as an array
- [`extract_domain()`](./extract_domain.md) - Extract domain from email address
- [`extract_domain_name()`](./extract_domain_name.md) - Extract domain name without TLD from email address
- [`extract_email()`](./extract_email.md) - Extract first valid email address from text
- [`extract_name_from_email()`](./extract_name_from_email.md) - Attempt to extract person's name from email username
- [`extract_tld()`](./extract_tld.md) - Extract top-level domain from email address
- [`extract_username()`](./extract_username.md) - Extract username (local part) from email address
- [`filter_corporate_emails()`](./filter_corporate_emails.md) - Return email only if corporate, otherwise return null
- [`filter_non_disposable_emails()`](./filter_non_disposable_emails.md) - Return email only if not disposable, otherwise return null
- [`filter_valid_emails()`](./filter_valid_emails.md) - Return email only if valid, otherwise return null
- [`fix_common_typos()`](./fix_common_typos.md) - Fix common domain typos in email addresses
- [`get_canonical_email()`](./get_canonical_email.md) - Get canonical form of email address for deduplication
- [`get_email_provider()`](./get_email_provider.md) - Get email provider name from domain
- [`has_plus_addressing()`](./has_plus_addressing.md) - Check if email uses plus addressing (e
- [`is_corporate_email()`](./is_corporate_email.md) - Check if email appears to be from a corporate domain (not free email provider)
- [`is_disposable_email()`](./is_disposable_email.md) - Check if email is from a disposable email service
- [`is_valid_domain()`](./is_valid_domain.md) - Check if email domain part is valid
- [`is_valid_email()`](./is_valid_email.md) - Check if email address has valid format
- [`is_valid_username()`](./is_valid_username.md) - Check if email username part is valid
- [`lowercase_domain()`](./lowercase_domain.md) - Convert only domain part to lowercase, preserve username case
- [`lowercase_email()`](./lowercase_email.md) - Convert entire email address to lowercase
- [`mask_email()`](./mask_email.md) - Mask email address for privacy (e
- [`normalize_gmail()`](./normalize_gmail.md) - Normalize Gmail addresses (remove dots, plus addressing, lowercase)
- [`remove_dots_from_gmail()`](./remove_dots_from_gmail.md) - Remove dots from Gmail addresses (Gmail ignores dots in usernames)
- [`remove_plus_addressing()`](./remove_plus_addressing.md) - Remove plus addressing from email (e
- [`remove_whitespace()`](./remove_whitespace.md) - Remove all whitespace from email address
- [`standardize_email()`](./standardize_email.md) - Apply standard email cleaning and normalization
