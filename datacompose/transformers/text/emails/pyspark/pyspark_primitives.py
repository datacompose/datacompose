import re
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    # For type checkers only - these imports are always available during type checking
    from pyspark.sql import Column
    from pyspark.sql import functions as F
else:
    # At runtime, handle missing PySpark gracefully
    try:
        from pyspark.sql import Column
        from pyspark.sql import functions as F
    except ImportError:
        # PySpark is not installed - functions will fail at runtime if called
        pass

try:
    # Try local utils import first (for generated code)
    from utils.primitives import PrimitiveRegistry
except ImportError:
    # Fall back to installed datacompose package
    from datacompose.operators.primitives import PrimitiveRegistry

emails = PrimitiveRegistry("emails")

# Common email domain typo mappings
DOMAIN_TYPO_MAPPINGS = {
    # Gmail typos
    "gmai.com": "gmail.com",
    "gmial.com": "gmail.com",
    "gmaill.com": "gmail.com",
    "gmail.co": "gmail.com",
    "gmail.cm": "gmail.com",
    "gmal.com": "gmail.com",
    "g-mail.com": "gmail.com",
    "gmailcom": "gmail.com",
    # Yahoo typos
    "yahooo.com": "yahoo.com",
    "yaho.com": "yahoo.com",
    "yahoo.co": "yahoo.com",
    "yahoo.cm": "yahoo.com",
    "yhoo.com": "yahoo.com",
    "ymail.co": "ymail.com",
    # Hotmail/Outlook typos
    "hotmial.com": "hotmail.com",
    "hotmall.com": "hotmail.com",
    "hotmai.com": "hotmail.com",
    "hotmail.co": "hotmail.com",
    "hotmail.cm": "hotmail.com",
    "hotmial.co.uk": "hotmail.co.uk",
    "outlok.com": "outlook.com",
    "outlook.co": "outlook.com",
    "outlookcom": "outlook.com",
    # AOL typos
    "aol.co": "aol.com",
    "aol.cm": "aol.com",
    "ao.com": "aol.com",
    # ISP typos
    "comcast.ent": "comcast.net",
    "verizon.ent": "verizon.net",
    "sbcglobal.ent": "sbcglobal.net",
    "att.ent": "att.net",
    "charter.ent": "charter.net",
    "cox.ent": "cox.net",
}

# TLD typo mappings
TLD_TYPO_MAPPINGS = {
    ".cmo": ".com",
    ".ocm": ".com",
    ".con": ".com",
    ".ent": ".net",
    ".nte": ".net",
    ".ten": ".net",
    ".rg": ".org",
    ".rog": ".org",
}


# ============================================================================
# Core Email Extraction Functions
# ============================================================================


@emails.register()
def extract_email(col: Column) -> Column:
    """
    Extract first valid email address from text.

    Args:
        col: Column containing text with potential email addresses

    Returns:
        Column with extracted email address or empty string
    """
    # Basic email pattern - captures most valid emails
    email_pattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.regexp_extract(col, email_pattern, 1)
    )


@emails.register()
def extract_all_emails(col: Column) -> Column:
    """
    Extract all email addresses from text as an array.

    Args:
        col: Column containing text with potential email addresses

    Returns:
        Column with array of email addresses
    """
    # Split by whitespace and common delimiters, then filter for email pattern
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

    # Split text and filter for email-like strings
    return F.expr(
        f"""
        filter(
            split(regexp_replace({col._jc}, '[,;\\s]+', ' '), ' '),
            x -> x rlike '{email_pattern}'
        )
    """
    )


@emails.register()
def extract_username(col: Column) -> Column:
    """
    Extract username (local part) from email address.

    Args:
        col: Column containing email address

    Returns:
        Column with username part or empty string
    """
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.regexp_extract(col, r"^([^@]+)@", 1)
    )


@emails.register()
def extract_domain(col: Column) -> Column:
    """
    Extract domain from email address.

    Args:
        col: Column containing email address

    Returns:
        Column with domain part or empty string
    """
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.regexp_extract(col, r"@([^@]+)$", 1)
    )


@emails.register()
def extract_domain_name(col: Column) -> Column:
    """
    Extract domain name without TLD from email address.

    Args:
        col: Column containing email address

    Returns:
        Column with domain name (e.g., "gmail" from "user@gmail.com")
    """
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.regexp_extract(col, r"@([^.@]+)\.", 1)
    )


@emails.register()
def extract_tld(col: Column) -> Column:
    """
    Extract top-level domain from email address.

    Args:
        col: Column containing email address

    Returns:
        Column with TLD (e.g., "com", "co.uk")
    """
    # This pattern captures everything after the last @ and first dot
    # Handles multi-part TLDs like co.uk, com.au, etc.
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.regexp_extract(col, r"@[^.@]+\.(.+)$", 1)
    )


# ============================================================================
# Email Validation Functions
# ============================================================================


@emails.register()
def is_valid_email(col: Column, min_length: int = 6, max_length: int = 254) -> Column:
    """
    Check if email address has valid format.

    Args:
        col: Column containing email address
        min_length: Minimum length for valid email
        max_length: Maximum length for valid email

    Returns:
        Column with boolean indicating validity
    """
    # RFC-compliant basic email validation
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

    # Extract username to check it separately
    username = extract_username(col)

    return F.when(col.isNull(), F.lit(False)).otherwise(
        col.rlike(email_pattern)
        & (F.length(col) >= F.lit(min_length))
        & (F.length(col) <= F.lit(max_length))
        & (F.length(username) <= F.lit(64))  # RFC 5321 username max length
        & ~col.rlike(r"\.\.")  # No consecutive dots anywhere
        & ~col.rlike(r"^\.")  # Doesn't start with dot
        & ~username.rlike(r"\.$")  # Username doesn't end with dot
        & ~col.rlike(r"\.@")  # No dot before @
    )


@emails.register()
def is_valid_username(col: Column, min_length: int = 1, max_length: int = 64) -> Column:
    """
    Check if email username part is valid.

    Args:
        col: Column containing email address
        min_length: Minimum length for valid username (default 1)
        max_length: Maximum length for valid username (default 64 per RFC)

    Returns:
        Column with boolean indicating username validity
    """
    username = extract_username(col)

    return (
        username.isNotNull()
        & (F.length(username) >= F.lit(min_length))
        & (F.length(username) <= F.lit(max_length))
        & ~username.rlike(r"^\.")  # Doesn't start with dot
        & ~username.rlike(r"\.$")  # Doesn't end with dot
        & ~username.rlike(r"\.\.")  # No consecutive dots
    )


@emails.register()
def is_valid_domain(col: Column) -> Column:
    """
    Check if email domain part is valid.

    Args:
        col: Column containing email address

    Returns:
        Column with boolean indicating domain validity
    """
    domain = extract_domain(col)

    return (
        domain.isNotNull()
        & (F.length(domain) > 0)
        & (F.length(domain) <= 253)
        & domain.rlike(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        & ~domain.rlike(r"^-")  # Doesn't start with hyphen
        & ~domain.rlike(r"-\.")  # No hyphen before dot
        & ~domain.rlike(r"\.\.")  # No consecutive dots
    )


@emails.register()
def has_plus_addressing(col: Column) -> Column:
    """
    Check if email uses plus addressing (e.g., user+tag@gmail.com).

    Args:
        col: Column containing email address

    Returns:
        Column with boolean indicating plus addressing usage
    """
    return F.when(col.isNull(), F.lit(False)).otherwise(col.rlike(r"^[^@]*\+[^@]*@"))


@emails.register()
def is_disposable_email(
    col: Column, disposable_domains: Optional[List[str]] = None
) -> Column:
    """
    Check if email is from a disposable email service.

    Args:
        col: Column containing email address
        disposable_domains: List of disposable domains to check against

    Returns:
        Column with boolean indicating if email is disposable
    """
    # Common disposable email domains
    default_disposable = [
        "10minutemail.com",
        "guerrillamail.com",
        "mailinator.com",
        "temp-mail.org",
        "throwaway.email",
        "yopmail.com",
        "tempmail.com",
        "trashmail.com",
        "getnada.com",
    ]

    domains_to_check = disposable_domains or default_disposable
    domain = extract_domain(col)

    # Check if domain is in disposable list
    conditions = F.lit(False)
    for disposable_domain in domains_to_check:
        conditions = conditions | (F.lower(domain) == disposable_domain.lower())

    return conditions


@emails.register()
def is_corporate_email(
    col: Column, free_providers: Optional[List[str]] = None
) -> Column:
    """
    Check if email appears to be from a corporate domain (not free email provider).

    Args:
        col: Column containing email address
        free_providers: List of free email provider domains to check against

    Returns:
        Column with boolean indicating if email is corporate

    Examples:
        # Use default free provider list
        df.withColumn("is_corp", emails.is_corporate_email(F.col("email")))

        # Add custom free providers to check
        custom_free = ["company-internal.com", "contractor-email.com"]
        df.withColumn("is_corp", emails.is_corporate_email(F.col("email"), custom_free))
    """
    # Common free email providers
    default_free_providers = [
        "gmail.com",
        "yahoo.com",
        "hotmail.com",
        "outlook.com",
        "aol.com",
        "icloud.com",
        "mail.com",
        "protonmail.com",
        "ymail.com",
        "live.com",
        "msn.com",
        "me.com",
    ]

    providers_to_check = (
        free_providers if free_providers is not None else default_free_providers
    )
    domain = extract_domain(col)

    # Check if domain is NOT in free provider list
    conditions = F.lit(True)
    for provider in providers_to_check:
        conditions = conditions & (F.lower(domain) != provider.lower())

    return F.when(domain.isNull() | (domain == ""), F.lit(False)).otherwise(conditions)


# ============================================================================
# Email Cleaning Functions
# ============================================================================


@emails.register()
def remove_whitespace(col: Column) -> Column:
    """
    Remove all whitespace from email address.

    Args:
        col: Column containing email address

    Returns:
        Column with whitespace removed
    """
    return F.when(col.isNull(), F.lit("")).otherwise(F.regexp_replace(col, r"\s+", ""))


@emails.register()
def lowercase_email(col: Column) -> Column:
    """
    Convert entire email address to lowercase.

    Args:
        col: Column containing email address

    Returns:
        Column with lowercased email
    """
    return F.when(col.isNull(), F.lit("")).otherwise(F.lower(col))


@emails.register()
def lowercase_domain(col: Column) -> Column:
    """
    Convert only domain part to lowercase, preserve username case.

    Args:
        col: Column containing email address

    Returns:
        Column with domain lowercased
    """
    username = extract_username(col)
    domain = extract_domain(col)

    return F.when(col.isNull() | ~col.contains("@"), col).otherwise(
        F.concat(username, F.lit("@"), F.lower(domain))
    )


@emails.register()
def remove_plus_addressing(col: Column) -> Column:
    """
    Remove plus addressing from email (e.g., user+tag@gmail.com -> user@gmail.com).

    Args:
        col: Column containing email address

    Returns:
        Column with plus addressing removed
    """
    return F.when(col.isNull(), F.lit("")).otherwise(
        F.regexp_replace(col, r"\+[^@]*(@)", "$1")
    )


@emails.register()
def remove_dots_from_gmail(col: Column) -> Column:
    """
    Remove dots from Gmail addresses (Gmail ignores dots in usernames).

    Args:
        col: Column containing email address

    Returns:
        Column with dots removed from Gmail usernames
    """
    username = extract_username(col)
    domain = extract_domain(col)

    # Only process Gmail addresses
    return (
        F.when(col.isNull() | ~col.contains("@"), col)
        .when(
            F.lower(domain).isin(["gmail.com", "googlemail.com"]),
            F.concat(F.regexp_replace(username, r"\.", ""), F.lit("@"), domain),
        )
        .otherwise(col)
    )


@emails.register()
def fix_common_typos(
    col: Column,
    custom_mappings: Optional[Dict[str, str]] = None,
    custom_tld_mappings: Optional[Dict[str, str]] = None,
) -> Column:
    """
    Fix common domain typos in email addresses.

    Args:
        col: Column containing email address
        custom_mappings: Additional domain mappings to apply (extends DOMAIN_TYPO_MAPPINGS)
        custom_tld_mappings: Additional TLD mappings to apply (extends TLD_TYPO_MAPPINGS)

    Returns:
        Column with typos fixed

    Examples:
        # Use default typo fixes
        df.withColumn("fixed", emails.fix_common_typos(F.col("email")))

        # Add custom domain typo mappings
        custom_domains = {
            "company.con": "company.com",
            "mycompany.co": "mycompany.com",
            "gmai.com": "gmail.com"  # Override default mapping
        }
        df.withColumn("fixed", emails.fix_common_typos(F.col("email"), custom_domains))

        # Add custom TLD mappings
        custom_tlds = {
            ".coom": ".com",
            ".nett": ".net"
        }
        df.withColumn("fixed", emails.fix_common_typos(
            F.col("email"),
            custom_tld_mappings=custom_tlds
        ))
    """
    domain = extract_domain(col)
    username = extract_username(col)

    # Combine default and custom mappings
    all_domain_mappings = {**DOMAIN_TYPO_MAPPINGS, **(custom_mappings or {})}
    all_tld_mappings = {**TLD_TYPO_MAPPINGS, **(custom_tld_mappings or {})}

    # Build case statement for all typo fixes
    fixed_domain = domain
    for typo, correct in all_domain_mappings.items():
        fixed_domain = F.when(
            F.lower(domain) == typo.lower(), F.lit(correct)
        ).otherwise(fixed_domain)

    # Also fix TLD typos
    for typo, correct in all_tld_mappings.items():
        fixed_domain = F.regexp_replace(fixed_domain, re.escape(typo) + r"$", correct)

    return F.when(col.isNull() | ~col.contains("@"), col).otherwise(
        F.concat(username, F.lit("@"), fixed_domain)
    )


# ============================================================================
# Email Standardization Functions
# ============================================================================


@emails.register()
def standardize_email(
    col: Column,
    lowercase: bool = True,
    remove_dots_gmail: bool = True,
    remove_plus: bool = False,
    fix_typos: bool = True,
) -> Column:
    """
    Apply standard email cleaning and normalization.

    Args:
        col: Column containing email address
        lowercase: Convert to lowercase
        remove_dots_gmail: Remove dots from Gmail addresses
        remove_plus: Remove plus addressing
        fix_typos: Fix common domain typos

    Returns:
        Column with standardized email
    """
    result = remove_whitespace(col)

    if fix_typos:
        result = fix_common_typos(result)

    if lowercase:
        result = lowercase_email(result)
    else:
        # At least lowercase the domain
        result = lowercase_domain(result)

    if remove_plus:
        result = remove_plus_addressing(result)

    if remove_dots_gmail:
        result = remove_dots_from_gmail(result)

    # Only return valid emails
    return F.when(is_valid_email(result), result).otherwise(F.lit(""))


@emails.register()
def normalize_gmail(col: Column) -> Column:
    """
    Normalize Gmail addresses (remove dots, plus addressing, lowercase).

    Args:
        col: Column containing email address

    Returns:
        Column with normalized Gmail address
    """
    domain = extract_domain(col)

    return F.when(
        F.lower(domain).isin(["gmail.com", "googlemail.com"]),
        standardize_email(
            col, lowercase=True, remove_dots_gmail=True, remove_plus=True
        ),
    ).otherwise(col)


@emails.register()
def get_canonical_email(col: Column) -> Column:
    """
    Get canonical form of email address for deduplication.
    Applies maximum normalization.

    Args:
        col: Column containing email address

    Returns:
        Column with canonical email form
    """
    return standardize_email(
        col, lowercase=True, remove_dots_gmail=True, remove_plus=True, fix_typos=True
    )


# ============================================================================
# Email Information Extraction
# ============================================================================


@emails.register()
def extract_name_from_email(col: Column) -> Column:
    """
    Attempt to extract person's name from email username.
    E.g., john.smith@example.com -> "John Smith"

    Args:
        col: Column containing email address

    Returns:
        Column with extracted name or empty string
    """
    username = extract_username(col)

    # Remove numbers and common prefixes/suffixes
    cleaned = F.regexp_replace(username, r"[0-9]+", "")
    cleaned = F.regexp_replace(
        cleaned, r"^(info|admin|support|sales|contact|hello|hi|hey)", ""
    )

    # Replace separators with spaces
    name = F.regexp_replace(cleaned, r"[._-]+", " ")

    # Capitalize words
    name = F.initcap(F.trim(name))

    # Only return if it looks like a name (has letters, reasonable length)
    return F.when(
        (F.length(name) >= 2) & (F.length(name) <= 50) & name.rlike(r"^[A-Za-z\s]+$"),
        name,
    ).otherwise(F.lit(""))


@emails.register()
def get_email_provider(col: Column) -> Column:
    """
    Get email provider name from domain.

    Args:
        col: Column containing email address

    Returns:
        Column with provider name
    """
    domain = extract_domain(col)

    # Map domains to provider names
    provider_mappings = {
        "gmail.com": "Gmail",
        "googlemail.com": "Gmail",
        "yahoo.com": "Yahoo",
        "ymail.com": "Yahoo",
        "hotmail.com": "Hotmail",
        "outlook.com": "Outlook",
        "live.com": "Outlook",
        "msn.com": "Outlook",
        "aol.com": "AOL",
        "icloud.com": "iCloud",
        "me.com": "iCloud",
        "mac.com": "iCloud",
        "protonmail.com": "ProtonMail",
        "proton.me": "ProtonMail",
    }

    result = F.lit("Other")
    for domain_str, provider in provider_mappings.items():
        result = F.when(F.lower(domain) == domain_str, F.lit(provider)).otherwise(
            result
        )

    return result


@emails.register()
def mask_email(col: Column, mask_char: str = "*", keep_chars: int = 3) -> Column:
    """
    Mask email address for privacy (e.g., joh***@gm***.com).

    Args:
        col: Column containing email address
        mask_char: Character to use for masking
        keep_chars: Number of characters to keep at start

    Returns:
        Column with masked email
    """
    username = extract_username(col)
    # domain = extract_domain(col)
    domain_name = extract_domain_name(col)
    tld = extract_tld(col)

    # Mask username (keep first few chars)
    masked_username = F.when(
        F.length(username) > keep_chars,
        F.concat(F.substring(username, 1, keep_chars), F.lit(mask_char * 3)),
    ).otherwise(F.lit(mask_char * 3))

    # Mask domain (keep first few chars)
    masked_domain_name = F.when(
        F.length(domain_name) > keep_chars,
        F.concat(F.substring(domain_name, 1, keep_chars), F.lit(mask_char * 3)),
    ).otherwise(F.lit(mask_char * 3))

    return F.when(col.isNull() | ~col.contains("@"), col).otherwise(
        F.concat(masked_username, F.lit("@"), masked_domain_name, F.lit("."), tld)
    )


# ============================================================================
# Email Filtering Functions
# ============================================================================


@emails.register()
def filter_valid_emails(col: Column) -> Column:
    """
    Return email only if valid, otherwise return null.

    Args:
        col: Column containing email address

    Returns:
        Column with valid email or null
    """
    return F.when(is_valid_email(col), col).otherwise(F.lit(None))


@emails.register()
def filter_corporate_emails(col: Column) -> Column:
    """
    Return email only if corporate, otherwise return null.

    Args:
        col: Column containing email address

    Returns:
        Column with corporate email or null
    """
    return F.when(is_corporate_email(col), col).otherwise(F.lit(None))


@emails.register()
def filter_non_disposable_emails(col: Column) -> Column:
    """
    Return email only if not disposable, otherwise return null.

    Args:
        col: Column containing email address

    Returns:
        Column with non-disposable email or null
    """
    return F.when(
        col.isNotNull() & (col != "") & ~is_disposable_email(col), col
    ).otherwise(F.lit(None))
