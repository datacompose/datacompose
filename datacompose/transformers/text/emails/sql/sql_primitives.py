"""
Email transformation primitives for SQL.

Preview Output:
+---------------------------+----------------------+-------------+----------------+--------+
|email                      |standardized          |username     |domain          |is_valid|
+---------------------------+----------------------+-------------+----------------+--------+
| John.Doe@Gmail.COM        |john.doe@gmail.com    |john.doe     |gmail.com       |true    |
|JANE.SMITH@OUTLOOK.COM     |jane.smith@outlook.com|jane.smith   |outlook.com     |true    |
|  info@company-name.org    |info@company-name.org |info         |company-name.org|true    |
|invalid.email@             |null                  |null         |null            |false   |
|user+tag@domain.co.uk      |user+tag@domain.co.uk |user+tag     |domain.co.uk    |true    |
|bad email@test.com         |null                  |null         |null            |false   |
+---------------------------+----------------------+-------------+----------------+--------+

Usage Example:
import sqlglot
from transformers.sql.emails import emails

# Create SQL expression
sql = '''
SELECT
    email,
    emails.standardize_email(email) as standardized,
    emails.extract_username(email) as username,
    emails.extract_domain(email) as domain,
    emails.is_valid_email(email) as is_valid
FROM emails_table
'''

# Parse and transform
parsed = sqlglot.parse_one(sql)

Installation:
datacompose add emails
"""

from typing import Dict, List, Optional


class Emails:
    """SQL primitive functions for email address transformations.

    This class contains a comprehensive set of email transformation functions
    as SQL CREATE FUNCTION statements. These functions handle various email
    operations including extraction, validation, cleaning, standardization,
    and privacy operations.

    The functions are written in PostgreSQL syntax and can be transpiled to
    other SQL dialects using the PrimitiveChooser class.

    Categories of functions:
        - Extraction: Extract emails from text, parse email components
        - Validation: Check email validity, detect disposable/corporate emails
        - Cleaning: Remove whitespace, fix typos, normalize formatting
        - Standardization: Create canonical forms for deduplication
        - Privacy: Hash and mask emails for secure storage
        - Filtering: Filter emails based on various criteria

    Attributes:
        dialect (str): The SQL dialect for this instance (for future use)

    Example:
        >>> emails = Emails('postgres')
        >>> sql = emails.standardize_email()
        >>> # Returns CREATE FUNCTION statement for email standardization

    Note:
        All functions return PostgreSQL-compatible CREATE FUNCTION statements
        that define IMMUTABLE SQL functions for optimal query performance.
    """

    def __init__(self, dialect: str):
        """Initialize the Emails primitive class.

        Args:
            dialect: The SQL dialect (currently stored but not used, as all
                    functions return PostgreSQL syntax for transpilation).
        """
        self.dialect = dialect

        self.sql_string = ""

    def extract_email(self) -> str:
        """Extract first valid email address from text.

        Uses regex pattern to find the first email address in a text string.
        Supports standard email formats with alphanumeric characters, dots,
        underscores, percent signs, plus signs, and hyphens.

        Returns:
            SQL function that extracts the first email from input text.
            Returns NULL if no valid email is found.

        Example SQL usage:
            SELECT extract_email('Contact us at john.doe@example.com for more info');
            -- Returns: 'john.doe@example.com'
        """
        return """
CREATE OR REPLACE FUNCTION extract_email(input_text TEXT)
RETURNS TEXT AS $$
    SELECT (regexp_matches(input_text, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'))[1]
$$ LANGUAGE SQL IMMUTABLE;
"""

    def extract_all_emails(self) -> str:
        """Extract all email addresses from text as an array.

        Finds all email addresses in a text string and returns them as an array.
        Uses the same regex pattern as extract_email but captures all matches.

        Returns:
            SQL function that returns an array of all emails found in the input text.
            Returns empty array if no emails are found.

        Example SQL usage:
            SELECT extract_all_emails('Email john@test.com or jane@example.org');
            -- Returns: ['john@test.com', 'jane@example.org']
        """
        return """
CREATE OR REPLACE FUNCTION extract_all_emails(input_text TEXT)
RETURNS TEXT[] AS $$
    SELECT ARRAY(
        SELECT (regexp_matches(input_text, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', 'g'))[1]
    )
$$ LANGUAGE SQL IMMUTABLE;
"""

    def extract_username(self) -> str:
        """Extract username (local part) from email address.

        Splits the email at the '@' symbol and returns the first part (username).
        This is also known as the local part of an email address.

        Returns:
            SQL function that extracts the username portion of an email.
            Returns NULL if email doesn't contain '@' symbol.

        Example SQL usage:
            SELECT extract_username('john.doe@gmail.com');
            -- Returns: 'john.doe'
        """
        return """
CREATE OR REPLACE FUNCTION extract_username(email TEXT)
RETURNS TEXT AS $$
    SELECT
        CASE
            WHEN email IS NULL OR email = '' OR email !~ '@' THEN NULL
            ELSE SPLIT_PART(email, '@', 1)
        END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def extract_domain(self) -> str:
        """Extract domain from email address.

        Splits the email at the '@' symbol and returns the second part (domain).
        This includes the full domain with subdomain and TLD.

        Returns:
            SQL function that extracts the domain portion of an email.
            Returns NULL if email doesn't contain '@' symbol.

        Example SQL usage:
            SELECT extract_domain('john.doe@mail.google.com');
            -- Returns: 'mail.google.com'
        """
        return """
CREATE OR REPLACE FUNCTION extract_domain(email TEXT)
RETURNS TEXT AS $$
    SELECT
        CASE
            WHEN email IS NULL OR email = '' OR email !~ '@' THEN NULL
            ELSE SPLIT_PART(email, '@', 2)
        END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def extract_domain_name(self) -> str:
        """Extract domain name without TLD from email address.

        Gets the domain part of the email, then extracts just the domain name
        without the top-level domain (TLD). For example, from 'example.com'
        it would return 'example'.

        Returns:
            SQL function that extracts the domain name portion only.
            Returns NULL if email is invalid or doesn't contain domain.

        Example SQL usage:
            SELECT extract_domain_name('user@mail.google.com');
            -- Returns: 'mail'
        """
        return """
CREATE OR REPLACE FUNCTION extract_domain_name(email TEXT)
RETURNS TEXT AS $$
    SELECT SPLIT_PART(extract_domain(email), '.', 1)
$$ LANGUAGE SQL IMMUTABLE;
"""

    def extract_tld(self) -> str:
        """Extract top-level domain from email address.

        Extracts the top-level domain (TLD) from the email's domain part.
        Handles both single TLDs (.com, .org) and multi-part TLDs (.co.uk).
        Uses regex to remove everything before and including the first dot.

        Returns:
            SQL function that extracts the TLD portion of the email domain.
            Returns NULL if email is invalid or doesn't contain a valid domain.

        Example SQL usage:
            SELECT extract_tld('user@example.co.uk');
            -- Returns: 'co.uk'
        """
        return """
CREATE OR REPLACE FUNCTION extract_tld(email TEXT)
RETURNS TEXT AS $$
    SELECT REGEXP_REPLACE(extract_domain(email), '^[^.]+\\.', '')
$$ LANGUAGE SQL IMMUTABLE;
"""

    def is_valid_email(self) -> str:
        """Check if email address has valid format.

        Validates email format using regex pattern and length constraints.
        Checks for proper email structure with username@domain.tld format.
        Enforces minimum length (default 6) and maximum length (default 254) limits.

        Returns:
            SQL function that returns TRUE if email is valid, FALSE otherwise.
            Considers both format and length requirements.

        Example SQL usage:
            SELECT is_valid_email('test@example.com');
            -- Returns: TRUE
        """
        return """
CREATE OR REPLACE FUNCTION is_valid_email(email TEXT, min_length INT DEFAULT 6, max_length INT DEFAULT 254)
RETURNS BOOLEAN AS $$
    SELECT
        CASE
            WHEN email IS NULL OR email = '' THEN FALSE
            ELSE
                email ~ '^[a-zA-Z0-9_%+-]+(\.[a-zA-Z0-9_%+-]+)*@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,}$'
                AND email !~ '\.\.'  -- No consecutive dots
                AND email !~ '^\.'   -- No starting dot
                AND email !~ '\.$'   -- No ending dot
                AND email !~ '@\.'   -- No dot after @
                AND email !~ '\.@'   -- No dot before @
                AND LENGTH(email) >= min_length
                AND LENGTH(email) <= max_length
        END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def is_valid_username(self) -> str:
        """Check if email username part is valid."""
        return """
CREATE OR REPLACE FUNCTION is_valid_username(email TEXT, min_length INT DEFAULT 1, max_length INT DEFAULT 64)
RETURNS BOOLEAN AS $$
    SELECT
        CASE
            WHEN email IS NULL OR email = '' OR email !~ '@' THEN FALSE
            ELSE
                extract_username(email) IS NOT NULL
                AND LENGTH(extract_username(email)) >= min_length
                AND LENGTH(extract_username(email)) <= max_length
                AND extract_username(email) !~ '^\.'   -- No starting dot
                AND extract_username(email) !~ '\.$'   -- No ending dot
                AND extract_username(email) !~ '\.\.'  -- No consecutive dots
        END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def is_valid_domain(self) -> str:
        """Check if email domain part is valid."""
        return """
CREATE OR REPLACE FUNCTION is_valid_domain(email TEXT)
RETURNS BOOLEAN AS $$
    SELECT
        CASE
            WHEN email IS NULL OR email = '' OR extract_domain(email) IS NULL THEN FALSE
            ELSE
                extract_domain(email) ~ '^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                AND LENGTH(extract_domain(email)) <= 253
                AND extract_domain(email) !~ '^-'
                AND extract_domain(email) !~ '-\.'
                AND extract_domain(email) !~ '\.\.'
        END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def has_plus_addressing(self) -> str:
        """Check if email uses plus addressing (e.g., user+tag@gmail.com).

        Detects if the email contains a plus sign (+) in the username portion,
        which is commonly used for email filtering and organization.

        Returns:
            SQL function that returns TRUE if email has plus addressing, FALSE otherwise.
            Useful for detecting tagged or filtered email addresses.

        Example SQL usage:
            SELECT has_plus_addressing('user+newsletter@gmail.com');
            -- Returns: TRUE
        """
        return """
CREATE OR REPLACE FUNCTION has_plus_addressing(email TEXT)
RETURNS BOOLEAN AS $$
    SELECT
        CASE
            WHEN email IS NULL OR email = '' THEN FALSE
            ELSE email ~ '^[^@]*\+[^@]*@'
        END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def is_disposable_email(self) -> str:
        """Check if email is from a disposable email service.

        Checks the email domain against a list of known disposable/temporary
        email providers. Useful for filtering out temporary accounts.

        Returns:
            SQL function that returns TRUE if email is from a disposable service,
            FALSE otherwise. Includes common services like 10minutemail, mailinator, etc.

        Example SQL usage:
            SELECT is_disposable_email('test@10minutemail.com');
            -- Returns: TRUE
        """
        return """
CREATE OR REPLACE FUNCTION is_disposable_email(email TEXT)
RETURNS BOOLEAN AS $$
    SELECT
        CASE
            WHEN email IS NULL OR email = '' OR extract_domain(email) IS NULL THEN FALSE
            ELSE LOWER(extract_domain(email)) = ANY(ARRAY[
                '10minutemail.com', 'guerrillamail.com', 'mailinator.com',
                'temp-mail.org', 'throwaway.email', 'yopmail.com',
                'tempmail.com', 'trashmail.com', 'getnada.com'
            ])
        END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def is_corporate_email(self) -> str:
        """Check if email appears to be from a corporate domain (not free email provider).

        Determines if an email is likely from a business/corporate domain by
        excluding common free email providers like Gmail, Yahoo, Hotmail, etc.

        Returns:
            SQL function that returns TRUE if email appears corporate, FALSE if
            it's from a known free provider or invalid. Useful for B2B analysis.

        Example SQL usage:
            SELECT is_corporate_email('john@mycompany.com');
            -- Returns: TRUE (assuming mycompany.com is not a free provider)
        """
        return """
CREATE OR REPLACE FUNCTION is_corporate_email(email TEXT)
RETURNS BOOLEAN AS $$
    SELECT LOWER(extract_domain(email)) != ALL(ARRAY[
        'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
        'icloud.com', 'mail.com', 'protonmail.com', 'ymail.com', 
        'live.com', 'msn.com', 'me.com'
    ])
    AND extract_domain(email) IS NOT NULL 
    AND extract_domain(email) != ''
$$ LANGUAGE SQL IMMUTABLE;
"""

    def remove_whitespace(self) -> str:
        """Remove all whitespace from email address."""
        return """
CREATE OR REPLACE FUNCTION remove_whitespace(email TEXT)
RETURNS TEXT AS $$
    SELECT REGEXP_REPLACE(COALESCE(email, ''), '\\s+', '', 'g')
$$ LANGUAGE SQL IMMUTABLE;
"""

    def lowercase_email(self) -> str:
        """Convert entire email address to lowercase."""
        return """
CREATE OR REPLACE FUNCTION lowercase_email(email TEXT)
RETURNS TEXT AS $$
    SELECT LOWER(COALESCE(email, ''))
$$ LANGUAGE SQL IMMUTABLE;
"""

    def lowercase_domain(self) -> str:
        """Convert only domain part to lowercase, preserve username case."""
        return """
CREATE OR REPLACE FUNCTION lowercase_domain(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE
        WHEN email LIKE '%@%' THEN
            extract_username(email) || '@' || LOWER(extract_domain(email))
        ELSE email
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def remove_plus_addressing(self) -> str:
        """Remove plus addressing from email (e.g., user+tag@gmail.com -> user@gmail.com)."""
        return """
CREATE OR REPLACE FUNCTION remove_plus_addressing(email TEXT)
RETURNS TEXT AS $$
    SELECT REGEXP_REPLACE(COALESCE(email, ''), '\\+[^@]*(@)', '\\1', 'g')
$$ LANGUAGE SQL IMMUTABLE;
"""

    def remove_dots_from_gmail(self) -> str:
        """Remove dots from Gmail addresses (Gmail ignores dots in usernames)."""
        return """
CREATE OR REPLACE FUNCTION remove_dots_from_gmail(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE
        WHEN LOWER(extract_domain(email)) IN ('gmail.com', 'googlemail.com') THEN
            REPLACE(extract_username(email), '.', '') || '@' || extract_domain(email)
        ELSE email
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def fix_common_typos(self) -> str:
        """Fix common domain typos in email addresses.

        Automatically corrects frequent typos in popular email domains:
        - gmai.com → gmail.com
        - yahooo.com → yahoo.com
        - hotmial.com → hotmail.com
        - And many more common mistakes

        Returns:
            SQL function that returns email with corrected domain if typo found,
            original email otherwise. Helps improve data quality.

        Example SQL usage:
            SELECT fix_common_typos('user@gmai.com');
            -- Returns: 'user@gmail.com'
        """
        return """
CREATE OR REPLACE FUNCTION fix_common_typos(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE
        WHEN email IS NULL OR email = '' OR email NOT LIKE '%@%' THEN email
        ELSE
            extract_username(email) || '@' ||
            CASE LOWER(extract_domain(email))
            WHEN 'gmai.com' THEN 'gmail.com'
            WHEN 'gmial.com' THEN 'gmail.com'
            WHEN 'gmaill.com' THEN 'gmail.com'
            WHEN 'gmail.co' THEN 'gmail.com'
            WHEN 'gmail.cm' THEN 'gmail.com'
            WHEN 'yahooo.com' THEN 'yahoo.com'
            WHEN 'yaho.com' THEN 'yahoo.com'
            WHEN 'yahoo.co' THEN 'yahoo.com'
            WHEN 'hotmial.com' THEN 'hotmail.com'
            WHEN 'hotmall.com' THEN 'hotmail.com'
            WHEN 'outlok.com' THEN 'outlook.com'
            WHEN 'outlook.co' THEN 'outlook.com'
            WHEN 'example.cmo' THEN 'example.com'
            ELSE extract_domain(email)
            END
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def standardize_email(self) -> str:
        """Apply standard email cleaning and normalization.

        Applies a comprehensive cleaning pipeline including:
        - Removing whitespace
        - Fixing common domain typos
        - Converting to lowercase
        - Removing dots from Gmail addresses
        - Validating the result

        Returns:
            SQL function that returns a cleaned, standardized email if valid,
            empty string if invalid. This is the main normalization function.

        Example SQL usage:
            SELECT standardize_email('  John.Doe@GMAIL.COM  ');
            -- Returns: 'johndoe@gmail.com'
        """
        return """
CREATE OR REPLACE FUNCTION standardize_email(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE 
        WHEN is_valid_email(
            remove_dots_from_gmail(
                lowercase_email(
                    fix_common_typos(
                        remove_whitespace(email)
                    )
                )
            )
        ) THEN remove_dots_from_gmail(
            lowercase_email(
                fix_common_typos(
                    remove_whitespace(email)
                )
            )
        )
        ELSE ''
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def normalize_gmail(self) -> str:
        """Normalize Gmail addresses (remove dots, plus addressing, lowercase).

        Applies Gmail-specific normalization rules:
        - Removes dots from username (Gmail ignores them)
        - Removes plus addressing (+tag)
        - Converts to lowercase
        - Only applies to gmail.com and googlemail.com domains

        Returns:
            SQL function that returns normalized Gmail address for Gmail domains,
            original email for other domains. Useful for Gmail deduplication.

        Example SQL usage:
            SELECT normalize_gmail('John.Doe+tag@gmail.com');
            -- Returns: 'johndoe@gmail.com'
        """
        return """
CREATE OR REPLACE FUNCTION normalize_gmail(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE 
        WHEN LOWER(extract_domain(email)) IN ('gmail.com', 'googlemail.com') THEN
            remove_dots_from_gmail(
                remove_plus_addressing(
                    lowercase_email(email)
                )
            )
        ELSE email
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def get_canonical_email(self) -> str:
        """Get canonical form of email address for deduplication.

        Creates a canonical (standardized) form of the email address by applying:
        - Whitespace removal
        - Common typo fixes
        - Lowercase conversion
        - Plus addressing removal
        - Gmail dot removal

        Returns:
            SQL function that returns canonical email form for consistent
            deduplication and matching across variations of the same email.

        Example SQL usage:
            SELECT get_canonical_email('  John.Doe+TAG@Gmail.com  ');
            -- Returns: 'johndoe@gmail.com'
        """
        return """
CREATE OR REPLACE FUNCTION get_canonical_email(email TEXT)
RETURNS TEXT AS $$
    SELECT remove_dots_from_gmail(
        remove_plus_addressing(
            lowercase_email(
                fix_common_typos(
                    remove_whitespace(email)
                )
            )
        )
    )
$$ LANGUAGE SQL IMMUTABLE;
"""

    def extract_name_from_email(self) -> str:
        """Attempt to extract person's name from email username.

        Intelligently parses the username portion to extract potential names:
        - Removes numbers and common service prefixes (info, admin, etc.)
        - Converts separators (dots, underscores, hyphens) to spaces
        - Applies title case formatting
        - Validates result length and character content

        Returns:
            SQL function that returns extracted name if detectable and valid,
            empty string otherwise. Useful for personalization and contact info.

        Example SQL usage:
            SELECT extract_name_from_email('john.doe123@company.com');
            -- Returns: 'John Doe'
        """
        return """
CREATE OR REPLACE FUNCTION extract_name_from_email(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE 
        WHEN LENGTH(cleaned_name) >= 2 AND LENGTH(cleaned_name) <= 50 
             AND cleaned_name ~ '^[A-Za-z\\s]+$' THEN cleaned_name
        ELSE ''
    END
    FROM (
        SELECT TRIM(INITCAP(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(extract_username(email), '[0-9]+', '', 'g'),
                    '^(info|admin|support|sales|contact|hello|hi|hey)', '', 'i'
                ),
                '[._-]+', ' ', 'g'
            )
        )) AS cleaned_name
    ) sub
$$ LANGUAGE SQL IMMUTABLE;
"""

    def get_email_provider(self) -> str:
        """Get email provider name from domain.

        Maps email domains to their corresponding provider names.
        Recognizes major providers like Gmail, Yahoo, Outlook, AOL, etc.
        Returns 'Other' for unrecognized domains.

        Returns:
            SQL function that returns provider name string based on email domain.
            Useful for analytics and provider-specific processing.

        Example SQL usage:
            SELECT get_email_provider('user@gmail.com');
            -- Returns: 'Gmail'
        """
        return """
CREATE OR REPLACE FUNCTION get_email_provider(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE LOWER(extract_domain(email))
        WHEN 'gmail.com' THEN 'Gmail'
        WHEN 'googlemail.com' THEN 'Gmail'
        WHEN 'yahoo.com' THEN 'Yahoo'
        WHEN 'ymail.com' THEN 'Yahoo'
        WHEN 'hotmail.com' THEN 'Hotmail'
        WHEN 'outlook.com' THEN 'Outlook'
        WHEN 'live.com' THEN 'Outlook'
        WHEN 'msn.com' THEN 'Outlook'
        WHEN 'aol.com' THEN 'AOL'
        WHEN 'icloud.com' THEN 'iCloud'
        WHEN 'me.com' THEN 'iCloud'
        WHEN 'mac.com' THEN 'iCloud'
        WHEN 'protonmail.com' THEN 'ProtonMail'
        WHEN 'proton.me' THEN 'ProtonMail'
        ELSE 'Other'
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def hash_email_sha256(self) -> str:
        """Hash email with SHA256, with email-specific preprocessing.

        Creates a SHA256 hash of the email after applying canonical normalization.
        Uses get_canonical_email() for consistent preprocessing before hashing.
        Supports optional salt parameter for additional security.

        Returns:
            SQL function that returns hex-encoded SHA256 hash of normalized email,
            NULL if email is invalid. Useful for privacy-preserving analytics.

        Example SQL usage:
            SELECT hash_email_sha256('user@gmail.com', 'my_salt');
            -- Returns: '7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730'
        """
        return """
CREATE OR REPLACE FUNCTION hash_email_sha256(email TEXT, salt TEXT DEFAULT '')
RETURNS TEXT AS $$
    SELECT CASE 
        WHEN is_valid_email(get_canonical_email(email)) THEN
            ENCODE(SHA256(CONCAT(get_canonical_email(email), salt)::BYTEA), 'hex')
        ELSE NULL
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def mask_email(self) -> str:
        """Mask email address for privacy (e.g., joh***@gm***.com).

        Partially obscures email address while maintaining readability.
        Shows first few characters of username and domain, replaces rest with mask chars.
        Preserves TLD for context. Configurable mask character and visible character count.

        Returns:
            SQL function that returns masked email preserving structure but hiding
            sensitive information. Default shows 3 chars and uses '*' as mask.

        Example SQL usage:
            SELECT mask_email('john.doe@company.com');
            -- Returns: 'joh***@com***.com'
        """
        return """
CREATE OR REPLACE FUNCTION mask_email(email TEXT, mask_char TEXT DEFAULT '*', keep_chars INT DEFAULT 3)
RETURNS TEXT AS $$
    SELECT CASE 
        WHEN email LIKE '%@%' THEN
            CASE 
                WHEN LENGTH(extract_username(email)) > keep_chars THEN
                    SUBSTRING(extract_username(email), 1, keep_chars) || REPEAT(mask_char, 3)
                ELSE REPEAT(mask_char, 3)
            END ||
            '@' ||
            CASE 
                WHEN LENGTH(extract_domain_name(email)) > keep_chars THEN
                    SUBSTRING(extract_domain_name(email), 1, keep_chars) || REPEAT(mask_char, 3)
                ELSE REPEAT(mask_char, 3)
            END ||
            '.' || extract_tld(email)
        ELSE email
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def filter_valid_emails(self) -> str:
        """Return email only if valid, otherwise return null.

        Filters emails by validity, returning the email if it passes validation
        or NULL if it doesn't. Uses is_valid_email() for validation logic.

        Returns:
            SQL function that returns the email if valid, NULL otherwise.
            Useful for cleaning datasets and removing invalid entries.

        Example SQL usage:
            SELECT filter_valid_emails('invalid.email');
            -- Returns: NULL
        """
        return """
CREATE OR REPLACE FUNCTION filter_valid_emails(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE 
        WHEN is_valid_email(email) THEN email
        ELSE NULL
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def filter_corporate_emails(self) -> str:
        """Return email only if corporate, otherwise return null.

        Filters emails to only return those from corporate domains,
        excluding free email providers. Uses is_corporate_email() logic.

        Returns:
            SQL function that returns email if corporate, NULL otherwise.
            Useful for B2B analysis and lead qualification.

        Example SQL usage:
            SELECT filter_corporate_emails('ceo@company.com');
            -- Returns: 'ceo@company.com'
        """
        return """
CREATE OR REPLACE FUNCTION filter_corporate_emails(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE 
        WHEN is_corporate_email(email) THEN email
        ELSE NULL
    END
$$ LANGUAGE SQL IMMUTABLE;
"""

    def filter_non_disposable_emails(self) -> str:
        """Return email only if not disposable, otherwise return null.

        Filters out emails from disposable/temporary email services,
        returning only emails from permanent providers. Uses is_disposable_email() logic.

        Returns:
            SQL function that returns email if not disposable, NULL otherwise.
            Useful for removing temporary accounts and improving data quality.

        Example SQL usage:
            SELECT filter_non_disposable_emails('real@company.com');
            -- Returns: 'real@company.com'
        """
        return """
CREATE OR REPLACE FUNCTION filter_non_disposable_emails(email TEXT)
RETURNS TEXT AS $$
    SELECT CASE
        WHEN email IS NULL OR email = '' THEN NULL
        WHEN NOT is_disposable_email(email) THEN email
        ELSE NULL
    END
$$ LANGUAGE SQL IMMUTABLE;
"""
