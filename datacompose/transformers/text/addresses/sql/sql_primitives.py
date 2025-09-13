"""
SQL Primitives for Addresses
"""

from typing import Dict, List, Optional
import sqlglotrs


class Addresses:
    def __init__(self, dialect: str):
        self.dialect = dialect

    def extract_street_number(self, col) -> str:
        """Extract street/house number from address."""
        pass

    def extract_street_prefix(self, col) -> str:
        """Extract directional prefix from street address."""
        pass

    def extract_street_name(self, col) -> str:
        """Extract street name from address."""
        pass

    def extract_street_suffix(self, col) -> str:
        """Extract street type/suffix from address."""
        pass

    def extract_full_street(self, col) -> str:
        """Extract complete street address (number + prefix + name + suffix)."""
        pass

    def standardize_street_prefix(
        self, col, custom_mappings: Optional[Dict[str, str]] = None
    ) -> str:
        """Standardize street directional prefixes to abbreviated form."""
        pass

    def standardize_street_suffix(
        self, col, custom_mappings: Optional[Dict[str, str]] = None
    ) -> str:
        """Standardize street type/suffix to USPS abbreviated form."""
        pass

    def extract_apartment_number(self, col) -> str:
        """Extract apartment/unit number from address."""
        pass

    def extract_floor(self, col) -> str:
        """Extract floor number from address."""
        pass

    def extract_building(self, col) -> str:
        """Extract building name or identifier from address."""
        pass

    def extract_unit_type(self, col) -> str:
        """Extract the type of unit (Apt, Suite, Unit, etc.) from address."""
        pass

    def standardize_unit_type(
        self, col, custom_mappings: Optional[Dict[str, str]] = None
    ) -> str:
        """Standardize unit type to common abbreviations."""
        pass

    def extract_secondary_address(self, col) -> str:
        """Extract complete secondary address information (unit type + number)."""
        pass

    def has_apartment(self, col) -> str:
        """Check if address contains apartment/unit information."""
        pass

    def remove_secondary_address(self, col) -> str:
        """Remove apartment/suite/unit information from address."""
        pass

    def extract_zip_code(self, col) -> str:
        """Extract US ZIP code (5-digit or ZIP+4 format) from text."""
        pass

    def validate_zip_code(self, col) -> str:
        """Validate if a ZIP code is in correct US format."""
        pass

    def is_valid_zip_code(self, col) -> str:
        """Alias for validate_zip_code for consistency."""
        pass

    def standardize_zip_code(self, col) -> str:
        """Standardize ZIP code format."""
        pass

    def get_zip_code_type(self, col) -> str:
        """Determine the type of ZIP code."""
        pass

    def split_zip_code(self, col) -> str:
        """Split ZIP+4 code into base and extension components."""
        pass

    def extract_city(self, col, custom_cities: Optional[List] = None) -> str:
        """Extract city name from US address text."""
        pass

    def extract_state(self, col, custom_states: Optional[Dict] = None) -> str:
        """Extract and standardize state to 2-letter abbreviation."""
        pass

    def validate_city(
        self,
        col,
        known_cities: Optional[List] = None,
        min_length: int = 2,
        max_length: int = 50,
    ) -> str:
        """Validate if a city name appears valid."""
        pass

    def validate_state(self, col) -> str:
        """Validate if state code is a valid US state abbreviation."""
        pass

    def standardize_city(self, col, custom_mappings: Optional[Dict] = None) -> str:
        """Standardize city name formatting."""
        pass

    def standardize_state(self, col) -> str:
        """Convert state to standard 2-letter format."""
        pass

    def get_state_name(self, col) -> str:
        """Convert state abbreviation to full name."""
        pass

    def extract_country(self, col) -> str:
        """Extract country from address."""
        pass

    def has_country(self, col) -> str:
        """Check if address contains country information."""
        pass

    def remove_country(self, col) -> str:
        """Remove country from address."""
        pass

    def standardize_country(self, col, custom_mappings: Optional[dict] = None) -> str:
        """Standardize country name to consistent format."""
        pass

    def extract_po_box(self, col) -> str:
        """Extract PO Box number from address."""
        pass

    def has_po_box(self, col) -> str:
        """Check if address contains PO Box."""
        pass

    def is_po_box_only(self, col) -> str:
        """Check if address is ONLY a PO Box (no street address)."""
        pass

    def remove_po_box(self, col) -> str:
        """Remove PO Box from address."""
        pass

    def standardize_po_box(self, col) -> str:
        """Standardize PO Box format to consistent representation."""
        pass

    def extract_private_mailbox(self, col) -> str:
        """Extract private mailbox (PMB) number from address."""
        pass
