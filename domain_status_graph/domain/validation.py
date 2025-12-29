"""
Domain validation and normalization using tldextract.

This module provides centralized domain validation and normalization functions
used across the codebase. All domain validation should use these functions
to ensure consistency.

Uses tldextract with the Public Suffix List for authoritative TLD validation.
"""

import re

import tldextract

# Known taxonomy domains to exclude from website extraction
# These are infrastructure domains, not company websites
KNOWN_TAXONOMY_ROOTS = {
    "sec.gov",
    "xbrl.org",
    "fasb.org",
    "w3.org",
    "xbrl.us",
    "xbrl.sec.gov",
    "edgar",
    "html",
    "xml",
}


def is_infrastructure_domain(domain: str) -> bool:
    """
    Check if domain is infrastructure (sec.gov, xbrl.org, data source domains, etc.).

    This is domain collection-specific logic to filter out infrastructure
    domains that shouldn't be returned as company domains.

    Args:
        domain: Domain string to check

    Returns:
        True if domain is infrastructure, False otherwise
    """
    # Check against known taxonomy roots
    if domain.lower() in KNOWN_TAXONOMY_ROOTS:
        return True

    # Additional infrastructure patterns
    infrastructure_patterns = [
        r"\.gov$",  # All .gov domains
        r"gaap\.org",
    ]

    # Check patterns
    if any(re.search(pattern, domain, re.IGNORECASE) for pattern in infrastructure_patterns):
        return True

    # Additional known infrastructure domains (data source domains)
    known_infrastructure = {
        "finviz.com",
        "yahoo.com",
        "google.com",  # Don't return these as company domains
    }

    return domain.lower() in known_infrastructure


def is_valid_domain(domain: str) -> bool:
    """
    Validate that a string is a real, valid domain using tldextract.

    Requirements:
    - Has valid TLD (checked against Public Suffix List)
    - Domain part is at least 2 characters
    - Total length reasonable (< 255 chars per RFC)
    - Not a known taxonomy/infrastructure domain

    Args:
        domain: Domain string to validate

    Returns:
        True if domain is valid, False otherwise
    """
    if not domain or len(domain) > 255:
        return False

    try:
        # Remove protocol and www if present
        domain_clean = re.sub(r"^https?://", "", domain.lower(), flags=re.IGNORECASE)
        domain_clean = re.sub(r"^www\.", "", domain_clean, flags=re.IGNORECASE)
        domain_clean = domain_clean.strip("/").split("/")[0]  # Remove path

        # Extract using tldextract (uses Public Suffix List)
        ext = tldextract.extract(domain_clean)

        # Must have domain and suffix (suffix is empty for invalid TLDs)
        if not ext.domain or not ext.suffix:
            return False

        # Domain must be at least 2 characters (single char domains are invalid)
        if len(ext.domain) < 2:
            return False

        # Suffix must be valid TLD (at least 2 chars)
        if not ext.suffix or len(ext.suffix) < 2:
            return False

        # Reject known taxonomy domains
        full_domain = f"{ext.domain}.{ext.suffix}"
        if full_domain.lower() in KNOWN_TAXONOMY_ROOTS:
            return False

        # Reject obviously invalid TLD patterns (very long compound words, etc.)
        # These are systemic patterns that indicate extraction errors, not real TLDs
        # We only reject patterns that are clearly wrong, not edge cases
        if len(ext.suffix) > 15:  # Very long TLDs are almost certainly extraction errors
            return False

        # tldextract uses the Public Suffix List, which is authoritative
        # If it extracted a suffix, we trust it. A few edge cases may slip through,
        # but that's acceptable rather than adding complexity.

        return True
    except Exception:
        return False


def root_domain(domain: str) -> str | None:
    """
    Extract root domain using tldextract for proper TLD handling.

    Handles complex TLDs like .co.uk, .com.au correctly.

    Args:
        domain: Domain string (may include www, protocol, etc.)

    Returns:
        Root domain (e.g., "apple.com" from "www.apple.com" or "investor.apple.com")
        Returns None if domain is invalid
    """
    if not domain:
        return None

    try:
        # Remove protocol and www if present
        domain_clean = re.sub(r"^https?://", "", domain.lower(), flags=re.IGNORECASE)
        domain_clean = re.sub(r"^www\.", "", domain_clean, flags=re.IGNORECASE)
        domain_clean = domain_clean.strip("/").split("/")[0]  # Remove path

        # Extract using tldextract
        ext = tldextract.extract(domain_clean)
        # Only return if we have both domain and suffix (suffix empty = invalid TLD)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
    except Exception:
        pass

    return None


def normalize_domain(domain: str) -> str | None:
    """
    Normalize domain to root domain format and validate.

    This is the main function to use for domain normalization.
    It extracts the root domain and validates it.

    Examples:
        "http://www.apple.com" -> "apple.com"
        "https://www.microsoft.com/" -> "microsoft.com"
        "www.google.com" -> "google.com"
        "investor.apple.com" -> "apple.com" (extract root domain)
        "example.co.uk" -> "example.co.uk" (handles complex TLDs)

    Args:
        domain: Raw domain string (may include protocol, www, path, etc.)

    Returns:
        Normalized domain (validated) or None if invalid
    """
    if not domain:
        return None

    # Use root_domain to extract proper domain (handles www, protocols, etc.)
    normalized = root_domain(domain)

    # Validate the extracted domain
    if normalized and is_valid_domain(normalized):
        return normalized

    return None
