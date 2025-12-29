# Quick Guide: Adding a New 10-K Parser

## The Pattern (3 Steps)

### Step 1: Create Your Parser Class

Create a new file or add to existing: `domain_status_graph/parsing/your_parser.py`

```python
from domain_status_graph.parsing.base import TenKParser
from pathlib import Path
from typing import Optional

class YourNewParser(TenKParser):
    """Extract [what you want] from 10-K filings."""

    @property
    def field_name(self) -> str:
        return "your_field_name"  # This becomes the key in the result dict

    def extract(
        self,
        file_path: Path,
        file_content: Optional[str] = None,
        **kwargs
    ) -> Optional[Any]:  # Return type depends on what you're extracting
        """
        Your extraction logic here.

        Args:
            file_path: Path to 10-K HTML/XML file
            file_content: Pre-read file content (use this for performance!)
            **kwargs: Additional context (cik, filings_dir, skip_datamule, etc.)

        Returns:
            Extracted value or None if extraction failed
        """
        # Use file_content if available (faster than re-reading)
        if file_content is None:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                file_content = f.read()

        # Your extraction logic
        # ...

        return extracted_value

    def validate(self, value: Any) -> bool:
        """Optional: Add custom validation."""
        return value is not None
```

### Step 2: Register Your Parser

Edit `domain_status_graph/parsing/base.py`, find the `get_default_parsers()` function, and add your parser:

```python
# In get_default_parsers() function:
def get_default_parsers() -> list:
    """Get the default list of parsers for 10-K parsing."""
    from domain_status_graph.parsing.filing_metadata import FilingMetadataParser

    return [
        WebsiteParser(),
        BusinessDescriptionParser(),
        RiskFactorsParser(),
        CompetitorParser(),
        FilingMetadataParser(),
        YourNewParser(),  # ← Add your parser here
    ]
```

### Step 3: Test It

```python
from domain_status_graph.parsing.base import YourNewParser
from pathlib import Path

parser = YourNewParser()
result = parser.extract(Path("data/10k_filings/0000320193/10k_2024.html"))
print(f"Extracted: {result}")
```

That's it! Your parser will now run automatically for all 4,706 companies.

## Example: Adding a Filing Date Parser

```python
# domain_status_graph/parsing/filing_date.py
from domain_status_graph.parsing.base import TenKParser
from pathlib import Path
from typing import Optional
import re

class FilingDateParser(TenKParser):
    """Extract filing date from 10-K filename or content."""

    @property
    def field_name(self) -> str:
        return "filing_date"

    def extract(
        self,
        file_path: Path,
        file_content: Optional[str] = None,
        **kwargs
    ) -> Optional[str]:
        """Extract filing date in YYYY-MM-DD format."""
        # Try filename first (e.g., 10k_2024.html)
        match = re.search(r'10k_(\d{4})\.html', file_path.name)
        if match:
            year = match.group(1)
            return f"{year}-12-31"  # Most 10-Ks are annual (Dec 31)

        # Or extract from HTML content if needed
        # ...

        return None

    def validate(self, value: Any) -> bool:
        """Validate date format."""
        if not value:
            return False
        try:
            from datetime import datetime
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except ValueError:
            return False
```

Then in `domain_status_graph/parsing/base.py`:

```python
# In get_default_parsers():
from domain_status_graph.parsing.filing_date import FilingDateParser

return [
    WebsiteParser(),
    BusinessDescriptionParser(),
    RiskFactorsParser(),
    CompetitorParser(),
    FilingMetadataParser(),
    FilingDateParser(),  # ← Add it here
]
```

## Benefits

✅ **Modular**: Each parser is independent
✅ **Testable**: Easy to unit test
✅ **Repeatable**: Same pattern every time
✅ **No code changes needed**: Just add to the list

## Current Parsers

- `WebsiteParser` - Extracts company website
- `BusinessDescriptionParser` - Extracts Item 1: Business
- `RiskFactorsParser` - Extracts Item 1A: Risk Factors
- `CompetitorParser` - Placeholder for competitors (TODO)
- `FilingMetadataParser` - Extracts filing date, accession number, fiscal year end

## Summary

**To add a new parser:**
1. Implement `TenKParser` interface in a new file
2. Add to `get_default_parsers()` in `domain_status_graph/parsing/base.py`
3. Done!

The interface handles everything else (file reading, error handling, validation, etc.).
