# Extensible 10-K Parsing: Pluggable Interface Pattern

## Overview

The 10-K parsing system uses a **pluggable interface pattern** (similar to Java interfaces) that makes it easy to add new extraction logic without modifying existing code.

## The Interface

### `TenKParser` (Base Class)

Located in `public_company_graph/parsing/base.py`:

```python
class TenKParser(ABC):
    """Base interface for 10-K parsers."""

    @property
    @abstractmethod
    def field_name(self) -> str:
        """The field name in the result dictionary."""
        pass

    @abstractmethod
    def extract(
        self,
        file_path: Path,
        file_content: Optional[str] = None,
        **kwargs
    ) -> Optional[Any]:
        """Extract data from a 10-K file."""
        pass

    def validate(self, value: Any) -> bool:
        """Validate extracted value (override for custom validation)."""
        return value is not None
```

## How to Add a New Parser

### Step 1: Implement the Interface

Create a new parser class that extends `TenKParser`:

```python
from public_company_graph.parsing.base import TenKParser
from pathlib import Path
from typing import Optional

class MyNewParser(TenKParser):
    """Extract [your data] from 10-K filings."""

    @property
    def field_name(self) -> str:
        return "my_field"  # This will be the key in the result dict

    def extract(
        self,
        file_path: Path,
        file_content: Optional[str] = None,
        **kwargs
    ) -> Optional[str]:  # Or whatever type you're extracting
        """
        Extract your data from the 10-K file.

        Args:
            file_path: Path to 10-K HTML/XML file
            file_content: Pre-read file content (use this for performance)
            **kwargs: Additional context (cik, filings_dir, skip_datamule, etc.)

        Returns:
            Extracted data or None if extraction failed
        """
        # Your extraction logic here
        # Use file_content if available (faster than re-reading)
        if file_content is None:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                file_content = f.read()

        # Parse and extract
        # ...

        return extracted_value

    def validate(self, value: Any) -> bool:
        """Optional: Add custom validation."""
        return value is not None and len(value) > 0
```

### Step 2: Register the Parser

Add your parser to `get_default_parsers()` in `public_company_graph/parsing/base.py`:

```python
def get_default_parsers() -> list:
    """Get the default list of parsers for 10-K parsing."""
    return [
        WebsiteParser(),
        BusinessDescriptionParser(),
        RiskFactorsParser(),
        CompetitorParser(),
        FilingMetadataParser(),
        MyNewParser(),  # Add your parser here
    ]
```

That's it! Your parser will now run automatically for all 10-K files.

## Example: Adding a Filing Date Parser

```python
from public_company_graph.parsing.base import TenKParser
from pathlib import Path
from typing import Optional
from datetime import datetime
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
            # Assume December 31st (most 10-Ks are annual)
            return f"{year}-12-31"

        # Or extract from HTML content
        if file_content:
            # Look for filing date in HTML
            # ...
            pass

        return None

    def validate(self, value: Any) -> bool:
        """Validate date format."""
        if not value:
            return False
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except ValueError:
            return False
```

Then add it to the parsers list:

```python
parsers = [
    WebsiteParser(),
    BusinessDescriptionParser(),
    CompetitorParser(),
    FilingDateParser(),  # New parser
]
```

## Benefits

✅ **Modular**: Each parser is independent
✅ **Testable**: Easy to unit test individual parsers
✅ **Extensible**: Add new parsers without touching existing code
✅ **Repeatable**: Same interface, same pattern, every time
✅ **Type-safe**: Clear contracts (field_name, extract, validate)

## Current Parsers

1. **WebsiteParser** - Extracts company website from cover page
2. **BusinessDescriptionParser** - Extracts Item 1: Business description
3. **CompetitorParser** - Placeholder for competitor extraction (TODO)

## Testing

You can test parsers independently:

```python
from public_company_graph.parsing.base import WebsiteParser
from pathlib import Path

parser = WebsiteParser()
result = parser.extract(Path("data/10k_filings/0000320193/10k_2024.html"))
print(f"Website: {result}")
```

## Summary

The parsing system is now:
- ✅ **Extensible**: Add new parsers by implementing `TenKParser`
- ✅ **Modular**: Each parser is independent and testable
- ✅ **Repeatable**: Same pattern for all parsers
- ✅ **Maintainable**: Clear interface, easy to understand

Just implement `TenKParser` and add it to the list - that's it!
