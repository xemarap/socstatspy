# Test Suite for socstatspy

## Quick Start

```bash
# Install dependencies
pip install -r requirements-test.txt

# Run tests
pytest

# Run with coverage
pytest --cov=socstatspy
```

## What's Tested

- Client initialization
- API calls (mocked)
- Data fetching
- DataFrame conversion
- Utility functions
- Error handling

## Files

- `test_socstatspy.py` - Main test file
- `conftest.py` - Shared fixtures
- `requirements_test.txt` - Test dependencies