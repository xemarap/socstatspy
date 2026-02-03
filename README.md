# socstatspy

A Python wrapper for Socialstyrelsen's (The National Board of Health and Welfare) Statistics Database API.

This package provides a convenient interface to access Swedish health and social statistics from Socialstyrelsen's public API.

[![Python Versions](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](#)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Note:** This is an independent project and is not associated with Socialstyrelsen.

## Features

- **Easy-to-use client** for interacting with the API
- **Automatic pagination** handling for large datasets
- **DataFrame conversion** with pandas integration
- **Metadata enrichment** to add human-readable labels
- **Comprehensive error handling** with retry logic
- **Rate limiting** respect to avoid overwhelming the API
- **Flexible filtering** for all distribution variables
- **Type hints** for better IDE support

## Installation

```bash
pip install git+https://github.com/xemarap/socstatspy.git
```

Or install from source:

```bash
git clone https://github.com/xemarap/socstatspy.git
cd socstatspy
pip install -e .
```

## Quick Start

Visit the Getting_started tutorial notebook for extensive usage guide.

```python
from socstatspy import SocstatsClient

# Initialize the client
client = SocstatsClient()

# List available subjects
subjects = client.list_subjects()
print(f"Available subjects: {len(subjects)}")

# Get data as a pandas DataFrame
df = client.get_data_as_dataframe(
    subject='dodsorsaker',
    matt=1,
    ar=[2020, 2021, 2022],      # ✅ List of years
    region=['1','12'],          # ✅ List of regions (Stockholm and Skåne)
    max_pages=2
)

print(df.head())
```

### 1. Exploring Available Data

```python
from socstatspy import SocstatsClient

client = SocstatsClient()

# List all subjects
subjects = client.list_subjects()
for subject in subjects:
    print(f"{subject['namn']}: {subject['text']}")

# Get distribution variables for a subject
variables = client.get_subject_variables('dodsorsaker')
print("Available variables:", [v['namn'] for v in variables])

# Get all available years
years = client.get_variable_values('dodsorsaker', variable='ar')
print("Available years:", [y['text'] for y in years])

# Get all regions
regions = client.get_variable_values('dodsorsaker', variable='region')
print("Available regions:", [r['text'] for r in regions[:5]])
```

### 2. Searching for Diagnoses

```python
# Search for heart-related diagnoses
heart_diagnoses = client.get_variable_values('dodsorsaker',
                                             variable='diagnos',
                                             text_filter='hjärt')

print(f"💓 Found {len(heart_diagnoses)} heart-related diagnoses\n")
print("First 5 results:")
for diag in heart_diagnoses[:5]:
    print(f"• {diag['id']:8s} - {diag['text']}")
```

### 3. Fetching Data with Metadata

```python
# Get enriched data with labels
df_enriched = client.get_data_as_dataframe(
    subject='dodsorsaker',
    matt=1,
    region=['1', '3'],      # Stockholm and Uppsala
    ar=2020,
    max_pages=1,
    include_metadata=True   # Set this parameter to True to get labels
)
```

### 4. Cache Management

The socstatspy wrapper uses in memory caching for retrieved metadata, it is not persisted to disk.
The cache persists only as long as the client object exists.

```python
# Clear metadata cache if needed
client.data_fetcher.clear_cache()

# Get cache information
cache_info = client.data_fetcher.get_cache_info()
print(f"Cached subjects: {cache_info['cached_subjects']}")
print(f"Total variables: {cache_info['total_variables']}")
```

## Contributing

Contributions are welcome! Please feel free to submit an Issue or a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Dependency Licenses

SocStatsPy includes the following dependencies:

**Runtime Dependencies:**
- requests
- pandas

**Development/Testing Dependencies (not distributed):**
- pytest
- pytest-cov

All dependency licenses are available in the `LICENSES/` directory.

## Acknowledgments

- Data provided by Socialstyrelsen (The National Board of Health and Welfare)
- API documentation: https://sdb.socialstyrelsen.se/sdbapi.aspx

## Support

For issues and questions:
- GitHub Issues: https://github.com/xemarap/socstatspy/issues
- API Documentation: https://sdb.socialstyrelsen.se/sdbapi.aspx
