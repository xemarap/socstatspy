"""
socstatspy - A Python wrapper for Socialstyrelsen's Statistics Database API

This package provides a convenient interface to access Swedish health and social statistics
from Socialstyrelsen (The National Board of Health and Welfare).

Main Classes:
    SocstatsClient: Main client for interacting with the API
    DataFetcher: Helper class for fetching and processing data
    
Example:
    >>> from socstatspy import SocstatsClient
    >>> client = SocstatsClient()
    >>> subjects = client.list_subjects()
    >>> data = client.get_data('dodsorsaker', matt=1, ar='2020,2021')
"""

from .client import SocstatsClient
from .data_fetcher import DataFetcher
from .exceptions import (
    SocstatsAPIError,
    SocstatsValidationError,
    SocstatsRateLimitError,
    SocstatsNotFoundError
)

__version__ = '0.1.0'
__author__ = 'socstatspy'
__all__ = [
    'SocstatsClient',
    'DataFetcher',
    'SocstatsAPIError',
    'SocstatsValidationError',
    'SocstatsRateLimitError',
    'SocstatsNotFoundError'
]
