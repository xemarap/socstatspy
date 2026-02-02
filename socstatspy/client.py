"""
Main client for interacting with Socialstyrelsen's API
"""

import requests
import time
from typing import Dict, List, Optional, Union, Any
from urllib.parse import urljoin
import logging
import pandas as pd

from .exceptions import (
    SocstatsAPIError,
    SocstatsValidationError,
    SocstatsRateLimitError,
    SocstatsNotFoundError
)
from .data_fetcher import DataFetcher

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SocstatsClient:
    """
    Main client for interacting with Socialstyrelsen's Statistics Database API.
    
    This client provides methods to:
    - List available API versions, languages, and subjects
    - Query metadata for distribution variables
    - Fetch statistical data with various filters
    - Handle pagination automatically
    - Convert results to pandas DataFrames
    
    Attributes:
        base_url (str): Base URL for the API
        version (str): API version to use (default: 'v1')
        language (str): Language for responses (default: 'sv')
        session (requests.Session): HTTP session for making requests
        
    Example:
        >>> client = SocstatsClient()
        >>> subjects = client.list_subjects()
        >>> data = client.get_data('dodsorsaker', matt=1, ar='2020')
    """
    
    BASE_URL = "https://sdb.socialstyrelsen.se/api"
    DEFAULT_VERSION = "v1"
    DEFAULT_LANGUAGE = "sv"
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds
    
    def __init__(
        self,
        version: str = None,
        language: str = None,
        timeout: int = 30,
        max_retries: int = None
    ):
        """
        Initialize the Socialstyrelsen API client.
        
        Args:
            version: API version to use (default: 'v1')
            language: Language for responses - 'sv' or 'en' (default: 'sv')
            timeout: Request timeout in seconds (default: 30)
            max_retries: Maximum number of retry attempts (default: 3)
        """
        self.base_url = self.BASE_URL
        self.version = version or self.DEFAULT_VERSION
        self.language = language or self.DEFAULT_LANGUAGE
        self.timeout = timeout
        self.max_retries = max_retries or self.MAX_RETRIES
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'socstatspy/0.1.0',
            'Accept': 'application/json'
        })
        
        # Initialize data fetcher
        self.data_fetcher = DataFetcher(self)
        
    def _build_url(self, *parts: str) -> str:
        """
        Build a complete URL from path components.
        
        Args:
            *parts: URL path components
            
        Returns:
            Complete URL string
        """
        # Filter out empty parts and join with '/'
        filtered_parts = [str(p) for p in parts if p]
        if not filtered_parts:
            return self.base_url
        
        # Join all parts with '/'
        path = '/'.join(filtered_parts)
        
        # Ensure base_url ends with '/' for proper joining
        base = self.base_url if self.base_url.endswith('/') else self.base_url + '/'
        
        return base + path
    
    def _make_request(
        self,
        endpoint: str,
        method: str = 'GET',
        params: Optional[Dict] = None,
        **kwargs
    ) -> Dict:
        """
        Make an HTTP request to the API with retry logic.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method (default: 'GET')
            params: Query parameters
            **kwargs: Additional arguments for requests
            
        Returns:
            JSON response as dictionary
            
        Raises:
            SocstatsAPIError: If request fails after retries
            SocstatsNotFoundError: If resource not found (404)
            SocstatsRateLimitError: If rate limit exceeded (429)
        """
        # The endpoint is already a complete URL built by the caller
        url = endpoint
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    timeout=self.timeout,
                    **kwargs
                )
                
                # Handle different status codes
                if response.status_code == 404:
                    raise SocstatsNotFoundError(
                        f"Resource not found: {url}"
                    )
                elif response.status_code == 429:
                    raise SocstatsRateLimitError(
                        "API rate limit exceeded. Please wait before making more requests."
                    )
                elif response.status_code >= 400:
                    raise SocstatsAPIError(
                        f"API request failed with status {response.status_code}: {response.text}"
                    )
                
                response.raise_for_status()
                
                # Return JSON response
                return response.json()
                
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Request timeout, retrying... (attempt {attempt + 1})")
                    time.sleep(self.RETRY_DELAY)
                else:
                    raise SocstatsAPIError(f"Request timeout after {self.max_retries} attempts")
                    
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Request failed, retrying... (attempt {attempt + 1}): {e}")
                    time.sleep(self.RETRY_DELAY)
                else:
                    raise SocstatsAPIError(f"Request failed after {self.max_retries} attempts: {e}")
    
    # ===== Meta API Methods =====
    
    def list_versions(self) -> List[Dict[str, str]]:
        """
        List all available API versions.
        
        Returns:
            List of version dictionaries with 'kod' and 'text' keys
            
        Example:
            >>> versions = client.list_versions()
            >>> print(versions)
            [{'kod': 'v1', 'text': 'Version 1'}]
        """
        endpoint = self._build_url()
        return self._make_request(endpoint)
    
    def list_languages(self) -> List[Dict[str, str]]:
        """
        List all available languages.
        
        Returns:
            List of language dictionaries with 'kod' and 'text' keys
            
        Example:
            >>> languages = client.list_languages()
            >>> print(languages)
            [{'kod': 'sv', 'text': 'Svenska'}, {'kod': 'en', 'text': 'English'}]
        """
        endpoint = self._build_url(self.version)
        return self._make_request(endpoint)
    
    def list_subjects(self, as_dataframe: bool = False) -> Union[List[Dict[str, str]], pd.DataFrame]:
        """
        List all available subjects (Ã¤mnen) in the database.
        
        Args:
            as_dataframe: If True, return as pandas DataFrame (default: False)
        
        Returns:
            List of subject dictionaries or pandas DataFrame with 'namn', 'text', and 'info' columns
            
        Example:
            >>> # Get as list
            >>> subjects = client.list_subjects()
            >>> for subject in subjects:
            ...     print(f"{subject['namn']}: {subject['text']}")
            >>> 
            >>> # Get as DataFrame
            >>> df = client.list_subjects(as_dataframe=True)
            >>> print(df)
        """
        endpoint = self._build_url(self.version, self.language)
        subjects = self._make_request(endpoint)
        
        if as_dataframe:
            return pd.DataFrame(subjects)
        return subjects
    
    def get_subject_variables(
        self, 
        subject: str, 
        as_dataframe: bool = False
    ) -> Union[List[Dict[str, str]], pd.DataFrame]:
        """
        Get all distribution variables (fÃ¶rdelningsvariabler) for a subject.
        
        Args:
            subject: Subject name (e.g., 'dodsorsaker', 'amning')
            as_dataframe: If True, return as pandas DataFrame (default: False)
            
        Returns:
            List of distribution variable dictionaries or pandas DataFrame
            
        Example:
            >>> # Get as list
            >>> variables = client.get_subject_variables('dodsorsaker')
            >>> print(variables)
            >>> 
            >>> # Get as DataFrame
            >>> df = client.get_subject_variables('dodsorsaker', as_dataframe=True)
            >>> print(df)
        """
        endpoint = self._build_url(self.version, self.language, subject)
        variables = self._make_request(endpoint)
        
        if as_dataframe:
            return pd.DataFrame(variables)
        return variables
    
    # ===== Variable Metadata Methods =====
    
    def get_variable_values(
        self,
        subject: str,
        variable: str,
        ids: Optional[Union[str, List[str]]] = None,
        text_filter: Optional[str] = None,
        as_dataframe: bool = False
    ) -> Union[List[Dict], pd.DataFrame]:
        """
        Get values for a specific distribution variable.
        
        Args:
            subject: Subject name
            variable: Variable name (e.g., 'diagnos', 'region', 'alder', 'kon', 'matt', 'ar')
            ids: Optional ID or list of IDs to filter by
            text_filter: Optional text to filter values by
            as_dataframe: If True, return as pandas DataFrame (default: False)
            
        Returns:
            List of variable value dictionaries or pandas DataFrame
            
        Example:
            >>> # Get all regions
            >>> regions = client.get_variable_values('dodsorsaker', 'region')
            >>> 
            >>> # Get specific regions by ID
            >>> regions = client.get_variable_values('dodsorsaker', 'region', ids=[1, 3])
            >>> 
            >>> # Filter by text
            >>> regions = client.get_variable_values('dodsorsaker', 'region', text_filter='botten')
            >>> 
            >>> # Get as DataFrame
            >>> df = client.get_variable_values('dodsorsaker', 'region', as_dataframe=True)
            >>> print(df)
        """
        if ids and text_filter:
            raise SocstatsValidationError("Cannot specify both 'ids' and 'text_filter'")
        
        # Build endpoint
        parts = [self.version, self.language, subject, variable]
        
        if ids:
            # Convert IDs to comma-separated string
            if isinstance(ids, list):
                ids = ','.join(str(i) for i in ids)
            parts.append(ids)
        elif text_filter:
            parts.extend(['text', text_filter])
        
        endpoint = self._build_url(*parts)
        values = self._make_request(endpoint)
        
        if as_dataframe:
            return pd.DataFrame(values)
        return values
    
    # ===== Data Fetching Methods =====
    
    def get_data(
        self,
        subject: str,
        matt: Optional[Union[int, str, List[Union[int, str]]]] = None,
        per_sida: int = 5000,
        sida: Optional[int] = None,
        auto_paginate: bool = True,
        max_pages: Optional[int] = None,
        **filters
    ) -> Dict:
        """
        Get statistical data for a subject with optional filters.
        
        Args:
            subject: Subject name (e.g., 'dodsorsaker', 'amning')
            matt: Measure ID (mÃƒÂ¥tt). If None, uses default for the subject
            per_sida: Number of records per page (default: 5000, max: 5000)
            sida: Specific page number (default: None, starts at 1)
            auto_paginate: Automatically fetch all pages (default: True)
            max_pages: Maximum number of pages to fetch (default: None, unlimited)
            **filters: Additional filters. Each filter can be:
                      - Single value: diagnos='99', kon=1
                      - List: ar=[2018, 2019, 2020], region=['1', '3']
                      - Range: ar=range(2018, 2024)
                      - String: ar='2020,2021' (legacy)
            
        Returns:
            Dictionary containing 'data' list and pagination info
            
        Example:
            >>> # Get data with specific filters
            >>> data = client.get_data('dodsorsaker', matt=1, diagnos='99', ar='2020,2021')
            >>> 
            >>> # Get single page
            >>> data = client.get_data('dodsorsaker', matt=1, sida=2, auto_paginate=False)
        """
        # Build endpoint
        parts = [self.version, self.language, subject, 'resultat']
        
        from .utils import format_id_list
        
        # Add matt filter (normalize to string)
        if matt is not None:
            parts.extend(['matt', format_id_list(matt)])
        
        # Add other filters (normalize all to strings)
        for key, value in filters.items():
            if value is not None:
                parts.extend([key, format_id_list(value)])
        
        # Build query parameters
        params = {}
        if per_sida != 5000:
            params['per_sida'] = per_sida
        if sida is not None:
            params['sida'] = sida
        
        # Make initial request
        endpoint = self._build_url(*parts)
        result = self._make_request(endpoint, params=params)
        
        # Handle pagination if requested
        if auto_paginate and result.get('nasta_sida') and sida is None:
            all_data = result.get('data', [])
            page_count = 1
            
            while result.get('nasta_sida'):
                if max_pages and page_count >= max_pages:
                    logger.info(f"Reached maximum page limit of {max_pages}")
                    break
                
                # Extract page number from next page URL
                next_url = result['nasta_sida']
                page_count += 1
                
                logger.info(f"Fetching page {page_count}...")
                
                # Make request to next page (next_url is already a complete URL)
                result = self._make_request(next_url)
                all_data.extend(result.get('data', []))
                
                # Small delay to be respectful to the API
                time.sleep(0.1)
            
            # Update result with all data
            result['data'] = all_data
            result['sida'] = 1
            result['per_sida'] = len(all_data)
            result['sidor'] = page_count
            
            logger.info(f"Fetched {len(all_data)} total records across {page_count} pages")
        
        return result
    
    def get_data_as_dataframe(
        self,
        subject: str,
        matt: Optional[Union[int, str, List[Union[int, str]]]] = None,
        auto_paginate: bool = True,
        max_pages: Optional[int] = None,
        include_metadata: bool = False,
        **filters
    ):
        """
        Get statistical data as a pandas DataFrame.
        
        Args:
            subject: Subject name
            matt: Measure ID
            auto_paginate: Automatically fetch all pages (default: True)
            max_pages: Maximum number of pages to fetch
            **filters: Additional filters
            
        Returns:
            pandas.DataFrame with the results
            
        Example:
            >>> df = client.get_data_as_dataframe('dodsorsaker', matt=1, ar='2020,2021')
            >>> print(df.head())
        """
        return self.data_fetcher.get_data_as_dataframe(
            subject=subject,
            matt=matt,
            auto_paginate=auto_paginate,
            max_pages=max_pages,
            include_metadata=include_metadata,
            **filters
        )
