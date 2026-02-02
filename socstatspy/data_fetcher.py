"""
Data fetcher and processor for converting API responses to pandas DataFrames
"""

import pandas as pd
from typing import Dict, Optional, Union, Any
import logging

logger = logging.getLogger(__name__)


class DataFetcher:
    """
    Helper class for fetching and processing data from the API.
    
    This class provides methods to convert API responses into pandas DataFrames
    and enrich them with metadata labels.
    """
    
    def __init__(self, client):
        """
        Initialize the DataFetcher.
        
        Args:
            client: SocstatsClient instance
        """
        self.client = client
        self._metadata_cache = {}
    
    def get_data_as_dataframe(
        self,
        subject: str,
        matt: Optional[Union[int, str]] = None,
        auto_paginate: bool = True,
        max_pages: Optional[int] = None,
        include_metadata: bool = False,
        **filters
    ) -> pd.DataFrame:
        """
        Get statistical data as a pandas DataFrame.
        
        Args:
            subject: Subject name
            matt: Measure ID
            auto_paginate: Automatically fetch all pages
            max_pages: Maximum number of pages to fetch
            **filters: Additional filters
            
        Returns:
            pandas.DataFrame with the results
        """
        # Get raw data
        result = self.client.get_data(
            subject=subject,
            matt=matt,
            auto_paginate=auto_paginate,
            max_pages=max_pages,
            **filters
        )
        
        # Convert to DataFrame
        data = result.get('data', [])
        if not data:
            logger.warning("No data returned from API")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        
        # Add metadata as attributes
        df.attrs['subject'] = result.get('amne', subject)
        df.attrs['total_pages'] = result.get('sidor', 1)
        df.attrs['records_per_page'] = result.get('per_sida', len(data))

        # Enrich with metadata labels if requested
        if include_metadata:
            metadata = self._get_subject_metadata(subject)
            df = self._enrich_dataframe(df, metadata)
        
        return df
    
    def _get_subject_metadata(self, subject: str) -> Dict[str, pd.DataFrame]:
        """
        Get and cache metadata for all variables in a subject.
        
        Args:
            subject: Subject name
            
        Returns:
            Dictionary mapping variable names to DataFrames with their values
        """
        cache_key = subject
        
        if cache_key in self._metadata_cache:
            return self._metadata_cache[cache_key]
        
        logger.info(f"Fetching metadata for subject: {subject}")
        
        metadata = {}
        
        # Get all distribution variables for the subject
        try:
            variables = self.client.get_subject_variables(subject)
            
            for var_info in variables:
                var_name = var_info.get('namn')
                if not var_name:
                    continue
                
                try:
                    # Fetch values for this variable
                    values = self.client.get_variable_values(subject, var_name)
                    
                    if values:
                        # Convert to DataFrame for easy lookup
                        metadata[var_name] = pd.DataFrame(values)
                        logger.debug(f"Cached metadata for {var_name}: {len(values)} entries")
                        
                except Exception as e:
                    logger.warning(f"Could not fetch metadata for variable '{var_name}': {e}")
                    continue
        
        except Exception as e:
            logger.warning(f"Could not fetch subject variables: {e}")
        
        # Cache the metadata
        self._metadata_cache[cache_key] = metadata
        
        return metadata
    
    def _enrich_dataframe(
    self,
    df: pd.DataFrame,
    metadata: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Enrich a dataframe by adding label columns for ID columns.
        
        This method automatically detects ID columns and maps them to their
        corresponding metadata, making it robust to future API changes.
        
        Args:
            df: Original dataframe with ID columns
            metadata: Dictionary of metadata DataFrames
            
        Returns:
            Enriched DataFrame with label columns
        """
        df_enriched = df.copy()
        
        # Columns that should NOT be mapped (data values, not IDs)
        EXCLUDED_COLUMNS = {'varde', 'sida', 'per_sida', 'sidor'}
        
        # Special case: 'ar' is both an ID and doesn't follow the 'Id' suffix pattern
        special_cases = {
            'ar': 'ar'  # column_name: metadata_key
        }
        
        # Process special cases first
        for col_name, meta_key in special_cases.items():
            if col_name in df_enriched.columns and meta_key in metadata:
                self._add_label_column(df_enriched, col_name, metadata[meta_key], meta_key)
        
        # Process all columns that end with 'Id'
        for col_name in df_enriched.columns:
            # Skip excluded columns
            if col_name in EXCLUDED_COLUMNS:
                continue
                
            # Skip if already processed in special cases
            if col_name in special_cases:
                continue
            
            # Check if column ends with 'Id' (case-sensitive)
            if col_name.endswith('Id'):
                # Derive metadata key by removing 'Id' suffix and lowercasing
                meta_key = col_name[:-2].lower()
                
                # Try to find matching metadata
                if meta_key in metadata:
                    self._add_label_column(df_enriched, col_name, metadata[meta_key], meta_key)
                else:
                    logger.debug(f"No metadata found for column '{col_name}' (looking for '{meta_key}')")
        
        return df_enriched

    def _add_label_column(
        self,
        df: pd.DataFrame,
        id_col: str,
        meta_df: pd.DataFrame,
        meta_key: str
    ) -> None:
        """
        Add a label column to the dataframe based on metadata.
        
        Args:
            df: DataFrame to add column to (modified in place)
            id_col: Name of the ID column
            meta_df: Metadata DataFrame containing id-to-text mapping
            meta_key: Metadata key name (for logging)
        """
        # Determine the correct ID and text field names in metadata
        id_field = 'id'
        text_field = 'text'
        
        if id_field not in meta_df.columns or text_field not in meta_df.columns:
            logger.warning(
                f"Metadata for '{meta_key}' missing required columns. "
                f"Has: {meta_df.columns.tolist()}, needs: ['id', 'text']"
            )
            return
        
        # Create a mapping dictionary
        id_to_text = dict(zip(meta_df[id_field], meta_df[text_field]))
        
        # Determine label column name
        if id_col == 'ar':
            label_col = 'ar_label'
        else:
            label_col = id_col.replace('Id', '_label')
        
        # Add the label column
        df[label_col] = df[id_col].map(id_to_text)
        
        # Log unmapped values (helpful for debugging)
        unmapped = df[id_col][df[label_col].isna()].unique()
        if len(unmapped) > 0 and len(unmapped) <= 5:  # Only log if few unmapped values
            logger.debug(f"Column '{id_col}': {len(unmapped)} unmapped values: {unmapped.tolist()}")
        
        logger.debug(f"Added label column: {label_col} (from {meta_key})")
    
    def clear_cache(self):
        """Clear the metadata cache."""
        self._metadata_cache.clear()
        logger.info("Metadata cache cleared")
    
    def get_cache_info(self) -> Dict[str, int]:
        """
        Get information about cached metadata.
        
        Returns:
            Dictionary with cache statistics
        """
        return {
            'cached_subjects': len(self._metadata_cache),
            'total_variables': sum(
                len(vars) for vars in self._metadata_cache.values()
            )
        }
