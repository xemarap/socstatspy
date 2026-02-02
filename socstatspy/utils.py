"""
Utility functions for socstatspy package
"""

from typing import List, Union, Optional
import re


def format_id_list(ids: Union[int, str, List[Union[int, str]], range]) -> str:
    """
    Format a list of IDs into a comma-separated string.
    
    Accepts single values, lists, ranges, or already-formatted strings.
    This allows flexible input formats while ensuring consistent API calls.
    
    Args:
        ids: Single ID, comma-separated string, list of IDs, or range object
        
    Returns:
        Comma-separated string of IDs
        
    Examples:
        >>> format_id_list([1, 2, 3])
        '1,2,3'
        >>> format_id_list('1,2,3')
        '1,2,3'
        >>> format_id_list(1)
        '1'
        >>> format_id_list(range(2018, 2021))
        '2018,2019,2020'
        >>> format_id_list(['B15', 'I21'])
        'B15,I21'
    """
    if isinstance(ids, (int, str)):
        return str(ids)
    elif isinstance(ids, (list, tuple, range)):
        return ','.join(str(i) for i in ids)
    else:
        raise ValueError(
            f"Invalid type for IDs: {type(ids)}. "
            f"Expected int, str, list, tuple, or range"
        )


def parse_year_range(year_range: str) -> List[int]:
    """
    Parse a year range string into a list of years.
    
    Args:
        year_range: Year range (e.g., '2020-2023' or '2020,2021,2022')
        
    Returns:
        List of years
        
    Example:
        >>> parse_year_range('2020-2023')
        [2020, 2021, 2022, 2023]
        >>> parse_year_range('2020,2022')
        [2020, 2022]
    """
    if '-' in year_range and ',' not in year_range:
        # Range format: 2020-2023
        match = re.match(r'(\d{4})-(\d{4})', year_range)
        if match:
            start, end = map(int, match.groups())
            return list(range(start, end + 1))
    
    # Comma-separated format
    return [int(y.strip()) for y in year_range.split(',')]


def validate_subject_name(subject: str) -> bool:
    """
    Validate subject name format.
    
    Args:
        subject: Subject name to validate
        
    Returns:
        True if valid, False otherwise
    """
    # Subject names should be lowercase alphanumeric with underscores
    return bool(re.match(r'^[a-z][a-z0-9_]*$', subject))


def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """
    Split a list into chunks of specified size.
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
        
    Example:
        >>> chunk_list([1, 2, 3, 4, 5], 2)
        [[1, 2], [3, 4], [5]]
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def build_filter_dict(**kwargs) -> dict:
    """
    Build a filter dictionary from keyword arguments, excluding None values.
    
    Args:
        **kwargs: Filter parameters
        
    Returns:
        Dictionary with non-None values
    """
    return {k: v for k, v in kwargs.items() if v is not None}
