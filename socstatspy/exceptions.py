"""
Custom exceptions for socstatspy package
"""


class SocstatsAPIError(Exception):
    """Base exception for all API-related errors"""
    pass


class SocstatsValidationError(SocstatsAPIError):
    """Raised when input validation fails"""
    pass


class SocstatsRateLimitError(SocstatsAPIError):
    """Raised when API rate limit is exceeded"""
    pass


class SocstatsNotFoundError(SocstatsAPIError):
    """Raised when requested resource is not found"""
    pass


class SocstatsPaginationError(SocstatsAPIError):
    """Raised when pagination fails"""
    pass
