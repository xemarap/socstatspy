"""
Shared test fixtures
"""

import pytest
from unittest.mock import Mock
from socstatspy.client import SocstatsClient


@pytest.fixture
def mock_client():
    """Mocked client for testing"""
    return Mock(spec=SocstatsClient)


@pytest.fixture
def sample_response():
    """Sample API response"""
    return {
        'data': [{'konId': 1, 'ar': 2020, 'varde': 100}],
        'sida': 1,
        'per_sida': 1,
        'sidor': 1
    }