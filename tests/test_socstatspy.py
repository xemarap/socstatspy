"""
Simple test suite for socstatspy
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
from socstatspy.client import SocstatsClient
from socstatspy.data_fetcher import DataFetcher
from socstatspy.exceptions import (
    SocstatsAPIError, 
    SocstatsValidationError,
    SocstatsNotFoundError,
    SocstatsRateLimitError
)
from socstatspy.utils import (
    format_id_list, 
    validate_subject_name, 
    parse_year_range,
    chunk_list
)


class TestClient:
    """Test SocstatsClient"""
    
    def test_initialization_defaults(self):
        client = SocstatsClient()
        assert client.version == 'v1'
        assert client.language == 'sv'
        assert client.timeout == 30
        assert client.max_retries == 3
    
    def test_initialization_custom(self):
        client = SocstatsClient(version='v2', language='en', timeout=60)
        assert client.version == 'v2'
        assert client.language == 'en'
        assert client.timeout == 60
    
    def test_build_url(self):
        client = SocstatsClient()
        url = client._build_url('v1', 'sv', 'dodsorsaker')
        assert 'v1/sv/dodsorsaker' in url
    
    @patch('requests.Session.request')
    def test_list_subjects(self, mock_request):
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: [
                {'namn': 'dodsorsaker', 'text': 'Dödsorsaker'},
                {'namn': 'amning', 'text': 'Amning'}
            ]
        )
        
        client = SocstatsClient()
        subjects = client.list_subjects()
        
        assert len(subjects) == 2
        assert subjects[0]['namn'] == 'dodsorsaker'
    
    @patch('requests.Session.request')
    def test_list_subjects_as_dataframe(self, mock_request):
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: [{'namn': 'dodsorsaker', 'text': 'Dödsorsaker'}]
        )
        
        client = SocstatsClient()
        df = client.list_subjects(as_dataframe=True)
        
        assert isinstance(df, pd.DataFrame)
        assert 'namn' in df.columns
    
    @patch('requests.Session.request')
    def test_get_subject_variables(self, mock_request):
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: [
                {'namn': 'kon', 'text': 'Kön'},
                {'namn': 'region', 'text': 'Region'}
            ]
        )
        
        client = SocstatsClient()
        variables = client.get_subject_variables('dodsorsaker')
        
        assert len(variables) == 2
        assert variables[0]['namn'] == 'kon'
    
    @patch('requests.Session.request')
    def test_get_variable_values(self, mock_request):
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: [
                {'id': 1, 'text': 'Man'},
                {'id': 2, 'text': 'Kvinna'}
            ]
        )
        
        client = SocstatsClient()
        values = client.get_variable_values('dodsorsaker', 'kon')
        
        assert len(values) == 2
        assert values[0]['id'] == 1
    
    @patch('requests.Session.request')
    def test_get_data_basic(self, mock_request):
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: {
                'data': [{'konId': 1, 'varde': 100}],
                'sida': 1
            }
        )
        
        client = SocstatsClient()
        result = client.get_data('dodsorsaker', matt=1)
        
        assert 'data' in result
        assert len(result['data']) == 1
    
    @patch('requests.Session.request')
    def test_get_data_with_filters(self, mock_request):
        mock_request.return_value = Mock(
            status_code=200,
            json=lambda: {
                'data': [{'konId': 1, 'ar': 2020, 'varde': 100}],
                'sida': 1
            }
        )
        
        client = SocstatsClient()
        result = client.get_data('dodsorsaker', matt=1, ar='2020', kon=1)
        
        assert len(result['data']) == 1
        assert result['data'][0]['ar'] == 2020
    
    @patch('requests.Session.request')
    @patch('time.sleep')
    def test_pagination(self, mock_sleep, mock_request):
        # First page with next link
        page1 = {
            'data': [{'varde': 100}],
            'nasta_sida': 'https://example.com?sida=2'
        }
        # Second page without next link
        page2 = {
            'data': [{'varde': 200}],
        }
        
        mock_request.side_effect = [
            Mock(status_code=200, json=lambda: page1),
            Mock(status_code=200, json=lambda: page2)
        ]
        
        client = SocstatsClient()
        result = client.get_data('dodsorsaker', matt=1, auto_paginate=True)
        
        assert len(result['data']) == 2
    
    @patch('requests.Session.request')
    def test_404_error(self, mock_request):
        mock_request.return_value = Mock(status_code=404)
        
        client = SocstatsClient()
        
        with pytest.raises(SocstatsNotFoundError):
            client.list_subjects()
    
    @patch('requests.Session.request')
    def test_rate_limit_error(self, mock_request):
        mock_request.return_value = Mock(status_code=429)
        
        client = SocstatsClient()
        
        with pytest.raises(SocstatsRateLimitError):
            client.list_subjects()
    
    def test_validation_error(self):
        client = SocstatsClient()
        
        with pytest.raises(SocstatsValidationError):
            client.get_variable_values('dodsorsaker', 'region', ids=[1], text_filter='test')


class TestDataFetcher:
    """Test DataFetcher"""
    
    @patch('socstatspy.client.SocstatsClient.get_data')
    def test_get_data_as_dataframe(self, mock_get_data):
        mock_get_data.return_value = {
            'data': [
                {'konId': 1, 'varde': 100},
                {'konId': 2, 'varde': 150}
            ],
            'sida': 1,
            'amne': 'dodsorsaker'
        }
        
        client = SocstatsClient()
        df = client.get_data_as_dataframe('dodsorsaker', matt=1)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'varde' in df.columns
        assert df.attrs['subject'] == 'dodsorsaker'
    
    @patch('socstatspy.client.SocstatsClient.get_data')
    def test_empty_data(self, mock_get_data):
        mock_get_data.return_value = {
            'data': [],
            'sida': 1
        }
        
        client = SocstatsClient()
        df = client.get_data_as_dataframe('dodsorsaker', matt=1)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
    
    def test_metadata_caching(self):
        mock_client = Mock(spec=SocstatsClient)
        mock_client.get_subject_variables.return_value = [
            {'namn': 'kon', 'text': 'Kön'}
        ]
        mock_client.get_variable_values.return_value = [
            {'id': 1, 'text': 'Man'}
        ]
        
        fetcher = DataFetcher(mock_client)
        
        # First call
        metadata1 = fetcher._get_subject_metadata('dodsorsaker')
        # Second call should use cache
        metadata2 = fetcher._get_subject_metadata('dodsorsaker')
        
        assert metadata1 == metadata2
        assert mock_client.get_subject_variables.call_count == 1
    
    def test_enrich_dataframe(self):
        mock_client = Mock(spec=SocstatsClient)
        fetcher = DataFetcher(mock_client)
        
        df = pd.DataFrame({
            'konId': [1, 2],
            'varde': [100, 150]
        })
        
        metadata = {
            'kon': pd.DataFrame({'id': [1, 2], 'text': ['Man', 'Kvinna']})
        }
        
        enriched = fetcher._enrich_dataframe(df, metadata)
        
        assert 'kon_label' in enriched.columns
        assert enriched.loc[0, 'kon_label'] == 'Man'
        assert enriched.loc[1, 'kon_label'] == 'Kvinna'


class TestUtils:
    """Test utility functions"""
    
    def test_format_id_list_single(self):
        assert format_id_list(1) == '1'
        assert format_id_list('test') == 'test'
    
    def test_format_id_list_multiple(self):
        assert format_id_list([1, 2, 3]) == '1,2,3'
        assert format_id_list(['A', 'B', 'C']) == 'A,B,C'
    
    def test_format_id_list_range(self):
        assert format_id_list(range(2018, 2021)) == '2018,2019,2020'
    
    def test_format_id_list_invalid(self):
        with pytest.raises(ValueError):
            format_id_list({'invalid': 'type'})
    
    def test_parse_year_range_dash(self):
        assert parse_year_range('2020-2023') == [2020, 2021, 2022, 2023]
    
    def test_parse_year_range_comma(self):
        assert parse_year_range('2020,2021,2023') == [2020, 2021, 2023]
    
    def test_validate_subject_name_valid(self):
        assert validate_subject_name('dodsorsaker') is True
        assert validate_subject_name('test_123') is True
        assert validate_subject_name('my_subject') is True
    
    def test_validate_subject_name_invalid(self):
        assert validate_subject_name('Invalid') is False
        assert validate_subject_name('123test') is False
        assert validate_subject_name('test-name') is False
        assert validate_subject_name('') is False
    
    def test_chunk_list(self):
        assert chunk_list([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
        assert chunk_list([1, 2, 3], 10) == [[1, 2, 3]]
        assert chunk_list([], 2) == []


class TestExceptions:
    """Test exception handling"""
    
    def test_exception_hierarchy(self):
        assert issubclass(SocstatsValidationError, SocstatsAPIError)
        assert issubclass(SocstatsNotFoundError, SocstatsAPIError)
        assert issubclass(SocstatsRateLimitError, SocstatsAPIError)
    
    def test_raise_validation_error(self):
        with pytest.raises(SocstatsValidationError) as exc_info:
            raise SocstatsValidationError("Invalid input")
        
        assert "Invalid input" in str(exc_info.value)
    
    def test_catch_as_api_error(self):
        try:
            raise SocstatsValidationError("Error")
        except SocstatsAPIError:
            pass  # Should catch as base class
        except Exception:
            pytest.fail("Should have caught as SocstatsAPIError")