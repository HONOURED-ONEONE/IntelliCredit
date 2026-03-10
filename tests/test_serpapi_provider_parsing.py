import pytest
from unittest.mock import patch, MagicMock
from providers.search.serpapi_provider import SerpApiProvider

@patch.dict('os.environ', {'SERPAPI_API_KEY': 'test_key'})
@patch('providers.search.serpapi_provider.requests.get')
def test_serpapi_parsing(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "organic_results": [
            {
                "title": "Test Title",
                "link": "https://example.com",
                "snippet": "Test snippet",
                "date": "2023-01-01"
            },
            {
                "title": "Missing Date Title",
                "link": "https://example.org",
                "snippet": "Snippet 2"
                # date missing
            }
        ]
    }
    mock_get.return_value = mock_response

    provider = SerpApiProvider({})
    results = provider.search("test query")

    assert len(results) == 2
    
    # Check first result
    assert results[0]["title"] == "Test Title"
    assert results[0]["url"] == "https://example.com"
    assert results[0]["snippet"] == "Test snippet"
    assert results[0]["date"] == "2023-01-01"
    assert results[0]["source_quality"] == 50

    # Check second result
    assert results[1]["title"] == "Missing Date Title"
    assert results[1]["url"] == "https://example.org"
    assert results[1]["date"] == ""
    assert results[1]["source_quality"] == 50

@patch.dict('os.environ', {'SERPAPI_API_KEY': 'test_key'})
@patch('providers.search.serpapi_provider.requests.get')
def test_serpapi_empty_results(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "search_metadata": {
            "status": "Success"
        }
        # organic_results missing
    }
    mock_get.return_value = mock_response

    provider = SerpApiProvider({})
    results = provider.search("empty query")

    assert isinstance(results, list)
    assert len(results) == 0

@patch.dict('os.environ', clear=True)
def test_serpapi_missing_key():
    provider = SerpApiProvider({})
    results = provider.search("no key query")
    assert results == []
