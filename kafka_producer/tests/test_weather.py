# import requests
from unittest.mock import patch

from weather import get_temperature


def test_get_temperature():
    # This is where we use patch
    with patch("weather.requests.get") as mock_get:
        # Set up our mock
        mock_get.return_value.json.return_value = {"temperature": 20}

        # Call the function we're testing
        result = get_temperature("Paris")

        # Check if the result is what we expect
        assert result == 20

    # Verify that our function called requests.get with the right URL
    # mock_get.assert_called_with("https://api.weather.com/Paris")
