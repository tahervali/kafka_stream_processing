from datetime import datetime
import uuid
from src.utils import generate_log_data


def test_generate_log_data():
    # Call the function to generate log data
    log_data = generate_log_data()

    # Assert 'view_id' is a valid UUID
    assert isinstance(
        uuid.UUID(log_data["view_id"]), uuid.UUID
    ), "view_id should be a valid UUID"

    # Assert 'start_timestamp' and 'end_timestamp' are valid ISO format strings and convert to datetime
    start_timestamp = datetime.fromisoformat(log_data["start_timestamp"])
    end_timestamp = datetime.fromisoformat(log_data["end_timestamp"])

    # Assert end_timestamp >= start_timestamp and end_timestamp <= current time
    assert (
        start_timestamp <= end_timestamp <= datetime.now()
    ), "end_timestamp should be >= start_timestamp and <= current time"

    # Assert 'banner_id' is an integer and within the correct range
    assert (
        1 <= log_data["banner_id"] <= 100000
    ), "banner_id should be between 1 and 100000"

    # Assert 'campaign_id' is within the specified range (10 to 140 in increments of 10)
    assert log_data["campaign_id"] in range(
        10, 141, 10
    ), "campaign_id should be between 10 and 140 in increments of 10"
