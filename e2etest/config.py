"""
Contains common configuration for all tests.
"""

from datetime import datetime, timezone
import os
from pathlib import Path
import json


class TestCfg:
    """
    Test configuration class. Contains useful configuration information and mock data.
    """

    # pylint: disable=too-many-instance-attributes,too-few-public-methods

    def __init__(self) -> None:
        self.realm = os.environ.get("E2E_REALM")
        self.device_id = os.environ.get("E2E_DEVICE_ID")
        self.credentials_secret = os.environ.get("E2E_CREDENTIALS_SECRET")
        self.api_url = os.environ.get("E2E_API_URL")
        self.appengine_token = os.environ.get("E2E_TOKEN")

        if not all(
            [
                self.realm,
                self.device_id,
                self.credentials_secret,
                self.api_url,
                self.appengine_token,
            ]
        ):
            raise ValueError("Missing one of the environment variables")

        self.pairing_url = self.api_url + "/pairing"

        self.interfaces_fld = Path.joinpath(Path.cwd(), "e2etest", "interfaces")

        self.interface_server_data = "org.astarte-platform.python.e2etest.ServerDatastream"
        self.interface_device_data = "org.astarte-platform.python.e2etest.DeviceDatastream"
        self.interface_server_aggr = "org.astarte-platform.python.e2etest.ServerAggregate"
        self.interface_device_aggr = "org.astarte-platform.python.e2etest.DeviceAggregate"
        self.interface_server_prop = "org.astarte-platform.python.e2etest.ServerProperty"
        self.interface_device_prop = "org.astarte-platform.python.e2etest.DeviceProperty"

        self.mock_data = {
            "double_endpoint": 5.4,
            "integer_endpoint": 42,
            "boolean_endpoint": True,
            "longinteger_endpoint": 45543543534,
            "string_endpoint": "hello",
            "binaryblob_endpoint": b"hello",
            "datetime_endpoint": datetime(2022, 11, 22, 10, 11, 21, 0, tzinfo=timezone.utc),
            "doublearray_endpoint": [22.2, 322.22, 12.3, 0.1],
            "integerarray_endpoint": [22, 322, 0, 10],
            "booleanarray_endpoint": [True, False, True, False],
            "longintegerarray_endpoint": [45543543534, 10, 0, 45543543534],
            "stringarray_endpoint": ["hello", " world"],
            "binaryblobarray_endpoint": [b"hello", b"world"],
            "datetimearray_endpoint": [
                datetime(2022, 11, 22, 10, 11, 21, 0, tzinfo=timezone.utc),
                datetime(2022, 10, 21, 12, 5, 33, 0, tzinfo=timezone.utc),
            ],
        }


MOCK_DATA = {
    "double_endpoint": 5.4,
    "integer_endpoint": 42,
    "boolean_endpoint": True,
    "longinteger_endpoint": 45543543534,
    "string_endpoint": "hello",
    "binaryblob_endpoint": b"hello",
    "datetime_endpoint": datetime(2022, 11, 22, 10, 11, 21, 0, tzinfo=timezone.utc),
    "doublearray_endpoint": [22.2, 322.22, 12.3, 0.1],
    "integerarray_endpoint": [22, 322, 0, 10],
    "booleanarray_endpoint": [True, False, True, False],
    "longintegerarray_endpoint": [45543543534, 10, 0, 45543543534],
    "stringarray_endpoint": ["hello", " world"],
    "binaryblobarray_endpoint": [b"hello", b"world"],
    "datetimearray_endpoint": [
        datetime(2022, 11, 22, 10, 11, 21, 0, tzinfo=timezone.utc),
        datetime(2022, 10, 21, 12, 5, 33, 0, tzinfo=timezone.utc),
    ],
}
