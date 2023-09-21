# This file is part of Astarte.
#
# Copyright 2023 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
"""
Contains common configuration for all tests.
"""

from datetime import datetime, timezone
import os
from pathlib import Path


class TestCfg:
    """
    Test configuration class. Contains useful configuration information and mock data.
    """

    # pylint: disable=too-many-instance-attributes,too-few-public-methods

    def __init__(self, number: int) -> None:
        self.realm = os.environ.get("E2E_REALM")
        self.device_id = os.environ.get(f"E2E_DEVICE_{number}_ID")
        self.credentials_secret = os.environ.get(f"E2E_CREDENTIALS_SECRET_{number}")
        self.appengine_url = os.environ.get("E2E_APPENGINE_URL")
        self.pairing_url = os.environ.get("E2E_PAIRING_URL")
        self.appengine_token = os.environ.get("E2E_APPENGINE_TOKEN")

        if not all(
            [
                self.realm,
                self.device_id,
                self.credentials_secret,
                self.appengine_url,
                self.pairing_url,
                self.appengine_token,
            ]
        ):
            raise ValueError("Missing one of the environment variables")

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
            # "binaryblob_endpoint": b"binblob",
            "datetime_endpoint": datetime(2022, 11, 22, 10, 11, 21, 0, tzinfo=timezone.utc),
            "doublearray_endpoint": [22.2, 322.22, 12.3, 0.1],
            "integerarray_endpoint": [22, 322, 0, 10],
            "booleanarray_endpoint": [True, False, True, False],
            "longintegerarray_endpoint": [45543543534, 10, 0, 45543543534],
            "stringarray_endpoint": ["hello", " world"],
            # "binaryblobarray_endpoint": [b"bin", b"blob"],
            "datetimearray_endpoint": [
                datetime(2022, 11, 22, 10, 11, 21, 0, tzinfo=timezone.utc),
                datetime(2022, 10, 21, 12, 5, 33, 0, tzinfo=timezone.utc),
            ],
        }
