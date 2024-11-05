# This file is part of Astarte.
#
# Copyright 2024 SECO Mind Srl
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

from datetime import datetime, timezone

from astarte.device import DeviceMqtt


def stream_individuals(device: DeviceMqtt):
    """
    Stream some hardcoded individual datastreams from a device to Astarte.
    """

    # Send the binary blob endpoints
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/binaryblob_endpoint",
        b"binblob",
        datetime.now(tz=timezone.utc),
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/binaryblobarray_endpoint",
        [b"bin", b"blob"],
        datetime.now(tz=timezone.utc),
    )

    # Send the boolean endpoints
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/boolean_endpoint",
        False,
        datetime.now(tz=timezone.utc),
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/booleanarray_endpoint",
        [False, True],
        datetime.now(tz=timezone.utc),
    )

    # Send the datetime endpoints
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/datetime_endpoint",
        datetime.now(tz=timezone.utc),
        datetime.now(tz=timezone.utc),
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/datetimearray_endpoint",
        [datetime.now(tz=timezone.utc)],
        datetime.now(tz=timezone.utc),
    )

    # Send the double endpoints
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/double_endpoint",
        21.3,
        datetime.now(tz=timezone.utc),
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/doublearray_endpoint",
        [1123.0, 12.232],
        datetime.now(tz=timezone.utc),
    )

    # Send the integer endpoints
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/integer_endpoint",
        11,
        datetime.now(tz=timezone.utc),
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/integerarray_endpoint",
        [452, 0],
        datetime.now(tz=timezone.utc),
    )

    # Send the long integer endpoints
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/longinteger_endpoint",
        2**34,
        datetime.now(tz=timezone.utc),
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/longintegerarray_endpoint",
        [2**34, 2**35 + 11],
        datetime.now(tz=timezone.utc),
    )

    # Send the string endpoints
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/string_endpoint",
        "Hello world!",
        datetime.now(tz=timezone.utc),
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceDatastream",
        "/stringarray_endpoint",
        ["Hello,", " world!"],
        datetime.now(tz=timezone.utc),
    )


def stream_aggregates(device: DeviceMqtt):
    """
    Stream some hardcoded aggregated datastreams from a device to Astarte.
    """

    aggregated_data = {
        "binaryblob_endpoint": bytes([0x53, 0x47, 0x56, 0x73, 0x62, 0x47, 0x38, 0x3D]),
        "binaryblobarray_endpoint": [
            bytes([0x53, 0x47, 0x56, 0x73, 0x62, 0x47, 0x38, 0x3D]),
            bytes([0x64, 0x32, 0x39, 0x79, 0x62]),
        ],
        "boolean_endpoint": True,
        "booleanarray_endpoint": [False, True, False],
        "datetime_endpoint": datetime.now(tz=timezone.utc),
        "datetimearray_endpoint": [
            datetime.now(tz=timezone.utc),
            datetime.now(tz=timezone.utc),
        ],
        "double_endpoint": 11.3259,
        "doublearray_endpoint": [11.3259, 43.453, 33.0],
        "integer_endpoint": 11,
        "integerarray_endpoint": [1, 0, 4444],
        "longinteger_endpoint": 564684845165,
        "longintegerarray_endpoint": [12, 2222222, 2],
        "string_endpoint": "Hello world",
        "stringarray_endpoint": ["Hello", "world", "!"],
    }
    device.send_aggregate(
        "org.astarte-platform.python.examples.DeviceAggregate",
        "/sensor11",
        aggregated_data,
    )


def set_properties(device: DeviceMqtt):
    """
    Set some hardcoded properties from a device to Astarte.
    """

    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/binaryblob_endpoint",
        bytes([0x53, 0x47, 0x56, 0x73, 0x62, 0x47, 0x38, 0x3D]),
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/binaryblobarray_endpoint",
        [
            bytes([0x53, 0x47, 0x56, 0x73, 0x62, 0x47, 0x38, 0x3D]),
            bytes([0x64, 0x32, 0x39, 0x79, 0x62]),
        ],
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/boolean_endpoint",
        True,
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/booleanarray_endpoint",
        [False, True, False],
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/datetime_endpoint",
        datetime.now(tz=timezone.utc),
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/datetimearray_endpoint",
        [datetime.now(tz=timezone.utc), datetime.now(tz=timezone.utc)],
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/double_endpoint",
        21.4,
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/doublearray_endpoint",
        [11.3259, 43.453, 33.0],
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/integer_endpoint",
        21,
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/integerarray_endpoint",
        [64, 0],
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/longinteger_endpoint",
        564684845165,
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/longintegerarray_endpoint",
        [12, 2222222, 2],
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/string_endpoint",
        "Hello world",
    )
    device.send(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/stringarray_endpoint",
        ["Hello", "world", "!"],
    )


def unset_properties(device: DeviceMqtt):
    """
    Set some hardcoded properties from a device to Astarte.
    """

    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/binaryblob_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/binaryblobarray_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/boolean_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/booleanarray_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/datetime_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/datetimearray_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/double_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/doublearray_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/integer_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/integerarray_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/longinteger_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/longintegerarray_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/string_endpoint",
    )
    device.unset_property(
        "org.astarte-platform.python.examples.DeviceProperty",
        "/s33/stringarray_endpoint",
    )
