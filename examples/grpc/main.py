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

""" Individual datastream example

Example showing how to send/receive individual datastreams.

"""
from datetime import datetime, timezone
from pathlib import Path
import time
from termcolor import cprint

from astarte.device import DeviceGrpc

_ROOT_DIR = Path(__file__).parent.absolute()
_INTERFACES_DIR = _ROOT_DIR.joinpath("interfaces")


def on_data_received_cbk(device: DeviceGrpc, interface_name: str, path: str, payload: dict):
    """
    Callback for a data reception event.
    """
    cprint(
        f"Received message for interface: {interface_name} and path: {path}.",
        color="cyan",
        flush=True,
    )
    cprint(f"    Payload: {payload}", color="cyan", flush=True)


# If called as a script
if __name__ == "__main__":
    # Instantiate the device
    device = DeviceGrpc(
        server_addr="localhost:50051", node_uuid="98bb9fe5-b4ce-4dea-9b88-8d1f8525e4b4"
    )
    # Load all the interfaces
    device.add_interfaces_from_dir(_INTERFACES_DIR)
    # Set all the callback functions
    device.on_data_received = on_data_received_cbk
    # # Connect the device
    device.connect()

    time.sleep(1)

    # Send the binary blob endpoints
    # device.send(
    #     "org.astarte-platform.python.examples.DeviceDatastream",
    #     "/binaryblob_endpoint",
    #     b"binblob",
    #     datetime.now(tz=timezone.utc),
    # )
    # device.send(
    #     "org.astarte-platform.python.examples.DeviceDatastream",
    #     "/binaryblobarray_endpoint",
    #     [b"bin", b"blob"],
    #     datetime.now(tz=timezone.utc),
    # )

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
        [datetime.now(tz=timezone.utc), datetime.now(tz=timezone.utc)],
        datetime.now(tz=timezone.utc),
    )

    # # Send the double endpoints
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

    # # Send the integer endpoints
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

    # # Send the long integer endpoints
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

    # # Send the string endpoints
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

    time.sleep(60)

    device.disconnect()
