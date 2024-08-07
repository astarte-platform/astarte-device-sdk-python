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

import argparse
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

from astarte.device import DeviceGrpc

_INTERFACES_DIR = Path(__file__).parent.joinpath("interfaces").absolute()
_CONFIGURATION_FILE = Path(__file__).parent.joinpath("config.toml").absolute()


def on_connected_cbk(_):
    """
    Callback for a connection event.
    """
    print("Device connected.")


def on_data_received_cbk(device: DeviceGrpc, interface_name: str, path: str, payload: dict):
    """
    Callback for a data reception event.
    """
    print(f"Received message for interface: {interface_name} and path: {path}.")
    print(f"    Payload: {payload}")


def on_disconnected_cbk(_, reason: int):
    """
    Callback for a disconnection event.
    """
    print("Device disconnected" + (f" because: {reason}." if reason else "."))


def stream_data(device: DeviceGrpc):
    """
    Stream some hardcoded tata data from a device to Astarte.
    """

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


# If called as a script
if __name__ == "__main__":

    # Accept an argument to specify a set time duration for the example
    parser = argparse.ArgumentParser(
        description="Datastream sample for the Astarte device SDK Python"
    )
    parser.add_argument(
        "-d",
        "--duration",
        type=int,
        default=30,
        help="Approximated duration in seconds for the example (default: 30)",
    )
    args = parser.parse_args()

    with open(_CONFIGURATION_FILE, "rb") as config_fp:
        config = tomllib.load(config_fp)
        _SERVER_ADDR = config["SERVER_ADDR"]
        _NODE_UUID = config["NODE_UUID"]

    # Instantiate the device
    device = DeviceGrpc(server_addr=_SERVER_ADDR, node_uuid=_NODE_UUID)
    # Load all the interfaces
    device.add_interfaces_from_dir(_INTERFACES_DIR)
    # Set all the callback functions
    device.set_events_callbacks(
        on_connected=on_connected_cbk,
        on_data_received=on_data_received_cbk,
        on_disconnected=on_disconnected_cbk,
    )
    # # Connect the device
    device.connect()
    while not device.is_connected():
        pass

    # Stream some data from device to Astarte
    stream_data(device)

    # Sleep for the example duration
    time.sleep(args.duration)

    device.disconnect()
