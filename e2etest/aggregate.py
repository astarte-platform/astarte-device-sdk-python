"""
Contains the tests for aggregated object datastreams.
"""

import time
import base64
import copy
from threading import Lock
from dateutil import parser
from termcolor import cprint

from astarte.device import Device
from e2etest.http_requests import get_server_interface, post_server_interface

from e2etest.config import TestCfg


def test_aggregate_from_device_to_server(device: Device, test_cfg: TestCfg):
    """
    Test for aggregated object datastreams in the direction from device to server
    """
    cprint(
        "\nSending device owned aggregates from device to server.",
        color="cyan",
        flush=True,
    )
    device.send_aggregate(test_cfg.interface_device_aggr, "/sensor-id", test_cfg.mock_data)

    time.sleep(1)

    cprint("\nChecking data stored on the server.", color="cyan", flush=True)
    json_res = get_server_interface(test_cfg, test_cfg.interface_device_aggr)
    parsed_res = json_res.get("data", {}).get("sensor-id")
    if not parsed_res:
        raise ValueError("Incorrectly formatted response from server")
    if isinstance(parsed_res, list):
        parsed_res = parsed_res[-1]

    # Remove timestamp
    parsed_res.pop("timestamp")

    # Parse longint from string to int
    parsed_res["longinteger_endpoint"] = int(parsed_res["longinteger_endpoint"])
    parsed_res["longintegerarray_endpoint"] = [
        int(dt) for dt in parsed_res["longintegerarray_endpoint"]
    ]

    # Parse datetime from string to datetime
    parsed_res["datetime_endpoint"] = parser.parse(parsed_res["datetime_endpoint"])
    parsed_res["datetimearray_endpoint"] = [
        parser.parse(dt) for dt in parsed_res["datetimearray_endpoint"]
    ]

    # Decode binary blob from base64
    parsed_res["binaryblob_endpoint"] = base64.b64decode(parsed_res["binaryblob_endpoint"])
    parsed_res["binaryblobarray_endpoint"] = [
        base64.b64decode(dt) for dt in parsed_res["binaryblobarray_endpoint"]
    ]

    # Check received and sent data match
    if parsed_res != test_cfg.mock_data:
        raise ValueError("Incorrect data stored on server")


def test_aggregate_from_server_to_device(test_cfg: TestCfg, rx_data_lock: Lock, rx_data: dict):
    """
    Test for aggregated object datastreams in the direction from server to device
    """
    cprint(
        "\nSending server owned aggregates from server to device.",
        color="cyan",
        flush=True,
    )

    data = copy.deepcopy(test_cfg.mock_data)
    # Needed untill Astarte is updated to v1.1. See:
    # https://github.com/astarte-platform/astarte/issues/754
    # https://github.com/astarte-platform/astarte/issues/753
    data.pop("binaryblob_endpoint")
    data.pop("datetime_endpoint")
    data.pop("binaryblobarray_endpoint")
    data.pop("datetimearray_endpoint")
    post_server_interface(test_cfg, test_cfg.interface_server_aggr, "/sensor-id", data)

    time.sleep(1)

    cprint("\nChecking data received by the device.", color="cyan", flush=True)
    with rx_data_lock:
        if not rx_data.get(test_cfg.interface_server_aggr):
            raise ValueError(
                f"No data from this interface has been received {test_cfg.interface_server_aggr}"
            )
        parsed_rx_data = rx_data.get(test_cfg.interface_server_aggr).get("/sensor-id")

    # Make sure all the data has been correctly received
    if parsed_rx_data != data:
        raise ValueError("Incorrectly formatted response from server")
