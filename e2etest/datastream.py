"""
Contains the tests for individual datastreams.
"""
import time
from threading import Lock
from termcolor import cprint

from e2etest.config import TestCfg
from e2etest.http_requests import (
    get_server_interface,
    post_server_interface,
    parse_received_data,
    prepare_transmit_data,
)

from astarte.device import Device


def test_datastream_from_device_to_server(device: Device, test_cfg: TestCfg):
    """
    Test for individual datastreams in the direction from device to server
    """
    cprint(
        "\nSending device owned datastreams from device to server.",
        color="cyan",
        flush=True,
    )
    for key, value in test_cfg.mock_data.items():
        device.send(test_cfg.interface_device_data, "/" + key, value)
        time.sleep(0.005)

    time.sleep(1)

    cprint("\nChecking data stored on the server.", color="cyan", flush=True)
    json_res = get_server_interface(test_cfg, test_cfg.interface_device_data)
    parsed_res = {key: value.get("value") for key, value in json_res.get("data", {}).items()}
    if (not parsed_res) or (not all(parsed_res.values())):
        raise ValueError("Incorrectly formatted response from server")

    # Make sure all the keys have been correctly received
    if parsed_res.keys() != test_cfg.mock_data.keys():
        raise ValueError("Incorrectly formatted response from server")

    parse_received_data(parsed_res)

    # Check received and sent data match
    if parsed_res != test_cfg.mock_data:
        raise ValueError("Incorrect data stored on server")


def test_datastream_from_server_to_device(test_cfg: TestCfg, rx_data_lock: Lock, rx_data: dict):
    """
    Test for individual datastreams in the direction from server to device
    """
    cprint(
        "\nSending server owned datastreams from server to device.",
        color="cyan",
        flush=True,
    )

    for key, value in test_cfg.mock_data.items():
        value = prepare_transmit_data(key, value)
        post_server_interface(test_cfg, test_cfg.interface_server_data, "/" + key, value)
        time.sleep(0.005)

    time.sleep(1)

    cprint("\nChecking data received by the device.", color="cyan", flush=True)

    with rx_data_lock:
        if not rx_data.get(test_cfg.interface_server_data):
            raise ValueError(
                f"No data from this interface has been received {test_cfg.interface_server_data}"
            )
        parsed_rx_data = rx_data.get(test_cfg.interface_server_data)

    # Make sure all the data has been correctly received
    if parsed_rx_data != {("/" + k): v for (k, v) in test_cfg.mock_data.items()}:
        cprint(parsed_rx_data, "red", flush=True)
        raise ValueError("Incorrectly formatted response from server")
