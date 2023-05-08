<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte Python Device SDK

Python Device SDK for [Astarte](https://github.com/astarte-platform/astarte). Create Astarte Devices
and Simulators with Python3.
It integrates with asyncio to ensure a smooth developer experience and to hide complex details
regarding threading and MQTT interactions.

## How to get with Pip

The Astarte device SDK can be obtained by running:
```
pip install astarte-device-sdk
```

## Basic usage

### Create a device

Initializing an instance of a device can be performed in three steps, as seen below.
```python
from astarte.device import Device

# Create the device instance
device = Device(
    device_id="device id",
    realm="realm",
    credentials_secret="credentials secret",
    pairing_base_url="pairing url",
    persistency_dir=".",
    loop=None,
    ignore_ssl_errors=False,
)
# Add a single interface from a .json file
device.add_interface_from_file(Path("interfaces/path/file.json"))
# Use `device.add_interfaces_from_dir(Path("interfaces/path"))` instead to add all the interfaces in a directory
# Connect to Astarte
device.connect()
```

### Publish data from device

Publishing new values can be performed using the `send` and `send_aggregate` functions.
```python
from astarte.device import Device
from datetime import datetime, timezone

# ... Create a device and connect it to Astarte ...

# Send an individual datastream or a property
device.send(
    interface_name="datastream_interface",
    interface_path=f"/path/name",
    payload="payload",
    timestamp=None,
)

# Send an aggregated object datastream
payload = {"endpoint1": "value1", "endpoint2": 42}
device.send_aggregate(
    interface_name="aggregate_interface",
    interface_path=f"/path/name",
    payload=payload,
    timestamp=datetime.now(tz=timezone.utc),
)
```

### Receive a server publication

The device automatically polls for new messages. The user can use a call back function to process
received data. Callback functions are also available for connect/disconnect events.
```python
from astarte.device import Device

def my_callback(device: Device, name: str, path: str, payload: dict):
    print(f"Received message for {name}{path}: {payload}")

# ... Create a device and connect it to Astarte ...

# Setup the callback
device.on_data_received = my_callback

# Keep the program running
while True:
    pass
```
