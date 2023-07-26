<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte device SDK Python datastreams example
This is an example of how to use the Device SDK to connect a device to Astarte
and handle datastream messages from/to the device.

## Usage
### 1. Device registration and credentials secret emission
The device must be registered beforehand to obtain its credentials-secret.

1. Using the astartectl command [astartectl](https://github.com/astarte-platform/astartectl).
2. Using the [Astarte Dashboard](https://docs.astarte-platform.org/snapshot/015-astarte_dashboard.html),
which is located at `https://dashboard.<your-astarte-domain>.`

### 2. Run example
Before running the example the following constants must have a value at
the start of `event_listener.py`

```python
_DEVICE_ID = 'DEVICE ID HERE'
_REALM = 'REALM HERE'
_CREDENTIAL_SECRET = 'CREDENTIAL SECRET HERE'
_PAIRING_URL = 'PAIRING URL HERE'
```

Then from this folder run the following:
```shell
pip install -e ../../
python event_listener.py
```
