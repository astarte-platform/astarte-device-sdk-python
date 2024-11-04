<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte device SDK Python datastreams example
This is an example of how to use the device SDK to connect a device to Astarte
and handle datastream messages from/to the device.

## Usage
### 1. Device registration and credentials secret emission
The device must be registered beforehand to obtain its credentials-secret.

1. Using the astartectl command [astartectl](https://github.com/astarte-platform/astartectl).
2. Using the [Astarte Dashboard](https://docs.astarte-platform.org/snapshot/015-astarte_dashboard.html),
which is located at `https://dashboard.<your-astarte-domain>.`

### 2. Configuration file
Before running the example the configuration file `config.toml` should be updated to contain user
specific configuration.

```toml
DEVICE_ID = 'DEVICE ID HERE'
REALM = 'REALM HERE'
CREDENTIALS_SECRET = 'CREDENTIAL SECRET HERE'
PAIRING_URL = 'PAIRING URL HERE'
```

### 3. Running the example

To run the example the Astarte device SDK should be installed. Installing the latest release can be
done through pip with:
```shell
pip install astarte-device-sdk
```
Then to start the example run in the example directory the following command:
```shell
python main.py
```
