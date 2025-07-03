<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte device SDK Python datastreams example
This is an example of how to use the device SDK to connect a device to Astarte
and handle datastream messages from/to the device.

## Downloading the sample

The sample is available on the
[GitHub repository](https://github.com/astarte-platform/astarte-device-sdk-python/tree/v0.14.0)
for this project.
ou can download and unpack the content of this sample folder with the following `curl` and `tar`
commands:
```shell
mkdir astarte-python-example && cd astarte-python-example
curl -L https://api.github.com/repos/astarte-platform/astarte-device-sdk-python/tarball/v0.14.0 | tar -xz --strip-components=3 --wildcards "astarte-platform-astarte-device-sdk-python*/examples/mqtt/"
```
Alternatively you can check out the full project with git. However, make sure you check out the
release tag corresponding to the correct version.

## Creating a virtual environment and installing the SDK

We strongly suggest the use of a virtual environment.
Create and activate a new venv.

On linux:
```shell
python -m venv .venv
source .venv/bin/activate
```
On windows:
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Next install the Astarte device SDK with pip:
```shell
pip install astarte-device-sdk==0.14.0
```

## Device registration and credentials secret emission
The device must be registered beforehand to obtain its credentials-secret.

1. Using the astartectl command [astartectl](https://github.com/astarte-platform/astartectl).
2. Using the [Astarte Dashboard](https://docs.astarte-platform.org/snapshot/015-astarte_dashboard.html),
which is located at `https://dashboard.<your-astarte-domain>.`

## Configuration file
Before running the example the configuration file `config.toml` should be updated to contain user
specific configuration.

```toml
DEVICE_ID = 'DEVICE ID HERE'
REALM = 'REALM HERE'
CREDENTIALS_SECRET = 'CREDENTIAL SECRET HERE'
PAIRING_URL = 'PAIRING URL HERE'
```

## Running the example

Then to start the example run in the example directory the following command:
```shell
python main.py
```
