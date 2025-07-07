<!--
Copyright 2023 SECO Mind Srl

SPDX-License-Identifier: Apache-2.0
-->

# Astarte device SDK Python GRPC example
This is an example of how to use the device SDK to connect a to an existing Astarte message hub
instance and handle datastream messages from/to the node.

## Prerequisites

An instance of the message hub should be running on an available machine. See the
[documentation](https://docs.rs/astarte-message-hub/latest/astarte_message_hub/) for the
Astarte message hub for more information.

## Downloading the sample

The sample is available on the
[GitHub repository](https://github.com/astarte-platform/astarte-device-sdk-python/tree/v0.14.0)
for this project.
You can download and unpack the content of this sample folder with the following `curl` and `tar`
commands:
```shell
mkdir astarte-python-example && cd astarte-python-example
curl -L https://api.github.com/repos/astarte-platform/astarte-device-sdk-python/tarball/v0.14.0 | tar -xz --strip-components=3 --wildcards "astarte-platform-astarte-device-sdk-python*/examples/grpc/"
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

## Device configuration

Before running the example the following variables must be set at the beginnign of the example
script.

```python
_SERVER_ADDR = "SERVER ADDRESS HERE"
_NODE_UUID = "NODE UUID HERE"
```

The `_NODE_UUID` should be set to an UUID that will be used as the node unique identifier.
Ensure that there is no other node connected to the message hub with the same UUID. See
the [UUID specification](https://datatracker.ietf.org/doc/html/rfc4122) for more information
regarding allowed values.

The `_SERVER_ADDR` should be set to the message hub server address. For the most common case of a
message hub server running on the same machine as all the nodes, the server address will be in
the format `localhost:GRPC_SOCKET_PORT` where `GRPC_SOCKET_PORT` is the port assinged to the GRPC
socket.

## Running the example

Then to start the example run in the example directory the following command:
```shell
python main.py
```
