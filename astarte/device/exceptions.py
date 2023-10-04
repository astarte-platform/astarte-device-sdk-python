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


class AstarteError(Exception):
    """Base class for Astarte Errors."""


class DeviceAlreadyRegisteredError(AstarteError):
    """Exception raised in case a device being registered has already been registered."""


class AuthorizationError(AstarteError):
    """Exception raised when Astarte APIs refuse authentication.

    Attributes:
        body -- the body of the API reply, which may carry further details
    """

    def __init__(self, body):
        self.body = body


class APIError(AstarteError):
    """Exception raised when Astarte APIs throw unhandled errors.

    Attributes:
        body -- the body of the API reply, which may carry further details
    """

    def __init__(self, body):
        self.body = body


class ValidationError(AstarteError):
    """Exception raised when validation has failed.

    Attributes:
        msg -- A message error carrying further details
    """

    def __init__(self, msg):
        self.msg = msg


class PersistencyDirectoryNotFoundError(AstarteError):
    """Exception raised when the provided persistency directory is not found.

    Attributes:
        body -- the body of the API reply, which may carry further details
    """

    def __init__(self, body):
        self.body = body


class InterfaceFileNotFoundError(AstarteError):
    """Exception raised when a file containing an interface definition does not exists.

    Attributes:
        msg -- A message error carrying further details
    """

    def __init__(self, msg):
        self.msg = msg


class InterfaceFileDecodeError(AstarteError):
    """Exception raised when an interface .json file is not correctly formatted.

    Attributes:
        msg -- A message error carrying further details
    """

    def __init__(self, msg):
        self.msg = msg


class InterfaceNotFoundError(AstarteError):
    """Exception raised when an interface is not found in the device introspection.

    Attributes:
        msg -- A message error carrying further details
    """

    def __init__(self, msg):
        self.msg = msg


class JWTGenerationError(AstarteError):
    """Exception raised when the generation of a Jason Web Token has failed.

    Attributes:
        msg -- A message error carrying further details
    """

    def __init__(self, msg):
        self.msg = msg


class DeviceConnectingError(AstarteError):
    """Exception raised when an operation is attempted while the device MQTT client has been
    started but is not yet connected.

    Attributes:
        msg -- A message error carrying further details
    """

    def __init__(self, msg):
        self.msg = msg


class DeviceDisconnectedError(AstarteError):
    """Exception raised if attempting a send while the device is disconnected from Astarte.

    Attributes:
        msg -- A message error carrying further details
    """

    def __init__(self, msg):
        self.msg = msg
