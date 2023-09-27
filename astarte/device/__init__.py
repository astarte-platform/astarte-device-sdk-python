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

# Version of the module
__version__ = "0.12.0"

# Export what we care about
from .device import Device
from .introspection import Introspection
from .interface import Interface
from .mapping import Mapping
from .pairing_handler import (
    register_device_with_jwt_token,
    register_device_with_private_key,
    generate_device_id,
    generate_random_device_id,
)
from .exceptions import (
    AstarteError,
    DeviceAlreadyRegisteredError,
    AuthorizationError,
    APIError,
    ValidationError,
    PersistencyDirectoryNotFoundError,
    InterfaceFileNotFoundError,
    InterfaceFileDecodeError,
    InterfaceNotFoundError,
    JWTGenerationError,
)
