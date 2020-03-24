# Copyright 2020 Ispirata S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

class AstarteError(Exception):
    """Base class for Astarte Errors."""
    pass

class DeviceAlreadyRegisteredError(AstarteError):
    """Exception raised in case a Device being registered has already been registered.
    """

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
