# This file is part of Astarte.
#
# Copyright 2025 SECO Mind Srl
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

from datetime import datetime
from typing import Tuple, Union

TypeAstarteDataScalar = Union[float, bool, int, str, bytes, datetime]
TypeAstarteDataVector = Union[
    list[float], list[bool], list[int], list[str], list[bytes], list[datetime]
]
TypeAstarteData = Union[TypeAstarteDataScalar, TypeAstarteDataVector]

TypeAstarteObject = dict[str, TypeAstarteData]
TypeInputPayload = Union[TypeAstarteObject, TypeAstarteData, None]

TypeConvertedAstarteMessage = Tuple[str, str, TypeInputPayload]
