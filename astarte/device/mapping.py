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

from __future__ import annotations

import re
from collections import namedtuple
from datetime import datetime
from math import isfinite
from typing import List, Union

from astarte.device.exceptions import InterfaceFileDecodeError, ValidationError

# Astarte Types definition
IntList = List[int]
FloatList = List[float]
StringList = List[str]
BsonList = List[bytes]
BoolList = List[bool]
DatetimeList = List[datetime]
MapType = Union[
    int,
    float,
    str,
    bytes,
    bool,
    datetime,
    IntList,
    FloatList,
    StringList,
    BsonList,
    BoolList,
    DatetimeList,
]

""" Lookup table for mapping Astarte type to Python type"""
AstarteTypesLookupElement = namedtuple("AstarteTypesLookupElement", ["type", "subtype"])
astarte_types_lookup = {
    "integer": AstarteTypesLookupElement(int, None),
    "longinteger": AstarteTypesLookupElement(int, None),
    "double": AstarteTypesLookupElement(float, None),
    "string": AstarteTypesLookupElement(str, None),
    "binaryblob": AstarteTypesLookupElement(bytes, None),
    "boolean": AstarteTypesLookupElement(bool, None),
    "datetime": AstarteTypesLookupElement(datetime, None),
    "integerarray": AstarteTypesLookupElement(list, int),
    "longintegerarray": AstarteTypesLookupElement(list, int),
    "doublearray": AstarteTypesLookupElement(list, float),
    "stringarray": AstarteTypesLookupElement(list, str),
    "binaryblobarray": AstarteTypesLookupElement(list, bytes),
    "booleanarray": AstarteTypesLookupElement(list, bool),
    "datetimearray": AstarteTypesLookupElement(list, datetime),
}

# Mapping quality of service
QOS_MAP: dict[str, int] = {"unreliable": 0, "guaranteed": 1, "unique": 2}

endpoint_regex = re.compile(r"^(\/(%{([a-zA-Z_]+[a-zA-Z0-9_]*)}|[a-zA-Z_]+[a-zA-Z0-9_]*)){1,64}$")


class Mapping:
    """
    Class that represent a data Mapping
    Mappings are designed around REST controller semantics: each mapping describes an endpoint
    which is resolved to a path, it is strongly typed, and can have additional options. Just like
    in REST controllers, Endpoints can be parametrized to build REST-like collection and trees.
    Parameters are identified by %{parameterName}, with each endpoint supporting any number of
    parameters (see
    `Limitations <https://docs.astarte-platform.org/snapshot/030-interface.html#limitations>`_).

    Attributes
    ----------
    endpoint: str
        Path of the Mapping
    type: str
        Type of the Mapping (see notes)
    explicit_timestamp: bool
        Flag that defines if the Mapping requires a timestamp associated to the Payload before send.
    reliability:
        Reliability level of the Mapping (see notes)
    allow_unset:
        Allow unsetting for properties

    Notes
    -----
        **Supported data types**

        The following types are supported:

        * double: A double-precision floating-point number as specified by binary64, by the IEEE
            754 standard (NaNs and other non-numerical values are not supported).
        * integer: A signed 32 bit integer.
        * boolean: Either true or false, adhering to JSON boolean type.
        * longinteger: A signed 64-bit integer (please note that longinteger is represented as a
            string by default in JSON-based APIs.).
        * string: An UTF-8 string, at most 65536 bytes long.
        * binaryblob: An arbitrary sequence of any byte that should be shorter than 64 KiB. (
            binaryblob is represented as a base64 string by default in JSON-based APIs.).
        * datetime: A UTC timestamp, internally represented as milliseconds since 1st Jan 1970
            using a signed 64 bits integer. (datetime is represented as an ISO 8601 string by
            default in JSON based APIs.)
        * doublearray, integerarray, booleanarray, longintegerarray, stringarray,
            binaryblobarray, datetimearray: A list of values, represented as a JSON Array.
            Arrays can have up to 1024 items and each item must respect the limits of its scalar
            type (i.e. each string in a stringarray must be at most 65535 bytes long, each binary
            blob in a binaryblobarray must be shorter than 64 KiB.)

        **Quality of Service**

        Data messages QoS is chosen according to mapping settings, such as reliability.
        Properties are always published using QoS 2.

        ============== ============== ===
        INTERFACE TYPE RELIABILITY    QOS
        ============== ============== ===
        properties     always unique	2
        datastream	   unreliable	    0
        datastream	   guaranteed	    1
        datastream	   unique	        2
        ============== ============== ===
    """

    def __init__(self, mapping_definition: dict, is_datastream: bool):
        """
        Parameters
        ----------
        mapping_definition: dict
            Mapping from the mappings array of an Astarte Interface definition in the form of a
            Python dictionary. Usually obtained by using json.loads() on an Interface file.
        is_datastream: bool
            True when the mapping belongs to a datastream interface, false otherwise.
        """
        self.endpoint: str = mapping_definition.get("endpoint")
        if not isinstance(self.endpoint, str):
            raise InterfaceFileDecodeError(
                "Endpoint is a required mapping field and should be a string."
            )
        if endpoint_regex.match(self.endpoint) is None:
            raise InterfaceFileDecodeError(
                f"The following endpoint is not correctly formatted {self.endpoint}."
            )

        self.type: str = mapping_definition.get("type")
        if self.type not in astarte_types_lookup:
            raise InterfaceFileDecodeError(
                "type is a required mapping field and should match one of the allowed types."
            )
        self.__actual_type = astarte_types_lookup.get(self.type)

        self.explicit_timestamp = mapping_definition.get("explicit_timestamp", False)
        if not isinstance(self.explicit_timestamp, bool):
            raise InterfaceFileDecodeError("Explicit timestamp should have a boolean value.")

        default_reliability = "unreliable" if is_datastream else "unique"
        reliability_str = mapping_definition.get("reliability", default_reliability)
        self.reliability: int = QOS_MAP.get(reliability_str)
        if not isinstance(self.reliability, int):
            raise InterfaceFileDecodeError(
                "reliability is a required mapping field and should be one of: 'unreliable', "
                "'guaranteed' or 'unique'."
            )

        self.allow_unset = mapping_definition.get("allow_unset", False)
        if not isinstance(self.allow_unset, bool):
            raise InterfaceFileDecodeError("Allow unset should have a boolean value.")

        if any(k in mapping_definition for k in ("explicit_timestamp", "reliability")) and (
            not is_datastream
        ):
            raise InterfaceFileDecodeError(
                "Fields 'reliability' and 'explicit_timestamp' have no meaning for properties."
            )
        if ("allow_unset" in mapping_definition) and is_datastream:
            raise InterfaceFileDecodeError("Field 'allow_unset' has no meaning for datastreams.")

    def validate_path(self, path: str):
        """
        Validate an endpoint against the endpoints declared in the mapping.

        Parameters
        ----------
        path: Str
            Path to validate.

        Raises
        ------
        ValidationError
            When validation has failed.
        """
        regex = re.sub(r"%{([a-zA-Z_]+[a-zA-Z0-9_]*)}", r"[a-zA-Z_]+[a-zA-Z0-9_]*", self.endpoint)
        if not re.match(regex + "$", path):
            raise ValidationError(f"Path {path} does not match the endpoint {self.endpoint}")

    def validate_timestamp(self, timestamp: datetime | None):
        """
        Mapping timestamp validation

        Parameters
        ----------
        timestamp: datetime or None
            Timestamp associated to the payload

        Raises
        ------
        ValidationError
            When validation has failed.
        """
        if self.explicit_timestamp and not timestamp:
            raise ValidationError(f"Timestamp required for {self.endpoint}")
        if not self.explicit_timestamp and timestamp:
            raise ValidationError(f"It's not possible to set the timestamp for {self.endpoint}")

    def validate_payload(self, payload: MapType):
        """
        Mapping data validation

        Parameters
        ----------
        payload: MapType
            Data to validate

        Raises
        ------
        ValidationError
            When validation has failed.
        """
        if self.__actual_type.type != list:
            self._validate_element(payload, self.__actual_type.type)
        else:
            # Using `is` instead of `isinstance` to avoid false negatives for inheritance of the
            # list elements.
            # pylint: disable-next=unidiomatic-typecheck
            if type(payload) is not list:
                raise ValidationError(f"Expecting list payload for {self.endpoint}")
            for element in payload:
                self._validate_element(element, self.__actual_type.subtype)

    def _validate_element(self, element: object, element_type: type):
        """
        Validate a single element.

        Parameters
        ----------
        element: object
            Element to validate.
        element_type: type
            Expected type of the element.

        Raises
        ------
        ValidationError
            When validation has failed.
        """
        MIN_INT32 = -2147483648
        MAX_INT32 = 2147483647

        # Using `is` instead of `isinstance` to avoid false positives for inheritance.
        # pylint: disable-next=unidiomatic-typecheck
        if type(element) is not element_type:
            raise ValidationError(
                f"{self.endpoint} is {self.type} but found {type(element)} in payload."
            )
        # Raise error for an integer outside allowed interval.
        if (self.type in {"integer", "integerarray"}) and not (MIN_INT32 <= element <= MAX_INT32):
            raise ValidationError(f"Value out of int32 range for {self.endpoint}")
        # Raise error for a double value which is not a number
        if (self.type in {"double", "doublearray"}) and not isfinite(element):
            raise ValidationError(f"Invalid float value for {self.endpoint}")
