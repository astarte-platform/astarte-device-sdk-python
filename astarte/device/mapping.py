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

from datetime import datetime
from math import isfinite
from re import sub, match
from typing import Union, List

from astarte.device.exceptions import ValidationError, InterfaceFileDecodeError

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

""" Mapping type to python type mapping"""
type_strings = {
    "integer": int,
    "longinteger": int,
    "double": float,
    "string": str,
    "binaryblob": bytes,
    "boolean": bool,
    "datetime": datetime,
    "integerarray": list,
    "longintegerarray": list,
    "doublearray": list,
    "stringarray": list,
    "binaryblobarray": list,
    "booleanarray": list,
    "datetimearray": list,
}

# Mapping quality of service
QOS_MAP = {"unreliable": 0, "guaranteed": 1, "unique": 2}


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

    def __init__(self, mapping_definition: dict, interface_type: str):
        """
        Parameters
        ----------
        mapping_definition: dict
            Mapping from the mappings array of an Astarte Interface definition in the form of a
            Python dictionary. Usually obtained by using json.loads() on an Interface file.
        interface_type: str
            Type of the parent Interface, used to determine the default reliability
        """
        self.endpoint: str = mapping_definition.get("endpoint")
        self.type: str = mapping_definition.get("type")
        self.__actual_type = type_strings.get(mapping_definition.get("type"))
        self.explicit_timestamp = mapping_definition.get("explicit_timestamp", False)
        default_reliability = "unreliable" if interface_type == "datastream" else "unique"
        self.reliability = QOS_MAP.get(mapping_definition.get("reliability", default_reliability))
        self.allow_unset = mapping_definition.get("allow_unset", False)

        if not (isinstance(self.endpoint, str) and self.__actual_type):
            raise InterfaceFileDecodeError("Error parsing the mapping.")
        if (interface_type != "datastream") and any(
            k in mapping_definition for k in ("explicit_timestamp", "reliability")
        ):
            raise InterfaceFileDecodeError(
                "Fields 'reliability' and 'explicit_timestamp' have no meaning for properties."
            )
        if ("allow_unset" in mapping_definition) and (interface_type != "properties"):
            raise InterfaceFileDecodeError("Field 'allow_unset' have no meaning for datastreams.")

    def validate_path(self, path: str) -> ValidationError | None:
        """
        Mapping data validation

        Parameters
        ----------
        path: Str
            Path to validate.

        Returns
        -------
        ValidationError or None
            None in case of successful validation, ValidationError otherwise
        """
        regex = sub(r"%{\w+}", r"[^/+#]+", self.endpoint)
        if not match(regex + "$", path):
            return ValidationError(f"Path {path} does not match the endpoint {self.endpoint}")
        return None

    def validate_timestamp(self, timestamp: datetime | None) -> ValidationError | None:
        """
        Mapping timestamp validation

        Parameters
        ----------
        timestamp: datetime or None
            Timestamp associated to the payload

        Returns
        -------
        ValidationError or None
            None in case of successful validation, ValidationError otherwise
        """
        if self.explicit_timestamp and not timestamp:
            return ValidationError(f"Timestamp required for {self.endpoint}")
        if not self.explicit_timestamp and timestamp:
            return ValidationError(f"It's not possible to set the timestamp for {self.endpoint}")
        return None

    def validate_payload(self, payload: MapType) -> ValidationError | None:
        """
        Mapping data validation

        Parameters
        ----------
        payload: MapType
            Data to validate

        Returns
        -------
        ValidationError or None
            None in case of successful validation, ValidationError otherwise
        """
        min_supported_int = -2147483648
        max_supported_int = 2147483647
        if payload in [None, []]:
            return ValidationError(f"Attempting to validate an empty payload for {self.endpoint}")
        # Check if the interface has explicit_timestamp when a timestamp is given (and viceversa)
        # Check the type of data is valid for that endpoint
        # pylint: disable-next=unidiomatic-typecheck
        if not type(payload) is self.__actual_type:
            return ValidationError(
                f"{self.endpoint} is {self.type} but {type(payload)} was provided"
            )
        # Must return False when trying to send an integer outside allowed interval.
        if self.type == "integer" and not min_supported_int <= payload <= max_supported_int:
            return ValidationError(f"Value out of int32 range for {self.endpoint}")
        # Must return False when trying to send a double value which is not a number
        if self.type == "double" and not isfinite(payload):
            return ValidationError(f"Invalid float value for {self.endpoint}")
        # Check types of all element in a list
        if self.__actual_type == list:
            # Check coherence
            if any(not type(elem) is type(payload[0]) for elem in payload):
                return ValidationError("Type incoherence in payload elements")
            subtype: type = type_strings.get(self.type.replace("array", ""))
            # pylint: disable-next=unidiomatic-typecheck
            if not type(payload[0]) is subtype:
                return ValidationError(
                    f"{self.endpoint} is {self.type} but a list of {type(payload[0])} was provided"
                )
            # Must return False when trying to send an integer outside allowed interval.
            if self.type == "integerarray" and any(
                elem < min_supported_int or elem > max_supported_int for elem in payload
            ):
                return ValidationError(f"Value out of int32 range for {self.endpoint}")
            # Must return False when trying to send a double value which is not a number
            if self.type == "doublearray" and any(not isfinite(elem) for elem in payload):
                return ValidationError(f"Invalid float value for {self.endpoint}")
        return None
