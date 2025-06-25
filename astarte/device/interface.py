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
from datetime import datetime

from astarte.device.exceptions import (
    InterfaceFileDecodeError,
    InterfaceNotFoundError,
    ValidationError,
)
from astarte.device.mapping import Mapping

DEVICE = "device"
SERVER = "server"

name_regex = re.compile(
    r"^([a-zA-Z][a-zA-Z0-9]*\.([a-zA-Z0-9][a-zA-Z0-9-]*\.)*)?[a-zA-Z][a-zA-Z0-9]*$"
)


class Interface:
    """
    Class that represent an Interface definition

    Interfaces are a core concept of Astarte which defines how data is exchanged between Astarte
    and its peers. They are not to be intended as OOP interfaces, but rather as the following
    definition:

    In Astarte each interface has an owner, can represent either a continuous data stream or a
    snapshot of a set of properties, and can be either aggregated into an object or be an
    independent set of individual members.

    Attributes
    ----------
        name: str
            Interface name
        version_major: int
            Interface version major number
        version_minor: int
            Interface version minor number
        type: str
            Interface type
        ownership: str
            Interface ownership
        aggregation: str
            Interface aggregation policy
        mappings: dict(Mapping)
            Interface mapping dictionary, keys are the endpoint of each mapping
    """

    def __init__(self, interface_definition: dict):
        """
        Parameters
        ----------
        interface_definition: dict
            An Astarte Interface definition in the form of a Python dictionary. Usually obtained
            by using json.loads on an Interface file.

        Raises
        ------
        ValueError
            if both version_major and version_minor numbers are set to 0
        """

        self.name: str = interface_definition.get("interface_name")
        if not isinstance(self.name, str):
            raise InterfaceFileDecodeError(
                "Interface name is a required interface field and should be a string."
            )
        if name_regex.match(self.name) is None:
            raise InterfaceFileDecodeError(
                f"Interface name is not correctly formatted: {self.name}"
            )

        self.version_major: int = interface_definition.get("version_major")
        if not isinstance(self.version_major, int):
            raise InterfaceFileDecodeError(
                "Major version is a required interface field and should be an integer."
            )

        self.version_minor: int = interface_definition.get("version_minor")
        if not isinstance(self.version_minor, int):
            raise InterfaceFileDecodeError(
                "Minor version is a required interface field and should be an integer."
            )

        if (not self.version_major) and (not self.version_minor):
            raise InterfaceFileDecodeError(
                f"Both Major and Minor versions set to 0 for interface {self.name}"
            )

        self.type: str = interface_definition.get("type")
        if self.type not in {"datastream", "properties"}:
            raise InterfaceFileDecodeError(
                "Interface type can be one of 'datastream' and 'properties."
            )

        self.ownership: str = interface_definition.get("ownership")
        if self.ownership not in (DEVICE, SERVER):
            raise InterfaceFileDecodeError(
                f"Interface ownership can be one of '{DEVICE}' and '{SERVER}'."
            )

        self.aggregation: str = interface_definition.get("aggregation", "individual")
        if self.aggregation not in {"individual", "object"}:
            raise InterfaceFileDecodeError(f"Invalid aggregation type for interface {self.name}.")

        if (self.type == "properties") and (self.aggregation == "object"):
            raise InterfaceFileDecodeError(
                "Invalid aggregation type 'object', properties can only be 'individual'."
            )

        self.mappings: list[Mapping] = []
        endpoints = []
        for mapping_definition in interface_definition.get("mappings", []):
            mapping = Mapping(mapping_definition, self.type == "datastream")
            if mapping.endpoint in endpoints:
                raise InterfaceFileDecodeError(
                    f"Duplicated mapping {mapping.endpoint} for interface {self.name}."
                )
            self.mappings.append(mapping)
            endpoints.append(mapping.endpoint)

        if not self.mappings:
            raise InterfaceFileDecodeError(f"No mappings in interface {self.name}.")

        if self.aggregation == "object":
            expl_ts_and_qos = [(m.explicit_timestamp, m.reliability) for m in self.mappings]
            if len(set(expl_ts_and_qos)) != 1:
                raise InterfaceFileDecodeError(
                    "All the mappings for objects should have the same explicit_timestamp and "
                    "reliability fields."
                )

    def is_aggregation_object(self) -> bool:
        """
        Check if the current Interface is a datastream with aggregation object
        Returns
        -------
        bool
            True if aggregation: object
        """
        return self.aggregation == "object"

    def is_server_owned(self) -> bool:
        """
        Check the Interface ownership
        Returns
        -------
        bool
            True if ownership: server
        """
        return self.ownership == SERVER

    def is_type_properties(self):
        """
        Check the Interface type
        Returns
        -------
        bool
            True if type: properties
        """
        return self.type == "properties"

    def is_property_endpoint_resettable(self, endpoint):
        """
        Check the resettability of an endpoint.
        Parameters
        ----------
        endpoint: str
            The Mapping endpoint

        Returns
        -------
        bool
            True if type is properties, endpoint is valid and resettable
        """
        if self.is_type_properties():
            mapping = self.get_mapping(endpoint)
            if mapping:
                return mapping.allow_unset
        return False

    def get_mapping(self, endpoint) -> Mapping | None:
        """
        Retrieve the Mapping with the given endpoint from the Interface
        Parameters
        ----------
        endpoint: str
            The Mapping endpoint

        Returns
        -------
        Mapping or None
            The Mapping if found, None otherwise
        """
        for mapping in self.mappings:
            try:
                mapping.validate_path(endpoint)
                return mapping
            except ValidationError:
                pass
        return None

    def get_reliability(self, endpoint: str) -> int:
        """
        Get the reliability for the mapping corresponding to the provided endpoint.

        Parameters
        ----------
        endpoint : str
            The Mapping endpoint to deduce reliability from.

        Returns
        -------
        int
            The deduced reliability, one of [0,1,2].

        Raises
        ------
        InterfaceNotFoundError
            If the interface is not declared in the introspection.
        """
        if self.is_type_properties():
            return 2

        if not self.is_aggregation_object():
            mapping = self.get_mapping(endpoint)
            if not mapping:
                raise InterfaceNotFoundError(f"Path {endpoint} not declared in {self.name}")
            return mapping.reliability

        mapping = self.mappings[0]
        return mapping.reliability

    def validate_path(self, path: str, payload):
        """
        Validate that the provided path conforms to the interface.

        Parameters
        ----------
        path: str
            Path to validate. In case of an individual interface it should correspond to the full
            endpoint, while in case of aggregated interfaces it should correspond to the common
            part to all the endpoints.
        payload: object
            Payload used to extrapolate the remaining endpoints for aggregated interfaces.

        Raises
        ------
        ValidationError
            When validation has failed.
        """
        if not self.is_aggregation_object():
            if not self.get_mapping(path):
                raise ValidationError(f"Path {path} not in the {self.name} interface.")
        else:
            for k in payload:
                if not self.get_mapping(f"{path}/{k}"):
                    raise ValidationError(f"Path {path}/{k} not in the {self.name} interface.")

    def validate_payload(self, path: str, payload):
        """
        Validate that the payload conforms to the interface definition.

        Parameters
        ----------
        path: str
            Path on which the payload has been received. This is assumed to correspond to a valid
            mapping (or partial mapping in case of aggregate interface). Should be first checked
            with validate_path().
        payload: object
            Data to validate

        Raises
        ------
        ValidationError
            When validation has failed.
        """

        # Validate the payload for the individual mapping
        if not self.is_aggregation_object():
            mapping: Mapping = self.get_mapping(path)
            if mapping is None:
                raise ValidationError(f"Mapping not found for path {path}.")
            mapping.validate_payload(payload)
            return

        # Validate the payload for the aggregate mapping
        if not isinstance(payload, dict):
            raise ValidationError(f"Payload not a dict for aggregated interface {self.name}.")
        for k, v in payload.items():
            mapping: Mapping = self.get_mapping(f"{path}/{k}")
            if mapping is None:
                raise ValidationError(f"Mapping not found for path {path}/{k}.")
            mapping.validate_payload(v)

        # Check all the interface endpoints are present in the payload
        if not self.is_server_owned():
            self._validate_object_completeness(path, payload)

    def validate_payload_and_timestamp(self, path: str, payload, timestamp: datetime | None):
        """
        Validate that path, payload and timestamp conform to the interface definition.

        Parameters
        ----------
        path: str
            Data endpoint in interface
        payload: object
            Data to validate
        timestamp: datetime or None
            Timestamp associated to the payload

        Raises
        ------
        ValidationError
            When validation has failed.
        """

        # Validate the payload for the individual mapping
        if not self.is_aggregation_object():
            mapping = self.get_mapping(path)
            if mapping is None:
                raise ValidationError(f"Path {path} not in the {self.name} interface.")
            mapping.validate_payload(payload)
            mapping.validate_timestamp(timestamp)
            return

        # Validate the payload for the aggregate mapping
        if not isinstance(payload, dict):
            raise ValidationError(f"Interface {self.name} is aggregate, payload not a dictionary.")
        for k, v in payload.items():
            mapping = self.get_mapping(f"{path}/{k}")
            if mapping is None:
                raise ValidationError(f"Path {path}/{k} not in the {self.name} interface.")
            mapping.validate_payload(v)
            mapping.validate_timestamp(timestamp)

        # Check all the mappings are present in the payload
        if not self.is_server_owned():
            self._validate_object_completeness(path, payload)

    def _validate_object_completeness(self, path: str, payload):
        """
        Validate that the payload contains all the endpoints for an aggregated interface.
        Shall only be used on device owned interfaces, as server interfaces could be sent
        incomplete.

        Parameters
        ----------
        path: str
            Path on which the payload has been received. This is assumed to correspond to a valid
            partial mapping.
        payload: object
            Data to validate

        Raises
        ------
        ValidationError
            When validation has failed.
        """
        path_segments = path.count("/") + 1
        for endpoint in [m.endpoint for m in self.mappings]:
            non_common_endpoint = "/".join(endpoint.split("/")[path_segments:])
            if non_common_endpoint not in payload:
                raise ValidationError(f"Path {endpoint} of {self.name} interface not in payload.")
