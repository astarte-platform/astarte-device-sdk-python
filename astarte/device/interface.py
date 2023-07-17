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

from astarte.device.mapping import Mapping
from astarte.device.exceptions import (
    ValidationError,
    InterfaceNotFoundError,
    InterfaceFileDecodeError,
)

DEVICE = "device"
SERVER = "server"


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
        self.version_major: int = interface_definition.get("version_major")
        self.version_minor: int = interface_definition.get("version_minor")
        self.type: str = interface_definition.get("type")
        self.ownership = interface_definition.get("ownership")

        if not (
            isinstance(self.name, str)
            and isinstance(self.version_major, int)
            and isinstance(self.version_minor, int)
            and self.type in {"datastream", "properties"}
            and self.ownership in (DEVICE, SERVER)
        ):
            raise InterfaceFileDecodeError(
                f"Error parsing the following interface definition: {interface_definition}"
            )

        if not self.version_major and not self.version_minor:
            raise InterfaceFileDecodeError(
                f"Both Major and Minor versions set to 0 for interface {self.name}"
            )

        self.aggregation = interface_definition.get("aggregation", "individual")
        if self.aggregation not in {"individual", "object"}:
            raise InterfaceFileDecodeError(f"Invalid aggregation type for interface {self.name}.")

        self.mappings = []
        endpoints = []
        for mapping_definition in interface_definition.get("mappings", []):
            mapping = Mapping(mapping_definition, self.type)
            if mapping.endpoint in endpoints:
                raise InterfaceFileDecodeError(
                    f"Duplicated mapping {mapping.endpoint} for interface {self.name}."
                )
            self.mappings.append(mapping)
            endpoints.append(mapping.endpoint)

        if not self.mappings:
            raise InterfaceFileDecodeError(f"No mappings in interface {self.name}.")

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
            if not mapping.validate_path(endpoint):
                return mapping
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
        if not self.is_aggregation_object():
            mapping = self.get_mapping(endpoint)
            if not mapping:
                raise InterfaceNotFoundError(f"Path {endpoint} not declared in {self.name}")
            return mapping.reliability
        return 2

    def validate_path(self, path: str, payload) -> ValidationError | None:
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

        Returns
        -------
        ValidationError or None
            None in case of successful validation, ValidationError otherwise
        """
        if not self.is_aggregation_object():
            if not self.get_mapping(path):
                return ValidationError(f"Path {path} not in the {self.name} interface.")
        else:
            for k in payload:
                if not self.get_mapping(f"{path}/{k}"):
                    return ValidationError(f"Path {path}/{k} not in the {self.name} interface.")
        return None

    def validate_payload(self, path: str, payload) -> ValidationError | None:
        """
        Validate that the payload conforms to the interface.

        Parameters
        ----------
        path: str
            Path on which the payload has been received. This is assumed to correspond to a valid
            mapping (or partial mapping in case of aggregate interface). Should be first checked
            with validate_path().
        payload: object
            Data to validate

        Returns
        -------
        ValidationError or None
            None in case of successful validation, ValidationError otherwise
        """
        if not self.is_aggregation_object():
            return self.get_mapping(path).validate_payload(payload)
        if not isinstance(payload, dict):
            return ValidationError(f"Payload not a dict for aggregated interface {self.name}.")
        for k, v in payload.items():
            payload_invalid = self.get_mapping(f"{path}/{k}").validate_payload(v)
            if payload_invalid:
                return payload_invalid
        # Check all the interface endpoints are present in the payload
        return self.validate_object_complete(path, payload)

    def validate_object_complete(self, path: str, payload):
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

        Returns
        -------
        ValidationError or None
            None in case of successful validation, ValidationError otherwise
        """
        if not self.is_server_owned():
            path_segments = path.count("/") + 1
            for endpoint in [m.endpoint for m in self.mappings]:
                non_common_endpoint = "/".join(endpoint.split("/")[path_segments:])
                if non_common_endpoint not in payload:
                    return ValidationError(
                        f"Path {endpoint} of {self.name} interface not in payload."
                    )
        return None

    def validate(self, path: str, payload, timestamp: datetime | None) -> ValidationError | None:
        """
        Interface Data validation.

        Parameters
        ----------
        path: str
            Data endpoint in interface
        payload: object
            Data to validate
        timestamp: datetime or None
            Timestamp associated to the payload

        Returns
        -------
        ValidationError or None
            None in case of successful validation, ValidationError otherwise
        """

        # Validate the payload for the individual mapping
        if not self.is_aggregation_object():
            mapping = self.get_mapping(path)
            if mapping:
                p_err = mapping.validate_payload(payload)
                t_err = mapping.validate_timestamp(timestamp)
                return p_err if p_err else t_err
            return ValidationError(f"Path {path} not in the {self.name} interface.")

        # Validate the payload for the aggregate mapping
        if not isinstance(payload, dict):
            return ValidationError(
                f"The interface {self.name} is aggregate, but the payload is not a dictionary."
            )
        for k, v in payload.items():
            mapping = self.get_mapping(f"{path}/{k}")
            if mapping:
                p_err = mapping.validate_payload(v)
                t_err = mapping.validate_timestamp(timestamp)
                if p_err or t_err:
                    return p_err if p_err else t_err
            else:
                return ValidationError(f"Path {path}/{k} not in the {self.name} interface.")

        # Check all the mappings are present in the payload
        return self.validate_object_complete(path, payload)
