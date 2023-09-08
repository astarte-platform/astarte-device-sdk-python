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

import collections.abc
import json
from datetime import datetime
from collections.abc import Callable
import threading
import asyncio
import grpc
from collections import namedtuple

# pylint: disable=no-name-in-module
from google.protobuf.timestamp_pb2 import Timestamp

from astarte.device.device import Device
from astarte.device.interface import Interface
from astarte.device.exceptions import ValidationError

from astarteplatform.msghub.message_hub_service_pb2_grpc import MessageHubStub
from astarteplatform.msghub.node_pb2 import Node
from astarteplatform.msghub.astarte_message_pb2 import AstarteMessage, AstarteUnset
from astarteplatform.msghub.astarte_type_pb2 import (
    AstarteDataType,
    AstarteDataTypeIndividual,
    AstarteBooleanArray,
    AstarteStringArray,
    AstarteDoubleArray,
    AstarteIntegerArray,
    AstarteLongIntegerArray,
    AstarteBinaryBlobArray,
    AstarteDateTimeArray,
)


def _encode_date_time(date_time: datetime):
    """
    Utility function used to convert a datetime to a google protobuf Timestamp structure.

    Parameters
    ----------
    date_time : datetime
        The datetime to convert.

    Returns
    -------
    google.protobuf.timestamp_pb2.Timestamp
        Converted datetime.
    """
    parsed_date_time = Timestamp()
    parsed_date_time.FromDatetime(date_time)
    return parsed_date_time


AstarteTypesLookup = namedtuple("AstarteTypesLookup", "grpc_arg_name grpc_class grpc_data_parser")

astarte_types_lookup = {
    "boolean": AstarteTypesLookup("astarte_boolean", None, None),
    "booleanarray": AstarteTypesLookup("astarte_boolean_array", AstarteBooleanArray, None),
    "string": AstarteTypesLookup("astarte_string", None, None),
    "stringarray": AstarteTypesLookup("astarte_string_array", AstarteStringArray, None),
    "double": AstarteTypesLookup("astarte_double", None, None),
    "doublearray": AstarteTypesLookup("astarte_double_array", AstarteDoubleArray, None),
    "integer": AstarteTypesLookup("astarte_integer", None, None),
    "integerarray": AstarteTypesLookup("astarte_integer_array", AstarteIntegerArray, None),
    "longinteger": AstarteTypesLookup("astarte_long_integer", None, None),
    "longintegerarray": AstarteTypesLookup(
        "astarte_long_integer_array", AstarteLongIntegerArray, None
    ),
    "binaryblob": AstarteTypesLookup("astarte_binary_blob", None, None),
    "binaryblobarray": AstarteTypesLookup(
        "astarte_binary_blob_array", AstarteBinaryBlobArray, None
    ),
    "datetime": AstarteTypesLookup("astarte_date_time", None, _encode_date_time),
    "datetimearray": AstarteTypesLookup(
        "astarte_date_time_array", AstarteDateTimeArray, lambda l: [_encode_date_time(e) for e in l]
    ),
}


def _encode_individual_payload(
    interface: Interface, path: str, payload: object | collections.abc.Mapping | None
):
    """
    Utility function used to encapsulate a payload in a valid protobuf structure.

    Parameters
    ----------
    interface : Interface
        The Interface to send data to.
    path: str
        The endpoint to send the data to
    payload : object, collections.abc.Mapping, optional
        The payload to send if present.

    Returns
    -------
    AstarteDataType | None
        The encapsulated payload
    """
    # pylint: disable=no-member
    if payload is None:
        return AstarteUnset()

    mapping = interface.get_mapping(path)

    parsed_payload = None

    if mapping.type in astarte_types_lookup:
        # Local variables for long lookups
        grpc_arg_name = astarte_types_lookup[mapping.type].grpc_arg_name
        grpc_class = astarte_types_lookup[mapping.type].grpc_class
        grpc_data_parser = astarte_types_lookup[mapping.type].grpc_data_parser
        # Encapsulate the payload in grpc types
        if grpc_data_parser:
            payload = grpc_data_parser(payload)
        if grpc_class:
            payload = grpc_class(values=payload)
        payload = AstarteDataTypeIndividual(**{grpc_arg_name: payload})
        parsed_payload = AstarteDataType(astarte_individual=payload)

    return parsed_payload

grpc_array_types = [
    AstarteDataType,
    AstarteDataTypeIndividual,
    AstarteBooleanArray,
    AstarteStringArray,
    AstarteDoubleArray,
    AstarteIntegerArray,
    AstarteLongIntegerArray,
    AstarteBinaryBlobArray,
    AstarteDateTimeArray,
]

def _decode_payload(grpc_message: AstarteMessage):
    '''
    Decode GRPC message.
    '''
    interface_name = grpc_message.interface_name
    path = grpc_message.path
    payload = None
    if grpc_message.HasField("astarte_data"):
        grpc_data = grpc_message.astarte_data
        if grpc_data.HasField("astarte_individual"):
            grpc_indiv_data_opt = grpc_data.astarte_individual.WhichOneof("individual_data")
            payload = getattr(grpc_data.astarte_individual, grpc_indiv_data_opt)
            if grpc_indiv_data_opt == "astarte_date_time":
                payload = payload.ToDatetime()
            if type(payload) in grpc_array_types:
                payload = list(payload.values)
            if grpc_indiv_data_opt == "astarte_date_time_array":
                payload = [e.ToDatetime() for e in payload]
        else:
            # Handle grpc_data.astarte_object
            pass
    else:
        # Handle grpc_message.astarte_unset
        pass
    if grpc_message.HasField("timestamp"):
        # Handle grpc_message.timestamp
        pass

    return (interface_name, path, payload)


class DeviceGrpc(Device):
    """
    Implementation fo a Device class for the GRPC transfer protocol.
    """

    def __init__(
        self,
        server_addr: str,
        node_uuid: str,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        """
        Parameters
        ----------
        server_addr : str
            Address for the GRPC server.
        node_uuid : str
            Unique identifier for this node.
        """
        super().__init__(loop)

        self.server_addr = server_addr
        self.node_uuid = node_uuid

        self.on_connected: Callable[DeviceGrpc, None] | None = None
        self.on_disconnected: Callable[[DeviceGrpc, int], None] | None = None

        self.__grpc_channel = None
        self.__grpc_stub = None
        self.__grpc_node = None
        self.__interfaces_bin = []
        self.__thread_handle = None

    def _add_interface_from_json(self, interface_json: json):
        """
        See parent class.

        Parameters
        ----------
        interface_json : json
            See parent class.
        """
        self.__interfaces_bin += [json.dumps(interface_json).encode()]
        self._introspection.add_interface(interface_json)

    def remove_interface(self, interface_name: str) -> None:
        """
        See parent class.

        Parameters
        ----------
        interface_name : str
            See parent class.
        """
        # TODO

    def connect(self) -> None:
        """
        Connect the device in a synchronous manner.
        """
        self.__grpc_channel = grpc.insecure_channel(self.server_addr)
        self.__grpc_stub = MessageHubStub(self.__grpc_channel)

        # pylint: disable=no-member
        self.__grpc_node = Node(uuid=self.node_uuid, interface_jsons=self.__interfaces_bin)
        stream = self.__grpc_stub.Attach(self.__grpc_node)

        self.__thread_handle = threading.Thread(target=self._rx_stream_handler, args=(stream,))
        self.__thread_handle.start()

    def disconnect(self) -> None:
        """
        Disconnects the node, detaching it from the message hub.
        """
        if self.__grpc_channel:
            self.__thread_handle.join(timeout=0.1)
            self.__grpc_stub.Detach(self.__grpc_node)
            self.__grpc_channel.close()

    def _send_generic(
        self,
        interface: Interface,
        path: str,
        payload: object | collections.abc.Mapping | None,
        timestamp: datetime | None,
    ) -> None:
        """
        Utility function used to publish a generic payload to an Astarte interface.

        Parameters
        ----------
        interface : Interface
            The Interface to send data to.
        path: str
            The endpoint to send the data to
        payload : object, collections.abc.Mapping, optional
            The payload to send if present.
        timestamp : datetime, optional
            If the Datastream has explicit_timestamp, you can specify a datetime object which
            will be registered as the timestamp for the value.

        Raises
        ------
        ValidationError
            When:
            - Attempting to send to a server owned interface.
            - Sending to an endpoint that is not present in the interface.
            - The payload validation fails.
        """
        if interface.is_server_owned():
            raise ValidationError(f"The interface {interface.name} is not owned by the device.")

        protobuf_timestamp = None
        if (payload is not None) and (timestamp is not None):
            protobuf_timestamp = Timestamp()
            protobuf_timestamp.FromDatetime(timestamp)
        elif not interface.get_mapping(path):
            raise ValidationError(f"Path {path} not in the {interface.name} interface.")

        # pylint: disable=no-member
        msg = AstarteMessage(
            interface_name=interface.name,
            path=path,
            timestamp=protobuf_timestamp,
            astarte_data=_encode_individual_payload(interface, path, payload),
        )
        self.__grpc_stub.Send(msg)

    def _rx_stream_handler(self, stream):  # pylint: disable=too-many-branches # TODO remove this
        """
        Handles the reception stream.

        Parameters
        ----------
        stream : str
            The GRPC receive stream.
        """
        for event in stream:
            (interface_name, path, payload) = _decode_payload(event)

            # Check if callback is set
            if not self.on_data_received:
                continue

            self._on_message_generic(interface_name, path, payload)

    def _store_property(self, *args) -> None:
        """
        Empty implementation for a store property.

        Parameters
        ----------
        args
            Unused.

        """
