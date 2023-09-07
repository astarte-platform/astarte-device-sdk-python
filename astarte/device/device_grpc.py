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
import grpc

# pylint: disable=no-name-in-module
from google.protobuf.timestamp_pb2 import Timestamp

from astarte.device.device import Device
from astarte.device.interface import Interface
from astarte.device.exceptions import ValidationError

from astarteplatform.msghub.message_hub_service_pb2_grpc import MessageHubStub
from astarteplatform.msghub import astarte_message_pb2, astarte_type_pb2, node_pb2


class DeviceGrpc(Device):
    """
    Implementation fo a Device class for the GRPC transfer protocol.
    """

    def __init__(
        self,
        server_addr: str,
        node_uuid: str,
    ):
        """
        Parameters
        ----------
        server_addr : str
            Address for the GRPC server.
        node_uuid : str
            Unique identifier for this node.
        """
        super().__init__()

        self.server_addr = server_addr
        self.node_uuid = node_uuid

        self.on_connected: Callable[DeviceGrpc, None] | None = None
        self.on_disconnected: Callable[[DeviceGrpc, int], None] | None = None
        self.on_data_received: Callable[[DeviceGrpc, str, str, object], None] | None = None

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
        self.__grpc_node = node_pb2.Node(uuid=self.node_uuid, interface_jsons=self.__interfaces_bin)
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
        msg = astarte_message_pb2.AstarteMessage(
            interface_name=interface.name,
            path=path,
            timestamp=protobuf_timestamp,
            astarte_data=_parse_individual_payload(interface, path, payload),
        )
        self.__grpc_stub.Send(msg)

    def _rx_stream_handler(self, stream):  # pylint: disable=no-self-use
        """
        Handles the reception stream.

        Parameters
        ----------
        stream : str
            The GRPC receive stream.
        """
        for event in stream:
            print(event)


def _parse_individual_payload(
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
    astarteplatform.msghub.astarte_type_pb2.AstarteDataType | None
        The encapsulated payload
    """
    # pylint: disable=no-member
    if payload is None:
        return astarte_type_pb2.AstarteUnset()

    mapping = interface.get_mapping(path)

    parsed_payload = None

    if mapping.type == "boolean":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(astarte_boolean=payload)
        )
    if mapping.type == "booleanarray":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_boolean_array=astarte_type_pb2.AstarteBooleanArray(values=payload)
            )
        )
    if mapping.type == "string":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(astarte_string=payload)
        )
    if mapping.type == "stringarray":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_string_array=astarte_type_pb2.AstarteStringArray(values=payload)
            )
        )
    if mapping.type == "double":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(astarte_double=payload)
        )
    if mapping.type == "doublearray":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_double_array=astarte_type_pb2.AstarteDoubleArray(values=payload)
            )
        )
    if mapping.type == "integer":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(astarte_integer=payload)
        )
    if mapping.type == "integerarray":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_integer_array=astarte_type_pb2.AstarteIntegerArray(values=payload)
            )
        )
    if mapping.type == "longinteger":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_long_integer=payload
            )
        )
    if mapping.type == "longintegerarray":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_long_integer_array=astarte_type_pb2.AstarteLongIntegerArray(values=payload)
            )
        )
    if mapping.type == "binaryblob":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_binary_blob=payload
            )
        )
    if mapping.type == "binaryblobarray":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_binary_blob_array=astarte_type_pb2.AstarteBinaryBlobArray(values=payload)
            )
        )
    if mapping.type == "datetime":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_date_time=_parse_date_time(payload)
            )
        )
    if mapping.type == "datetimearray":
        parsed_payload = astarte_type_pb2.AstarteDataType(
            astarte_individual=astarte_type_pb2.AstarteDataTypeIndividual(
                astarte_date_time_array=astarte_type_pb2.AstarteDateTimeArray(
                    values=[_parse_date_time(date_time) for date_time in payload]
                )
            )
        )

    return parsed_payload


def _parse_date_time(date_time: datetime):
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
