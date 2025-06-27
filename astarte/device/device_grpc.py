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
import logging
import queue
import typing
from collections import namedtuple
from datetime import datetime, timezone
from threading import Thread
from typing import Union

# disable lint related to https://github.com/protocolbuffers/protobuf/issues/16115
# pylint: disable=no-name-in-module
from astarteplatform.msghub.astarte_data_pb2 import (
    AstarteBinaryBlobArray,
    AstarteBooleanArray,
    AstarteData,
    AstarteDatastreamIndividual,
    AstarteDatastreamObject,
    AstarteDateTimeArray,
    AstarteDoubleArray,
    AstarteIntegerArray,
    AstarteLongIntegerArray,
    AstartePropertyIndividual,
    AstarteStringArray,
)
from astarteplatform.msghub.astarte_message_pb2 import AstarteMessage, MessageHubEvent
from astarteplatform.msghub.interface_pb2 import InterfacesJson, InterfacesName
from astarteplatform.msghub.message_hub_service_pb2_grpc import MessageHubStub
from astarteplatform.msghub.node_pb2 import Node
from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp

# pylint: enable=no-name-in-module
from grpc import (
    ChannelConnectivity,
    ClientCallDetails,
    UnaryStreamClientInterceptor,
    UnaryUnaryClientInterceptor,
    aio,
    insecure_channel,
    intercept_channel,
)
from grpc._channel import _MultiThreadedRendezvous

from astarte.device.database import StoredProperty
from astarte.device.device import ConnectionState, Device, TypeAstarteData
from astarte.device.exceptions import (
    DeviceConnectingError,
    DeviceDisconnectedError,
    DeviceGrpcDecodeError,
    ValidationError,
)
from astarte.device.interface import Interface
from astarte.device.mapping import Mapping
from astarte.device.types import TypeConvertedAstarteMessage

# All the individual_data options that contain arrays.
TypeProtobufAstarteDataVector: typing.TypeAlias = Union[
    AstarteBooleanArray,
    AstarteStringArray,
    AstarteDoubleArray,
    AstarteIntegerArray,
    AstarteLongIntegerArray,
    AstarteBinaryBlobArray,
    AstarteDateTimeArray,
]
TypeProtobufAstarteDataScalar: typing.TypeAlias = Union[float, bool, int, str, bytes, Timestamp]
TypeProtobufAstarteDataTypes: typing.TypeAlias = Union[
    TypeProtobufAstarteDataScalar, TypeProtobufAstarteDataScalar
]


class DeviceGrpc(Device):
    """
    Astarte device implementation using the GRPC transport protocol.

    **Threading and Concurrency**

    This SDK uses GRPC under the hood as a transport layer. As such, it is bound by GRPC's
    behavior in terms of threading. When a device connects, a new thread is spawned and an
    event loop is run there to manage all the connection events.

    This SDK spares the user from this detail - on the other hand, when configuring callbacks,
    threading has to be taken into account. When configuring the callback functions, it is
    possible to specify an asyncio.loop() to automatically manage this detail. When a loop is
    specified, all callbacks will be called in the context of that loop, guaranteeing
    thread-safety and making sure that the user does not have to take any further action beyond
    consuming the callback.

    When a loop is not specified, callbacks are invoked just as standard Python functions. This
    inevitably means that the user will have to take into account the fact that the callback
    will be invoked in the thread of the GRPC connection. In particular, blocking the execution
    of that thread might cause deadlocks and, in general, malfunctions in the SDK. For this
    reason, the usage of asyncio is strongly recommended.
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

        self._server_addr = server_addr
        self._node_uuid = node_uuid

        self.__grpc_channel = None
        self.__msghub_stub: MessageHubStub
        self.__msghub_node = None
        self.__interfaces_bins = {}
        self.__rx_thread_handle = None
        self.__stream_queue = queue.Queue(maxsize=1)
        self.__connection_state = ConnectionState.DISCONNECTED

    def add_interface_from_json(self, interface_json: dict):
        """
        See parent class.

        Parameters
        ----------
        interface_json : dict
            See parent class.

        Raises
        ------
        DeviceConnectingError
            When attempting to add an interface while the device if performing a connection.
        """
        if self.__connection_state is ConnectionState.CONNECTING:
            raise DeviceConnectingError("Interfaces cannot be added while device is connecting.")
        interface = Interface(interface_json)
        self._introspection.add_interface(interface)
        interface_bin = json.dumps(interface_json)
        self.__interfaces_bins[interface.name] = interface_bin
        if self.__connection_state is ConnectionState.CONNECTED:
            interfaces_json = InterfacesJson(interfaces_json=[interface_bin])
            self.__msghub_stub.AddInterfaces(interfaces_json)

    def remove_interface(self, interface_name: str) -> None:
        """
        See parent class.

        Parameters
        ----------
        interface_name : str
            See parent class.

        Raises
        ------
        DeviceConnectingError
            When attempting to add an interface while the device if performing a connection.
        """
        if self.__connection_state is ConnectionState.CONNECTING:
            raise DeviceConnectingError("Interfaces cannot be removed while device is connecting.")
        self._introspection.remove_interface(interface_name)
        if interface_name in self.__interfaces_bins:
            del self.__interfaces_bins[interface_name]
        if self.__connection_state is ConnectionState.CONNECTED:
            interfaces_name = InterfacesName(names=[interface_name])
            self.__msghub_stub.RemoveInterfaces(interfaces_name)

    def connect(self) -> None:
        """
        Connect the device in synchronous mode.
        """
        if self.__connection_state is ConnectionState.CONNECTED:
            logging.warning("Attempting to connect an already connected device.")
            return

        self.__connection_state = ConnectionState.CONNECTING

        self.__grpc_channel = insecure_channel(self._server_addr)
        self.__grpc_channel.subscribe(self._on_connectivity_change)
        unary_unary_interceptor = AstarteUnaryUnaryInterceptor(node_id=self._node_uuid)
        unary__stream_interceptor = AstarteUnaryStreamInterceptor(node_id=self._node_uuid)
        self.__grpc_channel = intercept_channel(
            self.__grpc_channel, unary_unary_interceptor, unary__stream_interceptor
        )
        self.__msghub_stub = MessageHubStub(self.__grpc_channel)

        self.__msghub_node = Node(interfaces_json=list(self.__interfaces_bins.values()))
        stream = self.__msghub_stub.Attach(self.__msghub_node)

        self.__stream_queue.put(stream)

        self.__rx_thread_handle = Thread(target=self._rx_stream_handler)
        self.__rx_thread_handle.daemon = True
        self.__rx_thread_handle.start()

    def _on_connectivity_change(self, connectivity: ChannelConnectivity):
        """
        Callback for GRPC connectivity change events.

        Parameters
        ----------
        connectivity: grpc.ChannelConnectivity
            New connectivity status for the GRPC channel.
        """
        logging.debug("GRPC channel connectivity change: %s", str(connectivity))

        if connectivity in [
            ChannelConnectivity.IDLE,
            ChannelConnectivity.TRANSIENT_FAILURE,
            ChannelConnectivity.SHUTDOWN,
        ]:
            last_connection_state = self.__connection_state
            self.__connection_state = ConnectionState.DISCONNECTED
            if last_connection_state is ConnectionState.CONNECTED:
                if self._on_disconnected:
                    if self._loop:
                        # Use threadsafe, as we're in a different thread here
                        self._loop.call_soon_threadsafe(self._on_disconnected, self, 0)
                    else:
                        self._on_disconnected(self, 0)
        elif connectivity is ChannelConnectivity.CONNECTING:
            pass
        elif connectivity is ChannelConnectivity.READY:
            self.__connection_state = ConnectionState.CONNECTED
            if self._on_connected:
                if self._loop:
                    # Use threadsafe, as we're in a different thread here
                    self._loop.call_soon_threadsafe(self._on_connected, self)
                else:
                    self._on_connected(self)
        else:
            logging.error("Unrecognized connectivity change: %s", str(connectivity))

    def _rx_stream_handler(self):
        """
        Handles the reception stream.

        Once a stream exits for whatever reason, it will wait for a new stream to be available in
        the __stream_queue.
        It will terminate only when None is extracted from the __stream_queue.
        """
        while True:
            stream = self.__stream_queue.get()
            if stream is None:
                break

            try:
                for msg_hub_event in stream:
                    astarte_message = _decode_msg_hub_event(msg_hub_event)
                    if astarte_message:
                        (interface_name, path, payload) = astarte_message
                        logging.debug(
                            "Received message on interface: %s, endpoint %s, content: %s",
                            str(interface_name),
                            str(path),
                            str(payload),
                        )
                        if self._on_data_received:
                            self._on_message_generic(interface_name, path, payload)
            except _MultiThreadedRendezvous as exc:
                logging.error("Status code change in the GRPC core: %s", str(exc.code()))

    def disconnect(self) -> None:
        """
        Disconnects the node, detaching it from the message hub.

        This method won't have any effect if the device is not already connected.
        """
        if self.__connection_state is not ConnectionState.CONNECTED:
            logging.warning("Attempting to disconnect a non-connected device.")
            return

        self.__connection_state = ConnectionState.DISCONNECTED

        if self.__grpc_channel:
            self.__stream_queue.put(None)
            self.__msghub_stub.Detach(Empty())
            self.__grpc_channel.close()

            if self._on_disconnected:
                if self._loop:
                    # Use threadsafe, as we're in a different thread here
                    self._loop.call_soon_threadsafe(self._on_disconnected, self, 0)
                else:
                    self._on_disconnected(self, 0)

    def is_connected(self) -> bool:
        """
        Returns whether the device is currently connected.

        Returns
        -------
        bool
            The device connection status.
        """
        return self.__connection_state is ConnectionState.CONNECTED

    def get_property(self, interface_name: str, path: str) -> TypeAstarteData | None:
        """
        Read the documentation of the base Device class.

        Parameters
        ----------
        interface_name : str
            Read the documentation of the base Device class.
        path : str
            Read the documentation of the base Device class.

        Returns
        -------
        Optional[TypeAstarteData]
            Read the documentation of the base Device class.

        Raises
        ------
        NotImplementedError this method is currently not implemented for the grpc client
        """
        raise NotImplementedError()

    def get_interface_props(self, interface_name: str) -> list[StoredProperty]:
        """
        Read the documentation of the base Device class.

        Parameters
        ----------
        interface_name : str
            Read the documentation of the base Device class.

        Returns
        -------
        list[StoredProperty]
            Read the documentation of the base Device class.

        Raises
        ------
        NotImplementedError this method is currently not implemented for the grpc client
        """
        raise NotImplementedError()

    def get_all_props(self) -> list[StoredProperty]:
        """
        Read the documentation of the base Device class.

        Returns
        -------
        list[StoredProperty]
            Read the documentation of the base Device class.

        Raises
        ------
        NotImplementedError this method is currently not implemented for the grpc client
        """
        raise NotImplementedError()

    def get_device_props(self) -> list[StoredProperty]:
        """
        Read the documentation of the base Device class.

        Returns
        -------
        list[StoredProperty]
            Read the documentation of the base Device class.

        Raises
        ------
        NotImplementedError this method is currently not implemented for the grpc client
        """
        raise NotImplementedError()

    def get_server_props(self) -> list[StoredProperty]:
        """
        Read the documentation of the base Device class.

        Returns
        -------
        list[StoredProperty]
            Read the documentation of the base Device class.

        Raises
        ------
        NotImplementedError this method is currently not implemented for the grpc client
        """
        raise NotImplementedError()

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
            will be registered as the timestamp for the object_data_v.

        Raises
        ------
        DeviceDisconnectedError
            When this function is called while the device is not connected to the message hub.
        ValidationError
            When:
            - Attempting to send to a server owned interface.
            - Sending to an endpoint that is not present in the interface.
            - The payload validation fails.
        """
        if self.__connection_state is not ConnectionState.CONNECTED:
            raise DeviceDisconnectedError("Send operation failed due to missing connection.")

        if interface.is_server_owned():
            raise ValidationError(f"The interface {interface.name} is not owned by the device.")

        protobuf_timestamp = None
        if payload is not None:
            if timestamp is not None:
                protobuf_timestamp = Timestamp()
                protobuf_timestamp.FromDatetime(timestamp)
        elif not interface.get_mapping(path):
            raise ValidationError(f"Path {path} not in the {interface.name} interface.")

        # Create an AstarteMessage object for the Send method
        astarte_message = _encode_astarte_message(interface, path, protobuf_timestamp, payload)
        logging.debug(
            "Send message on interface: %s, endpoint %s, content: %s",
            str(interface.name),
            str(path),
            str(payload),
        )
        self.__msghub_stub.Send(astarte_message)

    def _store_property(self, *args) -> None:
        """
        Empty implementation for a store property.

        Parameters
        ----------
        args
            Unused.
        """


def _encode_astarte_message(
    interface: Interface,
    path: str,
    timestamp: Timestamp | None,
    payload: dict[str, TypeAstarteData] | TypeAstarteData | None,
) -> AstarteMessage:
    """
    Encode a payload into an AstarteMessage object.

    Parameters
    ----------
    interface : Interface
        The Interface to send data to.
    path: str
        The endpoint to send the data to
    timestamp : Timestamp
        The timestamp to send if present.
    payload : object, collections.abc.Mapping, optional
        The payload to send if present.

    Returns
    -------
    AstarteMessage
        The encapsulated payload
    """
    if interface.is_property_individual():
        property_individual = None

        if payload is not None:
            mapping = interface.get_mapping(path)
            property_individual = _encode_astarte_data_type_individual(mapping, payload)

        return AstarteMessage(
            interface_name=interface.name,
            path=path,
            property_individual=AstartePropertyIndividual(data=property_individual),
        )

    if interface.is_datastream_object():
        object_data = {}

        for endpoint, endpoint_value in payload.items():
            mapping = interface.get_mapping("/".join([path, endpoint]))
            object_data[endpoint] = _encode_astarte_data_type_individual(mapping, endpoint_value)

        return AstarteMessage(
            interface_name=interface.name,
            path=path,
            datastream_object=AstarteDatastreamObject(data=object_data, timestamp=timestamp),
        )

    # datastream individual
    mapping = interface.get_mapping(path)
    astarte_data = _encode_astarte_data_type_individual(mapping, payload)

    return AstarteMessage(
        interface_name=interface.name,
        path=path,
        datastream_individual=AstarteDatastreamIndividual(timestamp=timestamp, data=astarte_data),
    )


def _encode_astarte_data_type_individual(mapping: Mapping, payload: TypeAstarteData) -> AstarteData:
    """
    Encode AstarteDataTypeIndividual object.

    Parameters
    ----------
    mapping : Mapping
        The mapping corresponding to the payload.
    payload : Timestamp
        The timestamp to send if present.

    Returns
    -------
    AstarteData
        The encapsulated payload
    """
    LookupEntry = namedtuple("LookupEntry", "data_type data_class data_parser")
    lookup_table = {
        "boolean": LookupEntry("boolean", None, None),
        "booleanarray": LookupEntry("boolean_array", AstarteBooleanArray, None),
        "string": LookupEntry("string", None, None),
        "stringarray": LookupEntry("string_array", AstarteStringArray, None),
        "double": LookupEntry("double", None, None),
        "doublearray": LookupEntry("double_array", AstarteDoubleArray, None),
        "integer": LookupEntry("integer", None, None),
        "integerarray": LookupEntry("integer_array", AstarteIntegerArray, None),
        "longinteger": LookupEntry("long_integer", None, None),
        "longintegerarray": LookupEntry("long_integer_array", AstarteLongIntegerArray, None),
        "binaryblob": LookupEntry("binary_blob", None, None),
        "binaryblobarray": LookupEntry("binary_blob_array", AstarteBinaryBlobArray, None),
        "datetime": LookupEntry("date_time", None, _encode_timestamp),
        "datetimearray": LookupEntry(
            "date_time_array",
            AstarteDateTimeArray,
            lambda l: [_encode_timestamp(e) for e in l],
        ),
    }

    data_parser = lookup_table[mapping.type].data_parser
    data_class = lookup_table[mapping.type].data_class
    data_type = lookup_table[mapping.type].data_type

    if data_parser:
        payload = data_parser(payload)
    if data_class:
        payload = data_class(values=payload)
    return AstarteData(**{data_type: payload})


def _encode_timestamp(timestamp: datetime) -> Timestamp:
    """
    Encode a datetime object into a Timestamp object.

    Parameters
    ----------
    timestamp : datetime
        The datetime to convert.

    Returns
    -------
    google.protobuf.timestamp_pb2.Timestamp
        The converted timestamp.
    """
    protobuf_timestamp = Timestamp()
    protobuf_timestamp.FromDatetime(timestamp)
    return protobuf_timestamp


def _decode_msg_hub_event(
    msg_hub_event: MessageHubEvent,
) -> TypeConvertedAstarteMessage | None:
    """
    Decode MessageHubEvent object.

    Parameters
    ----------
    msg_hub_event : MessageHubEvent
        The MessageHubEvent to decode.

    Returns
    -------
    tuple[str, str, object | collections.abc.Mapping | None] | None
        A tuple containing:
        - The interface name corresponding to the payload
        - The path corresponding to the payload
        - The decoded payload
        Or None if the received event was an error.
    """

    payload = None
    if msg_hub_event.HasField("error"):
        logging.error("Error from the message hub: %s", msg_hub_event.error.description)
    else:
        payload = _decode_astarte_message(msg_hub_event.message)

    return payload


def _decode_astarte_message(
    astarte_message: AstarteMessage,
) -> TypeConvertedAstarteMessage:
    """
    Decode AstarteMessage object.

    Parameters
    ----------
    astarte_message : AstarteMessage
        The AstarteMessage to decode.

    Returns
    -------
    tuple[str, str, object | collections.abc.Mapping | None]
        A tuple containing:
        - The interface name corresponding to the payload
        - The path corresponding to the payload
        - The decoded payload
    """
    payload = None
    if astarte_message.HasField("datastream_individual"):
        payload = _decode_astarte_data_type_individual(astarte_message.datastream_individual.data)
    elif astarte_message.HasField("datastream_object"):
        payload = _decode_astarte_data_type_object(astarte_message.datastream_object)
    elif astarte_message.HasField("property_individual"):
        if astarte_message.property_individual.HasField("data"):
            payload = _decode_astarte_data_type_individual(astarte_message.property_individual.data)
    else:
        logging.error("Unknown type returning None for astarte_message: %s", astarte_message)

    # For now ignore the received 'astarte_message.timestamp'
    return (astarte_message.interface_name, astarte_message.path, payload)


def _decode_astarte_data_type_object(astarte_data_type_object: AstarteDatastreamObject):
    """
    Decode AstarteDataTypeObject object.

    Parameters
    ----------
    astarte_data_type_object : AstarteDataTypeObject
        The AstarteDataTypeObject to decode.

    Returns
    -------
    dict
        A dictionary containing the decoded astarte_data_type_object
    """
    result = {}
    for endpoint, astarte_data_type_individual in astarte_data_type_object.data.items():
        result[endpoint] = _decode_astarte_data_type_individual(astarte_data_type_individual)
    return result


def _decode_astarte_data_type_individual(
    astarte_data_type_individual: AstarteData,
) -> TypeAstarteData:
    """
    Decode AstarteDataTypeIndividual object.

    Parameters
    ----------
    astarte_data_type_individual : AstarteDataTypeIndividual
        The AstarteDataTypeIndividual to decode.

    Returns
    -------
    obj
        An object containig the decoded astarte_data_type_individual

    Raises
    ------
    DeviceGrpcDecodeError
        When attempting to decode an AstarteData of an unknown type.
    """

    individual_data_opt = astarte_data_type_individual.WhichOneof("astarte_data")
    proto_individual_data: TypeProtobufAstarteDataTypes = getattr(
        astarte_data_type_individual, individual_data_opt
    )

    individual_data: TypeAstarteData

    if isinstance(proto_individual_data, Timestamp):
        individual_data = proto_individual_data.ToDatetime(timezone.utc)
    elif isinstance(proto_individual_data, typing.get_args(TypeProtobufAstarteDataVector)):
        if isinstance(proto_individual_data, AstarteDateTimeArray):
            individual_data = [e.ToDatetime(timezone.utc) for e in proto_individual_data.values]
        else:
            individual_data = list(proto_individual_data.values)
    elif isinstance(proto_individual_data, typing.get_args(TypeProtobufAstarteDataScalar)):
        individual_data = proto_individual_data
    else:
        error_str = f"Unexpected astarte data individual type: {proto_individual_data}"
        logging.error(error_str)
        raise DeviceGrpcDecodeError(error_str)

    return individual_data


class AstarteClientCallDetails(
    collections.namedtuple(
        "AstarteClientCallDetails",
        (
            "method",
            "timeout",
            "metadata",
            "credentials",
            "wait_for_ready",
            "compression",
        ),
    ),
    ClientCallDetails,
):
    """
    Astarte implementation for gRPC client call details.
    """


class AstarteUnaryUnaryInterceptor(UnaryUnaryClientInterceptor):
    """
    Astarte implementation for a gRPC unary-unary client interceptor.
    """

    def __init__(self, node_id):
        self.node_id = node_id

    def intercept_unary_unary(self, continuation, client_call_details, request):
        """
        Implementation for the abstract interceptor method.

        Parameters
        ----------
        continuation : Any
            See parent class.
        client_call_details : Any
            See parent class.
        request : Any
            See parent class.

        Returns
        ------
        Any
            See the parent class.
        """
        logging.debug("Called interceptor with client call details: %s", str(client_call_details))
        new_client_call_details = add_node_id_in_metadata(self.node_id, client_call_details)
        logging.debug("New client call details: %s", str(new_client_call_details))
        return continuation(new_client_call_details, request)


class AstarteUnaryStreamInterceptor(UnaryStreamClientInterceptor):
    """
    Astarte implementation for a gRPC unary-stream client interceptor.
    """

    def __init__(self, node_id):
        self.node_id = node_id

    def intercept_unary_stream(self, continuation, client_call_details, request):
        """
        Implementation for the abstract interceptor method.

        Parameters
        ----------
        continuation : Any
            See parent class.
        client_call_details : Any
            See parent class.
        request : Any
            See parent class.

        Returns
        ------
        Any
            See the parent class.
        """
        logging.debug("Called interceptor with client call details: %s", str(client_call_details))
        new_client_call_details = add_node_id_in_metadata(self.node_id, client_call_details)
        logging.debug("New client call details: %s", str(new_client_call_details))
        return continuation(new_client_call_details, request)


def add_node_id_in_metadata(
    node_id: str, client_call_details: aio._interceptor.ClientCallDetails
) -> AstarteClientCallDetails:
    """
    Add an Astarte message hub ID to che grpc client call details as a metadata fields.

    Note: This function doesn't perform in place changes to client_call_details. It returns a
    totally new set of client call details.

    Parameters
    ----------
    node_id : str
        The node ID to add.
    client_call_details : Any
        The client call details to modify.

    Returns
    ------
    grpc._interceptor._ClientCallDetails
        The new client call details.
    """
    metadata = client_call_details.metadata if client_call_details.metadata else []
    metadata.append(("node-id", node_id))
    new_client_call_details = AstarteClientCallDetails(
        client_call_details.method,
        client_call_details.timeout,
        metadata,
        client_call_details.credentials,
        client_call_details.wait_for_ready,
        client_call_details.compression,
    )
    return new_client_call_details
