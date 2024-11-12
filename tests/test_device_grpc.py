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

# pylint: disable=missing-function-docstring, too-many-public-methods, protected-access
# pylint: disable=no-self-use, too-many-arguments, no-name-in-module, too-many-locals
# pylint: disable=missing-class-docstring, too-many-statements

import unittest
from datetime import datetime, timezone
from threading import Thread
from unittest import mock

from astarteplatform.msghub.astarte_type_pb2 import (
    AstarteBinaryBlobArray,
    AstarteBooleanArray,
    AstarteDateTimeArray,
    AstarteDoubleArray,
    AstarteIntegerArray,
    AstarteLongIntegerArray,
    AstarteStringArray,
)
from google.protobuf.timestamp_pb2 import Timestamp
from grpc import ChannelConnectivity
from grpc._channel import _MultiThreadedRendezvous

from astarte.device import (
    DeviceConnectingError,
    DeviceDisconnectedError,
    DeviceGrpc,
    ValidationError,
)
from astarte.device.device import ConnectionState, Device
from astarte.device.device_grpc import (
    _decode_astarte_data_type_individual,
    _decode_astarte_data_type_object,
    _decode_astarte_message,
    _encode_astarte_data_type_individual,
    _encode_astarte_message,
    _encode_timestamp,
)
from astarte.device.introspection import Introspection


class TestMyAbstract(unittest.TestCase):
    @mock.patch.object(Device, "__init__", return_value=None)
    def test_devicegrpc_init_calls_parent_init(self, mock_device):
        DeviceGrpc(
            "server address",
            "node uuid",
        )
        mock_device.assert_called_once()

    @mock.patch("astarte.device.device_grpc.InterfacesJson")
    @mock.patch("astarte.device.device_grpc.json.dumps")
    @mock.patch("astarte.device.device_grpc.Interface")
    @mock.patch.object(Introspection, "add_interface")
    def test_devicegrpc_add_interface_from_json_non_connected_device(
        self, mock_add_interface, mock_interface, mock_json_dumps, mock_interface_json
    ):
        device = DeviceGrpc(
            "server address",
            "node uuid",
        )

        mock_interface.return_value.name = "<interface-name>"

        interface_json = {"json content": 42}
        device.add_interface_from_json(interface_json)

        mock_interface.assert_called_once_with(interface_json)
        mock_add_interface.assert_called_once_with(mock_interface.return_value)
        mock_json_dumps.assert_called_once_with(interface_json)
        assert device._DeviceGrpc__interfaces_bins == {
            mock_interface.return_value.name: mock_json_dumps.return_value.encode.return_value
        }
        mock_interface_json.assert_not_called()

    @mock.patch("astarte.device.device_grpc.json.dumps")
    @mock.patch("astarte.device.device_grpc.Interface")
    @mock.patch.object(Introspection, "add_interface")
    def test_devicegrpc_add_interface_from_json_connecting_device(
        self, mock_add_interface, mock_interface, mock_json_dumps
    ):
        device = DeviceGrpc(
            "server address",
            "node uuid",
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTING

        interface_json = {"json content": 42}
        self.assertRaises(
            DeviceConnectingError,
            lambda: device.add_interface_from_json(interface_json),
        )

        mock_interface.assert_not_called()
        mock_add_interface.assert_not_called()
        mock_json_dumps.assert_not_called()
        assert not device._DeviceGrpc__interfaces_bins

    @mock.patch("astarte.device.device_grpc.InterfacesJson")
    @mock.patch("astarte.device.device_grpc.json.dumps")
    @mock.patch("astarte.device.device_grpc.Interface")
    @mock.patch.object(Introspection, "add_interface")
    def test_devicegrpc_add_interface_from_json_connected_device(
        self, mock_add_interface, mock_interface, mock_json_dumps, mock_interfaces_json
    ):
        node_uuid = "node uuid"
        device = DeviceGrpc(
            "server address",
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        mock_interface.return_value.name = "<interface-name>"
        mock_device_stub = mock.MagicMock()
        mock_device_node = mock.MagicMock()
        device._DeviceGrpc__msghub_stub = mock_device_stub
        device._DeviceGrpc__msghub_node = mock_device_node

        interface_json = {"json content": 42}
        device.add_interface_from_json(interface_json)

        mock_interface.assert_called_once_with(interface_json)
        mock_add_interface.assert_called_once_with(mock_interface.return_value)
        mock_json_dumps.assert_called_once_with(interface_json)
        assert device._DeviceGrpc__interfaces_bins == {
            mock_interface.return_value.name: mock_json_dumps.return_value.encode.return_value
        }

        mock_interfaces_json.assert_called_once_with(
            interfaces_json=[mock_json_dumps.return_value.encode.return_value]
        )
        mock_device_stub.AddInterfaces.assert_called_once_with(mock_interfaces_json.return_value)

    @mock.patch.object(Introspection, "remove_interface")
    def test_devicegrpc_remove_interface_non_connected_device(self, mock_remove_interface):
        device = DeviceGrpc(
            "server address",
            "node uuid",
        )

        other_interface_1 = mock.MagicMock()
        other_interface_2 = mock.MagicMock()

        device._DeviceGrpc__interfaces_bins = {
            "<other-interface-name1>": other_interface_1,
            "<interface-name>": mock.MagicMock(),
            "<other-interface-name2>": other_interface_2,
        }

        interface_name = "<interface-name>"
        device.remove_interface(interface_name)

        mock_remove_interface.assert_called_once_with(interface_name)
        assert device._DeviceGrpc__interfaces_bins == {
            "<other-interface-name1>": other_interface_1,
            "<other-interface-name2>": other_interface_2,
        }

    @mock.patch.object(Introspection, "remove_interface")
    def test_devicegrpc_remove_interface_connecting_device(self, mock_remove_interface):
        device = DeviceGrpc(
            "server address",
            "node uuid",
        )

        other_interface_1 = mock.MagicMock()
        interface = mock.MagicMock()
        other_interface_2 = mock.MagicMock()

        device._DeviceGrpc__interfaces_bins = {
            "<other-interface-name1>": other_interface_1,
            "<interface-name>": interface,
            "<other-interface-name2>": other_interface_2,
        }
        device._DeviceGrpc__connection_state = ConnectionState.CONNECTING

        interface_name = "<interface-name>"
        self.assertRaises(
            DeviceConnectingError,
            lambda: device.remove_interface(interface_name),
        )

        mock_remove_interface.assert_not_called()
        assert device._DeviceGrpc__interfaces_bins == {
            "<other-interface-name1>": other_interface_1,
            "<interface-name>": interface,
            "<other-interface-name2>": other_interface_2,
        }

    @mock.patch("astarte.device.device_grpc.InterfacesName")
    @mock.patch.object(Introspection, "remove_interface")
    def test_devicegrpc_remove_interface_connected_device(
        self, mock_remove_interface, mock_interfaces_name
    ):
        node_uuid = "node uuid"
        device = DeviceGrpc(
            "server address",
            node_uuid,
        )

        other_interface_1 = mock.MagicMock()
        other_interface_2 = mock.MagicMock()

        device._DeviceGrpc__interfaces_bins = {
            "<other-interface-name1>": other_interface_1,
            "<interface-name>": mock.MagicMock(),
            "<other-interface-name2>": other_interface_2,
        }

        mock_device_stub = mock.MagicMock()
        mock_device_node = mock.MagicMock()
        device._DeviceGrpc__msghub_stub = mock_device_stub
        device._DeviceGrpc__msghub_node = mock_device_node

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        interface_name = "<interface-name>"
        device.remove_interface(interface_name)

        mock_remove_interface.assert_called_once_with(interface_name)
        assert device._DeviceGrpc__interfaces_bins == {
            "<other-interface-name1>": other_interface_1,
            "<other-interface-name2>": other_interface_2,
        }

        mock_interfaces_name.assert_called_once_with(names=[interface_name.encode()])
        mock_device_stub.RemoveInterfaces.assert_called_once_with(mock_interfaces_name.return_value)

    @mock.patch("astarte.device.device_grpc.Thread")
    @mock.patch("astarte.device.device_grpc.Node")
    @mock.patch("astarte.device.device_grpc.MessageHubStub")
    @mock.patch("astarte.device.device_grpc.grpc.insecure_channel")
    @mock.patch("astarte.device.device_grpc.grpc.intercept_channel")
    @mock.patch("astarte.device.device_grpc.AstarteUnaryStreamInterceptor")
    @mock.patch("astarte.device.device_grpc.AstarteUnaryUnaryInterceptor")
    def test_devicegrpc_connect(
        self,
        mock_AstarteUnaryUnaryInterceptor,
        mock_AstarteUnaryStreamInterceptor,
        mock_intercept_channel,
        mock_insecure_channel,
        mock_msg_hub_stub,
        mock_node,
        mock_thread,
    ):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        rx_message1 = mock.MagicMock()
        rx_message2 = mock.MagicMock()
        mock_msg_hub_stub.return_value.Attach.return_value = [rx_message1, rx_message2]

        device.connect()

        mock_insecure_channel.assert_called_once_with(server_address)
        mock_insecure_channel.return_value.subscribe.assert_called_once_with(
            device._on_connectivity_change
        )
        mock_intercept_channel.assert_called_once_with(
            mock_insecure_channel.return_value,
            mock_AstarteUnaryUnaryInterceptor.return_value,
            mock_AstarteUnaryStreamInterceptor.return_value,
        )
        mock_msg_hub_stub.assert_called_once_with(mock_intercept_channel.return_value)
        mock_node.assert_called_once_with(interfaces_json=[])
        mock_msg_hub_stub.return_value.Attach.assert_called_once_with(mock_node.return_value)

        mock_thread.assert_called_once_with(target=device._rx_stream_handler)
        assert mock_thread.return_value.daemon
        mock_thread.return_value.start.assert_called_once()

    @mock.patch("astarte.device.device_grpc.Thread")
    @mock.patch("astarte.device.device_grpc.Node")
    @mock.patch("astarte.device.device_grpc.MessageHubStub")
    @mock.patch("astarte.device.device_grpc.grpc.insecure_channel")
    def test_devicegrpc_connect_already_connected(
        self,
        mock_insecure_channel,
        mock_msg_hub_stub,
        mock_node,
        mock_thread,
    ):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        rx_message1 = mock.MagicMock()
        rx_message2 = mock.MagicMock()
        mock_msg_hub_stub.return_value.Attach.return_value = [rx_message1, rx_message2]

        device.connect()

        mock_insecure_channel.assert_not_called()
        mock_msg_hub_stub.assert_not_called()
        mock_node.assert_not_called()

        mock_thread.assert_not_called()

    @mock.patch("astarte.device.device_grpc.Thread")
    @mock.patch("astarte.device.device_grpc.Node")
    @mock.patch("astarte.device.device_grpc.MessageHubStub")
    @mock.patch("astarte.device.device_grpc.grpc.insecure_channel")
    @mock.patch("astarte.device.device_grpc.grpc.intercept_channel")
    @mock.patch("astarte.device.device_grpc.AstarteUnaryStreamInterceptor")
    @mock.patch("astarte.device.device_grpc.AstarteUnaryUnaryInterceptor")
    def test_devicegrpc__on_connectivity_change(
        self,
        mock_AstarteUnaryUnaryInterceptor,
        mock_AstarteUnaryStreamInterceptor,
        mock_intercept_channel,
        mock_insecure_channel,
        mock_msg_hub_stub,
        mock_node,
        mock_thread,
    ):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        rx_message1 = mock.MagicMock()
        rx_message2 = mock.MagicMock()
        mock_msg_hub_stub.return_value.Attach.return_value = [rx_message1, rx_message2]

        mock_on_connected = mock.MagicMock()
        mock_on_data_received = mock.MagicMock()
        mock_on_disconnected = mock.MagicMock()
        device.set_events_callbacks(
            on_connected=mock_on_connected,
            on_data_received=mock_on_data_received,
            on_disconnected=mock_on_disconnected,
        )

        device.connect()

        mock_insecure_channel.assert_called_once_with(server_address)
        mock_insecure_channel.return_value.subscribe.assert_called_once_with(
            device._on_connectivity_change
        )
        mock_intercept_channel.assert_called_once_with(
            mock_insecure_channel.return_value,
            mock_AstarteUnaryUnaryInterceptor.return_value,
            mock_AstarteUnaryStreamInterceptor.return_value,
        )
        mock_msg_hub_stub.assert_called_once_with(mock_intercept_channel.return_value)
        mock_node.assert_called_once_with(interfaces_json=[])
        mock_msg_hub_stub.return_value.Attach.assert_called_once_with(mock_node.return_value)

        mock_thread.assert_called_once_with(target=device._rx_stream_handler)
        assert mock_thread.return_value.daemon
        mock_thread.return_value.start.assert_called_once()

        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

        # Simulate change to the idle state
        device._on_connectivity_change(ChannelConnectivity.IDLE)
        self.assertEqual(device.is_connected(), False)

        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

        # Simulate change to the connecting state
        device._on_connectivity_change(ChannelConnectivity.CONNECTING)
        self.assertEqual(device.is_connected(), False)

        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

        # Simulate change to the ready state
        device._on_connectivity_change(ChannelConnectivity.READY)
        self.assertEqual(device.is_connected(), True)

        mock_on_connected.assert_called_once_with(device)
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

        # Simulate change to the idle state
        mock_on_connected.reset_mock()
        device._on_connectivity_change(ChannelConnectivity.IDLE)
        self.assertEqual(device.is_connected(), False)

        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_called_once_with(device, 0)

        # Simulate change to the non existing state (ensure code coverage)
        mock_on_disconnected.reset_mock()
        device._on_connectivity_change(None)
        self.assertEqual(device.is_connected(), False)

        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

    @mock.patch("astarte.device.device_grpc.Thread")
    @mock.patch("astarte.device.device_grpc.Node")
    @mock.patch("astarte.device.device_grpc.MessageHubStub")
    @mock.patch("astarte.device.device_grpc.grpc.insecure_channel")
    @mock.patch("astarte.device.device_grpc.grpc.intercept_channel")
    @mock.patch("astarte.device.device_grpc.AstarteUnaryStreamInterceptor")
    @mock.patch("astarte.device.device_grpc.AstarteUnaryUnaryInterceptor")
    def test_devicegrpc__on_connectivity_change_threaded(
        self,
        mock_AstarteUnaryUnaryInterceptor,
        mock_AstarteUnaryStreamInterceptor,
        mock_intercept_channel,
        mock_insecure_channel,
        mock_msg_hub_stub,
        mock_node,
        mock_thread,
    ):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        rx_message1 = mock.MagicMock()
        rx_message2 = mock.MagicMock()
        mock_msg_hub_stub.return_value.Attach.return_value = [rx_message1, rx_message2]

        mock_on_connected = mock.MagicMock()
        mock_on_data_received = mock.MagicMock()
        mock_on_disconnected = mock.MagicMock()
        mock_loop = mock.MagicMock()
        device.set_events_callbacks(
            on_connected=mock_on_connected,
            on_data_received=mock_on_data_received,
            on_disconnected=mock_on_disconnected,
            loop=mock_loop,
        )

        device.connect()

        mock_insecure_channel.assert_called_once_with(server_address)
        mock_insecure_channel.return_value.subscribe.assert_called_once_with(
            device._on_connectivity_change
        )
        mock_intercept_channel.assert_called_once_with(
            mock_insecure_channel.return_value,
            mock_AstarteUnaryUnaryInterceptor.return_value,
            mock_AstarteUnaryStreamInterceptor.return_value,
        )
        mock_msg_hub_stub.assert_called_once_with(mock_intercept_channel.return_value)
        mock_node.assert_called_once_with(interfaces_json=[])
        mock_msg_hub_stub.return_value.Attach.assert_called_once_with(mock_node.return_value)

        mock_thread.assert_called_once_with(target=device._rx_stream_handler)
        assert mock_thread.return_value.daemon
        mock_thread.return_value.start.assert_called_once()

        mock_loop.call_soon_threadsafe.assert_not_called()
        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

        # Simulate change to the idle state
        device._on_connectivity_change(ChannelConnectivity.IDLE)

        mock_loop.call_soon_threadsafe.assert_not_called()
        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

        # Simulate change to the connecting state
        device._on_connectivity_change(ChannelConnectivity.CONNECTING)

        mock_loop.call_soon_threadsafe.assert_not_called()
        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

        # Simulate change to the ready state
        device._on_connectivity_change(ChannelConnectivity.READY)

        mock_loop.call_soon_threadsafe.assert_called_once_with(mock_on_connected, device)
        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

        # Simulate change to the idle state
        mock_loop.reset_mock()
        device._on_connectivity_change(ChannelConnectivity.IDLE)

        mock_loop.call_soon_threadsafe.assert_called_once_with(mock_on_disconnected, device, 0)
        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

    @mock.patch.object(Device, "_on_message_generic")
    @mock.patch("astarte.device.device_grpc._decode_msg_hub_event")
    @mock.patch("astarte.device.device_grpc._decode_astarte_message")
    @mock.patch("astarte.device.device_grpc.Thread")
    @mock.patch("astarte.device.device_grpc.Node")
    @mock.patch("astarte.device.device_grpc.MessageHubStub")
    @mock.patch("astarte.device.device_grpc.grpc.insecure_channel")
    @mock.patch("astarte.device.device_grpc.grpc.intercept_channel")
    @mock.patch("astarte.device.device_grpc.AstarteUnaryStreamInterceptor")
    @mock.patch("astarte.device.device_grpc.AstarteUnaryUnaryInterceptor")
    def test_devicegrpc__rx_stream_handler(
        self,
        mock_AstarteUnaryUnaryInterceptor,
        mock_AstarteUnaryStreamInterceptor,
        mock_intercept_channel,
        mock_insecure_channel,
        mock_msg_hub_stub,
        mock_node,
        mock_thread,
        mock__decode_astarte_message,
        mock__decode_msg_hub_event,
        mock__on_message_generic,
    ):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        rx_message1 = mock.MagicMock()
        rx_message2 = mock.MagicMock()
        rx_message3 = mock.MagicMock()

        # mock_msg_hub_stub.return_value.Attach.return_value = [rx_message1, rx_message2]
        class StreamStub:
            def __init__(self, msgs: list):
                self.msgs = msgs

            def __iter__(self):
                return self

            def __next__(self):
                if not self.msgs:
                    raise StopIteration
                if not self.msgs[0]:
                    raise _MultiThreadedRendezvous(mock.MagicMock(), None, None, None)
                return self.msgs.pop(0)

        mock_msg_hub_stub.return_value.Attach.return_value = StreamStub(
            [rx_message1, rx_message2, None, rx_message3]
        )

        mock_on_connected = mock.MagicMock()
        mock_on_data_received = mock.MagicMock()
        mock_on_disconnected = mock.MagicMock()
        device.set_events_callbacks(
            on_connected=mock_on_connected,
            on_data_received=mock_on_data_received,
            on_disconnected=mock_on_disconnected,
        )

        device.connect()

        mock_insecure_channel.assert_called_once_with(server_address)
        mock_insecure_channel.return_value.subscribe.assert_called_once_with(
            device._on_connectivity_change
        )
        mock_intercept_channel.assert_called_once_with(
            mock_insecure_channel.return_value,
            mock_AstarteUnaryUnaryInterceptor.return_value,
            mock_AstarteUnaryStreamInterceptor.return_value,
        )
        mock_msg_hub_stub.assert_called_once_with(mock_intercept_channel.return_value)
        mock_node.assert_called_once_with(interfaces_json=[])
        mock_msg_hub_stub.return_value.Attach.assert_called_once_with(mock_node.return_value)

        mock_thread.assert_called_once_with(target=device._rx_stream_handler)
        assert mock_thread.return_value.daemon
        mock_thread.return_value.start.assert_called_once()

        mock_on_connected.assert_not_called()
        mock_on_data_received.assert_not_called()
        mock_on_disconnected.assert_not_called()

        # Start a helper thread to put another entry in the queue of the device
        thread_handle = Thread(target=lambda: device._DeviceGrpc__stream_queue.put(None))
        thread_handle.daemon = True
        thread_handle.start()

        # Manually start of the _rx_stream_handler() method in the current thread
        rx_msg1_decoded = ("interface 1 name", "path 1", mock.MagicMock())
        rx_msg2_decoded = ("interface 2 name", "path 2", mock.MagicMock())
        mock__decode_msg_hub_event.side_effect = [rx_msg1_decoded, rx_msg2_decoded]
        device._rx_stream_handler()

        calls = [mock.call(rx_message1), mock.call(rx_message2)]
        mock__decode_msg_hub_event.assert_has_calls(calls)
        self.assertEqual(mock__decode_msg_hub_event.call_count, 2)

        calls = [
            mock.call(rx_msg1_decoded[0], rx_msg1_decoded[1], rx_msg1_decoded[2]),
            mock.call(rx_msg2_decoded[0], rx_msg2_decoded[1], rx_msg2_decoded[2]),
        ]
        mock__on_message_generic.assert_has_calls(calls)
        self.assertEqual(mock__on_message_generic.call_count, 2)

    @mock.patch("astarte.device.device_grpc.Empty")
    def test_devicegrpc_disconnect(self, mock_empty):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        mock_grpc_channel = mock.MagicMock()
        mock_msghub_stub = mock.MagicMock()
        mock_msghub_node = mock.MagicMock()
        device._DeviceGrpc__grpc_channel = mock_grpc_channel
        device._DeviceGrpc__msghub_stub = mock_msghub_stub
        device._DeviceGrpc__msghub_node = mock_msghub_node

        mock_on_connected = mock.MagicMock()
        mock_on_data_received = mock.MagicMock()
        mock_on_disconnected = mock.MagicMock()
        device.set_events_callbacks(
            on_connected=mock_on_connected,
            on_data_received=mock_on_data_received,
            on_disconnected=mock_on_disconnected,
        )

        device.disconnect()

        mock_msghub_stub.Detach.assert_called_once_with(mock_empty.return_value)
        mock_grpc_channel.close.assert_called_once()
        mock_on_disconnected.assert_called_once_with(device, 0)

    @mock.patch("astarte.device.device_grpc.Empty")
    def test_devicegrpc_disconnect_threaded(self, mock_empty):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        mock_grpc_channel = mock.MagicMock()
        mock_msghub_stub = mock.MagicMock()
        mock_msghub_node = mock.MagicMock()
        device._DeviceGrpc__grpc_channel = mock_grpc_channel
        device._DeviceGrpc__msghub_stub = mock_msghub_stub
        device._DeviceGrpc__msghub_node = mock_msghub_node

        mock_on_connected = mock.MagicMock()
        mock_on_data_received = mock.MagicMock()
        mock_on_disconnected = mock.MagicMock()
        mock_loop = mock.MagicMock()
        device.set_events_callbacks(
            on_connected=mock_on_connected,
            on_data_received=mock_on_data_received,
            on_disconnected=mock_on_disconnected,
            loop=mock_loop,
        )

        device.disconnect()

        mock_msghub_stub.Detach.assert_called_once_with(mock_empty.return_value)
        mock_grpc_channel.close.assert_called_once()
        mock_loop.call_soon_threadsafe.assert_called_once_with(mock_on_disconnected, device, 0)
        mock_on_disconnected.assert_not_called()

    def test_devicegrpc_disconnect_while_not_connected(self):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.DISCONNECTED

        mock_grpc_channel = mock.MagicMock()
        mock_msghub_stub = mock.MagicMock()
        mock_msghub_node = mock.MagicMock()
        device._DeviceGrpc__grpc_channel = mock_grpc_channel
        device._DeviceGrpc__msghub_stub = mock_msghub_stub
        device._DeviceGrpc__msghub_node = mock_msghub_node

        mock_on_connected = mock.MagicMock()
        mock_on_data_received = mock.MagicMock()
        mock_on_disconnected = mock.MagicMock()
        device.set_events_callbacks(
            on_connected=mock_on_connected,
            on_data_received=mock_on_data_received,
            on_disconnected=mock_on_disconnected,
        )

        device.disconnect()

        mock_msghub_stub.Detach.assert_not_called()
        mock_grpc_channel.close.assert_not_called()
        mock_on_disconnected.assert_not_called()

    @mock.patch("astarte.device.device_grpc._encode_astarte_message")
    def test_devicegrpc__send_generic(self, mock__encode_astarte_message):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        mock_interface = mock.MagicMock()
        mock_payload = mock.MagicMock()
        timestamp = datetime.now()

        mock_interface.is_server_owned.return_value = False

        mock_msghub_stub = mock.MagicMock()
        device._DeviceGrpc__msghub_stub = mock_msghub_stub

        device._send_generic(mock_interface, "path", mock_payload, timestamp)

        mock_interface.is_server_owned.assert_called_once()
        protobuf_timestamp = Timestamp()
        protobuf_timestamp.FromDatetime(timestamp)
        mock__encode_astarte_message.assert_called_once_with(
            mock_interface, "path", protobuf_timestamp, mock_payload
        )
        mock_msghub_stub.Send.assert_called_once_with(mock__encode_astarte_message.return_value)

    @mock.patch("astarte.device.device_grpc._encode_astarte_message")
    def test_devicegrpc__send_generic_no_timestamp(self, mock__encode_astarte_message):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        mock_interface = mock.MagicMock()
        mock_payload = mock.MagicMock()

        mock_interface.is_server_owned.return_value = False

        mock_msghub_stub = mock.MagicMock()
        device._DeviceGrpc__msghub_stub = mock_msghub_stub

        device._send_generic(mock_interface, "path", mock_payload, None)

        mock_interface.is_server_owned.assert_called_once()
        mock__encode_astarte_message.assert_called_once_with(
            mock_interface, "path", None, mock_payload
        )
        mock_msghub_stub.Send.assert_called_once_with(mock__encode_astarte_message.return_value)

    @mock.patch("astarte.device.device_grpc._encode_astarte_message")
    def test_devicegrpc__send_generic_no_payload(self, mock__encode_astarte_message):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        mock_interface = mock.MagicMock()
        timestamp = datetime.now()

        mock_interface.is_server_owned.return_value = False
        mock_mapping = mock.MagicMock()
        mock_interface.get_mapping.return_value = mock_mapping

        mock_msghub_stub = mock.MagicMock()
        device._DeviceGrpc__msghub_stub = mock_msghub_stub

        device._send_generic(mock_interface, "path", None, timestamp)

        mock_interface.is_server_owned.assert_called_once()
        mock_interface.get_mapping.assert_called_once_with("path")
        mock__encode_astarte_message.assert_called_once_with(mock_interface, "path", None, None)
        mock_msghub_stub.Send.assert_called_once_with(mock__encode_astarte_message.return_value)

    @mock.patch("astarte.device.device_grpc._encode_astarte_message")
    def test_devicegrpc__send_generic_no_payload_not_in_introspection_raises(
        self, mock__encode_astarte_message
    ):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        mock_interface = mock.MagicMock()
        timestamp = datetime.now()

        mock_interface.is_server_owned.return_value = False
        mock_interface.get_mapping.return_value = None

        mock_msghub_stub = mock.MagicMock()
        device._DeviceGrpc__msghub_stub = mock_msghub_stub

        self.assertRaises(
            ValidationError, lambda: device._send_generic(mock_interface, "path", None, timestamp)
        )

        mock_interface.is_server_owned.assert_called_once()
        mock_interface.get_mapping.assert_called_once_with("path")
        mock__encode_astarte_message.assert_not_called()
        mock_msghub_stub.Send.assert_not_called()

    @mock.patch("astarte.device.device_grpc._encode_astarte_message")
    def test_devicegrpc__send_generic_server_owned_raises(self, mock__encode_astarte_message):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTED

        mock_interface = mock.MagicMock()
        timestamp = datetime.now()

        mock_interface.is_server_owned.return_value = True

        mock_msghub_stub = mock.MagicMock()
        device._DeviceGrpc__msghub_stub = mock_msghub_stub

        self.assertRaises(
            ValidationError, lambda: device._send_generic(mock_interface, "path", None, timestamp)
        )

        mock_interface.is_server_owned.assert_called_once()
        mock_interface.get_mapping.assert_not_called()
        mock__encode_astarte_message.assert_not_called()
        mock_msghub_stub.Send.assert_not_called()

    @mock.patch("astarte.device.device_grpc._encode_astarte_message")
    def test_devicegrpc__send_generic_while_not_connected_raises(
        self, mock__encode_astarte_message
    ):
        server_address = "server address"
        node_uuid = "node uuid"
        device = DeviceGrpc(
            server_address,
            node_uuid,
        )

        device._DeviceGrpc__connection_state = ConnectionState.CONNECTING

        mock_interface = mock.MagicMock()
        timestamp = datetime.now()

        mock_interface.is_server_owned.return_value = False

        mock_msghub_stub = mock.MagicMock()
        device._DeviceGrpc__msghub_stub = mock_msghub_stub

        self.assertRaises(
            DeviceDisconnectedError,
            lambda: device._send_generic(mock_interface, "path", None, timestamp),
        )

        mock_interface.is_server_owned.assert_not_called()
        mock_interface.get_mapping.assert_not_called()
        mock__encode_astarte_message.assert_not_called()
        mock_msghub_stub.Send.assert_not_called()

    @mock.patch("astarte.device.device_grpc.AstarteUnset")
    @mock.patch("astarte.device.device_grpc.AstarteMessage")
    def test_encode_astarte_message_empty_message(self, mock_astarte_message, mock_astarte_unset):
        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_path = "path"
        mock_timestamp = mock.MagicMock()
        encoded_message = _encode_astarte_message(mock_interface, mock_path, mock_timestamp, None)

        mock_astarte_unset.assert_called_once()
        mock_astarte_message.assert_called_once_with(
            interface_name="interface name",
            path="path",
            timestamp=mock_timestamp,
            astarte_unset=mock_astarte_unset.return_value,
        )
        self.assertEqual(encoded_message, mock_astarte_message.return_value)

    @mock.patch("astarte.device.device_grpc.AstarteDataType")
    @mock.patch("astarte.device.device_grpc._encode_astarte_data_type_individual")
    @mock.patch("astarte.device.device_grpc.AstarteMessage")
    def test_encode_astarte_message_individual_payload(
        self,
        mock_astarte_message,
        mock__encode_astarte_data_type_individual,
        mock_astarte_data_type,
    ):
        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_aggregation_object.return_value = False
        mock_path = "path"
        mock_timestamp = mock.MagicMock()
        mock_payload = mock.MagicMock()

        encoded_message = _encode_astarte_message(
            mock_interface, mock_path, mock_timestamp, mock_payload
        )

        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.get_mapping.assert_called_once_with(mock_path)
        mock__encode_astarte_data_type_individual.assert_called_once_with(
            mock_interface.get_mapping.return_value, mock_payload
        )
        mock_astarte_data_type.assert_called_once_with(
            astarte_individual=mock__encode_astarte_data_type_individual.return_value
        )
        mock_astarte_message.assert_called_once_with(
            interface_name=mock_interface.name,
            path=mock_path,
            timestamp=mock_timestamp,
            astarte_data=mock_astarte_data_type.return_value,
        )
        self.assertEqual(encoded_message, mock_astarte_message.return_value)

    @mock.patch("astarte.device.device_grpc.AstarteDataTypeObject")
    @mock.patch("astarte.device.device_grpc.AstarteDataType")
    @mock.patch("astarte.device.device_grpc._encode_astarte_data_type_individual")
    @mock.patch("astarte.device.device_grpc.AstarteMessage")
    def test_encode_astarte_message_object_payload(
        self,
        mock_astarte_message,
        mock__encode_astarte_data_type_individual,
        mock_astarte_data_type,
        mock_astarte_data_type_object,
    ):
        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_aggregation_object.return_value = True
        mock_path = "path"
        mock_timestamp = mock.MagicMock()
        mock_payload = {"endpoint1": mock.MagicMock(), "endpoint2": mock.MagicMock()}
        mock_mapping1 = mock.MagicMock()
        mock_mapping2 = mock.MagicMock()
        mock_interface.get_mapping.side_effect = [mock_mapping1, mock_mapping2]
        mock_astarte_data_type1 = mock.MagicMock()
        mock_astarte_data_type2 = mock.MagicMock()
        mock__encode_astarte_data_type_individual.side_effect = [
            mock_astarte_data_type1,
            mock_astarte_data_type2,
        ]

        encoded_message = _encode_astarte_message(
            mock_interface, mock_path, mock_timestamp, mock_payload
        )

        mock_interface.is_aggregation_object.assert_called_once()
        calls = [mock.call("path/endpoint1"), mock.call("path/endpoint2")]
        mock_interface.get_mapping.assert_has_calls(calls)
        self.assertEqual(mock_interface.get_mapping.call_count, 2)
        calls = [
            mock.call(mock_mapping1, mock_payload["endpoint1"]),
            mock.call(mock_mapping2, mock_payload["endpoint2"]),
        ]
        mock__encode_astarte_data_type_individual.assert_has_calls(calls)
        self.assertEqual(mock__encode_astarte_data_type_individual.call_count, 2)
        mock_astarte_data_type_object.assert_called_once_with(
            object_data={"endpoint1": mock_astarte_data_type1, "endpoint2": mock_astarte_data_type2}
        )
        mock_astarte_data_type.assert_called_once_with(
            astarte_object=mock_astarte_data_type_object.return_value
        )
        mock_astarte_message.assert_called_once_with(
            interface_name=mock_interface.name,
            path=mock_path,
            timestamp=mock_timestamp,
            astarte_data=mock_astarte_data_type.return_value,
        )
        self.assertEqual(encoded_message, mock_astarte_message.return_value)

    @mock.patch("astarte.device.device_grpc.AstarteDateTimeArray")
    @mock.patch("astarte.device.device_grpc.AstarteBinaryBlobArray")
    @mock.patch("astarte.device.device_grpc.AstarteLongIntegerArray")
    @mock.patch("astarte.device.device_grpc.AstarteIntegerArray")
    @mock.patch("astarte.device.device_grpc.AstarteDoubleArray")
    @mock.patch("astarte.device.device_grpc.AstarteStringArray")
    @mock.patch("astarte.device.device_grpc.AstarteBooleanArray")
    @mock.patch("astarte.device.device_grpc._encode_timestamp")
    @mock.patch("astarte.device.device_grpc.AstarteDataTypeIndividual")
    def test_encode_astarte_data_type(
        self,
        mock_astarte_data_type_individual,
        mock__encode_timestamp,
        mock_astarte_boolean_array,
        mock_astarte_string_array,
        mock_astarte_double_array,
        mock_astarte_integer_array,
        mock_astarte_longinteger_array,
        mock_astarte_binaryblob_array,
        mock_astarte_datetime_array,
    ):
        mock_mapping = mock.MagicMock()
        mock_payload = mock.MagicMock()

        # Simpler non-array types
        mock_mapping.type = "boolean"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(astarte_boolean=mock_payload)
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "string"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(astarte_string=mock_payload)
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "double"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(astarte_double=mock_payload)
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "integer"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(astarte_integer=mock_payload)
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "longinteger"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(astarte_long_integer=mock_payload)
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "binaryblob"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(astarte_binary_blob=mock_payload)
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        # More complex datetime
        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "datetime"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock__encode_timestamp.assert_called_once_with(mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(
            astarte_date_time=mock__encode_timestamp.return_value
        )
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        # Simpler array types
        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "booleanarray"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_boolean_array.assert_called_once_with(values=mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(
            astarte_boolean_array=mock_astarte_boolean_array.return_value
        )
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "stringarray"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_string_array.assert_called_once_with(values=mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(
            astarte_string_array=mock_astarte_string_array.return_value
        )
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "doublearray"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_double_array.assert_called_once_with(values=mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(
            astarte_double_array=mock_astarte_double_array.return_value
        )
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "integerarray"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_integer_array.assert_called_once_with(values=mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(
            astarte_integer_array=mock_astarte_integer_array.return_value
        )
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "longintegerarray"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_longinteger_array.assert_called_once_with(values=mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(
            astarte_long_integer_array=mock_astarte_longinteger_array.return_value
        )
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        mock_astarte_data_type_individual.reset_mock()
        mock_mapping.type = "binaryblobarray"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        mock_astarte_binaryblob_array.assert_called_once_with(values=mock_payload)
        mock_astarte_data_type_individual.assert_called_once_with(
            astarte_binary_blob_array=mock_astarte_binaryblob_array.return_value
        )
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

        # More complex datetime array
        mock_astarte_data_type_individual.reset_mock()
        mock__encode_timestamp.reset_mock()
        mock_payload = [mock.MagicMock(), mock.MagicMock()]
        mock_encoded_payload1 = mock.MagicMock()
        mock_encoded_payload2 = mock.MagicMock()
        mock__encode_timestamp.side_effect = [mock_encoded_payload1, mock_encoded_payload2]
        mock_mapping.type = "datetimearray"
        encoded_data_type = _encode_astarte_data_type_individual(mock_mapping, mock_payload)
        calls = [mock.call(mock_payload[0]), mock.call(mock_payload[1])]
        mock__encode_timestamp.assert_has_calls(calls)
        self.assertEqual(mock__encode_timestamp.call_count, 2)
        mock_astarte_datetime_array.assert_called_once_with(
            values=[mock_encoded_payload1, mock_encoded_payload2]
        )
        mock_astarte_data_type_individual.assert_called_once_with(
            astarte_date_time_array=mock_astarte_datetime_array.return_value
        )
        self.assertEqual(encoded_data_type, mock_astarte_data_type_individual.return_value)

    @mock.patch("astarte.device.device_grpc.Timestamp")
    def test_encode_timestamp(self, mock_grpc_timestamp):
        mock_timestamp = mock.MagicMock()
        encoded_timestamp = _encode_timestamp(mock_timestamp)
        mock_grpc_timestamp.assert_called_once()
        mock_grpc_timestamp.return_value.FromDatetime.assert_called_once_with(mock_timestamp)
        self.assertEqual(encoded_timestamp, mock_grpc_timestamp.return_value)

    @mock.patch("astarte.device.device_grpc._decode_astarte_data_type_individual")
    def test_decode_astarte_message_individual(self, mock__decode_astarte_data_type_individual):
        mock_astarte_message = mock.MagicMock()
        mock_astarte_message.HasField.return_value = True
        mock_astarte_message.astarte_data.HasField.return_value = True
        encoded_message = _decode_astarte_message(mock_astarte_message)

        mock_astarte_message.HasField.assert_called_once_with("astarte_data")
        mock_astarte_message.astarte_data.HasField.assert_called_once_with("astarte_individual")
        mock__decode_astarte_data_type_individual.assert_called_once_with(
            mock_astarte_message.astarte_data.astarte_individual
        )
        self.assertEqual(
            encoded_message,
            (
                mock_astarte_message.interface_name,
                mock_astarte_message.path,
                mock__decode_astarte_data_type_individual.return_value,
            ),
        )

    @mock.patch("astarte.device.device_grpc._decode_astarte_data_type_object")
    def test_decode_astarte_message_object(self, mock__decode_astarte_data_type_object):
        mock_astarte_message = mock.MagicMock()
        mock_astarte_message.HasField.return_value = True
        mock_astarte_message.astarte_data.HasField.return_value = False
        encoded_message = _decode_astarte_message(mock_astarte_message)

        mock_astarte_message.HasField.assert_called_once_with("astarte_data")
        mock_astarte_message.astarte_data.HasField.assert_called_once_with("astarte_individual")
        mock__decode_astarte_data_type_object.assert_called_once_with(
            mock_astarte_message.astarte_data.astarte_object
        )
        self.assertEqual(
            encoded_message,
            (
                mock_astarte_message.interface_name,
                mock_astarte_message.path,
                mock__decode_astarte_data_type_object.return_value,
            ),
        )

    def test_decode_astarte_message_unset(self):
        mock_astarte_message = mock.MagicMock()
        mock_astarte_message.HasField.return_value = False
        decoded_message = _decode_astarte_message(mock_astarte_message)

        mock_astarte_message.HasField.assert_called_once_with("astarte_data")
        mock_astarte_message.astarte_data.HasField.assert_not_called()
        self.assertEqual(
            decoded_message, (mock_astarte_message.interface_name, mock_astarte_message.path, None)
        )

    @mock.patch("astarte.device.device_grpc._decode_astarte_data_type_individual")
    def test_decode_astarte_data_type_object(self, mock__decode_astarte_data_type_individual):
        mock_data_type = mock.MagicMock()
        mock_object_data1 = ("endpoint 1", mock.MagicMock())
        mock_object_data2 = ("endpoint 2", mock.MagicMock())
        mock_data_type.object_data.items.return_value = [mock_object_data1, mock_object_data2]
        mock_decoded_data1 = mock.MagicMock()
        mock_decoded_data2 = mock.MagicMock()
        mock__decode_astarte_data_type_individual.side_effect = [
            mock_decoded_data1,
            mock_decoded_data2,
        ]
        decoded_message = _decode_astarte_data_type_object(mock_data_type)

        mock_data_type.object_data.items.assert_called_once()
        calls = [mock.call(mock_object_data1[1]), mock.call(mock_object_data2[1])]
        mock__decode_astarte_data_type_individual.assert_has_calls(calls)
        self.assertEqual(mock__decode_astarte_data_type_individual.call_count, 2)
        self.assertEqual(
            decoded_message, {"endpoint 1": mock_decoded_data1, "endpoint 2": mock_decoded_data2}
        )

    def test_decode_astarte_data_type_individual_boolean(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_boolean"
        mock_data_type.astarte_boolean = mock.MagicMock(spec=bool)
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, mock_data_type.astarte_boolean)

    def test_decode_astarte_data_type_individual_string(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_string"
        mock_data_type.astarte_string = mock.MagicMock(spec=str)
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, mock_data_type.astarte_string)

    def test_decode_astarte_data_type_individual_double(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_double"
        mock_data_type.astarte_double = mock.MagicMock(spec=float)
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, mock_data_type.astarte_double)

    def test_decode_astarte_data_type_individual_integer(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_integer"
        mock_data_type.astarte_integer = mock.MagicMock(spec=int)
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, mock_data_type.astarte_integer)

    def test_decode_astarte_data_type_individual_longinteger(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_long_integer"
        mock_data_type.astarte_long_integer = mock.MagicMock(spec=int)
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, mock_data_type.astarte_long_integer)

    def test_decode_astarte_data_type_individual_binaryblob(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_binary_blob"
        mock_data_type.astarte_binary_blob = mock.MagicMock(spec=bytes)
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, mock_data_type.astarte_binary_blob)

    def test_decode_astarte_data_type_individual_datetime(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_date_time"
        mock_data_type.astarte_date_time.ToDatetime.return_value = mock.MagicMock(spec=datetime)
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        mock_data_type.astarte_date_time.ToDatetime.assert_called_once_with(timezone.utc)
        self.assertEqual(decoded_message, mock_data_type.astarte_date_time.ToDatetime.return_value)

    def test_decode_astarte_data_type_individual_booleanarray(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_boolean_array"
        individual_data = mock.MagicMock(spec=AstarteBooleanArray)
        individual_data.values = [mock.MagicMock(), mock.MagicMock()]
        mock_data_type.astarte_boolean_array = individual_data
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, individual_data.values)

    def test_decode_astarte_data_type_individual_stringarray(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_string_array"
        individual_data = mock.MagicMock(spec=AstarteStringArray)
        individual_data.values = [mock.MagicMock(), mock.MagicMock()]
        mock_data_type.astarte_string_array = individual_data
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, individual_data.values)

    def test_decode_astarte_data_type_individual_doublearray(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_double_array"
        individual_data = mock.MagicMock(spec=AstarteDoubleArray)
        individual_data.values = [mock.MagicMock(), mock.MagicMock()]
        mock_data_type.astarte_double_array = individual_data
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, individual_data.values)

    def test_decode_astarte_data_type_individual_integerarray(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_integer_array"
        individual_data = mock.MagicMock(spec=AstarteIntegerArray)
        individual_data.values = [mock.MagicMock(), mock.MagicMock()]
        mock_data_type.astarte_integer_array = individual_data
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, individual_data.values)

    def test_decode_astarte_data_type_individual_longintegerarray(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_long_integer_array"
        individual_data = mock.MagicMock(spec=AstarteLongIntegerArray)
        individual_data.values = [mock.MagicMock(), mock.MagicMock()]
        mock_data_type.astarte_long_integer_array = individual_data
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, individual_data.values)

    def test_decode_astarte_data_type_individual_binaryblobarray(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_binary_blob_array"
        individual_data = mock.MagicMock(spec=AstarteBinaryBlobArray)
        individual_data.values = [mock.MagicMock(), mock.MagicMock()]
        mock_data_type.astarte_binary_blob_array = individual_data
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        self.assertEqual(decoded_message, individual_data.values)

    def test_decode_astarte_data_type_individual_datetimearray(self):
        mock_data_type = mock.MagicMock()
        mock_data_type.WhichOneof.return_value = "astarte_date_time_array"
        mock_data_type.astarte_date_time.ToDatetime.return_value = mock.MagicMock(spec=datetime)
        individual_data = mock.MagicMock(spec=AstarteDateTimeArray)
        individual_data.values = [mock.MagicMock(), mock.MagicMock()]
        mock_data_type.astarte_date_time_array = individual_data
        decoded_message = _decode_astarte_data_type_individual(mock_data_type)

        mock_data_type.WhichOneof.assert_called_once_with("individual_data")
        individual_data.values[0].ToDatetime.assert_called_once_with(timezone.utc)
        individual_data.values[1].ToDatetime.assert_called_once_with(timezone.utc)
        self.assertEqual(
            decoded_message,
            [
                individual_data.values[0].ToDatetime.return_value,
                individual_data.values[1].ToDatetime.return_value,
            ],
        )
