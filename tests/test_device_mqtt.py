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

# pylint: disable=useless-suppression,missing-function-docstring,missing-class-docstring
# pylint: disable=too-many-statements,too-many-instance-attributes,missing-return-doc
# pylint: disable=missing-return-type-doc,no-value-for-parameter,protected-access,
# pylint: disable=too-many-public-methods,no-self-use, too-many-locals

import ssl
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import paho
from paho.mqtt.client import Client

from astarte.device import DeviceMqtt
from astarte.device.database import AstarteDatabaseSQLite
from astarte.device.device import ConnectionState
from astarte.device.exceptions import (
    APIError,
    DeviceConnectingError,
    DeviceDisconnectedError,
    InterfaceNotFoundError,
    PersistencyDirectoryNotFoundError,
    ValidationError,
)
from astarte.device.introspection import Introspection


class UnitTests(unittest.TestCase):
    def setUp(self):
        pass

    @mock.patch.object(AstarteDatabaseSQLite, "__init__", return_value=None)
    @mock.patch("astarte.device.device_mqtt.os.mkdir")
    @mock.patch("astarte.device.device_mqtt.os.path.isdir", side_effect=[True, False, False, False])
    def test_initialization_ok(self, isdir_mock, mkdir_mock, mock_db):
        DeviceMqtt(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            "./tests",
            None,
            False,
        )
        calls = [
            mock.call("./tests"),
            mock.call("./tests/device_id"),
            mock.call("./tests/device_id/crypto"),
            mock.call("./tests/device_id/caching"),
        ]
        isdir_mock.assert_has_calls(calls)
        self.assertEqual(isdir_mock.call_count, 4)
        calls = [
            mock.call("./tests/device_id"),
            mock.call("./tests/device_id/crypto"),
            mock.call("./tests/device_id/caching"),
        ]
        mkdir_mock.assert_has_calls(calls)
        self.assertEqual(mkdir_mock.call_count, 3)
        mock_db.assert_called_once_with(Path("./tests/device_id/caching/astarte.db"))

    def test_initialization_raises(self):
        self.assertRaises(
            PersistencyDirectoryNotFoundError,
            lambda: DeviceMqtt(
                "device_id",
                "realm_name",
                "credential_secret",
                "pairing_base_url",
                "pers_dir",
                None,
                False,
            ),
        )

    @mock.patch.object(AstarteDatabaseSQLite, "__init__", return_value=None)
    @mock.patch("astarte.device.device_mqtt.os.path.isdir", return_value=True)
    def helper_initialize_device(self, mock_isdir, mock_db):
        device = DeviceMqtt(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            "./tests",
            ignore_ssl_errors=False,
        )
        self.assertEqual(mock_isdir.call_count, 4)
        mock_db.assert_called_once_with(Path("./tests/device_id/caching/astarte.db"))
        return device

    @mock.patch("astarte.device.device_mqtt.Interface")
    @mock.patch.object(Introspection, "add_interface")
    def test_add_interface_from_json_while_not_connected(self, mock_add_interface, mock_interface):
        device = self.helper_initialize_device()

        interface_json = {"json content": 42}
        device.add_interface_from_json(interface_json)

        mock_interface.assert_called_once_with(interface_json)
        mock_add_interface.assert_called_once_with(mock_interface.return_value)

    # __send_introspection is tested together with the connect method
    @mock.patch.object(DeviceMqtt, "_DeviceMqtt__send_introspection")
    @mock.patch.object(Client, "subscribe")
    @mock.patch("astarte.device.device_mqtt.Interface")
    @mock.patch.object(Introspection, "add_interface")
    def test_add_interface_from_json_while_connected(
        self, mock_add_interface, mock_interface, mock_subscribe, mock__send_introspection
    ):
        device = self.helper_initialize_device()

        mock_interface.return_value.name = "<interface-name>"
        mock_interface.return_value.is_server_owned.return_value = True

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_json = {"json content": 42}
        device.add_interface_from_json(interface_json)

        mock_interface.assert_called_once_with(interface_json)
        mock_add_interface.assert_called_once_with(mock_interface.return_value)
        mock_subscribe.assert_called_once_with("realm_name/device_id/<interface-name>/#", qos=2)
        mock__send_introspection.assert_called_once()

    # __send_introspection is tested together with the connect method
    @mock.patch.object(DeviceMqtt, "_DeviceMqtt__send_introspection")
    @mock.patch.object(Client, "subscribe")
    @mock.patch("astarte.device.device_mqtt.Interface")
    @mock.patch.object(Introspection, "add_interface")
    def test_add_interface_from_json_while_connected_client_owned_interface(
        self, mock_add_interface, mock_interface, mock_subscribe, mock__send_introspection
    ):
        device = self.helper_initialize_device()

        mock_interface.return_value.is_server_owned.return_value = False

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_json = {"json content": 42}
        device.add_interface_from_json(interface_json)

        mock_interface.assert_called_once_with(interface_json)
        mock_add_interface.assert_called_once_with(mock_interface.return_value)
        mock_subscribe.assert_not_called()
        mock__send_introspection.assert_called_once()

    # __send_introspection is tested together with the connect method
    @mock.patch.object(DeviceMqtt, "_DeviceMqtt__send_introspection")
    @mock.patch.object(Client, "subscribe")
    @mock.patch("astarte.device.device_mqtt.Interface")
    @mock.patch.object(Introspection, "add_interface")
    def test_add_interface_from_json_while_connecting_raises(
        self, mock_add_interface, mock_interface, mock_subscribe, mock__send_introspection
    ):
        device = self.helper_initialize_device()

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTING

        interface_json = {"json content": 42}
        self.assertRaises(
            DeviceConnectingError, lambda: device.add_interface_from_json(interface_json)
        )

        mock_add_interface.assert_not_called()
        mock_interface.assert_not_called()
        mock_subscribe.assert_not_called()
        mock__send_introspection.assert_not_called()

    @mock.patch.object(Introspection, "remove_interface")
    @mock.patch.object(Introspection, "get_interface")
    def test_remove_interface_while_not_connected(self, mock_get_interface, mock_remove_interface):
        device = self.helper_initialize_device()

        interface_name = "interface name"
        device.remove_interface(interface_name)
        mock_get_interface.assert_called_once_with(interface_name)
        mock_remove_interface.assert_called_once_with(interface_name)

    # __send_introspection is tested together with the connect method
    @mock.patch.object(AstarteDatabaseSQLite, "delete_props_from_interface")
    @mock.patch.object(Client, "unsubscribe")
    @mock.patch.object(DeviceMqtt, "_DeviceMqtt__send_introspection")
    @mock.patch.object(Introspection, "remove_interface")
    @mock.patch.object(Introspection, "get_interface")
    def test_remove_interface_while_connected(
        self,
        mock_get_interface,
        mock_remove_interface,
        mock__send_introspection,
        mock_unsubscribe,
        mock_delete_props_from_interface,
    ):
        device = self.helper_initialize_device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = True
        mock_interface.is_type_properties.return_value = True
        mock_get_interface.return_value = mock_interface

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_name = "interface name"
        device.remove_interface(interface_name)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_remove_interface.assert_called_once_with(interface_name)
        mock_delete_props_from_interface.assert_called_once_with(interface_name)
        mock__send_introspection.assert_called_once()
        mock_interface.is_server_owned.assert_called_once()
        mock_unsubscribe.assert_called_once_with(f"realm_name/device_id/{interface_name}/#")

    # __send_introspection is tested together with the connect method
    @mock.patch.object(AstarteDatabaseSQLite, "delete_props_from_interface")
    @mock.patch.object(Client, "unsubscribe")
    @mock.patch.object(DeviceMqtt, "_DeviceMqtt__send_introspection")
    @mock.patch.object(Introspection, "remove_interface")
    @mock.patch.object(Introspection, "get_interface")
    def test_remove_interface_while_connected_device_owned_datastream(
        self,
        mock_get_interface,
        mock_remove_interface,
        mock__send_introspection,
        mock_unsubscribe,
        mock_delete_props_from_interface,
    ):
        device = self.helper_initialize_device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_name = "interface name"
        device.remove_interface(interface_name)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_remove_interface.assert_called_once_with(interface_name)
        mock_delete_props_from_interface.assert_not_called()
        mock__send_introspection.assert_called_once()
        mock_interface.is_server_owned.assert_called_once()
        mock_unsubscribe.assert_not_called()

    # __send_introspection is tested together with the connect method
    @mock.patch.object(AstarteDatabaseSQLite, "delete_props_from_interface")
    @mock.patch.object(Client, "unsubscribe")
    @mock.patch.object(DeviceMqtt, "_DeviceMqtt__send_introspection")
    @mock.patch.object(Introspection, "remove_interface")
    @mock.patch.object(Introspection, "get_interface")
    def test_remove_interface_while_connecting_raises(
        self,
        mock_get_interface,
        mock_remove_interface,
        mock__send_introspection,
        mock_unsubscribe,
        mock_delete_props_from_interface,
    ):
        device = self.helper_initialize_device()

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTING

        interface_name = "interface name"

        self.assertRaises(DeviceConnectingError, lambda: device.remove_interface(interface_name))

        mock_get_interface.assert_not_called()
        mock_remove_interface.assert_not_called()
        mock_delete_props_from_interface.assert_not_called()
        mock__send_introspection.assert_not_called()
        mock_unsubscribe.assert_not_called()

    # __send_introspection is tested together with the connect method
    @mock.patch.object(AstarteDatabaseSQLite, "delete_props_from_interface")
    @mock.patch.object(Client, "unsubscribe")
    @mock.patch.object(DeviceMqtt, "_DeviceMqtt__send_introspection")
    @mock.patch.object(Introspection, "remove_interface")
    @mock.patch.object(Introspection, "get_interface")
    def test_remove_interface_interface_not_in_introspection_raises(
        self,
        mock_get_interface,
        mock_remove_interface,
        mock__send_introspection,
        mock_unsubscribe,
        mock_delete_props_from_interface,
    ):
        device = self.helper_initialize_device()

        mock_get_interface.return_value = None

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_name = "interface name"
        self.assertRaises(InterfaceNotFoundError, lambda: device.remove_interface(interface_name))

        mock_get_interface.assert_called_once_with(interface_name)
        mock_remove_interface.assert_not_called()
        mock_delete_props_from_interface.assert_not_called()
        mock__send_introspection.assert_not_called()
        mock_unsubscribe.assert_not_called()

    def test_get_device_id(self):
        device = self.helper_initialize_device()
        self.assertEqual(device.get_device_id(), "device_id")

    @mock.patch.object(Client, "loop_start")
    @mock.patch.object(Client, "connect_async")
    @mock.patch("astarte.device.device_mqtt.urlparse")
    @mock.patch(
        "astarte.device.device_mqtt.pairing_handler.obtain_device_transport_information",
        return_value={
            "protocols": {
                "astarte_mqtt_v1": {"broker_url": "some_url"},
                "protocol2": {"broker_url": "some_url"},
            }
        },
    )
    @mock.patch.object(Client, "tls_insecure_set")
    @mock.patch.object(Client, "tls_set")
    @mock.patch("astarte.device.device_mqtt.pairing_handler.obtain_device_certificate")
    @mock.patch("astarte.device.device_mqtt.crypto.device_has_certificate", return_value=False)
    def test_connect(
        self,
        mock_has_certificate,
        mock_obtain_certificate,
        mock_tls_set,
        mock_tls_insecure_set,
        mock_obtain_transport_information,
        mock_urlparse,
        mock_connect_async,
        mock_loop_start,
    ):
        device = self.helper_initialize_device()

        mock_urlparse.return_value.hostname = "mocked hostname"
        mock_urlparse.return_value.port = "mocked port"

        device.connect()
        mock_has_certificate.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            False,
            "./tests/device_id/crypto",
        )
        mock_obtain_certificate.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            "./tests/device_id/crypto",
            False,
        )
        mock_tls_set.assert_called_once_with(
            ca_certs=None,
            certfile="./tests/device_id/crypto/device.crt",
            keyfile="./tests/device_id/crypto/device.key",
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS,
            ciphers=None,
        )
        mock_tls_insecure_set.assert_called_once_with(False)
        mock_obtain_transport_information.assert_called_once_with(
            "device_id", "realm_name", "credential_secret", "pairing_base_url", False
        )
        mock_urlparse.assert_called_once_with("some_url")
        mock_connect_async.assert_called_once_with("mocked hostname", "mocked port")
        mock_loop_start.assert_called_once()

    @mock.patch.object(Client, "loop_start")
    @mock.patch.object(Client, "connect_async")
    @mock.patch("astarte.device.device_mqtt.urlparse")
    @mock.patch(
        "astarte.device.device_mqtt.pairing_handler.obtain_device_transport_information",
        return_value={"protocols": {"astarte_mqtt_v1": {"broker_url": "some_url"}}},
    )
    @mock.patch.object(Client, "tls_insecure_set")
    @mock.patch.object(Client, "tls_set")
    @mock.patch("astarte.device.device_mqtt.pairing_handler.obtain_device_certificate")
    @mock.patch("astarte.device.device_mqtt.crypto.device_has_certificate", return_value=False)
    def test_connect_already_connected(
        self,
        mock_has_certificate,
        mock_obtain_certificate,
        mock_tls_set,
        mock_tls_insecure_set,
        mock_obtain_transport_information,
        mock_urlparse,
        mock_connect_async,
        mock_loop_start,
    ):
        device = self.helper_initialize_device()

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        mock_urlparse.return_value.hostname = "mocked hostname"
        mock_urlparse.return_value.port = "mocked port"

        device.connect()

        mock_has_certificate.assert_not_called()
        mock_obtain_certificate.assert_not_called()
        mock_tls_set.assert_not_called()
        mock_tls_insecure_set.assert_not_called()
        mock_obtain_transport_information.assert_not_called()
        mock_urlparse.assert_not_called()
        mock_connect_async.assert_not_called()
        mock_loop_start.assert_not_called()

    @mock.patch.object(Client, "loop_start")
    @mock.patch.object(Client, "connect_async")
    @mock.patch("astarte.device.device_mqtt.urlparse")
    @mock.patch(
        "astarte.device.device_mqtt.pairing_handler.obtain_device_transport_information",
        return_value={"protocols": {"astarte_mqtt_v1": {"broker_url": "some_url"}}},
    )
    @mock.patch.object(Client, "tls_insecure_set")
    @mock.patch.object(Client, "tls_set")
    @mock.patch("astarte.device.device_mqtt.pairing_handler.obtain_device_certificate")
    @mock.patch("astarte.device.device_mqtt.crypto.device_has_certificate", return_value=False)
    def test_connect_crypto_already_configured(
        self,
        mock_has_certificate,
        mock_obtain_certificate,
        mock_tls_set,
        mock_tls_insecure_set,
        mock_obtain_transport_information,
        mock_urlparse,
        mock_connect_async,
        mock_loop_start,
    ):
        device = self.helper_initialize_device()

        device._DeviceMqtt__is_crypto_setup = True

        mock_urlparse.return_value.hostname = "mocked hostname"
        mock_urlparse.return_value.port = "mocked port"

        device.connect()

        mock_has_certificate.assert_not_called()
        mock_obtain_certificate.assert_not_called()
        mock_tls_set.assert_not_called()
        mock_tls_insecure_set.assert_not_called()
        mock_obtain_transport_information.assert_called_once_with(
            "device_id", "realm_name", "credential_secret", "pairing_base_url", False
        )
        mock_urlparse.assert_called_once_with("some_url")
        mock_connect_async.assert_called_once_with("mocked hostname", "mocked port")
        mock_loop_start.assert_called_once()

    @mock.patch.object(Client, "loop_start")
    @mock.patch.object(Client, "connect_async")
    @mock.patch("astarte.device.device_mqtt.urlparse")
    @mock.patch(
        "astarte.device.device_mqtt.pairing_handler.obtain_device_transport_information",
        return_value={"protocols": {"astarte_mqtt_v1": {"broker_url": "some_url"}}},
    )
    @mock.patch.object(Client, "tls_insecure_set")
    @mock.patch.object(Client, "tls_set")
    @mock.patch("astarte.device.device_mqtt.pairing_handler.obtain_device_certificate")
    @mock.patch("astarte.device.device_mqtt.crypto.device_has_certificate", return_value=True)
    def test_connect_crypto_already_has_certificate(
        self,
        mock_has_certificate,
        mock_obtain_certificate,
        mock_tls_set,
        mock_tls_insecure_set,
        mock_obtain_transport_information,
        mock_urlparse,
        mock_connect_async,
        mock_loop_start,
    ):
        device = self.helper_initialize_device()

        mock_urlparse.return_value.hostname = "mocked hostname"
        mock_urlparse.return_value.port = "mocked port"

        device.connect()

        mock_has_certificate.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            False,
            "./tests/device_id/crypto",
        )
        mock_obtain_certificate.assert_not_called()
        mock_tls_set.assert_called_once_with(
            ca_certs=None,
            certfile="./tests/device_id/crypto/device.crt",
            keyfile="./tests/device_id/crypto/device.key",
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS,
            ciphers=None,
        )
        mock_tls_insecure_set.assert_called_once_with(False)
        mock_obtain_transport_information.assert_called_once_with(
            "device_id", "realm_name", "credential_secret", "pairing_base_url", False
        )
        mock_urlparse.assert_called_once_with("some_url")
        mock_connect_async.assert_called_once_with("mocked hostname", "mocked port")
        mock_loop_start.assert_called_once()

    @mock.patch.object(Client, "loop_start")
    @mock.patch.object(Client, "connect_async")
    @mock.patch("astarte.device.device_mqtt.urlparse")
    @mock.patch(
        "astarte.device.device_mqtt.pairing_handler.obtain_device_transport_information",
        return_value={"protocols": {"astarte_mqtt_v1": {"broker_url": "some_url"}}},
    )
    @mock.patch.object(Client, "tls_insecure_set")
    @mock.patch.object(Client, "tls_set")
    @mock.patch("astarte.device.device_mqtt.pairing_handler.obtain_device_certificate")
    @mock.patch("astarte.device.device_mqtt.crypto.device_has_certificate", return_value=False)
    def test_connect_crypto_ignore_ssl_errors(
        self,
        mock_has_certificate,
        mock_obtain_certificate,
        mock_tls_set,
        mock_tls_insecure_set,
        mock_obtain_transport_information,
        mock_urlparse,
        mock_connect_async,
        mock_loop_start,
    ):
        ignore_ssl_errors = True

        device = self.helper_initialize_device()

        device._DeviceMqtt__ignore_ssl_errors = ignore_ssl_errors

        mock_urlparse.return_value.hostname = "mocked hostname"
        mock_urlparse.return_value.port = "mocked port"

        device.connect()

        mock_has_certificate.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            True,
            "./tests/device_id/crypto",
        )
        mock_obtain_certificate.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            "./tests/device_id/crypto",
            ignore_ssl_errors,
        )
        mock_tls_set.assert_called_once_with(
            ca_certs=None,
            certfile="./tests/device_id/crypto/device.crt",
            keyfile="./tests/device_id/crypto/device.key",
            cert_reqs=ssl.CERT_NONE,
            tls_version=ssl.PROTOCOL_TLS,
            ciphers=None,
        )
        mock_tls_insecure_set.assert_called_once_with(ignore_ssl_errors)
        mock_obtain_transport_information.assert_called_once_with(
            "device_id", "realm_name", "credential_secret", "pairing_base_url", ignore_ssl_errors
        )
        mock_urlparse.assert_called_once_with("some_url")
        mock_connect_async.assert_called_once_with("mocked hostname", "mocked port")
        mock_loop_start.assert_called_once()

    @mock.patch.object(Client, "loop_start")
    @mock.patch.object(Client, "connect_async")
    @mock.patch("astarte.device.device_mqtt.urlparse")
    @mock.patch(
        "astarte.device.device_mqtt.pairing_handler.obtain_device_transport_information",
        return_value={
            "protocols": {
                "astarte_mqtt_v1": {"broker_url": "some_url"},
                "protocol2": {"broker_url": "some_url"},
            }
        },
    )
    @mock.patch.object(Client, "tls_insecure_set")
    @mock.patch.object(Client, "tls_set")
    @mock.patch("astarte.device.device_mqtt.pairing_handler.obtain_device_certificate")
    @mock.patch("astarte.device.device_mqtt.crypto.device_has_certificate", return_value=False)
    def test_connect_invalid_broker_url(
        self,
        mock_has_certificate,
        mock_obtain_certificate,
        mock_tls_set,
        mock_tls_insecure_set,
        mock_obtain_transport_information,
        mock_urlparse,
        mock_connect_async,
        mock_loop_start,
    ):
        device = self.helper_initialize_device()

        mock_urlparse.return_value.hostname = None
        mock_urlparse.return_value.port = None

        self.assertRaises(APIError, device.connect)

        mock_has_certificate.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            False,
            "./tests/device_id/crypto",
        )
        mock_obtain_certificate.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            "./tests/device_id/crypto",
            False,
        )
        mock_tls_set.assert_called_once_with(
            ca_certs=None,
            certfile="./tests/device_id/crypto/device.crt",
            keyfile="./tests/device_id/crypto/device.key",
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS,
            ciphers=None,
        )
        mock_tls_insecure_set.assert_called_once_with(False)
        mock_obtain_transport_information.assert_called_once_with(
            "device_id", "realm_name", "credential_secret", "pairing_base_url", False
        )
        mock_urlparse.assert_called_once_with("some_url")
        mock_connect_async.assert_not_called()
        mock_loop_start.assert_not_called()

    @mock.patch.object(Client, "disconnect")
    def test_disconnect(self, mock_disconnect):
        device = self.helper_initialize_device()

        device.disconnect()
        mock_disconnect.assert_not_called()

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        device.disconnect()
        mock_disconnect.assert_called_once()

    def test_is_connected(self):
        device = self.helper_initialize_device()

        self.assertFalse(device.is_connected())

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        self.assertTrue(device.is_connected())

    @mock.patch.object(Client, "publish")
    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.dumps")
    @mock.patch.object(Introspection, "get_interface")
    def test_send(self, mock_get_interface, mock_bson_dumps, mock_db_store, mock_mqtt_publish):
        device = self.helper_initialize_device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_aggregation_object.return_value = False
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        mock_bson_dumps.return_value = bytes("bson content", "utf-8")

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 12
        timestamp = datetime.now()
        device.send(interface_name, interface_path, payload, timestamp)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_called_once_with(
            interface_path, payload, timestamp
        )
        mock_bson_dumps.assert_called_once_with({"v": payload, "t": timestamp})
        mock_interface.is_type_properties.assert_called_once_with()
        mock_db_store.assert_not_called()
        mock_interface.get_reliability.assert_called_once_with(interface_path)
        mock_mqtt_publish.assert_called_once_with(
            "realm_name/device_id/" + interface_name + interface_path,
            bytes("bson content", "utf-8"),
            qos=mock_interface.get_reliability.return_value,
        )

    @mock.patch.object(Client, "publish")
    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.dumps")
    @mock.patch.object(Introspection, "get_interface")
    def test_send_device_not_connected_raises_device_disconnected_err(
        self, mock_get_interface, mock_bson_dumps, mock_db_store, mock_mqtt_publish
    ):
        device = self.helper_initialize_device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_aggregation_object.return_value = False
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        mock_bson_dumps.return_value = bytes("bson content", "utf-8")

        device._DeviceMqtt__connection_state = ConnectionState.DISCONNECTED

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 12
        timestamp = datetime.now()
        self.assertRaises(
            DeviceDisconnectedError,
            lambda: device.send(interface_name, interface_path, payload, timestamp),
        )

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_called_once_with(
            interface_path, payload, timestamp
        )
        mock_bson_dumps.assert_not_called()
        mock_interface.is_type_properties.assert_not_called()
        mock_db_store.assert_not_called()
        mock_interface.get_reliability.assert_not_called()
        mock_mqtt_publish.assert_not_called()

    @mock.patch.object(Client, "publish")
    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.dumps")
    @mock.patch.object(Introspection, "get_interface")
    def test_send_zero_is_ok(
        self, mock_get_interface, mock_bson_dumps, mock_db_store, mock_mqtt_publish
    ):
        device = self.helper_initialize_device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = False
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        mock_bson_dumps.return_value = bytes("bson content", "utf-8")

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 0
        timestamp = datetime.now()
        device.send(interface_name, interface_path, payload, timestamp)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_called_once_with(
            interface_path, payload, timestamp
        )
        mock_bson_dumps.assert_called_once_with({"v": payload, "t": timestamp})
        mock_interface.is_type_properties.assert_called_once_with()
        mock_db_store.assert_not_called()
        mock_interface.get_reliability.assert_called_once_with(interface_path)
        mock_mqtt_publish.assert_called_once_with(
            "realm_name/device_id/" + interface_name + interface_path,
            bytes("bson content", "utf-8"),
            qos=mock_interface.get_reliability.return_value,
        )

    @mock.patch.object(Client, "publish")
    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.dumps")
    @mock.patch.object(Introspection, "get_interface")
    def test_send_a_property_is_ok(
        self, mock_get_interface, mock_bson_dumps, mock_db_store, mock_mqtt_publish
    ):
        device = self.helper_initialize_device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = False
        mock_interface.is_type_properties.return_value = True
        mock_get_interface.return_value = mock_interface

        mock_bson_dumps.return_value = bytes("bson content", "utf-8")

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 12
        timestamp = datetime.now()
        device.send(interface_name, interface_path, payload, timestamp)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_called_once_with(
            interface_path, payload, timestamp
        )
        mock_bson_dumps.assert_called_once_with({"v": payload, "t": timestamp})
        mock_interface.is_type_properties.assert_called_once_with()
        mock_db_store.assert_called_once_with(
            interface_name, mock_get_interface.return_value.version_major, interface_path, payload
        )
        mock_interface.get_reliability.assert_called_once_with(interface_path)
        mock_mqtt_publish.assert_called_once_with(
            "realm_name/device_id/" + interface_name + interface_path,
            bytes("bson content", "utf-8"),
            qos=mock_interface.get_reliability.return_value,
        )

    @mock.patch.object(Client, "publish")
    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.dumps")
    @mock.patch.object(Introspection, "get_interface")
    def test_send_aggregate(
        self, mock_get_interface, mock_bson_dumps, mock_db_store, mock_mqtt_publish
    ):
        device = self.helper_initialize_device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = True
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        mock_bson_dumps.return_value = bytes("bson content", "utf-8")

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_name = "interface name"
        interface_path = "interface path"
        payload = {"something": 12}
        timestamp = datetime.now()
        device.send_aggregate(interface_name, interface_path, payload, timestamp)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_called_once_with(
            interface_path, payload, timestamp
        )
        mock_bson_dumps.assert_called_once_with({"v": payload, "t": timestamp})
        mock_db_store.assert_not_called()
        mock_interface.get_reliability.assert_called_once_with(interface_path)
        mock_mqtt_publish.assert_called_once_with(
            "realm_name/device_id/" + interface_name + interface_path,
            bytes("bson content", "utf-8"),
            qos=mock_interface.get_reliability.return_value,
        )

    @mock.patch.object(Client, "publish")
    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.dumps")
    @mock.patch.object(Introspection, "get_interface")
    def test_unset_property(
        self, mock_get_interface, mock_bson_dumps, mock_db_store, mock_mqtt_publish
    ):
        device = self.helper_initialize_device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_type_properties.return_value = True
        mock_get_interface.return_value = mock_interface

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_name = "interface name"
        interface_path = "interface path"
        device.unset_property(interface_name, interface_path)

        mock_get_interface.assert_called_once_with(interface_name)
        self.assertEqual(mock_interface.is_type_properties.call_count, 2)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_not_called()
        mock_bson_dumps.assert_not_called()
        mock_db_store.assert_called_once_with(
            interface_name, mock_get_interface.return_value.version_major, interface_path, None
        )
        mock_interface.get_mapping.assert_called_once_with(interface_path)
        mock_interface.get_reliability.assert_called_once_with(interface_path)
        mock_mqtt_publish.assert_called_once_with(
            "realm_name/device_id/" + interface_name + interface_path,
            bytes("", "utf-8"),
            qos=mock_interface.get_reliability.return_value,
        )

    @mock.patch.object(Client, "publish")
    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.dumps")
    @mock.patch.object(Introspection, "get_interface")
    def test_unset_property_non_existing_mapping_raises(
        self, mock_get_interface, mock_bson_dumps, mock_db_store, mock_mqtt_publish
    ):
        device = self.helper_initialize_device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_type_properties.return_value = True
        mock_interface.get_mapping.return_value = None
        mock_get_interface.return_value = mock_interface

        device._DeviceMqtt__connection_state = ConnectionState.CONNECTED

        interface_name = "interface name"
        interface_path = "interface path"
        self.assertRaises(
            ValidationError, lambda: device.unset_property(interface_name, interface_path)
        )

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_type_properties.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_not_called()
        mock_bson_dumps.assert_not_called()
        mock_db_store.assert_not_called()
        mock_interface.get_mapping.assert_called_once_with(interface_path)
        mock_interface.get_reliability.assert_not_called()
        mock_mqtt_publish.assert_not_called()

    @mock.patch.object(Client, "publish")
    @mock.patch.object(AstarteDatabaseSQLite, "load_all_props")
    @mock.patch.object(Introspection, "get_all_interfaces")
    @mock.patch.object(Introspection, "get_all_server_owned_interfaces")
    @mock.patch.object(Client, "subscribe")
    def test__on_connect_without_set_properties(
        self,
        mock_subscribe,
        mock_get_all_server_owned_interfaces,
        mock_get_all_interfaces,
        mock_load_all_props,
        mock_publish,
    ):
        device = self.helper_initialize_device()

        # Mocks for __setup_subscriptions
        mapping_1 = mock.MagicMock()
        interface_1 = mock.MagicMock()
        interface_1.name = "<interface 1 name>"
        interface_1.mappings = {"mapping endpoint": mapping_1}
        mapping_2 = mock.MagicMock()
        interface_2 = mock.MagicMock()
        interface_2.name = "<interface 2 name>"
        interface_2.mappings = {"mapping endpoint": mapping_2}
        mock_get_all_server_owned_interfaces.return_value = [interface_1, interface_2]

        # Mocks for __send_introspection
        interface_3 = mock.MagicMock()
        interface_3.name = "<interface 3 name>"
        interface_3.version_major = "<interface 3 vers major>"
        interface_3.version_minor = "<interface 3 vers minor>"
        interface_4 = mock.MagicMock()
        interface_4.name = "<interface 4 name>"
        interface_4.version_major = "<interface 4 vers major>"
        interface_4.version_minor = "<interface 4 vers minor>"
        mock_get_all_interfaces.return_value = [interface_3, interface_4]

        # Mocks for __send_set_device_properties
        mock_load_all_props.return_value = []

        on_connected_mock = mock.MagicMock()
        device.set_events_callbacks(on_connected=on_connected_mock)
        device._DeviceMqtt__on_connect(
            None, None, flags={"session present": False}, rc=paho.mqtt.client.MQTT_ERR_SUCCESS
        )

        # Checks for __setup_subscriptions
        mock_subscribe.assert_has_calls(
            [
                mock.call("realm_name/device_id/control/consumer/properties", qos=2),
                mock.call("realm_name/device_id/<interface 1 name>/#", qos=2),
                mock.call("realm_name/device_id/<interface 2 name>/#", qos=2),
            ],
            any_order=True,
        )
        self.assertEqual(mock_subscribe.call_count, 3)
        mock_get_all_server_owned_interfaces.assert_called_once()

        # Checks for __send_introspection and __send_empty_cache
        mock_get_all_interfaces.assert_called_once()
        mock_load_all_props.assert_called_once_with()
        calls = [
            mock.call(
                "realm_name/device_id",
                "<interface 3 name>:<interface 3 vers major>:<interface 3 vers minor>;"
                "<interface 4 name>:<interface 4 vers major>:<interface 4 vers minor>",
                2,
            ),
            mock.call("realm_name/device_id/control/emptyCache", payload=b"1", retain=False, qos=2),
            mock.call(
                "realm_name/device_id/control/producer/properties",
                payload=bytearray(b"\x00\x00\x00\x00x\x9c\x03\x00\x00\x00\x00\x01"),
                retain=False,
                qos=2,
            ),
        ]
        mock_publish.assert_has_calls(calls, any_order=True)
        self.assertEqual(mock_publish.call_count, 3)

        # Callback checks
        on_connected_mock.assert_called_once_with(device)

    @mock.patch.object(Client, "publish")
    @mock.patch.object(DeviceMqtt, "_send_generic")
    @mock.patch.object(AstarteDatabaseSQLite, "delete_prop")
    @mock.patch.object(Introspection, "get_interface")
    @mock.patch.object(AstarteDatabaseSQLite, "load_all_props")
    @mock.patch.object(Introspection, "get_all_interfaces")
    @mock.patch.object(Introspection, "get_all_server_owned_interfaces")
    @mock.patch.object(Client, "subscribe")
    def test__on_connect_with_set_properties(
        self,
        mock_subscribe,
        mock_get_all_server_owned_interfaces,
        mock_get_all_interfaces,
        mock_load_all_props,
        mock_get_interface,
        mock_delete_prop,
        mock_send_generic,
        mock_publish,
    ):
        device = self.helper_initialize_device()

        # Mocks for __setup_subscriptions
        mapping_1 = mock.MagicMock()
        interface_1 = mock.MagicMock()
        interface_1.name = "<interface 1 name>"
        interface_1.mappings = {"mapping endpoint": mapping_1}
        mapping_2 = mock.MagicMock()
        interface_2 = mock.MagicMock()
        interface_2.name = "<interface 2 name>"
        interface_2.mappings = {"mapping endpoint": mapping_2}
        mock_get_all_server_owned_interfaces.return_value = [interface_1, interface_2]

        # Mocks for __send_introspection
        interface_3 = mock.MagicMock()
        interface_3.name = "<interface 3 name>"
        interface_3.version_major = "<interface 3 vers major>"
        interface_3.version_minor = "<interface 3 vers minor>"
        interface_4 = mock.MagicMock()
        interface_4.name = "<interface 4 name>"
        interface_4.version_major = "<interface 4 vers major>"
        interface_4.version_minor = "<interface 4 vers minor>"
        mock_get_all_interfaces.return_value = [interface_3, interface_4]

        # Mocks for __send_set_device_properties
        interface_5 = mock.MagicMock()
        interface_5.name = "<interface 5 name>"
        interface_5.is_server_owned.return_value = False
        interface_6 = mock.MagicMock()
        interface_6.name = "<interface 6 name>"
        interface_6.is_server_owned.return_value = True
        load_all_props_ret = [
            (interface_5.name, "", "<endpoint 1>", mock.MagicMock()),
            (interface_6.name, "", "<endpoint 2>", mock.MagicMock()),
            ("<interface 7 name>", "", "<endpoint 3>", mock.MagicMock()),
        ]
        mock_load_all_props.side_effect = lambda: (
            (yield load_all_props_ret[0]),
            (yield load_all_props_ret[1]),
            (yield load_all_props_ret[2]),
        )
        mock_get_interface.side_effect = [interface_5, interface_6, None]

        on_connected_mock = mock.MagicMock()
        device.set_events_callbacks(on_connected=on_connected_mock)
        device._DeviceMqtt__on_connect(
            None, None, flags={"session present": False}, rc=paho.mqtt.client.MQTT_ERR_SUCCESS
        )

        # Checks for __setup_subscriptions
        mock_subscribe.assert_has_calls(
            [
                mock.call("realm_name/device_id/control/consumer/properties", qos=2),
                mock.call("realm_name/device_id/<interface 1 name>/#", qos=2),
                mock.call("realm_name/device_id/<interface 2 name>/#", qos=2),
            ],
            any_order=True,
        )
        self.assertEqual(mock_subscribe.call_count, 3)
        mock_get_all_server_owned_interfaces.assert_called_once()

        # Checks for __send_introspection and __send_empty_cache
        mock_get_all_interfaces.assert_called_once()
        mock_load_all_props.assert_called_once_with()
        calls = [
            mock.call(interface_5.name),
            mock.call(interface_6.name),
            mock.call("<interface 7 name>"),
        ]
        mock_get_interface.assert_has_calls(calls)
        self.assertEqual(mock_get_interface.call_count, 3)
        interface_5.is_server_owned.assert_called_once()
        interface_6.is_server_owned.assert_called_once()
        mock_delete_prop.assert_called_once_with(load_all_props_ret[2][0], load_all_props_ret[2][2])
        mock_send_generic.assert_called_once_with(
            interface_5, load_all_props_ret[0][2], load_all_props_ret[0][3], timestamp=None
        )
        calls = [
            mock.call(
                "realm_name/device_id",
                "<interface 3 name>:<interface 3 vers major>:<interface 3 vers minor>;"
                "<interface 4 name>:<interface 4 vers major>:<interface 4 vers minor>",
                2,
            ),
            mock.call("realm_name/device_id/control/emptyCache", payload=b"1", retain=False, qos=2),
            mock.call(
                "realm_name/device_id/control/producer/properties",
                payload=bytearray(
                    b"\x1e\x00\x00\x00x\x9c\xb3\xc9\xcc+I-JKLNU0U\xc8K\xccM\xb5\xb3I\xcdK)\xc8\x07\n+\x18\xda\x01\x00\xa5\xcd\nn"
                ),
                retain=False,
                qos=2,
            ),
        ]
        mock_publish.assert_has_calls(calls)
        self.assertEqual(mock_publish.call_count, 3)

        # Callback checks
        on_connected_mock.assert_called_once_with(device)

    @mock.patch.object(Client, "publish")
    @mock.patch.object(Introspection, "get_all_interfaces")
    @mock.patch.object(Introspection, "get_all_server_owned_interfaces")
    @mock.patch.object(Client, "subscribe")
    def test__on_connect_connection_result_no_connection(
        self,
        mock_subscribe,
        mock_get_all_server_owned_interfaces,
        mock_get_all_interfaces,
        mock_publish,
    ):
        device = self.helper_initialize_device()

        on_connected_mock = mock.MagicMock()
        device.set_events_callbacks(on_connected=on_connected_mock)
        device._DeviceMqtt__on_connect(
            None, None, flags={"session present": False}, rc=paho.mqtt.client.MQTT_ERR_NO_CONN
        )

        # Checks for __setup_subscriptions
        mock_subscribe.assert_not_called()
        mock_get_all_server_owned_interfaces.assert_not_called()

        # Checks for __send_introspection and __send_empty_cache
        mock_get_all_interfaces.assert_not_called()
        mock_publish.assert_not_called()

        # Callback checks
        on_connected_mock.assert_not_called()

    @mock.patch.object(Client, "publish")
    @mock.patch.object(AstarteDatabaseSQLite, "load_all_props")
    @mock.patch.object(Introspection, "get_all_interfaces")
    @mock.patch.object(Introspection, "get_all_server_owned_interfaces")
    @mock.patch.object(Client, "subscribe")
    def test__on_connect_with_threading(
        self,
        mock_subscribe,
        mock_get_all_server_owned_interfaces,
        mock_get_all_interfaces,
        mock_load_all_props,
        mock_publish,
    ):
        device = self.helper_initialize_device()

        # Mocks for __setup_subscriptions
        mapping_1 = mock.MagicMock()
        interface_1 = mock.MagicMock()
        interface_1.name = "<interface 1 name>"
        interface_1.mappings = {"mapping endpoint": mapping_1}
        mapping_2 = mock.MagicMock()
        interface_2 = mock.MagicMock()
        interface_2.name = "<interface 2 name>"
        interface_2.mappings = {"mapping endpoint": mapping_2}
        mock_get_all_server_owned_interfaces.return_value = [interface_1, interface_2]

        # Mocks for __send_introspection
        interface_3 = mock.MagicMock()
        interface_3.name = "<interface 3 name>"
        interface_3.version_major = "<interface 3 vers major>"
        interface_3.version_minor = "<interface 3 vers minor>"
        interface_4 = mock.MagicMock()
        interface_4.name = "<interface 4 name>"
        interface_4.version_major = "<interface 4 vers major>"
        interface_4.version_minor = "<interface 4 vers minor>"
        mock_get_all_interfaces.return_value = [interface_3, interface_4]

        # Mocks for __send_set_device_properties
        mock_load_all_props.return_value = []

        on_connected_mock = mock.MagicMock()
        mock_loop = mock.MagicMock()
        device.set_events_callbacks(on_connected=on_connected_mock, loop=mock_loop)
        device._DeviceMqtt__on_connect(
            None, None, flags={"session present": False}, rc=paho.mqtt.client.MQTT_ERR_SUCCESS
        )

        # Checks for __setup_subscriptions
        mock_subscribe.assert_has_calls(
            [
                mock.call("realm_name/device_id/control/consumer/properties", qos=2),
                mock.call("realm_name/device_id/<interface 1 name>/#", qos=2),
                mock.call("realm_name/device_id/<interface 2 name>/#", qos=2),
            ],
            any_order=True,
        )
        self.assertEqual(mock_subscribe.call_count, 3)
        mock_get_all_server_owned_interfaces.assert_called_once()

        # Checks for __send_introspection and __send_empty_cache
        mock_get_all_interfaces.assert_called_once()
        mock_load_all_props.assert_called_once_with()
        calls = [
            mock.call(
                "realm_name/device_id",
                "<interface 3 name>:<interface 3 vers major>:<interface 3 vers minor>;"
                "<interface 4 name>:<interface 4 vers major>:<interface 4 vers minor>",
                2,
            ),
            mock.call("realm_name/device_id/control/emptyCache", payload=b"1", retain=False, qos=2),
            mock.call(
                "realm_name/device_id/control/producer/properties",
                payload=bytearray(b"\x00\x00\x00\x00x\x9c\x03\x00\x00\x00\x00\x01"),
                retain=False,
                qos=2,
            ),
        ]
        mock_publish.assert_has_calls(calls, any_order=True)
        self.assertEqual(mock_publish.call_count, 3)

        # Callback checks
        mock_loop.call_soon_threadsafe.assert_called_once_with(on_connected_mock, device)
        on_connected_mock.assert_not_called()

    @mock.patch.object(Client, "loop_stop")
    def test__on_disconnect_good_shutdown(self, mock_loop_stop):
        device = self.helper_initialize_device()

        on_disconnected_mock = mock.MagicMock()
        device.set_events_callbacks(on_disconnected=on_disconnected_mock)
        device._DeviceMqtt__on_disconnect(None, None, rc=paho.mqtt.client.MQTT_ERR_SUCCESS)

        on_disconnected_mock.assert_called_once_with(device, paho.mqtt.client.MQTT_ERR_SUCCESS)
        mock_loop_stop.assert_called_once()

    @mock.patch.object(Client, "loop_stop")
    def test__on_disconnect_good_shutdown_with_threading(self, mock_loop_stop):
        device = self.helper_initialize_device()

        mock_loop = mock.MagicMock()
        on_disconnected_mock = mock.MagicMock()
        device.set_events_callbacks(on_disconnected=on_disconnected_mock, loop=mock_loop)
        device._DeviceMqtt__on_disconnect(None, None, rc=paho.mqtt.client.MQTT_ERR_SUCCESS)

        on_disconnected_mock.assert_not_called()
        mock_loop.call_soon_threadsafe.assert_called_once_with(
            on_disconnected_mock, device, paho.mqtt.client.MQTT_ERR_SUCCESS
        )
        mock_loop_stop.assert_called_once()

    @mock.patch.object(DeviceMqtt, "connect")
    @mock.patch.object(Client, "loop_stop")
    @mock.patch("astarte.device.device_mqtt.crypto.certificate_is_valid", return_value=False)
    def test__on_disconnect_invalid_certificate(
        self, mock_cartificate_is_valid, mock_loop_stop, mock_connect
    ):
        device = self.helper_initialize_device()

        on_disconnected_mock = mock.MagicMock()
        device.set_events_callbacks(on_disconnected=on_disconnected_mock)
        device._DeviceMqtt__on_disconnect(None, None, rc=paho.mqtt.client.MQTT_ERR_NO_CONN)

        on_disconnected_mock.assert_called_once_with(device, paho.mqtt.client.MQTT_ERR_NO_CONN)
        mock_cartificate_is_valid.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            False,
            "./tests/device_id/crypto",
        )
        mock_loop_stop.assert_called_once()
        mock_connect.assert_called_once()

    @mock.patch.object(DeviceMqtt, "connect")
    @mock.patch.object(Client, "loop_stop")
    @mock.patch("astarte.device.device_mqtt.crypto.certificate_is_valid", return_value=True)
    def test__on_disconnect_other_reason(
        self, mock_cartificate_is_valid, mock_loop_stop, mock_connect
    ):
        device = self.helper_initialize_device()

        on_disconnected_mock = mock.MagicMock()
        device.set_events_callbacks(on_disconnected=on_disconnected_mock)
        device._DeviceMqtt__on_disconnect(None, None, rc=paho.mqtt.client.MQTT_ERR_NO_CONN)

        on_disconnected_mock.assert_called_once_with(device, paho.mqtt.client.MQTT_ERR_NO_CONN)
        mock_cartificate_is_valid.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            False,
            "./tests/device_id/crypto",
        )
        mock_loop_stop.assert_not_called()
        mock_connect.assert_not_called()

    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.loads")
    @mock.patch.object(Introspection, "get_interface")
    def test__on_message(self, mock_get_interface, mock_bson_loads, mock_db_store):
        device = self.helper_initialize_device()

        mock_bson_loads.return_value = {"v": "payload_value"}

        mock_message = mock.MagicMock()
        mock_message.topic = "realm_name/device_id/interface_name/endpoint/path"

        mock_get_interface.return_value.is_type_properties.return_value = True

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        device._DeviceMqtt__on_message(None, None, msg=mock_message)

        mock_bson_loads.assert_called_once_with(mock_message.payload)
        mock_get_interface.assert_called_once_with("interface_name")
        mock_get_interface.return_value.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_property_endpoint_resettable.assert_not_called()
        mock_get_interface.return_value.validate_path.assert_called_once_with(
            "/endpoint/path", "payload_value"
        )
        mock_get_interface.return_value.validate_payload.assert_called_once_with(
            "/endpoint/path", "payload_value"
        )
        mock_get_interface.return_value.is_type_properties.assert_called_once()
        mock_db_store.assert_called_once_with(
            mock_get_interface.return_value.name,
            mock_get_interface.return_value.version_major,
            "/endpoint/path",
            "payload_value",
        )
        on_data_received_mock.assert_called_once_with(
            device, "interface_name", "/endpoint/path", "payload_value"
        )

    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.loads")
    @mock.patch.object(Introspection, "get_interface")
    def test__on_message_with_threading(self, mock_get_interface, mock_bson_loads, mock_db_store):
        device = self.helper_initialize_device()

        mock_bson_loads.return_value = {"v": "payload_value"}

        mock_message = mock.MagicMock()
        mock_message.topic = "realm_name/device_id/interface_name/endpoint/path"

        mock_get_interface.return_value.is_type_properties.return_value = False

        mock_loop = mock.MagicMock()
        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock, loop=mock_loop)
        device._DeviceMqtt__on_message(None, None, msg=mock_message)

        mock_bson_loads.assert_called_once_with(mock_message.payload)
        mock_get_interface.assert_called_once_with("interface_name")
        mock_get_interface.return_value.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_property_endpoint_resettable.assert_not_called()
        mock_get_interface.return_value.validate_path.assert_called_once_with(
            "/endpoint/path", "payload_value"
        )
        mock_get_interface.return_value.validate_payload.assert_called_once_with(
            "/endpoint/path", "payload_value"
        )
        mock_get_interface.return_value.is_type_properties.assert_called_once()
        mock_db_store.assert_not_called()
        mock_loop.call_soon_threadsafe.assert_called_once_with(
            on_data_received_mock, device, "interface_name", "/endpoint/path", "payload_value"
        )
        on_data_received_mock.assert_not_called()

    @mock.patch.object(AstarteDatabaseSQLite, "store_prop")
    @mock.patch("astarte.device.device_mqtt.bson.loads")
    @mock.patch.object(Introspection, "get_interface")
    def test__on_message_incorrect_base_topic(
        self, mock_get_interface, mock_bson_loads, mock_db_store
    ):
        device = self.helper_initialize_device()

        mock_message = mock.MagicMock()
        mock_message.topic = "something/device_id/interface_name/endpoint/path"

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        device._DeviceMqtt__on_message(None, None, msg=mock_message)

        mock_bson_loads.assert_not_called()
        mock_get_interface.assert_not_called()
        mock_db_store.assert_not_called()
        on_data_received_mock.assert_not_called()

    @mock.patch.object(DeviceMqtt, "_DeviceMqtt__purge_server_properties")
    @mock.patch("astarte.device.device_mqtt.bson.loads")
    @mock.patch.object(Introspection, "get_interface")
    def test__on_message_control_message(
        self, mock_get_interface, mock_bson_loads, mock_purge_server
    ):
        device = self.helper_initialize_device()

        mock_message = mock.MagicMock()
        mock_message.topic = "realm_name/device_id/control/consumer/properties"

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        device._DeviceMqtt__on_message(None, None, msg=mock_message)

        mock_purge_server.assert_called_once_with(payload=mock_message.payload)
        mock_bson_loads.assert_not_called()
        mock_get_interface.assert_not_called()
        on_data_received_mock.assert_not_called()

    @mock.patch("astarte.device.device_mqtt.bson.loads")
    @mock.patch.object(Introspection, "get_interface")
    def test__on_message_no_callback(self, mock_get_interface, mock_bson_loads):
        device = self.helper_initialize_device()

        mock_message = mock.MagicMock()
        mock_message.topic = "realm_name/device_id/interface_name/endpoint/path"

        device._DeviceMqtt__on_message(None, None, msg=mock_message)

        mock_bson_loads.assert_not_called()
        mock_get_interface.assert_not_called()

    @mock.patch("astarte.device.device_mqtt.bson.loads")
    @mock.patch.object(Introspection, "get_interface")
    def test__on_message_payload_missing_value_field(self, mock_get_interface, mock_bson_loads):
        device = self.helper_initialize_device()

        mock_bson_loads.return_value = {}

        mock_message = mock.MagicMock()
        mock_message.topic = "realm_name/device_id/interface_name/endpoint/path"

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        device._DeviceMqtt__on_message(None, None, msg=mock_message)

        mock_bson_loads.assert_called_once_with(mock_message.payload)
        mock_get_interface.assert_not_called()
        mock_get_interface.return_value.is_server_owned.assert_not_called()
        mock_get_interface.return_value.is_property_endpoint_resettable.assert_not_called()
        mock_get_interface.return_value.validate_path.assert_not_called()
        mock_get_interface.return_value.validate_payload.assert_not_called()
        mock_get_interface.return_value.is_type_properties.assert_not_called()
        on_data_received_mock.assert_not_called()

    # The function __purge_server_properties is complex and gets called following a specific event
    # for this reason it will be tested in isolation
    @mock.patch.object(AstarteDatabaseSQLite, "delete_prop")
    @mock.patch.object(AstarteDatabaseSQLite, "load_all_props")
    @mock.patch.object(Introspection, "get_interface")
    def test_DeviceMqtt__purge_server_properties_empty_list(
        self, mock_get_interface, mock_load_all_props, mock_delete_prop
    ):
        device = self.helper_initialize_device()

        # Mocks for __send_set_device_properties
        interface_1 = mock.MagicMock()
        interface_1.name = "<interface 1 name>"
        interface_1.is_server_owned.return_value = False
        interface_2 = mock.MagicMock()
        interface_2.name = "<interface 2 name>"
        interface_2.is_server_owned.return_value = True
        interface_3 = mock.MagicMock()
        interface_3.name = "<interface 3 name>"
        interface_3.is_server_owned.return_value = True
        load_all_props_ret = [
            (interface_1.name, "", "<endpoint 1>", mock.MagicMock()),
            (interface_2.name, "", "<endpoint 2>", mock.MagicMock()),
            (interface_3.name, "", "<endpoint 3>", mock.MagicMock()),
        ]
        mock_load_all_props.side_effect = lambda: (
            (yield load_all_props_ret[0]),
            (yield load_all_props_ret[1]),
            (yield load_all_props_ret[2]),
        )
        mock_get_interface.side_effect = [interface_1, interface_2, interface_3]

        base_payload = b"\x00\x00\x00\x00x\x9c\x03\x00\x00\x00\x00\x01"
        device._DeviceMqtt__purge_server_properties(base_payload)

        calls = [
            mock.call(interface_1.name),
            mock.call(interface_2.name),
            mock.call(interface_3.name),
        ]
        mock_get_interface.assert_has_calls(calls)
        self.assertEqual(mock_get_interface.call_count, 3)

        calls = [
            mock.call(interface_2.name, "<endpoint 2>"),
            mock.call(interface_3.name, "<endpoint 3>"),
        ]
        mock_delete_prop.assert_has_calls(calls)
        self.assertEqual(mock_delete_prop.call_count, 2)

    # The function __purge_server_properties is complex and gets called following a specific event
    # for this reason it will be tested in isolation
    @mock.patch.object(AstarteDatabaseSQLite, "delete_prop")
    @mock.patch.object(AstarteDatabaseSQLite, "load_all_props")
    @mock.patch.object(Introspection, "get_interface")
    def test_DeviceMqtt__purge_server_properties_non_empty_list(
        self, mock_get_interface, mock_load_all_props, mock_delete_prop
    ):
        device = self.helper_initialize_device()

        # Mocks for __send_set_device_properties
        interface_1 = mock.MagicMock()
        interface_1.name = "<interface 1>"
        interface_1.is_server_owned.return_value = False
        interface_2 = mock.MagicMock()
        interface_2.name = "<interface 2>"
        interface_2.is_server_owned.return_value = True
        interface_3 = mock.MagicMock()
        interface_3.name = "<interface 3>"
        interface_3.is_server_owned.return_value = True
        load_all_props_ret = [
            (interface_1.name, "", "/endpoint/path1", mock.MagicMock()),
            (interface_2.name, "", "/endpoint/path2", mock.MagicMock()),
            (interface_3.name, "", "/endpoint/path3", mock.MagicMock()),
        ]
        mock_load_all_props.side_effect = lambda: (
            (yield load_all_props_ret[0]),
            (yield load_all_props_ret[1]),
            (yield load_all_props_ret[2]),
        )
        mock_get_interface.side_effect = [interface_2, None, interface_1, interface_2, interface_3]

        base_payload = b"9\x00\x00\x00x\x9c\xb3\xc9\xcc+I-JKLNU0\xb2\xd3O\xcdK)\xc8\x07\x8a\xe8\x17$\x96d\x18Y\xdb $\x8d\xd1$\x8d\x01P\xfd\x14t"
        device._DeviceMqtt__purge_server_properties(base_payload)

        calls = [
            mock.call(interface_1.name),
            mock.call(interface_2.name),
            mock.call(interface_3.name),
        ]
        mock_get_interface.assert_has_calls(calls)
        self.assertEqual(mock_get_interface.call_count, 5)

        calls = [
            mock.call(interface_3.name, "/endpoint/path3"),
        ]
        mock_delete_prop.assert_has_calls(calls)
        self.assertEqual(mock_delete_prop.call_count, 1)

    # The function __purge_server_properties is complex and gets called following a specific event
    # for this reason it will be tested in isolation
    @mock.patch.object(AstarteDatabaseSQLite, "delete_prop")
    @mock.patch.object(AstarteDatabaseSQLite, "load_all_props")
    @mock.patch.object(Introspection, "get_interface")
    def test_DeviceMqtt__purge_server_properties_interface_not_in_introspection(
        self, mock_get_interface, mock_load_all_props, mock_delete_prop
    ):
        device = self.helper_initialize_device()

        # Mocks for __send_set_device_properties
        interface_1 = mock.MagicMock()
        interface_1.name = "<interface 1 name>"
        interface_1.is_server_owned.return_value = False
        interface_2 = mock.MagicMock()
        interface_2.name = "<interface 2 name>"
        interface_2.is_server_owned.return_value = True
        interface_3 = mock.MagicMock()
        interface_3.name = "<interface 3 name>"
        interface_3.is_server_owned.return_value = True
        load_all_props_ret = [
            (interface_1.name, "", "<endpoint 1>", mock.MagicMock()),
            (interface_2.name, "", "<endpoint 2>", mock.MagicMock()),
            (interface_3.name, "", "<endpoint 3>", mock.MagicMock()),
        ]
        mock_load_all_props.side_effect = lambda: (
            (yield load_all_props_ret[0]),
            (yield load_all_props_ret[1]),
            (yield load_all_props_ret[2]),
        )
        mock_get_interface.side_effect = [interface_1, None, interface_3]

        base_payload = b"\x00\x00\x00\x00x\x9c\x03\x00\x00\x00\x00\x01"
        device._DeviceMqtt__purge_server_properties(base_payload)

        calls = [
            mock.call(interface_1.name),
            mock.call(interface_2.name),
            mock.call(interface_3.name),
        ]
        mock_get_interface.assert_has_calls(calls)
        self.assertEqual(mock_get_interface.call_count, 3)

        calls = [
            mock.call(interface_2.name, "<endpoint 2>"),
            mock.call(interface_3.name, "<endpoint 3>"),
        ]
        mock_delete_prop.assert_has_calls(calls)
        self.assertEqual(mock_delete_prop.call_count, 2)
