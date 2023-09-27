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

import logging
import asyncio
import collections.abc
import os
import ssl
import json
import zlib
from pathlib import Path
from collections.abc import Callable
from datetime import datetime
from urllib.parse import urlparse

import bson
import paho.mqtt.client as mqtt
from astarte.device import crypto, pairing_handler
from astarte.device.interface import Interface
from astarte.device.introspection import Introspection
from astarte.device.database import AstarteDatabaseSQLite, AstarteDatabase
from astarte.device.exceptions import (
    ValidationError,
    PersistencyDirectoryNotFoundError,
    InterfaceFileNotFoundError,
    InterfaceFileDecodeError,
    InterfaceNotFoundError,
)


class Device:  # pylint: disable=too-many-instance-attributes
    """
    Basic class to define an Astarte Device.

    Device represents an Astarte Device. It is the base class used for managing the Device
    lifecycle and data. Users should instantiate a Device with the right credentials and connect
    it to the configured instance to start working with it.

    **Threading and Concurrency**

    This SDK uses paho-mqtt under the hood to provide Transport connectivity. As such,
    it is bound by paho-mqtt's behaviors in terms of threading. When a Device connects,
    a new thread is spawned and an event loop is run there to manage all the connection events.

    This SDK spares the user from this detail - on the other hand, when configuring callbacks,
    threading has to be taken into account. When creating a Device, it is possible to specify an
    asyncio.loop() to automatically manage this detail. When a loop is specified, all callbacks
    will be called in the context of that loop, guaranteeing thread-safety and making sure that
    the user does not have to take any further action beyond consuming the callback.

    When a loop is not specified, callbacks are invoked just as standard Python functions. This
    inevitably means that the user will have to take into account the fact that the callback will
    be invoked in the Thread of the MQTT connection. In particular, blocking the execution of
    that thread might cause deadlocks and, in general, malfunctions in the SDK. For this reason, the
    usage of asyncio is strongly recommended.

    Attributes
    ----------
    on_connected : Callable[[Device], None]
        A function that will be invoked everytime the device successfully connects.
    on_disconnected : Callable[[Device, int], None]
        A function that will be invoked everytime the device disconnects. The int parameter bears
        the disconnect reason.
    on_data_received : Callable[[Device, string, string, object], None]
        A function that will be invoked everytime data is received from Astarte. Parameters are
        the Device itself, the Interface name, the Interface path, and the payload. The payload
        will reflect the type defined in the Interface.
    """

    def __init__(
        self,
        device_id: str,
        realm: str,
        credentials_secret: str,
        pairing_base_url: str,
        persistency_dir: str,
        database: AstarteDatabase | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
        ignore_ssl_errors: bool = False,
    ):
        """
        Parameters
        ----------
        device_id : str
            The Device ID for this Device. It has to be a valid Astarte Device ID.
        realm : str
            The Realm this Device will be connecting against.
        credentials_secret : str
            The Credentials Secret for this Device. The Device class assumes your Device has
            already been registered - if that is not the case, you can use either
            :py:func:`register_device_with_jwt_token` or
            :py:func:`register_device_with_private_key`.
        pairing_base_url : str
            The Base URL of Pairing API of the Astarte Instance the Device will connect to.
        persistency_dir : str
            Path to an existing directory which will be used for holding persistency for this
            device: certificates, caching and more. It doesn't have to be unique per device,
            a subdirectory for the given Device ID will be created.
        database : AstarteDatabase (optional)
            User instantiated database to use for caching properties. When None, a native SQLite
            database will be created and used in the persistency_dir.
        loop : asyncio.loop (optional)
            An optional loop which will be used for invoking callbacks. When this is not none,
            Device will call any specified callback through loop.call_soon_threadsafe, ensuring
            that the callbacks will be run in thread the loop belongs to. Usually, you want
            to set this to get_running_loop(). When not sent, callbacks will be invoked as a
            standard function - keep in mind this means your callbacks might create deadlocks.
        ignore_ssl_errors: bool (optional)
            Useful if you're using the Device to connect to a test instance of Astarte with
            self-signed certificates, it is not recommended to leave this `true` in production.
            Defaults to `false`, if `true` the device will ignore SSL errors during connection.
        Raises
        ------
        PersistencyDirectoryNotFoundError
            If the provided persistency directory does not exists.
        """
        if not os.path.isdir(persistency_dir):
            raise PersistencyDirectoryNotFoundError(f"{persistency_dir} is not a directory")

        if not os.path.isdir(os.path.join(persistency_dir, device_id)):
            os.mkdir(os.path.join(persistency_dir, device_id))

        crypto_dir = os.path.join(persistency_dir, device_id, "crypto")
        if not os.path.isdir(crypto_dir):
            os.mkdir(crypto_dir)

        caching_dir = os.path.join(persistency_dir, device_id, "caching")
        if not database and not os.path.isdir(caching_dir):
            os.mkdir(caching_dir)

        # Define private and public attributes
        self.__device_id = device_id
        self.__realm = realm
        self.__pairing_base_url = pairing_base_url
        self.__crypto_dir = crypto_dir
        self.__prop_database = (
            database
            if database
            else AstarteDatabaseSQLite(Path(os.path.join(caching_dir, "astarte.db")))
        )
        self.__credentials_secret = credentials_secret
        # TODO: Implement device registration using token on connect
        # self.__jwt_token: str | None = None
        self.__is_crypto_setup = False
        self.__introspection = Introspection()
        self.__is_connected = False
        self.__loop = loop
        self.__ignore_ssl_errors = ignore_ssl_errors

        self.on_connected: Callable[Device, None] | None = None
        self.on_disconnected: Callable[[Device, int], None] | None = None
        self.on_data_received: Callable[[Device, str, str, object], None] | None = None

        self.__setup_mqtt_client()

    def __setup_mqtt_client(self) -> None:
        """
        Utility function used to setup an MQTT client
        """
        self.__mqtt_client = mqtt.Client()
        self.__mqtt_client.on_connect = self.__on_connect
        self.__mqtt_client.on_disconnect = self.__on_disconnect
        self.__mqtt_client.on_message = self.__on_message

    def add_interface(self, interface_definition: dict) -> None:
        """
        Adds an Interface to the Device

        This will add an Interface definition to the Device. It has to be called before
        :py:func:`connect`, as it will be used for building the Device Introspection.

        Parameters
        ----------
        interface_definition : dict
            An Astarte Interface definition in the form of a Python dictionary. Usually obtained
            by using json.loads() on an Interface file.
        """
        self.__introspection.add_interface(interface_definition)

    def add_interface_from_file(self, interface_file: Path):
        """
        Adds an interface to the device

        This will add an interface definition to the device. It has to be called before
        :py:func:`connect`, as it will be used for building the device introspection.

        Parameters
        ----------
        interface_file : Path
            An absolute path to an Astarte interface json file.
        Raises
        ------
        InterfaceFileNotFoundError
            If the provided interface file does not exists.
        InterfaceFileDecodeError
            If speficied file is not a parsable .json file.
        """
        if not interface_file.is_file():
            raise InterfaceFileNotFoundError(f'"{interface_file}" does not exist or is not a file')
        try:
            with open(interface_file, "r", encoding="utf-8") as interface_fp:
                self.__introspection.add_interface(json.load(interface_fp))
        except json.JSONDecodeError as exc:
            raise InterfaceFileDecodeError(
                f'"{interface_file}" is not a parsable json file'
            ) from exc

    def add_interfaces_from_dir(self, interfaces_dir: Path):
        """
        Adds a series of interfaces to the device

        This will add all the interfaces contained in the provided folder to the device.
        It has to be called before :py:func:`connect`, as it will be used for building the device
        introspection.

        Parameters
        ----------
        interfaces_dir : Path
            An absolute path to an a folder containing some Astarte interface .json files.
        Raises
        ------
        InterfaceFileNotFoundError
            If specified directory does not exists.
        """
        if not interfaces_dir.exists():
            raise InterfaceFileNotFoundError(f'"{interfaces_dir}" does not exist')
        if not interfaces_dir.is_dir():
            raise InterfaceFileNotFoundError(f'"{interfaces_dir}" is not a directory')
        for interface_file in [i for i in interfaces_dir.iterdir() if i.suffix == ".json"]:
            self.add_interface_from_file(interface_file)

    def remove_interface(self, interface_name: str) -> None:
        """
        Removes an Interface from the Device

        Removes an Interface definition from the Device. It has to be called before
        :py:func:`connect`, as it will be used for building the Device Introspection.

        Parameters
        ----------
        interface_name : str
            The name of an Interface previously added with :py:func:`add_interface`.
        """
        self.__introspection.remove_interface(interface_name)

    def get_device_id(self) -> str:
        """
        Returns the Device ID of the Device.

        Returns
        -------
        str
            The Id of the device
        """
        return self.__device_id

    def __setup_crypto(self) -> None:
        """
        Utility function used to setup cytptography
        """
        if self.__is_crypto_setup:
            return

        if not crypto.device_has_certificate(self.__crypto_dir):
            pairing_handler.obtain_device_certificate(
                self.__device_id,
                self.__realm,
                self.__credentials_secret,
                self.__pairing_base_url,
                self.__crypto_dir,
                self.__ignore_ssl_errors,
            )
        # Initialize MQTT Client
        if self.__ignore_ssl_errors:
            cert_reqs = ssl.CERT_NONE
        else:
            cert_reqs = ssl.CERT_REQUIRED

        self.__mqtt_client.tls_set(
            ca_certs=None,
            certfile=os.path.join(self.__crypto_dir, "device.crt"),
            keyfile=os.path.join(self.__crypto_dir, "device.key"),
            cert_reqs=cert_reqs,
            tls_version=ssl.PROTOCOL_TLS,
            ciphers=None,
        )
        self.__mqtt_client.tls_insecure_set(self.__ignore_ssl_errors)

        self.__is_crypto_setup = True

    def connect(self) -> None:
        """
        Connects the Device asynchronously.

        When calling connect, a new connection thread is spawned and the Device will start a
        connection routine. The function might return before the Device connects: you want to
        use the on_connected callback to ensure you are notified upon connection.

        In case the Device gets disconnected unexpectedly, it will try to reconnect indefinitely
        until disconnect() is called.
        """
        if self.__is_connected:
            return

        self.__setup_crypto()

        transport_info = pairing_handler.obtain_device_transport_information(
            self.__device_id,
            self.__realm,
            self.__credentials_secret,
            self.__pairing_base_url,
            self.__ignore_ssl_errors,
        )
        broker_url = ""
        # We support only MQTTv1
        for transport, transport_data in transport_info["protocols"].items():
            if transport != "astarte_mqtt_v1":
                continue
            # Get the Broker URL
            broker_url = transport_data["broker_url"]

        # Grab the URL components we care about
        parsed_url = urlparse(broker_url)

        self.__mqtt_client.connect_async(parsed_url.hostname, parsed_url.port)
        self.__mqtt_client.loop_start()

    def disconnect(self) -> None:
        """
        Disconnects the Device asynchronously.

        When calling disconnect, the connection thread is requested to terminate the
        disconnection, and the thread is stopped when the disconnection happens.
        The function might return before the Device connects: you want to use the on_disconnected
        callback to ensure you are notified upon connection. When doing so, check the return
        code parameter: if it is 0, it means the disconnection happened following an explicit
        disconnection request.
        """
        if not self.__is_connected:
            return

        self.__mqtt_client.disconnect()
        self.__mqtt_client.loop_stop(force=False)

    def is_connected(self) -> bool:
        """
        Returns whether the Device is currently connected.

        Returns
        -------
        bool
            The device connection status.
        """
        return self.__is_connected

    def send(
        self,
        interface_name: str,
        interface_path: str,
        payload: object,
        timestamp: datetime | None = None,
    ) -> None:
        """
        Sends an individual message to an interface.

        Parameters
        ----------
        interface_name : str
            The name of an the Interface to send data to.
        interface_path : str
            The path on the Interface to send data to.
        payload : object
            The value to be sent. The type should be compatible to the one specified in the
            interface path.
        timestamp : datetime, optional
            If sending a Datastream with explicit_timestamp, you can specify a datetime object
            which will be registered as the timestamp for the value.

        Raises
        ------
        InterfaceNotFoundError
            If the interface is not declared in the introspection
        ValidationError
            If the interface or the payload are not compatible.
        """
        interface = self.__introspection.get_interface(interface_name)
        if not interface:
            raise InterfaceNotFoundError(
                f"Interface {interface_name} not declared in introspection"
            )
        if interface.is_aggregation_object():
            raise ValidationError(
                f"Interface {interface_name} is an aggregate interface. You should use "
                f"send_aggregate."
            )

        if isinstance(payload, collections.abc.Mapping):
            raise ValidationError("Payload for individual interfaces should not be a dictionary")

        self.__send_generic(
            interface,
            interface_path,
            payload,
            timestamp,
        )

    def send_aggregate(
        self,
        interface_name: str,
        interface_path: str,
        payload: collections.abc.Mapping,
        timestamp: datetime | None = None,
    ) -> None:
        """
        Sends an aggregate message to an interface

        Parameters
        ----------
        interface_name : str
            The name of the Interface to send data to.
        interface_path: str
            The endpoint to send the data to
        payload : dict
            A dictionary containing the path:value map for the aggregate.
        timestamp : datetime, optional
            If the Datastream has explicit_timestamp, you can specify a datetime object which
            will be registered as the timestamp for the value.

        Raises
        ------
        InterfaceNotFoundError
            If the interface is not declared in the introspection
        ValidationError
            If the interface or the payload are not compatible.
        """
        interface = self.__introspection.get_interface(interface_name)
        if not interface:
            raise InterfaceNotFoundError(
                f"Interface {interface_name} not declared in introspection"
            )
        if not interface.is_aggregation_object():
            raise ValidationError(
                f"Interface {interface_name} is not an aggregate interface. You should use send."
            )

        if not isinstance(payload, collections.abc.Mapping):
            raise ValidationError("Payload for aggregate interfaces should be a dictionary")

        self.__send_generic(
            interface,
            interface_path,
            payload,
            timestamp,
        )

    def unset_property(self, interface_name: str, interface_path: str) -> None:
        """
        Unset the specified property on an interface.

        Parameters
        ----------
        interface_name : str
            The name of the Interface where the property to unset is located.
        interface_path : str
            The path on the Interface to unset.

        Raises
        ------
        InterfaceNotFoundError
            If the interface is not declared in the introspection
        ValidationError
            If the interface is of type datastream.
        """

        interface = self.__introspection.get_interface(interface_name)
        if not interface:
            raise InterfaceNotFoundError(
                f"Interface {interface_name} not declared in introspection"
            )
        if not interface.is_type_properties():
            raise ValidationError(
                f"Interface {interface_name} is a datastream interface. You can only unset a "
                f"property."
            )

        self.__send_generic(
            interface,
            interface_path,
            None,
            None,
        )

    def __send_generic(
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
        # Check the interface has device ownership
        if interface.is_server_owned():
            raise ValidationError(f"The interface {interface.name} is not owned by the device.")

        bson_payload = b""
        if payload is not None:
            validation_result = interface.validate(path, payload, timestamp)
            if validation_result:
                raise validation_result

            object_payload = {"v": payload}
            if timestamp:
                object_payload["t"] = timestamp
            bson_payload = bson.dumps(object_payload)
        elif not interface.get_mapping(path):
            raise ValidationError(f"Path {path} not in the {interface.name} interface.")

        if interface.is_type_properties():
            self.__prop_database.store_prop(
                interface.name,
                interface.version_major,
                path,
                payload,
            )

        self.__mqtt_client.publish(
            f"{self.__get_base_topic()}/{interface.name}{path}",
            bson_payload,
            qos=interface.get_reliability(path),
        )

    def __get_base_topic(self) -> str:
        """
        Utility function that returns the composition between realm and device id as often used
        in astarte API URLs

        Returns
        -------
        str
            The composition between realm and device id as used in Astarte API URLs
        """
        return f"{self.__realm}/{self.__device_id}"

    def __on_connect(self, _client, _userdata, flags: dict, rc):
        """
        Callback function for MQTT connection

        Parameters
        ----------
        flags: dict
            it contains response flags from the broker:
            flags['session present'] - this flag is only useful for clients that are using
            [clean session] set to 0. If a client with [clean session] = 0 reconnects to a broker
            to which it has been connected previously, this flag indicates whether the broker still
            has the session information of the client. If 1, the session still exists.
        rc: int
            the connection result

        """
        if rc:
            logging.error("Connection failed! %s", rc)
            return

        self.__is_connected = True

        if not flags["session present"]:
            logging.debug("Session flag is not present, performing a clean session procedure")
            self.__setup_subscriptions()
            self.__send_introspection()
            self.__send_empty_cache()
            self.__send_device_owned_properties()

        if self.on_connected:
            if self.__loop:
                # Use threadsafe, as we're in a different thread here
                self.__loop.call_soon_threadsafe(self.on_connected, self)
            else:
                self.on_connected(self)

    def __on_disconnect(self, _client, _userdata, rc):
        """
        Callback function for MQTT disconnection

        Parameters
        ----------
        rc: int
            the disconnection result
            The rc parameter indicates the disconnection state. If
            MQTT_ERR_SUCCESS (0), the callback was called in response to
            a disconnect() call. If any other value the disconnection
            was unexpected, such as might be caused by a network error.

        Returns
        -------

        """
        self.__is_connected = False

        if self.on_disconnected:
            if self.__loop:
                # Use threadsafe, as we're in a different thread here
                self.__loop.call_soon_threadsafe(self.on_disconnected, self, rc)
            else:
                self.on_disconnected(self, rc)

        # If rc was explicit, stop the loop (after the callback)
        if not rc:
            self.__mqtt_client.loop_stop()
        # Else check certificate expiration and try reconnection
        # TODO: check for MQTT_ERR_TLS when Paho correctly returns it
        elif not crypto.certificate_is_valid(self.__crypto_dir):
            self.__mqtt_client.loop_stop()
            # If the certificate must be regenerated, old mqtt client is no longer valid as it is
            # bound to the wrong TLS params and paho does not allow to replace them a second time
            self.__setup_mqtt_client()
            self.connect()

    def __on_message(self, _client, _userdata, msg):
        """
        Callback function for MQTT data received

        Parameters
        ----------
        msg: paho.mqtt.MQTTMessage
            an instance of MQTTMessage.
            This is a class with members topic, payload, qos, retain.

        Returns
        -------

        """
        # Check correct base topic
        if not msg.topic.startswith(self.__get_base_topic()):
            logging.warning("Received unexpected message on topic %s, %s", msg.topic, msg.payload)
            return

        # Parse control message in a separate function
        if msg.topic == f"{self.__get_base_topic()}/control/consumer/properties":
            logging.info("Received purge properties control message.")
            self.__purge_server_properties(payload=msg.payload)
            return

        # Check if callback is set
        if not self.on_data_received:
            return

        # Get interface name and path
        topic_tokens = msg.topic.replace(f"{self.__get_base_topic()}/", "").split("/")
        interface_name = topic_tokens[0]
        interface_path = "/" + "/".join(topic_tokens[1:])

        # Check if interface name is correct
        interface = self.__introspection.get_interface(interface_name)
        if not interface:
            logging.warning(
                "Received unexpected message for unregistered interface %s: %s, %s",
                interface_name,
                msg.topic,
                msg.payload,
            )
            return

        # Check over ownership of the interface
        if not interface.is_server_owned():
            logging.warning(
                "Received unexpected message for device owned interface %s: %s, %s",
                interface_name,
                msg.topic,
                msg.payload,
            )
            return

        # Extract payload from BSON
        data_payload = None
        if msg.payload:
            payload_object = bson.loads(msg.payload)
            if "v" not in payload_object:
                logging.warning(
                    "Received unexpected BSON Object on topic %s, %s", msg.topic, payload_object
                )
                return
            data_payload = payload_object["v"]
        # Ensure that an empty payload is only for resettable properties
        elif not interface.is_property_endpoint_resettable(interface_path):
            logging.warning(
                "Received empty payload for non property interface %s or non resettable %s endpoint",
                interface_name,
                interface_path,
            )
            return

        # Check the received path corresponds to the one in the interface
        if interface.validate_path(interface_path, data_payload):
            logging.warning(
                "Received message on incorrect endpoint for interface %s: %s, %s",
                interface_name,
                msg.topic,
                msg.payload,
            )
            return

        # Check the payload matches with the interface
        if data_payload:
            if interface.validate_payload(interface_path, data_payload):
                logging.warning(
                    "Received incompatible payload for interface %s: %s, %s",
                    interface_name,
                    msg.topic,
                    payload_object,
                )
                return

        # For properties, store them in the properties database
        if interface.is_type_properties():
            self.__prop_database.store_prop(
                interface.name, interface.version_major, interface_path, data_payload
            )

        if self.__loop:
            # Use threadsafe, as we're in a different thread here
            self.__loop.call_soon_threadsafe(
                self.on_data_received,
                self,
                interface_name,
                interface_path,
                data_payload,
            )
        else:
            self.on_data_received(self, interface_name, interface_path, data_payload)

    def __setup_subscriptions(self) -> None:
        """
        Utility function used to subscribe to the server owned interfaces
        """
        self.__mqtt_client.subscribe(
            f"{self.__get_base_topic()}/control/consumer/properties", qos=2
        )
        for interface in self.__introspection.get_all_server_owned_interfaces():
            self.__mqtt_client.subscribe(f"{self.__get_base_topic()}/{interface.name}/#", qos=2)

    def __send_introspection(self) -> None:
        """
        Utility function used to send the introspection to Astarte
        """

        # Build the introspection message
        introspection_message = ""
        for interface in self.__introspection.get_all_interfaces():
            introspection_message += (
                f"{interface.name}:{interface.version_major}:{interface.version_minor};"
            )
        introspection_message = introspection_message[:-1]
        self.__mqtt_client.publish(self.__get_base_topic(), introspection_message, 2)

    def __send_empty_cache(self) -> None:
        """
        Utility function used to send the "empty cache" message to Astarte
        """
        self.__mqtt_client.publish(
            f"{self.__get_base_topic()}/control/emptyCache",
            payload=b"1",
            retain=False,
            qos=2,
        )

    def __send_device_owned_properties(self) -> None:
        """
        Utility function used to send all the device properties present in the database to Astarte.
        It also sends the purge properties message to Astarte.
        """
        interfaces_list = []
        for interface_name, _, interface_path, value in self.__prop_database.load_all_props():
            interface = self.__introspection.get_interface(interface_name)
            if not interface:
                self.__prop_database.delete_prop(interface_name, interface_path)
            elif not interface.is_server_owned():
                self.__send_generic(interface, interface_path, value, timestamp=None)
                interfaces_list += [interface_name + interface_path]
        interfaces_str = ";".join(interfaces_list)
        payload = bytearray(len(interfaces_str).to_bytes(4, byteorder="little"))
        payload.extend(zlib.compress(interfaces_str.encode("utf-8")))

        self.__mqtt_client.publish(
            f"{self.__get_base_topic()}/control/producer/properties",
            payload=payload,
            retain=False,
            qos=2,
        )

    def __purge_server_properties(self, payload) -> None:
        """
        Purges the server owned properties not contained in the payload.

        Parameters
        ----------
        payload : str
            The purge properties message payload, contains a list of properties to save from
            purging.
        """
        allowed_properties = []
        decompressed_payload = zlib.decompress(payload[4:]).decode("utf-8")
        if decompressed_payload:
            # Parse the received list of set properties.
            for full_path in [p.split("/") for p in decompressed_payload.split(";")]:
                interface_name = full_path[0]
                if not self.__introspection.get_interface(interface_name):
                    logging.debug("Purge list entry %s missing from introspection.", interface_name)
                    continue
                allowed_properties.append((interface_name, "/" + "/".join(full_path[1:])))

        # Delete all the properties not in the received list.
        for interface_name, _, interface_path, _ in self.__prop_database.load_all_props():
            if (
                self.__introspection.get_interface(interface_name).is_server_owned()
                and (interface_name, interface_path) not in allowed_properties
            ):
                logging.debug("Removing the property at: %s/%s.", interface_name, interface_path)
                self.__prop_database.delete_prop(interface_name, interface_path)
