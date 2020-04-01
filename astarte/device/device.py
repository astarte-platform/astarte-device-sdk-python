# Copyright 2020 Ispirata S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import asyncio
import collections.abc
import os
import bson
import paho.mqtt.client as mqtt
import ssl
from urllib.parse import urlparse
from . import crypto, pairing_handler


class Device:
    """
    Basic class to define an Astarte Device.

    Device represents an Astarte Device. It is the base class used for managing the Device lifecycle and data.
    Users should instantiate a Device with the right credentials and connect it to the configured instance to
    start working with it.

    Threading and Concurrency
    ----------
    This SDK uses paho-mqtt under the hood to provide Transport connectivity. As such, it is bound by paho-mqtt's behaviors
    in terms of threading. When a Device connects, a new thread is spawned and an event loop is run there to manage all the
    connection events.

    This SDK spares the user from this detail - on the other hand, when configuring callbacks, threading has to be taken into
    account. When creating a Device, it is possible to specify an asyncio.loop to automatically manage this detail. When a loop
    is specified, all callbacks will be called in the context of that loop, guaranteeing thread-safety and making sure that
    the user does not have to take any further action beyond consuming the callback.

    When a loop is not specified, callbacks are invoked just as standard Python functions. This inevitably means that the user
    will have to take into account the fact that the callback will be invoked in the Thread of the MQTT connection. In particular,
    blocking the execution of that thread might cause deadlocks and, in general, malfunctions in the SDK. For this reason, the
    usage of asyncio is strongly recommended.

    Attributes
    ----------
    on_connected : Callable[[Device], None]
        A function that will be invoked everytime the device successfully connects.
    on_disconnected : Callable[[Device, int], None]
        A function that will be invoked everytime the device disconnects. The int parameter bears the disconnect reason.
    on_data_received : Callable[[Device, string, string, object], None]
        A function that will be invoked everytime data is received from Astarte. Parameters are the Device itself, the Interface
        name, the Interface path, and the payload. The payload will reflect the type defined in the Interface.
    """

    def __init__(self,
                 device_id,
                 realm,
                 credentials_secret,
                 pairing_base_url,
                 persistency_dir,
                 loop=None):
        """
        Parameters
        ----------
        device_id : str
            The Device ID for this Device. It has to be a valid Astarte Device ID.
        realm : str
            The Realm this Device will be connecting against.
        credentials_secret : str
            The Credentials Secret for this Device. The Device class assumes your Device has already been registered - if that
            is not the case, you can use either :py:func:`register_device_with_jwt_token` or :py:func:`register_device_with_private_key`.
        pairing_base_url : str
            The Base URL of Pairing API of the Astarte Instance the Device will connect to.
        persistency_dir : str
            Path to an existing directory which will be used for holding persistency for this device: certificates, caching and more.
            It doesn't have to be unique per device, a subdirectory for the given Device ID will be created.
        loop : asyncio.loop, optional
            An optional loop which will be used for invoking callbacks. When this is not none, Device will call any specified callback
            through loop.call_soon_threadsafe, ensuring that the callbacks will be run in thread the loop belongs to. Usually, you want
            to set this to get_running_loop(). When not sent, callbacks will be invoked as a standard function - keep in mind this means
            your callbacks might create deadlocks.
        """
        self.__device_id = device_id
        self.__realm = realm
        self.__pairing_base_url = pairing_base_url
        self.__persistency_dir = persistency_dir
        self.__credentials_secret = credentials_secret
        self.__jwt_token = None
        self.__mqtt_client = mqtt.Client()
        self.__is_crypto_setup = False
        self.__interfaces = {}
        self.__is_connected = False
        self.__loop = loop

        self.on_connected = None
        self.on_disconnected = None
        self.on_aggregate_data_received = None
        self.on_data_received = None

        # Check if the persistency dir exists
        if not os.path.isdir(persistency_dir):
            raise FileNotFoundError(f'{persistency_dir} is not a directory')

        if not os.path.isdir(os.path.join(persistency_dir, device_id)):
            os.mkdir(os.path.join(persistency_dir, device_id))

        if not os.path.isdir(os.path.join(persistency_dir, device_id,
                                          "crypto")):
            os.mkdir(os.path.join(persistency_dir, device_id, "crypto"))
        self.__mqtt_client.on_connect = self.__on_connect
        self.__mqtt_client.on_disconnect = self.__on_disconnect
        self.__mqtt_client.on_message = self.__on_message

    def add_interface(self, interface_definition):
        """
        Adds an Interface to the Device

        This will add an Interface definition to the Device. It has to be called before :py:func:`connect`, as it will be
        used for building the Device Introspection.
        
        Parameters
        ----------
        interface_definition : dict
            An Astarte Interface definition in the form of a Python dictionary. Usually obtained by using json.loads on an Interface file.
        """
        self.__interfaces[
            interface_definition["interface_name"]] = interface_definition

    def remove_interface(self, interface_name):
        """
        Removes an Interface from the Device

        Removes an Interface definition from the Device. It has to be called before :py:func:`connect`, as it will be
        used for building the Device Introspection.
        
        Parameters
        ----------
        interface_name : dict
            The name of an Interface previously added with :py:func:`add_interface`.
        """
        if interface_name in self.__interfaces:
            del self.__interfaces[interface_name]

    def get_device_id(self) -> str:
        """
        Returns the Device ID of the Device.
        """
        return self.__device_id

    def __setup_crypto(self):
        if self.__is_crypto_setup:
            return

        if not crypto.device_has_certificate(
                os.path.join(self.__persistency_dir, self.__device_id,
                             "crypto")):
            pairing_handler.obtain_device_certificate(
                self.__device_id, self.__realm, self.__credentials_secret,
                self.__pairing_base_url,
                os.path.join(self.__persistency_dir, self.__device_id,
                             "crypto"))
        # Initialize MQTT Client
        self.__mqtt_client.tls_set(
            ca_certs=None,
            certfile=os.path.join(self.__persistency_dir, self.__device_id,
                                  "crypto", "device.crt"),
            keyfile=os.path.join(self.__persistency_dir, self.__device_id,
                                 "crypto", "device.key"),
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS,
            ciphers=None)

    def connect(self):
        """
        Connects the Device asynchronously.

        When calling connect, a new connection thread is spawned and the Device will start a connection routine.
        The function might return before the Device connects: you want to use the on_connected callback to ensure
        you are notified upon connection.

        In case the Device gets disconnected unexpectedly, it will try to reconnect indefinitely until disconnect()
        is called.
        """
        if self.__is_connected:
            return

        if not self.__is_crypto_setup:
            self.__setup_crypto()

        transport_info = pairing_handler.obtain_device_transport_information(
            self.__device_id, self.__realm, self.__credentials_secret,
            self.__pairing_base_url)
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

    def disconnect(self):
        """
        Disconnects the Device asynchronously.

        When calling disconnect, the connection thread is requested to terminate the disconnection, and the thread is stopped
        when the disconnection happens.
        The function might return before the Device connects: you want to use the on_disconnected callback to ensure
        you are notified upon connection. When doing so, check the return code parameter: if it is 0, it means the disconnection
        happened following an explicit disconnection request.
        """
        if not self.__is_connected:
            return

        self.__mqtt_client.disconnect()

    def is_connected(self) -> bool:
        """
        Returns whether the Device is currently connected.
        """
        return self.__is_connected

    def send(self, interface_name, interface_path, payload, timestamp=None):
        """
        Sends an individual message to an interface.

        Parameters
        ----------
        interface_name : str
            The name of an the Interface to send data to.
        interface_path : str
            The path on the Interface to send data to.
        payload : object
            The value to be sent. The type should be compatible to the one specified in the interface path.
        timestamp : datetime, optional
            If sending a Datastream with explicit_timestamp, you can specify a datetime object which will be registered
            as the timestamp for the value.
        """
        if self.__is_interface_aggregate(interface_name):
            raise TypeError(
                f'Interface {interface_name} is an aggregate interface. You should use send_aggregate.'
            )

        # TODO: validate paths

        if isinstance(payload, collections.abc.Mapping):
            raise TypeError('Payload for individual interfaces should not be a dictionary')

        object_payload = {'v': payload}
        if timestamp:
            object_payload['t'] = timestamp

        self.__send_generic(
            f'{self.__get_base_topic()}/{interface_name}{interface_path}',
            object_payload)

    def send_aggregate(self,
                       interface_name,
                       interface_path,
                       payload,
                       timestamp=None):
        """
        Sends an aggregate message to an interface

        Parameters
        ----------
        interface_name : str
            The name of an the Interface to send data to.
        payload : dict
            A dictionary containing the path:value map for the aggregate.
        timestamp : datetime, optional
            If the Datastream has explicit_timestamp, you can specify a datetime object which will be registered
            as the timestamp for the value.
        """
        if not self.__is_interface_aggregate(interface_name):
            raise TypeError(
                f'Interface {interface_name} is not an aggregate interface. You should use send.'
            )

        # TODO: validate paths

        if not isinstance(payload, collections.abc.Mapping):
            raise TypeError('Payload for aggregate interfaces should be a dictionary')

        # The payload should carry the aggregate object
        object_payload = {'v': payload}
        if timestamp:
            object_payload['t'] = timestamp

        self.__send_generic(f'{self.__get_base_topic()}/{interface_name}{interface_path}',
                            object_payload)


    def __send_generic(self, topic, object_payload, qos=2):
        self.__mqtt_client.publish(topic, bson.dumps(object_payload), qos=qos)

    def __is_interface_aggregate(self, interface_name):
        if not interface_name in self.__interfaces:
            raise FileNotFoundError(
                f'Interface {interface_name} not declared in introspection')

        try:
            if self.__interfaces[interface_name]['aggregation'] == 'object':
                return True
        except KeyError:
            # It's good, as the default is aggregate
            pass

        return False

    def __get_base_topic(self):
        return f'{self.__realm}/{self.__device_id}'

    def __on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            print("Error while connecting: " + str(rc))

        self.__is_connected = True
        client.subscribe(self.__get_base_topic())
        for k, v in self.__interfaces.items():
            try:
                if v['ownership'] == 'server':
                    client.subscribe(f'{self.__get_base_topic()}/{k}')
                    client.subscribe(f'{self.__get_base_topic()}/{k}/#')
            except:
                # Default is device
                pass

        # Send the introspection
        self.__send_introspection()

        if self.on_connected:
            if self.__loop:
                # Use threadsafe, as we're in a different thread here
                self.__loop.call_soon_threadsafe(self.on_connected, self)
            else:
                self.on_connected(self)

    def __on_disconnect(self, client, userdata, rc):
        self.__is_connected = False

        if self.on_disconnected:
            if self.__loop:
                # Use threadsafe, as we're in a different thread here
                self.__loop.call_soon_threadsafe(self.on_disconnected, self,
                                                 rc)
            else:
                self.on_disconnected(self, rc)
        
        # If rc was explicit, stop the loop (after the callback)
        if rc == 0:
            self.__mqtt_client.loop_stop()

    def __on_message(self, client, userdata, msg):
        if not msg.topic.startswith(self.__get_base_topic()):
            print(
                f'Received unexpected message on topic {msg.topic}, {msg.payload}'
            )
            return
        if msg.topic == self.__get_base_topic():
            print("received control message")
            return

        if not self.on_data_received:
            return

        real_topic = msg.topic.replace(f'{self.__get_base_topic()}/', '')
        topic_tokens = real_topic.split('/')
        interface_name = topic_tokens[0]
        if interface_name not in self.__interfaces:
            print(
                f'Received unexpected message for unregistered interface {interface_name}: {msg.topic}, {msg.payload}'
            )
            return

        interface_path = '/' + '/'.join(topic_tokens[1:])
        payload_object = bson.loads(msg.payload)
        if 'v' not in payload_object:
            print(
                f'Received unexpected BSON Object on topic {msg.topic}, {payload_object}'
            )
            return
        timestamp = payload_object['t'] if 't' in payload_object else None

        if self.__loop:
            # Use threadsafe, as we're in a different thread here
            self.__loop.call_soon_threadsafe(self.on_data_received, self,
                                             interface_name, interface_path,
                                             payload_object['v'])
        else:
            self.on_data_received(self, interface_name, interface_path,
                                  payload_object['v'])

    def __send_introspection(self):
        # Build the introspection message
        introspection_message = ""
        for interface_name, interface_definition in self.__interfaces.items():
            introspection_message += f'{interface_name}:{interface_definition["version_major"]}:{interface_definition["version_minor"]};'
        introspection_message = introspection_message[:-1]
        self.__mqtt_client.publish(self.__get_base_topic(),
                                   introspection_message, 2)
