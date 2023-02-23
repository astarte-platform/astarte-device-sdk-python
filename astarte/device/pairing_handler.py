# Copyright 2020-2021 SECO Mind S.r.l.
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
from typing import List

import requests
from . import crypto, exceptions
from base64 import urlsafe_b64encode
from uuid import UUID, uuid5, uuid4


def register_device_with_private_key(device_id: str, realm: str, private_key_file: str,
                                     pairing_base_url: str) -> str:
    """
    Registers a Device against an Astarte instance/realm with a Private Key

    Returns the Credentials secret for the Device

    Parameters
    ----------
    device_id : str
        The Device ID to register.
    realm : str
        The Realm in which to register the Device.
    private_key_file : str
        Path to the Private Key file for the Realm. It will be used to Authenticate against Pairing API.
    pairing_base_url : str
        The Base URL of Pairing API of the Astarte Instance the Device will be registered in.
    """
    return __register_device(
        device_id, realm,
        __register_device_headers_with_private_key(private_key_file),
        pairing_base_url)


def register_device_with_jwt_token(device_id: str, realm: str, jwt_token: str,
                                   pairing_base_url: str,
                                   ignore_ssl_errors: bool = False) -> str:
    """
    Registers a Device against an Astarte instance/realm with a JWT Token

    Returns the Credentials secret for the Device

    Parameters
    ----------
    device_id : str
        The Device ID to register.
    realm : str
        The Realm in which to register the Device.
    jwt_token : str
        A JWT Token to Authenticate against Pairing API. The token must have access to Pairing API and to the agent API paths.
    pairing_base_url : str
        The Base URL of Pairing API of the Astarte Instance the Device will be registered in.
    ignore_ssl_errors: bool
        Useful if you're registering a device into a test instance of Astarte with self signed
        certificates. It is not recommended to leave this `true` in production.
        Defaults to `false`, if `true` SSL errors will be ignored when registering a device.
    """
    return __register_device(
        device_id, realm, __register_device_headers_with_jwt_token(jwt_token),
        pairing_base_url, ignore_ssl_errors)


def generate_device_id(namespace: UUID, unique_data: str) -> str:
    """
    Deterministically generate a device Id based on UUID namespace identifier and unique data.

    Parameters
    ----------
    namespace: UUID
        UUID namespace of the device_id
    unique_data: str
        device unique data used to generate the device_id

    Returns
    -------
    str
        the generated device Id, using the standard Astarte Device ID encoding (base64 urlencoding without padding).

    """

    device_id = uuid5(namespace=namespace, name=unique_data)

    # encode the device_id, strip down the padding and return it as a string
    return urlsafe_b64encode(device_id.bytes).replace(b'=', b'').decode("utf-8")


def generate_random_device_id() -> str:
    """
    Quick way to generate a device Id.
    Pay attention that each time this value is different and the use in production is discouraged.

    Returns
    -------
    str
        the generated device Id, using the standard Astarte Device ID encoding (base64 urlencoding without padding).

    """
    device_id = uuid4()

    # encode the device_id, strip down the padding and return it as a string
    return urlsafe_b64encode(device_id.bytes).replace(b'=', b'').decode("utf-8")


def obtain_device_certificate(device_id: str, realm: str, credentials_secret: str,
                              pairing_base_url: str, crypto_store_dir: str,
                              ignore_ssl_errors: bool) -> None:
    # Get a CSR first
    csr = crypto.generate_csr(realm, device_id, crypto_store_dir)
    # Prepare the Pairing API request
    headers = {'Authorization': f'Bearer {credentials_secret}'}
    data = {'data': {'csr': csr.decode('ascii')}}

    res = requests.post(
        f'{pairing_base_url}/v1/{realm}/devices/{device_id}/protocols/astarte_mqtt_v1/credentials',
        json=data,
        headers=headers,
        verify=not ignore_ssl_errors)
    if res.status_code == 401 or res.status_code == 403:
        raise exceptions.AuthorizationError(res.json())
    elif res.status_code != 201:
        raise exceptions.APIError(res.json())

    crypto.import_device_certificate(res.json()['data']['client_crt'],
                              crypto_store_dir)


def obtain_device_transport_information(device_id: str, realm: str, credentials_secret: str,
                                        pairing_base_url: str, ignore_ssl_errors: bool) -> dict:
    # Prepare the Pairing API request
    headers = {'Authorization': f'Bearer {credentials_secret}'}

    res = requests.get(f'{pairing_base_url}/v1/{realm}/devices/{device_id}',
                       headers=headers,
                       verify=not ignore_ssl_errors)
    if res.status_code == 401 or res.status_code == 403:
        raise exceptions.AuthorizationError(res.json())
    elif res.status_code != 200:
        raise exceptions.APIError(res.json())

    return res.json()["data"]


def __register_device(device_id: str, realm: str, headers: dict, pairing_base_url: str,
                      ignore_ssl_errors: bool) -> str:
    data = {'data': {'hw_id': device_id}}

    res = requests.post(f'{pairing_base_url}/v1/{realm}/agent/devices',
                        json=data,
                        headers=headers,
                        verify=not ignore_ssl_errors)
    if res.status_code == 401 or res.status_code == 403:
        raise exceptions.AuthorizationError(res.json())
    elif res.status_code == 422:
        raise exceptions.DeviceAlreadyRegisteredError()
    elif res.status_code != 201:
        raise exceptions.APIError(res.json())

    return res.json()['data']['credentials_secret']


def __register_device_headers_with_private_key(private_key_file) -> dict:
    headers = {}
    try:
        headers[
            'Authorization'] = f'Bearer {__generate_token(private_key_file, type="pairing")}'
        return headers
    except:
        raise TypeError(
            "Supplied Realm Key could not be used to generate a valid Token.")


def __register_device_headers_with_jwt_token(jwt_token: str) -> dict:
    return {'Authorization': f'Bearer {jwt_token}'}


# This throws FileNotFoundError if the private key does not exist
def __generate_token(private_key_file: str,
                     type: str = "appengine",
                     auth_paths: List[str] = [".*::.*"],
                     expiry: int = 30) -> str:
    import datetime
    import jwt
    api_claims = {
        "appengine": "a_aea",
        "realm": "a_rma",
        "housekeeping": "a_ha",
        "channels": "a_ch",
        "pairing": "a_pa"
    }

    with open(private_key_file, "r") as pk:
        private_key_pem = pk.read()

        if type == "channels" and auth_paths == [".*::.*"]:
            real_auth_paths = ["JOIN::.*", "WATCH::.*"]
        else:
            real_auth_paths = [".*::.*"]
        now = datetime.datetime.utcnow()
        claims = {api_claims[type]: real_auth_paths, "iat": now}
        if expiry > 0:
            claims["exp"] = now + datetime.timedelta(seconds=expiry)

        encoded = jwt.encode(claims, private_key_pem, algorithm="RS256")
        return encoded.decode()
