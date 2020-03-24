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


import json
import requests
from . import crypto, exceptions


def register_device_with_private_key(device_id, realm, private_key_file,
                                     pairing_base_url) -> str:
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


def register_device_with_jwt_token(device_id, realm, jwt_token,
                                   pairing_base_url) -> str:
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
    """
    return __register_device(
        device_id, realm, __register_device_headers_with_jwt_token(jwt_token),
        pairing_base_url)


def obtain_device_certificate(device_id, realm, credentials_secret,
                              pairing_base_url, crypto_store_dir):
    # Get a CSR first
    csr = crypto.generate_csr(realm, device_id, crypto_store_dir)
    # Prepare the Pairing API request
    headers = {'Authorization': f'Bearer {credentials_secret}'}
    data = {'data': {'csr': csr.decode('ascii')}}

    res = requests.post(
        f'{pairing_base_url}/v1/{realm}/devices/{device_id}/protocols/astarte_mqtt_v1/credentials',
        json=data,
        headers=headers)
    if res.status_code == 401 or res.status_code == 403:
        raise exceptions.AuthorizationError(res.json())
    elif res.status_code != 201:
        raise exceptions.APIError(res.json())

    crypto.import_device_certificate(res.json()['data']['client_crt'],
                              crypto_store_dir)


def obtain_device_transport_information(device_id, realm, credentials_secret,
                                        pairing_base_url):
    # Prepare the Pairing API request
    headers = {'Authorization': f'Bearer {credentials_secret}'}

    res = requests.get(f'{pairing_base_url}/v1/{realm}/devices/{device_id}',
                       headers=headers)
    if res.status_code == 401 or res.status_code == 403:
        raise exceptions.AuthorizationError(res.json())
    elif res.status_code != 200:
        raise exceptions.APIError(res.json())

    return res.json()["data"]


def __register_device(device_id, realm, headers, pairing_base_url) -> str:
    data = {'data': {'hw_id': device_id}}

    res = requests.post(f'{pairing_base_url}/v1/{realm}/agent/devices',
                        json=data,
                        headers=headers)
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


def __register_device_headers_with_jwt_token(jwt_token) -> dict:
    return {'Authorization': f'Bearer {jwt_token}'}


# This throws FileNotFoundError if the private key does not exist
def __generate_token(private_key_file,
                     type="appengine",
                     auth_paths=[".*::.*"],
                     expiry=30) -> str:
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
