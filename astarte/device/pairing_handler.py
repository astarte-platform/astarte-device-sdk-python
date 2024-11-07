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

import http
from base64 import urlsafe_b64encode
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4, uuid5

import jwt
import requests

from . import crypto, exceptions

DEFAULT_TIMEOUT = 30


def register_device_with_private_key(
    device_id: str,
    realm: str,
    private_key_file: str,
    pairing_base_url: str,
    ignore_ssl_errors: bool,
) -> str:
    """
    Registers a device against an Astarte instance/realm with a Private Key

    Returns the Credentials secret for the device

    Parameters
    ----------
    device_id : str
        The device ID to register.
    realm : str
        The Realm in which to register the device.
    private_key_file : str
        Path to the Private Key file for the Realm. It will be used to Authenticate against
        Pairing API.
    pairing_base_url : str
        The Base URL of Pairing API of the Astarte Instance the device will be registered in.
    ignore_ssl_errors: str
        Set to True to ignore SSL errors

    Returns
    -------
    str
        The credentials secret obtained after the registration
    """
    return __register_device(
        device_id,
        realm,
        __register_device_headers_with_private_key(private_key_file),
        pairing_base_url,
        ignore_ssl_errors,
    )


def register_device_with_jwt_token(
    device_id: str,
    realm: str,
    jwt_token: str,
    pairing_base_url: str,
    ignore_ssl_errors: bool = False,
) -> str:
    """
    Registers a device against an Astarte instance/realm with a JWT Token

    Returns the Credentials secret for the device

    Parameters
    ----------
    device_id : str
        The device ID to register.
    realm : str
        The Realm in which to register the device.
    jwt_token : str
        A JWT Token to Authenticate against Pairing API. The token must have access to Pairing
        API and to the agent API paths.
    pairing_base_url : str
        The Base URL of Pairing API of the Astarte Instance the device will be registered in.
    ignore_ssl_errors: bool
        Useful if you're registering a device into a test instance of Astarte with self-signed
        certificates. It is not recommended to leave this `true` in production.
        Defaults to `false`, if `true` SSL errors will be ignored when registering a device.

    Returns
    -------
    str
        The credentials secret obtained after the registration
    """
    return __register_device(
        device_id,
        realm,
        __register_device_headers_with_jwt_token(jwt_token),
        pairing_base_url,
        ignore_ssl_errors,
    )


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
        the generated device Id, using the standard Astarte device ID encoding (base64
        urlencoding without padding).

    """

    device_id = uuid5(namespace=namespace, name=unique_data)

    # encode the device_id, strip down the padding and return it as a string
    return urlsafe_b64encode(device_id.bytes).replace(b"=", b"").decode("utf-8")


def generate_random_device_id() -> str:
    """
    Quick way to generate a device Id.
    Pay attention that each time this value is different and the use in production is discouraged.

    Returns
    -------
    str
        the generated device Id, using the standard Astarte device ID encoding (base64
        urlencoding without padding).

    """
    device_id = uuid4()

    # encode the device_id, strip down the padding and return it as a string
    return urlsafe_b64encode(device_id.bytes).replace(b"=", b"").decode("utf-8")


def obtain_device_certificate(
    device_id: str,
    realm: str,
    credentials_secret: str,
    pairing_base_url: str,
    crypto_store_dir: str,
    ignore_ssl_errors: bool,
) -> None:
    """
    Utility function that gets a device certificate from Astarte based on a locally generated csr

    Parameters
    ----------
    device_id: str
        The device ID
    realm: str
        The Astarte realm where the device is registered
    credentials_secret: str
        The credentials secret for the device in the given realm
    pairing_base_url: str
        The base URL for the Astarte pairing APIs
    crypto_store_dir: str
        Path to the folder where crypto information are stored
    ignore_ssl_errors: str
        Set to True to ignore SSL errors

    Raises
    ------
    AuthorizationError
        If the authentication provided was not correct
    APIError
        If a generic Error was returned by the APIs
    """

    # Get a CSR first
    csr = crypto.generate_csr(realm, device_id, crypto_store_dir)
    # Prepare the Pairing API request
    headers = {"Authorization": f"Bearer {credentials_secret}"}
    data = {"data": {"csr": csr.decode("ascii")}}

    res = requests.post(
        f"{pairing_base_url}/v1/{realm}/devices/{device_id}/protocols/astarte_mqtt_v1/credentials",
        json=data,
        headers=headers,
        verify=not ignore_ssl_errors,
        timeout=DEFAULT_TIMEOUT,
    )
    if res.status_code in {http.HTTPStatus.UNAUTHORIZED, http.HTTPStatus.FORBIDDEN}:
        raise exceptions.AuthorizationError(res.json())
    if res.status_code != http.HTTPStatus.CREATED:
        raise exceptions.APIError(res.json())

    crypto.import_device_certificate(res.json()["data"]["client_crt"], crypto_store_dir)


def verify_device_certificate(
    device_id: str,
    realm: str,
    credentials_secret: str,
    pairing_base_url: str,
    ignore_ssl_errors: bool,
    cert_pem: str,
) -> bool:
    """
    Utility function that verifies the validity of a device certificate with Astarte

    Parameters
    ----------
    device_id: str
        The device ID
    realm: str
        The Astarte realm where the device is registered
    pairing_base_url: str
        The base URL for the Astarte pairing APIs
    credentials_secret: str
        The credentials secret for the device in the given realm
    ignore_ssl_errors: str
        Set to True to ignore SSL errors
    cert_pem: str
        Certificate to verify in the PEM format

    Raises
    ------
    AuthorizationError
        If the authentication provided was not correct
    APIError
        If a generic Error was returned by the APIs

    Returns
    -------
    bool
        True if the certificate is valid, False otherwise.
    """

    # Prepare the Pairing API request
    headers = {"Authorization": f"Bearer {credentials_secret}"}
    data = {"data": {"client_crt": cert_pem}}

    res = requests.post(
        f"{pairing_base_url}/v1/{realm}/devices/{device_id}/protocols/astarte_mqtt_v1/credentials/verify",
        json=data,
        headers=headers,
        verify=not ignore_ssl_errors,
        timeout=DEFAULT_TIMEOUT,
    )
    if res.status_code in {http.HTTPStatus.UNAUTHORIZED, http.HTTPStatus.FORBIDDEN}:
        raise exceptions.AuthorizationError(res.json())
    if res.status_code != http.HTTPStatus.OK:
        raise exceptions.APIError(res.json())

    return res.json().get("data", False).get("valid", False)


def obtain_device_transport_information(
    device_id: str,
    realm: str,
    credentials_secret: str,
    pairing_base_url: str,
    ignore_ssl_errors: bool,
) -> dict:
    """
    Utility function that requests the device transport information to Astarte

    Parameters
    ----------
    device_id: str
        The device ID
    realm: str
        The Astarte realm where the device is registered
    credentials_secret: str
        The credentials secret for the device in the given realm
    pairing_base_url: str
        The base URL for the Astarte pairing APIs
    ignore_ssl_errors: str
        Set to True to ignore SSL errors

    Returns
    -------
    dict
        The device transport information

    Raises
    ------
    AuthorizationError
        If the authentication provided was not correct
    APIError
        If a generic Error was returned by the APIs

    """
    # Prepare the Pairing API request
    headers = {"Authorization": f"Bearer {credentials_secret}"}

    res = requests.get(
        f"{pairing_base_url}/v1/{realm}/devices/{device_id}",
        headers=headers,
        verify=not ignore_ssl_errors,
        timeout=DEFAULT_TIMEOUT,
    )
    if res.status_code in {http.HTTPStatus.UNAUTHORIZED, http.HTTPStatus.FORBIDDEN}:
        raise exceptions.AuthorizationError(res.json())
    if res.status_code != http.HTTPStatus.OK:
        raise exceptions.APIError(res.json())

    return res.json()["data"]


def __register_device(
    device_id: str,
    realm: str,
    headers: dict,
    pairing_base_url: str,
    ignore_ssl_errors: bool,
) -> str:
    """
    Private utility Function that registers a new device

    Parameters
    ----------
    device_id: str
        Device ID
    realm: str
        Astarte realm
    headers: dict
        HTTP connection headers
    pairing_base_url: str
        Base URL for the Astarte Pairing APIs
    ignore_ssl_errors: bool
        Set to True to ignore SSL errors

    Returns
    -------
    str
        The credentials secret obtained after the registration

    Raises
    ------
    AuthorizationError
        If the authentication provided was not correct
    DeviceAlreadyRegisteredError
        If the device was already registered
    APIError
        If a generic Error was returned by the APIs
    """
    data = {"data": {"hw_id": device_id}}

    res = requests.post(
        f"{pairing_base_url}/v1/{realm}/agent/devices",
        json=data,
        headers=headers,
        verify=not ignore_ssl_errors,
        timeout=DEFAULT_TIMEOUT,
    )
    if res.status_code in {http.HTTPStatus.UNAUTHORIZED, http.HTTPStatus.FORBIDDEN}:
        raise exceptions.AuthorizationError(res.json())
    if res.status_code == http.HTTPStatus.UNPROCESSABLE_ENTITY:
        raise exceptions.DeviceAlreadyRegisteredError()
    if res.status_code != http.HTTPStatus.CREATED:
        raise exceptions.APIError(res.json())

    return res.json()["data"]["credentials_secret"]


def __register_device_headers_with_private_key(private_key_file) -> dict:
    """
    Private utility function that generates the Authorization header for astarte HTTP APIs from a
    private key file.

    Parameters
    ----------
    private_key_file: str
        Path to the private key file.

    Returns
    -------
    dict
        The Authorization Header dict.

    Raises
    ------
    JWTGenerationError
        If there is an error generating the token from the certificate
    """
    headers = {}
    try:
        headers["Authorization"] = (
            f'Bearer {__generate_token(private_key_file, key_type="pairing")}'
        )
        return headers
    except jwt.exceptions.PyJWTError as exc:
        raise exceptions.JWTGenerationError("Error encoding or decoding the JWT token.") from exc
    except IOError as exc:
        raise exceptions.JWTGenerationError("Error opening the private key file.") from exc


def __register_device_headers_with_jwt_token(jwt_token: str) -> dict:
    """
    Private utility function that generates the Authorization header for astarte HTTP APIs

    Parameters
    ----------
    jwt_token: str
        The authorization token for the HTTP Request

    Returns
    -------
    dict
        The Authorization Header dict

    """
    return {"Authorization": f"Bearer {jwt_token}"}


# This throws FileNotFoundError if the private key does not exist
def __generate_token(
    private_key_file: str,
    key_type: str = "appengine",
    auth_paths: list[str] | None = None,
    expiry: int = 30,
) -> str:
    """
    Private utility function that generates a valid token from a private key

    Parameters
    ----------
    private_key_file: str
        Path to where the private key is stored
    key_type: str
        Type of Astarte service the private key was made for
    auth_paths: list[str] | None
        Authorization paths used if key_type is channels
    expiry: int
        How many seconds the token validity will last

    Returns
    -------
    str
        The generated token
    """

    api_claims = {
        "appengine": "a_aea",
        "realm": "a_rma",
        "housekeeping": "a_ha",
        "channels": "a_ch",
        "pairing": "a_pa",
    }

    with open(private_key_file, "r", encoding="utf-8") as pk:
        private_key_pem = pk.read()

        if key_type == "channels" and not auth_paths:
            real_auth_paths = ["JOIN::.*", "WATCH::.*"]
        else:
            real_auth_paths = [".*::.*"]
        now = datetime.now(timezone.utc)
        claims = {api_claims[key_type]: real_auth_paths, "iat": now}
        if expiry > 0:
            claims["exp"] = now + timedelta(seconds=expiry)

        encoded = jwt.encode(claims, private_key_pem, algorithm="RS256")
        return encoded.decode()
