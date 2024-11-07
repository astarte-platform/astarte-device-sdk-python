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

from os import path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from astarte.device import pairing_handler


def generate_csr(realm: str, device_id: str, crypto_store_dir: str) -> bytes:
    """
    Utility function that generate the csr for the device

    Parameters
    ----------
    realm: str
        The Astarte realm where the device will be registered
    device_id: str
        The device ID
    crypto_store_dir: str
        Path to the folder where crypto information is stored

    Returns
    -------
    bytes
        The device certificate signing request file
    """
    key = None
    # Do we need to generate a keypair?
    if not path.exists(path.join(crypto_store_dir, "device.key")):
        # Generate our key
        key = ec.generate_private_key(curve=ec.SECP256R1())
        # Write our key to disk for safe keeping
        with open(path.join(crypto_store_dir, "device.key"), "wb") as f:
            f.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
    else:
        # Load the key
        with open(path.join(crypto_store_dir, "device.key"), "rb") as key_file:
            key = serialization.load_pem_private_key(key_file.read(), password=None)

    csr = (
        x509.CertificateSigningRequestBuilder().subject_name(
            x509.Name(
                [
                    # Provide various details about who we are.
                    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Devices"),
                    x509.NameAttribute(NameOID.COMMON_NAME, f"{realm}/{device_id}"),
                ]
            )
        )
        # Sign the CSR with our private key.
        .sign(key, hashes.SHA256())
    )

    # Return the CSR
    return csr.public_bytes(serialization.Encoding.PEM)


def import_device_certificate(client_crt: str, crypto_store_dir: str) -> None:
    """
    Deserialize a client certificate and store the public information permanently in the file system

    Parameters
    ----------
    client_crt: str
        Serialized client certificate
    crypto_store_dir: str
        Directory where to store the public bytes of the certificate

    """
    certificate = x509.load_pem_x509_certificate(client_crt.encode("ascii"))

    # Store the certificate
    with open(path.join(crypto_store_dir, "device.crt"), "wb") as f:
        f.write(certificate.public_bytes(encoding=serialization.Encoding.PEM))


def device_has_certificate(
    device_id: str,
    realm: str,
    credentials_secret: str,
    pairing_base_url: str,
    ignore_ssl_errors: bool,
    crypto_store_dir: str,
) -> bool:
    """
    Utility function that checks if a certificate is present for the device

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
    crypto_store_dir: str
        Path to the folder where crypto information is stored

    Returns
    -------
    bool
        True if the certificate is present, False otherwise

    """
    cert_path = path.join(crypto_store_dir, "device.crt")
    key_path = path.join(crypto_store_dir, "device.key")

    return (
        path.exists(cert_path)
        and path.exists(key_path)
        and certificate_is_valid(
            device_id,
            realm,
            credentials_secret,
            pairing_base_url,
            ignore_ssl_errors,
            crypto_store_dir,
        )
    )


def certificate_is_valid(
    device_id: str,
    realm: str,
    credentials_secret: str,
    pairing_base_url: str,
    ignore_ssl_errors: bool,
    crypto_store_dir: str,
) -> bool:
    """
    Utility function that checks the certificate validity

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
    crypto_store_dir: str
        Path to the folder where crypto information are stored

    Returns
    -------
    bool
        True if the certificate is valid, False otherwise.

    """

    cert_path = path.join(crypto_store_dir, "device.crt")
    with open(cert_path, "r", encoding="utf-8") as file:
        cert_pem = file.read()
        if cert_pem:
            return pairing_handler.verify_device_certificate(
                device_id, realm, credentials_secret, pairing_base_url, ignore_ssl_errors, cert_pem
            )
    return False
