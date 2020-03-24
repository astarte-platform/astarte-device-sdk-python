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


from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes
from os import path


def generate_csr(realm, device_id, crypto_store_dir):
    key = None
    # Do we need to generate a keypair?
    if not path.exists(path.join(crypto_store_dir, "device.key")):
        # Generate our key
        key = rsa.generate_private_key(public_exponent=65537,
                                       key_size=2048,
                                       backend=default_backend())
        # Write our key to disk for safe keeping
        with open(path.join(crypto_store_dir, "device.key"), "wb") as f:
            f.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                ))
    else:
        # Load the key
        with open(path.join(crypto_store_dir, "device.key"), "rb") as key_file:
            key = serialization.load_pem_private_key(key_file.read(),
                                                     password=None,
                                                     backend=default_backend())

    csr = x509.CertificateSigningRequestBuilder().subject_name(
        x509.Name([
            # Provide various details about who we are.
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Devices"),
            x509.NameAttribute(NameOID.COMMON_NAME, f"{realm}/{device_id}"),
            # Sign the CSR with our private key.
        ])).sign(key, hashes.SHA256(), default_backend())

    # Return the CSR
    return csr.public_bytes(serialization.Encoding.PEM)


def import_device_certificate(client_crt, crypto_store_dir):
    certificate = x509.load_pem_x509_certificate(client_crt.encode('ascii'),
                                                 default_backend())

    # Store the certificate
    with open(path.join(crypto_store_dir, "device.crt"), "wb") as f:
        f.write(certificate.public_bytes(encoding=serialization.Encoding.PEM))


def device_has_certificate(crypto_store_dir):
    return path.exists(path.join(crypto_store_dir,
                                 "device.crt")) and path.exists(
                                     path.join(crypto_store_dir, "device.key"))
