# This file is part of Astarte.
#
# Copyright 2025 SECO Mind Srl
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
"""
E2E test utilites
"""

import pickle
import sqlite3
from pathlib import Path
from typing import Union

from astarte.device.database import StoredProperty
from astarte.device.interface import InterfaceOwnership


def peek_database(persistency_dir: Path, device_id: str, interface_name: Union[str, None] = None):
    """
    Take a peek in the device database.
    """
    where_clause = ""
    parameters = ()

    if interface_name is not None:
        where_clause = f"WHERE interface = ?"
        parameters = (interface_name,)

    database_path = persistency_dir.joinpath(device_id, "caching", "astarte.db")
    properties = (
        sqlite3.connect(database_path)
        .cursor()
        .execute(
            f"SELECT interface, major, path, ownership, value FROM properties {where_clause}",
            parameters,
        )
        .fetchall()
    )
    parsed_properties = []
    for interface, major, path, ownership, value in properties:
        parsed_properties += [
            (
                interface,
                major,
                path,
                InterfaceOwnership(ownership),
                pickle.loads(value),
            )
        ]
    return parsed_properties


def property_to_tuple(property: StoredProperty) -> tuple:
    return (
        property.interface,
        property.major,
        property.path,
        property.ownership,
        property.value,
    )


def properties_to_tuples(properties: list[StoredProperty]) -> list[tuple]:
    return [property_to_tuple(p) for p in properties]
