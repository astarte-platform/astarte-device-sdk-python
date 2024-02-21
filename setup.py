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

import ast
import io
import re
import os
from setuptools import find_namespace_packages, setup

DEPENDENCIES = [
    "requests>=2.22.0",
    "paho-mqtt==1.6.1",
    "cryptography",
    "bson",
    "PyJWT>=1.7.0",
]
EXCLUDE_FROM_PACKAGES = ["contrib", "docs", "tests*", "venv"]
CURDIR = os.path.abspath(os.path.dirname(__file__))

with io.open(os.path.join(CURDIR, "README.md"), "r", encoding="utf-8") as f:
    README = f.read()


def get_version():
    main_file = os.path.join(CURDIR, "astarte", "device", "__init__.py")
    _version_re = re.compile(r"__version__\s+=\s+(?P<version>.*)")
    with open(main_file, "r", encoding="utf8") as f:
        match = _version_re.search(f.read())
        version = match.group("version") if match is not None else '"unknown"'
    return str(ast.literal_eval(version))


setup(
    name="astarte-device-sdk",
    version=get_version(),
    author="Simone Orru",
    author_email="simone.orru@secomind.com",
    description="Astarte Device SDK for Python",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/astarte-platform/astarte-device-sdk-python",
    packages=find_namespace_packages(include=["astarte.*"], exclude=EXCLUDE_FROM_PACKAGES),
    include_package_data=True,
    keywords=[],
    scripts=[],
    zip_safe=False,
    install_requires=DEPENDENCIES,
    extras_require={
        "static": ["black", "pylint"],
        "unit": ["setuptools", "pytest", "pytest-cov"],
        "e2e": ["termcolor", "python-dateutil"],
    },
    test_suite="tests.test_project",
    python_requires=">=3.8",
    # license and classifier list:
    # https://pypi.org/pypi?%3Aaction=list_classifiers
    license="License :: OSI Approved :: Apache Software License",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        # "Private :: Do Not Upload"
    ],
)
