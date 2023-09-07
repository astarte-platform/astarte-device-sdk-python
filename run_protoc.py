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

from pathlib import Path
import os
import subprocess

def build_protobuf():
    include_fld = Path(os.getcwd()).joinpath("proto")
    src_fld = include_fld.joinpath("astarteplatform").joinpath("msghub")
    srcs = [src_fld.joinpath(f) for f in src_fld.glob("*.proto")]
    out_fld = Path(os.getcwd())
    for src in srcs:
        cmd = [
            "python",
            "-m grpc_tools.protoc",
            f"-I=\"{include_fld}\"",
            f"--python_out=\"{out_fld}\"",
            f"--pyi_out=\"{out_fld}\"",
            f"--grpc_python_out=\"{out_fld}\"",
            f"{src}"
        ]
        subprocess.run(" ".join(cmd), shell=True)

build_protobuf()
