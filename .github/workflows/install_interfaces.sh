# This file is part of Astarte.
#
# Copyright 2024 SECO Mind Srl
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

#!/bin/bash

# Check if an argument was provided
if [[ $# -eq 0 ]]; then
  echo "Please provide an argument."
  exit 1
fi

while true; do
  echo "Installing the interfaces using astartectl"
  astartectl realm-management interfaces sync $1/*.json --non-interactive
  sleep 5
  echo "Checking the installed interfaces."
  installed_interfaces=$(astartectl realm-management interfaces ls)
  missing_interface=false
  for file in $1/*.json; do
    interface_name=$(basename "$file" .json)
    if ! grep -q "$interface_name" <<< "$installed_interfaces"; then
      echo "Error: Interface $interface_name not found in $installed_interfaces"
      missing_interface=true
    fi
  done
  if [[ "$missing_interface" == "true" ]]; then
    continue
  else
    echo "All interfaces have been installed correctly"
    break
  fi
done
