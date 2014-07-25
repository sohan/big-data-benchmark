#!/bin/bash

# Copyright 2013 The Regents of The University California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

VERTICA_HOST="127.0.0.1"
VERTICA_PORT="5433"
VERTICA_USERNAME="dbadmin"
VERTICA_PASSWORD="changeme"
VERTICA_DATABASE="dw"
VERTICA_HOST="127.0.0.1"
VERTICA_HOST="127.0.0.1"
IDENTITY_FILE="/home/user/.ssh/id_rsa"
NUM_TRIALS=3
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RUN_DIR=${DIR}/..
queries=(1a)
out_file="log/vertica_`date +%s`"

for i in "${queries[@]}"; do
  $RUN_DIR/run-query.sh \
    --vertica \
    --aws-key-id="`echo $AWS_ACCESS_KEY_ID`" \
    --aws-key="`echo $AWS_SECRET_ACCESS_KEY`" \
    --query-num=$i \
    --num-trials=$NUM_TRIALS \
    --vertica-host="$VERTICA_HOST" \
    --vertica-identity-file="$IDENTITY_FILE" \
    --vertica-username="$VERTICA_USERNAME" \
    --vertica-password="$VERTICA_PASSWORD" \
    --vertica-port="$VERTICA_PORT" \
    --vertica-database="$VERTICA_DATABASE" | tee -a "$out_file"

done
