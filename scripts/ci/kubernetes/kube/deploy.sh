#!/usr/bin/env bash
#
#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

set -x

IMAGE=${1:-airflow/ci}
TAG=${2:-latest}
DIRNAME=$(cd "$(dirname "$0")"; pwd)
TEMPLATE_DIRNAME=${DIRNAME}/templates
BUILD_DIRNAME=${DIRNAME}/build

usage() {
    cat << EOF
  usage: $0 options
  OPTIONS:
    -d Use PersistentVolume or GitSync for dags_folder. Available options are "persistent" or "git"
EOF
    exit 1;
}

while getopts ":d:" OPTION; do
  case ${OPTION} in
    d)
      DAGS_VOLUME=${OPTARG};;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      usage
      ;;
  esac
done

case ${DAGS_VOLUME} in
  "persistent")
    GIT_SYNC=0
    ;;
  "git")
    GIT_SYNC=1
    ;;
  *)
    echo "Value \"$DAGS_VOLUME\" for dags_folder is not valid." >&2
    usage
    ;;
esac

if [ ! -d "$BUILD_DIRNAME" ]; then
  mkdir -p ${BUILD_DIRNAME}
fi

rm -f ${BUILD_DIRNAME}/*

define_variables_by_git_status GIT_SYNC
define_sed_name
generate_airflow_deployment
fix_file_permissions
deploy_airflow_on_k8s


# wait for up to 10 minutes for everything to be deployed
for i in {1..150}
do
  echo "------- Running kubectl get pods -------"
  PODS=$(kubectl get pods | awk 'NR>1 {print $0}')
  echo "$PODS"
  NUM_AIRFLOW_READY=$(echo $PODS | grep airflow | awk '{print $2}' | grep -E '([0-9])\/(\1)' | wc -l | xargs)
  NUM_POSTGRES_READY=$(echo $PODS | grep postgres | awk '{print $2}' | grep -E '([0-9])\/(\1)' | wc -l | xargs)
  if [ "$NUM_AIRFLOW_READY" == "1" ] && [ "$NUM_POSTGRES_READY" == "1" ]; then
    break
  fi
  sleep 4
done

POD=$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep airflow | head -1)

echo "------- pod description -------"
kubectl describe pod $POD
echo "------- webserver init container logs - init -------"
kubectl logs $POD init
if [ "${GIT_SYNC}" = 1 ]; then
    echo "------- webserver init container logs - git-sync-clone -------"
    kubectl logs $POD git-sync-clone
fi
echo "------- webserver logs -------"
kubectl logs $POD webserver
echo "------- scheduler logs -------"
kubectl logs $POD scheduler
echo "--------------"
