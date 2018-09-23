#!/usr/bin/env bash

define_variables_by_git_status(){
GIT_SYNC=$1
if [ "${GIT_SYNC}" = 0 ]; then
    INIT_DAGS_VOLUME_NAME=airflow-dags
    POD_AIRFLOW_DAGS_VOLUME_NAME=airflow-dags
    CONFIGMAP_DAGS_FOLDER=/root/airflow/dags
    CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT=
    CONFIGMAP_DAGS_VOLUME_CLAIM=airflow-dags
else
    INIT_DAGS_VOLUME_NAME=airflow-dags-fake
    POD_AIRFLOW_DAGS_VOLUME_NAME=airflow-dags-git
    CONFIGMAP_DAGS_FOLDER=/root/airflow/dags/repo/airflow/example_dags_kubernetes
    CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT=/root/airflow/dags
    CONFIGMAP_DAGS_VOLUME_CLAIM=
fi
CONFIGMAP_GIT_REPO=${TRAVIS_REPO_SLUG:-apache/incubator-airflow}
CONFIGMAP_BRANCH=${TRAVIS_BRANCH:-master}
}

define_sed_name(){

_UNAME_OUT=$(uname -s)
case "${_UNAME_OUT}" in
    Linux*)     _MY_OS=linux;;
    Darwin*)    _MY_OS=darwin;;
    *)          echo "${_UNAME_OUT} is unsupported."
                exit 1;;
esac
echo "Local OS is ${_MY_OS}"

case $_MY_OS in
  linux)
    SED_COMMAND=sed
  ;;
  darwin)
    SED_COMMAND=gsed
    if ! $(type "$SED_COMMAND" &> /dev/null) ; then
      echo "Could not find \"$SED_COMMAND\" binary, please install it. On OSX brew install gnu-sed" >&2
      exit 1
    fi
  ;;
  *)
    echo "${_UNAME_OUT} is unsupported."
    exit 1
  ;;
esac

if [ "${GIT_SYNC}" = 0 ]; then
  ${SED_COMMAND} -e "s/{{INIT_GIT_SYNC}}//g" \
      ${TEMPLATE_DIRNAME}/airflow.template.yaml > ${BUILD_DIRNAME}/airflow.yaml
else
  ${SED_COMMAND} -e "/{{INIT_GIT_SYNC}}/{r $TEMPLATE_DIRNAME/init_git_sync.template.yaml" -e 'd}' \
      ${TEMPLATE_DIRNAME}/airflow.template.yaml > ${BUILD_DIRNAME}/airflow.yaml
fi

}

generate_airflow_deployment(){
${SED_COMMAND} -i "s|{{CONFIGMAP_GIT_REPO}}|$CONFIGMAP_GIT_REPO|g" ${BUILD_DIRNAME}/airflow.yaml
${SED_COMMAND} -i "s|{{CONFIGMAP_BRANCH}}|$CONFIGMAP_BRANCH|g" ${BUILD_DIRNAME}/airflow.yaml
${SED_COMMAND} -i "s|{{INIT_DAGS_VOLUME_NAME}}|$INIT_DAGS_VOLUME_NAME|g" ${BUILD_DIRNAME}/airflow.yaml
${SED_COMMAND} -i "s|{{POD_AIRFLOW_DAGS_VOLUME_NAME}}|$POD_AIRFLOW_DAGS_VOLUME_NAME|g" ${BUILD_DIRNAME}/airflow.yaml

${SED_COMMAND} "s|{{CONFIGMAP_DAGS_FOLDER}}|$CONFIGMAP_DAGS_FOLDER|g" \
    ${TEMPLATE_DIRNAME}/configmaps.template.yaml > ${BUILD_DIRNAME}/configmaps.yaml
${SED_COMMAND} -i "s|{{CONFIGMAP_GIT_REPO}}|$CONFIGMAP_GIT_REPO|g" ${BUILD_DIRNAME}/configmaps.yaml
${SED_COMMAND} -i "s|{{CONFIGMAP_BRANCH}}|$CONFIGMAP_BRANCH|g" ${BUILD_DIRNAME}/configmaps.yaml
${SED_COMMAND} -i "s|{{CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT}}|$CONFIGMAP_GIT_DAGS_FOLDER_MOUNT_POINT|g" ${BUILD_DIRNAME}/configmaps.yaml
${SED_COMMAND} -i "s|{{CONFIGMAP_DAGS_VOLUME_CLAIM}}|$CONFIGMAP_DAGS_VOLUME_CLAIM|g" ${BUILD_DIRNAME}/configmaps.yaml
}

fix_file_permissions(){
if [ `whoami` = "travis" ]; then
  sudo chown -R travis.travis $HOME/.kube $HOME/.minikube
fi
}

deploy_airflow_on_k8s(){
kubectl delete -f $DIRNAME/postgres.yaml
kubectl delete -f $BUILD_DIRNAME/airflow.yaml
kubectl delete -f $DIRNAME/secrets.yaml

set -e

kubectl apply -f $DIRNAME/secrets.yaml
kubectl apply -f $BUILD_DIRNAME/configmaps.yaml
kubectl apply -f $DIRNAME/postgres.yaml
kubectl apply -f $DIRNAME/volumes.yaml
kubectl apply -f $BUILD_DIRNAME/airflow.yaml
}
