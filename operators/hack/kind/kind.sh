#!/bin/bash

############################################################################
# Wrapper script that will:                                                #
# 1. Setup new Kind cluster                                                #
# 2. Setup a local storage class from Rancher                              #
# 2. Execute any command in the context of the newly created Kind cluster  #
# Requirements: 8 cores / 16 GB                                            #
############################################################################

# Exit immediately for non zero status
set -e
# Print commands
set -x

NODE_IMAGE=${NODE_IMAGE:-"kindest/node:v1.13.4"}

scriptpath="$( cd "$(dirname "$0")" ; pwd -P )"

function check_kind() {
  echo "Check if Kind is installed..."
  if ! kind --help > /dev/null; then
    echo "Looks like Kind is not installed."
    exit 1
  fi
}

function cleanup_kind_cluster() {
    if [[ -z "${SKIP_CLEANUP:-}" ]]; then
      echo "Cleaning up kind cluster"
      kind delete cluster --name=eck-e2e
    fi
}

function setup_kind_cluster() {
  # Installing Kind
  check_kind

  # Delete any previous e2e KinD cluster
  echo "Deleting previous Kind cluster with name=eck-e2e"
  if ! (kind delete cluster --name=eck-e2e) > /dev/null; then
    echo "No existing kind cluster with name eck-e2e. Continue..."
  fi

  trap cleanup_kind_cluster EXIT

  # Create KinD cluster
  if ! (kind create cluster --name=eck-e2e --config ${scriptpath}/cluster.yml --loglevel debug --retain --image "${NODE_IMAGE}"); then
    echo "Could not setup Kind environment. Something wrong with Kind setup."
    exit 1
  fi

  KUBECONFIG="$(kind get kubeconfig-path --name="eck-e2e")"
  export KUBECONFIG

  # setup storage
  kubectl delete storageclass standard || true
  kubectl apply -f "${scriptpath}/local-path-storage.yaml"
}

while (( "$#" )); do
  case "$1" in
    --skip-setup)
      SKIP_SETUP=true
      shift
    ;;
    --skip-cleanup)
      SKIP_CLEANUP=true
      shift
    ;;
    --load-images) # images that can't (or should not) be loaded from a remote regsitry
      LOAD_IMAGES=$2
      shift 2
    ;;
    --load-stack-images) # try to load images as a best effort
      STACK_IMAGES=$2
      shift 2
    ;;
    -*)
      echo "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *) # preserve positional arguments
      PARAMS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "${SKIP_SETUP:-}" ]]; then
  time setup_kind_cluster
fi

# Trading some network I/O for some disk I/O ...
if [[ -n "${LOAD_IMAGES}" ]]; then
  IMAGES=(${LOAD_IMAGES//,/ })
  for image in "${IMAGES[@]}"; do
          kind --loglevel debug --name eck-e2e load docker-image "${image}"
  done
fi

if [[ -n "${STACK_IMAGES}" ]]; then
  VERSIONS=(${STACK_IMAGES//,/ })
  for version in "${VERSIONS[@]}"; do
          echo "Pre-loading Elasticsearch docker image"
          image="docker.elastic.co/elasticsearch/elasticsearch:${version}"
          docker pull "${image}"
          kind --loglevel debug --name eck-e2e load docker-image "${image}"
          echo "Pre-loading Kibana docker image"
          image="docker.elastic.co/kibana/kibana:${version}"
          docker pull "${image}"
          kind --loglevel debug --name eck-e2e load docker-image "${image}"
          echo "Pre-loading APM Server docker image"
          image="docker.elastic.co/apm/apm-server:${version}"
          docker pull "${image}"
          kind --loglevel debug --name eck-e2e load docker-image "${image}"
  done
fi

## Deploy CRDs
make install-crds

## Pre-create NS for local e2e tests
kubectl create ns e2e-mercury

## Deploy operator
make apply-operators MANAGED_NAMESPACE=e2e-mercury

## Start end-to-end tests
if [ ${#PARAMS[@]} -gt 0 ]; then
${PARAMS[*]}
fi




