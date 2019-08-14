#!/bin/bash

##################################################################################
# Utility script to:                                                             #
# 1. Setup new Kind cluster                                                      #
# 2. Setup a local storage class from Rancher                                    #
# 3. Run any command in the context of the newly created Kind cluster (optional) #
##################################################################################

# Exit immediately for non zero status
set -e
# Print commands
set -x

KIND_LOG_LEVEL=${KIND_LOG_LEVEL:-warning}
NODES=3
MANIFEST=/tmp/cluster.yml

workers=

scriptpath="$( cd "$(dirname "$0")" ; pwd -P )"

function check_kind() {
  echo "Check if Kind is installed..."
  if ! kind --help > /dev/null; then
    echo "Looks like Kind is not installed."
    exit 1
  fi
}

function create_manifest() {
cat <<EOT > ${MANIFEST}
kind: Cluster
apiVersion: kind.sigs.k8s.io/v1alpha3
nodes:
  - role: control-plane
EOT
  if [[ ${NODES} -gt 0 ]]; then
    for i in $(seq 1 $NODES);
    do
      echo '  - role: worker' >> ${MANIFEST}
      if [[ $i -gt 1 ]]; then
      workers="${workers},eck-e2e-worker${i}"
      else
      workers="eck-e2e-worker"
      fi

    done
  else
    # There's only a controle plane
    workers=eck-e2e-control-plane
  fi

}

function cleanup_kind_cluster() {
  echo "Cleaning up kind cluster"
  kind delete cluster --name=eck-e2e
}

function setup_kind_cluster() {
  # Check that Kind is available
  check_kind

  # Create the manifest according to the desired topology
  create_manifest

  # Delete any previous e2e Kind cluster
  echo "Deleting previous Kind cluster with name=eck-e2e"
  if ! (kind delete cluster --name=eck-e2e) > /dev/null; then
    echo "No existing kind cluster with name eck-e2e. Continue..."
  fi

  config_opts=""
  if [[ ${NODES} -gt 0 ]]; then
    config_opts="--config ${MANIFEST}"
  fi
  # Create Kind cluster
  if ! (kind create cluster --name=eck-e2e ${config_opts} --loglevel "${KIND_LOG_LEVEL}" --retain --image "${NODE_IMAGE}"); then
    echo "Could not setup Kind environment. Something wrong with Kind setup."
    exit 1
  fi

  KUBECONFIG="$(kind get kubeconfig-path --name="eck-e2e")"
  export KUBECONFIG

  # setup storage
  kubectl delete storageclass standard || true
  kubectl apply -f "${scriptpath}/local-path-storage.yaml"

  echo "Kind setup complete"
}

if [ -z "${NODE_IMAGE}" ]; then
    echo "NODE_IMAGE is not set"
    exit 1
fi

while (( "$#" )); do
  case "$1" in
    --stop) # just stop and exit
      cleanup_kind_cluster
      exit 0
    ;;
    --skip-setup)
      SKIP_SETUP=true
      shift
    ;;
    --load-images) # images that can't (or should not) be loaded from a remote regsitry
      LOAD_IMAGES=$2
      shift 2
    ;;
    --nodes) # how many nodes
      NODES=$2
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

# Load images in the nodes, e.g. the operator image or the e2e container
if [[ -n "${LOAD_IMAGES}" ]]; then
  IMAGES=(${LOAD_IMAGES//,/ })
  for image in "${IMAGES[@]}"; do
          kind --loglevel "${KIND_LOG_LEVEL}" --name eck-e2e load docker-image --nodes ${workers} "${image}"
  done
fi

## Start end-to-end tests
if [ ${#PARAMS[@]} -gt 0 ]; then
${PARAMS[*]}
fi




