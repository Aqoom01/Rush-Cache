#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
export FD_MATRIX_PROFILES=${FD_MATRIX_PROFILES:-product-swim-lifeguard,product-redis-cluster,product-phi-gossip,product-ignite-tcp-ring,product-aerospike-mesh}

exec "$script_dir/run-udp-pre-ec2-matrix.sh" "$@"
