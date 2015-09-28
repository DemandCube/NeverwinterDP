#!/bin/bash

#./clusterCommander.py digitalocean --launch --create-containers ./profile/stability/main.yml --subdomain stability

ROOT=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

$ROOT/run-dataflow-chain.sh --num-of-message=100000000  --generator-max-wait-time=30000 
#--max-run-time=86400000 --validator-disable --dump-period=120000