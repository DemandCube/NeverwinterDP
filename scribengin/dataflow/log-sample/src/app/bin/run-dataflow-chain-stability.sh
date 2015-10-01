#!/bin/bash

#./clusterCommander.py digitalocean --launch --create-containers ./profile/stability/main.yml --subdomain stability

ROOT=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )


$ROOT/run-dataflow-chain.sh --num-of-message=25000000  --generator-max-wait-time=30000   --validator-disable --generator-send-period=1  --max-run-time=604800000
#$ROOT/run-dataflow-chain.sh --num-of-message=25000000  --generator-max-wait-time=30000   --validator-disable --generator-send-period=5
#$ROOT/run-dataflow-chain.sh --num-of-message=25000000  --generator-max-wait-time=60000 --generator-send-period=50 --max-run-time=86400000
#--max-run-time=86400000 --validator-disable --dump-period=120000