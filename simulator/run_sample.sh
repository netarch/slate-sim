#!/bin/bash

output_dir="log"
app="three_depth"
load_balancer="RoundRobin"
workload="sample-opt_opt_te_metric25-3"
fixed_autoscaler=0
autoscaler_period=15000
delayed_information=1
c0_request_arrival_file="request_arrival/sample-request_arrival-cluster0.txt"
c1_request_arrival_file="request_arrival/sample-request_arrival-cluster1.txt"

for routing_algorithm in "LCLB" "MCLB" "heuristic_TE"
do
    start=`date +%s`
    python3 simulator.py --app ${app} \
                        --c0_request_arrival_file ${c0_request_arrival_file} \
                        --c1_request_arrival_file ${c1_request_arrival_file} \
                        --workload ${workload} \
                        --load_balancer ${load_balancer} \
                        --fixed_autoscaler ${fixed_autoscaler} \
                        --autoscaler_period ${autoscaler_period} \
                        --delayed_information ${delayed_information} \
                        --routing_algorithm ${routing_algorithm} \
                        --output_dir ${output_dir} &
    end=`date +%s`
    runtime=$((end-start))
    echo "${routing_algorithm}: ${runtime}s"
done
