#!/bin/bash

output_dir="log"
app="three_depth"
load_balancer="RoundRobin"
workload="6d9c26b9"
fixed_autoscaler=0

c0_request_arrival_file="../parser/new_request_arrival_time_clsuter_0-6d9c26b9.txt"
c1_request_arrival_file="../parser/new_request_arrival_time_clsuter_1-6d9c26b9.txt"

# for routing_algorithm in "LCLB" "MCLB" "heuristic_TE"
for routing_algorithm in "LCLB"
do
    start=`date +%s`
    python3 simulator.py --app ${app} \
                        --workload ${workload} \
                        --c0_request_arrival_file ${c0_request_arrival_file} \
                        --c1_request_arrival_file ${c1_request_arrival_file} \
                        --load_balancer ${load_balancer} \
                        --fixed_autoscaler ${fixed_autoscaler} \
                        --routing_algorithm ${routing_algorithm} \
                        --output_dir ${output_dir}
    end=`date +%s`
    runtime=$((end-start))
    echo "${routing_algorithm}: ${runtime}s"
done