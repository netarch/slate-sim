#!/bin/bash

output_dir="log"
app="three_depth"
load_balancer="RoundRobin"
window_size=100
base_rps=8
experiment="Alibaba_trace"
workload="6d9c26b9"
request_arrival_file="../request_arrival_time/request_arrival_time_6d9c26b9_burst_40_high.txt"
# request_arrival_file="../parser/request_arrival_time_6d9c26b9.txt"
# request_arrival_file="../parser/final_request_arrival_time_clsuter_0-6d9c26b9.txt"
fixed_autoscaler=0

# for routing_algorithm in "LCLB" "MCLB" "heuristic_TE"
for routing_algorithm in "LCLB"
do
    python3 old-simulator.py --app ${app} \
                        --base_rps ${base_rps} \
                        --experiment ${experiment} \
                        --request_arrival_file ${request_arrival_file} \
                        --workload ${workload} \
                        --load_balancer ${load_balancer} \
                        --fixed_autoscaler ${fixed_autoscaler} \
                        --routing_algorithm ${routing_algorithm} \
                        --output_dir ${output_dir}
done