#!/bin/bash

output_dir="log"
app="three_depth"
load_balancer="RoundRobin"
window_size=100
base_rps=8
experiment="Alibaba_trace"
workload="6d9c26b9"
# request_arrival_file="request_arrival_time_6d9c26b9_burst_40_high.txt"
request_arrival_file="../parser/request_arrival_time_6d9c26b9.txt"
fixed_autoscaler=0

# routing_algorithm="heuristic_TE"
# routing_algorithm="MCLB"
routing_algorithm="LCLB"

python3 simulator.py --app ${app} \
                    --base_rps ${base_rps} \
                    --experiment ${experiment} \
                    --request_arrival_file ${request_arrival_file} \
                    --workload ${workload} \
                    --load_balancer ${load_balancer} \
                    --fixed_autoscaler ${fixed_autoscaler} \
                    --routing_algorithm ${routing_algorithm} \
                    --output_dir ${output_dir}
