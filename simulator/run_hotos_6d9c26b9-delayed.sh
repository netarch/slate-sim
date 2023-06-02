#!/bin/bash

output_dir="log"
app="three_depth"
load_balancer="RoundRobin"
workload="hotos-6d9c26b9-delayed"
fixed_autoscaler=0
delayed_information=1

c0_request_arrival_file="hotos_6d9c26b9-request_arrival-cluster0.txt"
c1_request_arrival_file="hotos_6d9c26b9-request_arrival-cluster1.txt"

#for routing_algorithm in "LCLB"
#for routing_algorithm in "heuristic_TE"
for routing_algorithm in "LCLB" "MCLB" "heuristic_TE"
do
    start=`date +%s`
    python3 simulator.py --app ${app} \
                        --workload ${workload} \
                        --c0_request_arrival_file ${c0_request_arrival_file} \
                        --c1_request_arrival_file ${c1_request_arrival_file} \
                        --load_balancer ${load_balancer} \
                        --fixed_autoscaler ${fixed_autoscaler} \
                        --delayed_information ${delayed_information} \
                        --routing_algorithm ${routing_algorithm} \
                        --output_dir ${output_dir} &
    end=`date +%s`
    runtime=$((end-start))
    echo "${routing_algorithm}: ${runtime}s"
done
