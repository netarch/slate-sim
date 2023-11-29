#!/bin/bash

output_dir="log"
app="three_depth"
load_balancer="RoundRobin"
workload="smallsample"
fixed_autoscaler=0
autoscaler_period=15000
desired_autoscaler_metric=0.50
delayed_information=1
c0_request_arrival_file="request_arrival/small_sample-cluster0.txt"
c1_request_arrival_file="request_arrival/small_sample-cluster1.txt"

#for routing_algorithm in "LCLB" "MCLB" "heuristic_TE"
for routing_algorithm in "queueing_prediction"
do
    start=`date +%s`
    # python3 simulator_cp.py --app ${app} \
    python3 simulator.py --app ${app} \
                        --c0_request_arrival_file ${c0_request_arrival_file} \
                        --c1_request_arrival_file ${c1_request_arrival_file} \
                        --workload ${workload} \
                        --load_balancer ${load_balancer} \
                        --fixed_autoscaler ${fixed_autoscaler} \
                        --autoscaler_period ${autoscaler_period} \
                        --desired_autoscaler_metric ${desired_autoscaler_metric} \
                        --delayed_information ${delayed_information} \
                        --routing_algorithm ${routing_algorithm} \
                        --output_dir ${output_dir}
    end=`date +%s`
    runtime=$((end-start))
    echo "${routing_algorithm}: ${runtime}s"
done
