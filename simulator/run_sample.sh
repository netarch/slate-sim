#!/bin/bash

output_dir="log"
app="three_depth"
load_balancer="RoundRobin"
workload="sample2"
fixed_autoscaler=0
c0_request_arrival_file="sample-request_arrival-cluster0.txt"
c1_request_arrival_file="sample-request_arrival-cluster1.txt"

# for routing_algorithm in "LCLB" "MCLB" "heuristic_TE"
# do
#     start=`date +%s`
#     python3 simulator.py --app ${app} \
#                         --c0_request_arrival_file ${c0_request_arrival_file} \
#                         --c1_request_arrival_file ${c1_request_arrival_file} \
#                         --workload ${workload} \
#                         --load_balancer ${load_balancer} \
#                         --fixed_autoscaler ${fixed_autoscaler} \
#                         --routing_algorithm ${routing_algorithm} \
#                         --output_dir ${output_dir}
#     end=`date +%s`
#     runtime=$((end-start))
#     echo "${routing_algorithm}: ${runtime}s"
# done


# routing_algorithm="LCLB"
# python3 simulator.py --app ${app} \
#                     --c0_request_arrival_file ${c0_request_arrival_file} \
#                     --c1_request_arrival_file ${c1_request_arrival_file} \
#                     --workload ${workload} \
#                     --load_balancer ${load_balancer} \
#                     --fixed_autoscaler ${fixed_autoscaler} \
#                     --routing_algorithm ${routing_algorithm} \
#                     --output_dir ${output_dir} &
routing_algorithm="MCLB"
python3 simulator.py --app ${app} \
                    --c0_request_arrival_file ${c0_request_arrival_file} \
                    --c1_request_arrival_file ${c1_request_arrival_file} \
                    --workload ${workload} \
                    --load_balancer ${load_balancer} \
                    --fixed_autoscaler ${fixed_autoscaler} \
                    --routing_algorithm ${routing_algorithm} \
                    --output_dir ${output_dir} &
routing_algorithm="heuristic_TE"
python3 simulator.py --app ${app} \
                    --c0_request_arrival_file ${c0_request_arrival_file} \
                    --c1_request_arrival_file ${c1_request_arrival_file} \
                    --workload ${workload} \
                    --load_balancer ${load_balancer} \
                    --fixed_autoscaler ${fixed_autoscaler} \
                    --routing_algorithm ${routing_algorithm} \
                    --output_dir ${output_dir}