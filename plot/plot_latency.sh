#!/bin/bash

project_home="/srv/scratch/gangmuk2/clusterdata_with_parser/cluster-trace-microservices-v2021/data/MSRTQps/csv_files/slate-sim/simulator"

file1=${project_home}"/log/sample/three_depth/RoundRobin-LCLB/latency-cluster_0.txt"
file2=${project_home}"/log/sample/three_depth/RoundRobin-MCLB/latency-cluster_0.txt"
file3=${project_home}"/log/sample/three_depth/RoundRobin-heuristic_TE/latency-cluster_0.txt"
python3 plot_latency_cdf.py ${file1} ${file2} ${file3}


file1=${project_home}"/log/sample/three_depth/RoundRobin-LCLB/latency-cluster_1.txt"
file2=${project_home}"/log/sample/three_depth/RoundRobin-MCLB/latency-cluster_1.txt"
file3=${project_home}"/log/sample/three_depth/RoundRobin-heuristic_TE/latency-cluster_1.txt"
python3 plot_latency_cdf.py ${file1} ${file2} ${file3}