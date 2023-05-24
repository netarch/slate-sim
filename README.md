# Simulator for microservice architecture

## How to use the simulator
1. ```git clone [this repo]```
2. run shell script.
3. You can find result log in output directory.
    - latency , autoscaling log, request arrival time .


the format of the output log directroy name
[date_time]-[app]-[workload]-[load balancer]-[routing algorithm]

Currently available shell scripts for experiment. (You can write your own script.)
- `run_high_burst_trace_6d9c26b9.sh`


## Example run script
```shell
dir=[path to slate-sim directory]
app="three_depth" # one_service
load_balancer="RoundRobin"
window_size=100
base_rps=8
experiment="Alibaba_trace" # Microbenchmark
workload="6d9c26b9" # exp_burst_4x, exp_burst_8x
request_arrival_file=${dir}"/request_arrival_time_6d9c26b9_burst_40_high.txt"
fixed_autoscaler=0
routing_algorithm="heuristic_TE" # "LCLB", "MCLB"

output_dir="log"

python3 ${dir}/simulator.py --app ${app} \
                    --base_rps ${base_rps} \
                    --experiment ${experiment} \
                    --request_arrival_file ${request_arrival_file} \
                    --workload ${workload} \
                    --load_balancer ${load_balancer} \
                    --fixed_autoscaler ${fixed_autoscaler} \
                    --routing_algorithm ${routing_algorithm} \
                    --output_dir ${output_dir}
```
- base_rps: base rps for Microbenchmark experiment
- experiment: Alibaba_trace or Microbenchmark
- request_arrival_file: path to request_arrival_file if you want to run Alibaba_trace experiment.
- workload: 
  - service id if you want to run Alibaba_trace.
  - workload name if you want to run Microbenchmark. (e.g., exp_burst_4x, exp_burst_8x)
- load_balancer: load balancer policy. (Currently inter-cluster load balancing will also use this load balancing policy.) It does not support different load balancing polices for different services yet.
- fixed_autoscaler: It will be removed eventually.
- routing_algorithm: multi-cluster service routing algorithm
  - LCLB: Local-cluster load balancing.
  - MCLB: Multi-cluster load balancing.
  - heuristic_TE: current service layer traffice engineering.
- output_dir: the name of a directory to store logs. If this dir does not exist, it will automatically create this directory.


## What you can find in the output log directory
- Metadata of the experiment.
- Latency log. (It will be used as an input to latency cdf plot.)
- Resource provisioining trend graph.
- Resource provisioning log.
- Request arrival time (which is used as a final request arrival input).

## How to plot latency CDF graph
```shell
python3 plot_cdf_new.py [latency_file_0] [latency_file_1] [latency_file_2]
```
Example
```shell
python3 plot_cdf_new.py 
latency-three_depth-6d9c26b9-RoundRobin-LCLB-cluster_0.txt latency-three_depth-6d9c26b9-RoundRobin-MCLB-cluster_0.txt latency-three_depth-6d9c26b9-RoundRobin-heuristic_TE-cluster_0.txt
```

## Alibaba dataset structure
There are six different categories of cluster trace in alibaba cluster data. What we need for `slate-sim` is in **`cluster-trace-microservices-v2021`**.
```shell
drwxrwxr-x 7 gangmuk2 gangmuk2 4.0K Jan  6 21:09 cluster-trace-gpu-v2020
drwxrwxr-x 3 gangmuk2 gangmuk2 4.0K Jan  6 21:09 cluster-trace-microarchitecture-v2022
drwxrwxr-x 4 gangmuk2 gangmuk2 4.0K Jan  6 21:13 cluster-trace-microservices-v2021
drwxrwxr-x 3 gangmuk2 gangmuk2 4.0K Jan  6 21:09 cluster-trace-microservices-v2022
drwxrwxr-x 2 gangmuk2 gangmuk2 4.0K Jan  6 21:09 cluster-trace-v2017
drwxrwxr-x 2 gangmuk2 gangmuk2 4.0K Jan  6 21:09 cluster-trace-v2018
```

There are four different data in `cluster-trace-microservices-v2021`.
- MSCallGraph: RPC calls (this is the largest dataset). It has every inter-process call between services. Theoretically, you should be able to construct entire call graph of each user response.
- MSResource: Resource utilization of each service which means each pod.
- **MSRTQps**: Response time of each services (This will be used to generate workload which will be used in simulator.)
- Node: Node utilization
```shell
drwxrwxr-x 3 gangmuk2 gangmuk2  20K Jan  7 12:46 MSCallGraph
drwxrwxr-x 2 gangmuk2 gangmuk2 4.0K Jan  7 03:02 MSResource
drwxrwxr-x 2 gangmuk2 gangmuk2 4.0K Jan  6 23:03 MSRTQps
drwxrwxr-x 2 gangmuk2 gangmuk2 4.0K Jan  6 21:13 Node```
```

MSRTQps consists of 25 files. The size of each file is around 800MB except for the last file(MSRTQps_24.tar.gz) whose size is 32MB. Unzipped file name is `MSRTQps_\*.csv`.
```shell
MSRTQps_0.tar.gz   MSRTQps_14.tar.gz  MSRTQps_19.tar.gz  MSRTQps_23.tar.gz  MSRTQps_5.tar.gz
MSRTQps_10.tar.gz  MSRTQps_15.tar.gz  MSRTQps_1.tar.gz   MSRTQps_24.tar.gz  MSRTQps_6.tar.gz
MSRTQps_11.tar.gz  MSRTQps_16.tar.gz  MSRTQps_20.tar.gz  MSRTQps_2.tar.gz   MSRTQps_7.tar.gz
MSRTQps_12.tar.gz  MSRTQps_17.tar.gz  MSRTQps_21.tar.gz  MSRTQps_3.tar.gz   MSRTQps_8.tar.gz
MSRTQps_13.tar.gz  MSRTQps_18.tar.gz  MSRTQps_22.tar.gz  MSRTQps_4.tar.gz   MSRTQps_9.tar.gz
```

### MSRTQps file format
|   | timestamp | msname                                                           | msinstanceid                                                     | metric            | value               |
|---|-----------|------------------------------------------------------------------|-----------------------------------------------------------------|-------------------|---------------------|
| 0 | 360000    | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | 5cb6ebc2cd245149dc4f7d36bca956f912554e8a0e17a79b05e2f59939919d82 | consumerRPC_MCR   | 17.7                |
| 1 | 720000    | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | 3525018ef4a856ec7e34e14ca9b2acd6d1515d9eeb6a4dd04473db0242589c82 | providerRPC_MCR   | 5.95                |
| 2 | 1200000   | 0e337f047a062e592acb1be850542d2ac66829277ec311eed311f21ac96266e3 | a223cd83228ee78027a6883b48d69899579c77357ff6edbedd40bdf686bb8951 | consumerRPC_MCR   | 22.35               |
| 3 | 60000     | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | 7dbf49e98cbac87cd78aa927cde9df54616f4541bbe2522b88a174cc09673ab3 | providerRPC_MCR   | 6.266666666666667  |
| 4 | 600000    | d03bb97862607468fe3153b28d41a20de1e3144a5662642b8d4c1062c550f622 | d2064c1a91d974098dc3a9eb48eea61182cd3bbc1885c1b48d206740c47e53ae | consumerRPC_MCR   | 55.63333333333333 |
| 5 | 1380000   | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | 8a50f66e901da479248a7f4e8394345fe92631c21add0d4063a69cfe09dc498d | consumerRPC_MCR   | 23.733333333333334 |
| 6 | 60000     | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | d95f2fe4b1c361fc24cbe6bd7629fcae3098bd60a4065518e96453169de11314 | providerRPC_MCR   | 6.15                |
| 7 | 1140000   | af5d63e40f2bc053c32d2b51ba

### extract_and_merge_provider_rpc_mcr.ipynb
It reads MSRTQps_\*.csv, merges all of them to one file and sorts by ["msname", "timestamp"].

- Output format

|   | timestamp | msname                                                           | msinstanceid                                                     | metric            | value               |
|---|-----------|------------------------------------------------------------------|-----------------------------------------------------------------|-------------------|---------------------|
| 0 | 360000    | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | 5cb6ebc2cd245149dc4f7d36bca956f912554e8a0e17a79b05e2f59939919d82 | consumerRPC_MCR   | 17.7                |
| 1 | 720000    | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | 3525018ef4a856ec7e34e14ca9b2acd6d1515d9eeb6a4dd04473db0242589c82 | providerRPC_MCR   | 5.95                |
| 2 | 1200000   | 0e337f047a062e592acb1be850542d2ac66829277ec311eed311f21ac96266e3 | a223cd83228ee78027a6883b48d69899579c77357ff6edbedd40bdf686bb8951 | consumerRPC_MCR   | 22.35               |
| 3 | 60000     | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | 7dbf49e98cbac87cd78aa927cde9df54616f4541bbe2522b88a174cc09673ab3 | providerRPC_MCR   | 6.266666666666667  |
| 4 | 600000    | d03bb97862607468fe3153b28d41a20de1e3144a5662642b8d4c1062c550f622 | d2064c1a91d974098dc3a9eb48eea61182cd3bbc1885c1b48d206740c47e53ae | consumerRPC_MCR   | 55.63333333333333 |
| 5 | 1380000   | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | 8a50f66e901da479248a7f4e8394345fe92631c21add0d4063a69cfe09dc498d | consumerRPC_MCR   | 23.733333333333334 |
| 6 | 60000     | af5d63e40f2bc053c32d2b51ba6ca28739e93661fd816b61e1d2e90736c9643f | d95f2fe4b1c361fc24cbe6bd7629fcae3098bd60a4065518e96453169de11314 | providerRPC_MCR   | 6.15                |
| 7 | 1140000   | af5d63e40f2bc053c32d2b51ba


### parse_provider_rpc_mcr.py
Read all MSRTQps_\*.csv files. Filter `providerRPC_MCR` column only and merge them into one file. The output 

## Workload generation process
### Generating workload from Alibaba trace
1. Clone Alibaba clusterdata trace and download the traces.
   - https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2021
2. **MS_MCR_RT_Table** has call rate data. The call rate means the number of requests that each replica of a microservice received **every minute**.
3. To make microservice level number of request per minute, it collapses the number of requests per minute of the entire replicas in a microservice . Then, it becomes the number of requests per minute of the microservice.
4. Run `python3 generate_requeset_interval_from_trace.py --trace_file_path [path to Alibaba trace file] --target_base_rps [base_rps]`
    `trace_file_path` is the file created by the steps from 1 to 3.
    - You specify the base RPS(request per second) to normalize the RPS.
    - It takes XX seconds when it runs YY microservice with 10 base RPS.
    - It will write a RPS file.
5. The generated RPS file will not be used as it is. It will be processed one more time by `simulator.py` before it is used as a request arrival time input for a simulation experiment.

### Generating microbenchmark workload synthetically
1. Define your own workload for microbenchmark experiment.
   1. Go to **workload_generator.py**
   2. Go to **def generate_workload(...)** method.
   3. Define your own workload.
2. Specify that you want to use that workload in argparse argument when executing the simulator.py program. (**--workload**)
