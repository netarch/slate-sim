# Simulator for microservice architecture

## How to use the simulator
1. run shell script.
2. You can find result log in output directory.
    - latency
        - the format of the output log directroy name: `[date_time]-[app]-[workload]-[load balancer]-[routing algorithm]`
    - autoscaling log
    - request arrival time only exists in `LCLB` directory
3. To plot the graph,
    - resource consumption timeline & normalized resource consumption: `calc_resource_consumption.ipynb`
    - resource consumption timeline: `plot_latency.sh <workload>`
        - workload: 6d9c26b9, sample, smallsample


## Example run script
```shell
#!/bin/bash

output_dir="log"
app="three_depth"
load_balancer="RoundRobin"
fixed_autoscaler=0
autoscaler_period=15000
desired_autoscaler_metric=0.20
delayed_information=1
workload="6d9c26b9-delay${delayed_information}-auto${autoscaler_period}"
c0_request_arrival_file="request_arrival/new_request_arrival_time_clsuter_0-6d9c26b9.txt"
c1_request_arrival_file="request_arrival/new_request_arrival_time_clsuter_1-6d9c26b9.txt"

# for routing_algorithm in "LCLB"
for routing_algorithm in "LCLB" "MCLB" "heuristic_TE"
do
    start=`date +%s`
    python3 simulator.py --app ${app} \
                        --workload ${workload} \
                        --c0_request_arrival_file ${c0_request_arrival_file} \
                        --c1_request_arrival_file ${c1_request_arrival_file} \
                        --load_balancer ${load_balancer} \
                        --fixed_autoscaler ${fixed_autoscaler} \
                        --autoscaler_period ${autoscaler_period} \
                        --desired_autoscaler_metric ${desired_autoscaler_metric} \
                        --delayed_information ${delayed_information} \
                        --routing_algorithm ${routing_algorithm} \
                        --output_dir ${output_dir} &
    end=`date +%s`
    runtime=$((end-start))
    echo "${routing_algorithm}: ${runtime}s"
done

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
  - capacity_TE
  - queueing_prediction
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

## Alibaba dataset
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

### extract_and_merge_provider_rpc_mcr.ipynb (Optional, not necessary step)
It reads MSRTQps_\*.csv, merges all of them to one file and sorts by ["msname", "timestamp"].

- Output format

|   | timestamp | msname                                                           | msinstanceid                                                     | metric         | value              |
|---|-----------|------------------------------------------------------------------|-----------------------------------------------------------------|-----------------|--------------------|
| 0 | 0         | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 | 8e92aa9e2e3e0e35f69b3769f47dcbb43d3c21b35c0408bceaba550234c63118 | providerRPC_MCR | 42.9               |
| 1 | 0         | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 | 7a9660086346243cdc4611e2849d11633f1ef84abcfdb128436327f1f423ac72 | providerRPC_MCR | 41.21666666666667 |
| 2 | 0         | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 | 7fd50816dda300d9ffe2eb92f77695be94ec4eb1dbbc9e0214d491efd4cf4ca9 | providerRPC_MCR | 43.25              |
| 3 | 0         | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 | ba9cc9090ddf34bf76921911de8b2029c81ba636991651f6abbc5ee850fd554a | providerRPC_MCR | 41.66666666666666 |
| 4 | 0         | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 | 1ccd6de6a1e8207e41acc67edee4245c77e51ed738e44ed001d73284f8ae5fe0 | providerRPC_MCR | 41.88333333333333 |
| 5 | 0         | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 | a4d2b9c3134ef048a2481a2e303532f25ae02f5f2a5b13411594b059139ac115 | providerRPC_MCR | 40.05              |
| 6 | 0         | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 | 48c3618015c6904799d0f0048ccbd6a53a526bb1f5059ca97d7986855ae2d2a4 | providerRPC_MCR | 42.63333333333333 |
| 7 | 0         | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 | d4268912905166dd3818345010fc515

Now we will collapse `msinstanceid` and leave msname only by summing up all `providerRPC_MCR` value within the same timestamp.

### parse_provider_rpc_mcr.py
- Input: MSRTQps_\*.csv file
- Parsing step
1. Read MSRTQps_i.csv files.
2. Filter a service name in the `msname` column. Let's name the output MSRTQps_i_svc
3. Filter `providerRPC_MCR` in the `metric` column.
4. for each timestamp in `timestamp` column.
    1. Filter the timestamp. (_now we have a single provideRPC_MCR value at timestamp t_)
    2. Append this value to the list
    Now each data structure has `providerRPC_MCR` of one service sorted by timestamp.
    ```python
    dictionary
    { service A : [ MCR value at t=0, MCR value at t=1, ... ] },
    { service B : [ MCR value at t=0, MCR value at t=1, ... ] },
    ...
    ```
5. Get statistics of each service's MCR list.

- Output

|    | msname                                                           | timestamp | num |      sum |      avg |     std | min |    max | max/min | max/p1 |   p0.1 |      p1 |      p5 |     p10 |      p25 |      p50 |      p75 |     p90 |      p95 |      p99 |    p99.9 |
|---:|:-----------------------------------------------------------------|-----------|-----:|---------:|---------:|--------:|----:|-------:|--------:|-------:|-------:|--------:|--------:|--------:|---------:|---------:|---------:|--------:|---------:|---------:|---------:|
|  0 | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 |          0 | 437 | 18437.87 | 42.19191 | 2.168923 |   0 | 45.117 |     inf | 1.11823 | 17.4618 | 40.3467 |   40.98 |    41.3 | 41.76667 | 42.23333 | 42.81667 | 43.32333 | 43.57333 | 44.094   |
|  1 | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 |     60000 | 437 | 18787.08 | 42.99104 | 2.232557 |   0 | 46.317 |     inf | 1.12813 | 17.4763 | 41.056  |   41.747 |    42.02 |     42.5 |    43.05 |    43.65 | 44.18333 |     44.59 | 45.232   |
|  2 | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 |    120000 | 437 | 18893.15 | 43.23375 | 2.244835 |   0 | 45.717 |     inf | 1.10544 | 17.9269 | 41.356  |   41.947 |    42.24 |     42.7 | 43.33333 | 43.91667 | 44.46667 | 44.82667 | 45.16667 |
|  3 | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 |    180000 | 437 | 18877.18 | 43.19722 | 2.225311 |   0 | 45.717 |     inf | 1.10262 | 17.9196 | 41.462  |   41.883 |    42.27 |     42.7 |      43.3 |    43.87 | 44.28333 | 44.62333 | 45.31667 |
|  4 | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 |    240000 | 437 | 18976.15 | 43.42368 | 2.223884 |   0 | 45.45  |     inf | 1.09267 | 17.9777 | 41.5953 |   42.23  |    42.48 |     42.98 |     43.53 |     44.1 |    44.51667 | 44.75    |
|  5 | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 |    300000 | 437 | 19139.48 | 43.79744 | 2.257042 |   0 | 46.067 |     inf | 1.10278 | 18.0068 | 41.7733 |   42.513 |    42.79 |     43.28 |     43.98 |     44.47 |     44.95 | 45.20333 | 45.734   |
|  6 | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 |    360000 | 437 | 19241.37 | 44.03059 | 2.300797 |   0 | 46.6   |     inf | 1.11286 | 18.0722 | 41.874  |   42.58  |    42.87 |    43.55 | 44.13333 |     44.77 |    45.247 | 45.55667 | 46.16667 |
|  7 | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 |    420000 | 437 | 19393.55 | 44.37883 | 2.288714 |   0 | 47.35  |     inf | 1.12622 | 18.1231 | 42.0433 |   43.097 |    43.41 |    43.92 | 44.48333 |    45.117 |    45.49 |    45.8   | 46.308   |
|  8 | 002251d4123496684687c2acad43bdef9419a5e4fc01a65d2c558af92a5ad649 |    480000 | 437 | 194       | 44.38    | 2.288714 |   0 | 47.35  |     inf | 1.12622 | 18.1231 | 42.0433 |   43.097 |    43.41 |    43.92 | 44.48333 |    45.117 |    45.49 |    45.8   | 46.308   |

- percentile columns and `max/min`, `max/p1` columns stands for the variance between different instances (replicas) of the same service **in the same timestampe**.
- In later analyzer and parser, **`sum`** column will be used as MCR of each timestamp.



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
