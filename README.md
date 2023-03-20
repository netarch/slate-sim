# Simulator for microservice architecture

### How to use the simulator
1. ```git clone [this repo]```
2. run shell script.
3. You can find result log in output directory.
    - latency , autoscaling log, request arrival time .


the format of the output log directroy name
[date_time]-[app]-[workload]-[load balancer]-[routing algorithm]

Currently available shell scripts for experiment. (You can write your own script.)
- `run_high_burst_trace_6d9c26b9.sh`


### Script
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


### What you can find in the output log directory
- Metadata of the experiment.
- Latency log. (It will be used as an input to latency cdf plot.)
- Resource provisioining trend graph.
- Resource provisioning log.
- Request arrival time (which is used as a final request arrival input).

### How to plot latency CDF graph
```shell
python3 plot_cdf_new.py [latency_file_0] [latency_file_1] [latency_file_2]
```
Example
```shell
python3 plot_cdf_new.py 
latency-three_depth-6d9c26b9-RoundRobin-LCLB-cluster_0.txt latency-three_depth-6d9c26b9-RoundRobin-MCLB-cluster_0.txt latency-three_depth-6d9c26b9-RoundRobin-heuristic_TE-cluster_0.txt
```

### Workload generation process
#### Generating workload from Alibaba trace
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

#### Generating microbenchmark workload synthetically
1. Define your own workload for microbenchmark experiment.
   1. Go to **workload_generator.py**
   2. Go to **def generate_workload(...)** method.
   3. Define your own workload.
2. Specify that you want to use that workload in argparse argument when executing the simulator.py program. (**--workload**)
