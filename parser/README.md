# Alibaba dataset(MCR) parser
MCR: microservice call rate (it is basically load)

There are three steps in parsing Alibaba dataset. Each step is represented by `parser_1.py`, `parser_2.py` and `parser_3.ipynb`.

## Parser 0 (file is missing...)
- Input: `MSRTQps_0.csv` - `MSRTQps_24.csv`. Files from Alibaba `cluster-trace-microservices-v2021/data/MSRTQps`
- Ouput: `providerRPC_MCR-stat-MSRTQps_0.csv` - `providerRPC_MCR-stat-MSRTQps_24.csv`.

## Parser 1
- Input: `providerRPC_MCR-stat-MSRTQps_0.csv` - `providerRPC_MCR-stat-MSRTQps_24.csv`.
- Output: `merged_providerRPC_MCR.csv`

## Parser 2
- Input: `merged_providerRPC_MCR.csv`
- Output: `svc_per_line.csv`

## Parser 3
- Input: 
    - input 1: `svc_per_line.csv`
    - input 2: msname (microservice name), e.g., `6d9c26b9`
- Output: 
    - cluster 0 request arrival time: `final_request_arrival_time_clsuter_0-msname-8.txt`
    - cluster 1 request arrival time: `final_request_arrival_time_clsuter_1-msname-8.txt`
- Task:
    - Major: generate final request arrival time that will be input to `simulator.py`
    - Minor: burst count and burst statistics
