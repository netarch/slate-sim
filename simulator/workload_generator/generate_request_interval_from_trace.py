import argparse
import time
import sys
import workload_generator as wrk_g
from utils import *


def generate_workload_from_alibaba_trace(file_path, base_rps):
    def read_call_rate(file_path):
        file1 = open(file_path, 'r')
        lines = file1.readlines()
        file1.close()
        msname = lines.pop(0)
        call_rate = list()
        for i in range(len(lines)):
            call_rate.append(float(lines[i]))
        return msname, call_rate
    msname, rpm = read_call_rate(file_path)
    total_seconds = len(rpm) * 60
    print("total minutes: " + str(len(rpm)))
    print("total seconds: " + str(total_seconds))
    def rpm_to_rps(rpm):
        return [ x/60 for x in rpm]
    def normalize(rps_list, target_base_rps):
        # min_rps = min(rps_list)
        min_rps = np.percentile(rps_list, 50)
        norm_weight = target_base_rps/min_rps
        print("target_base_rps:", target_base_rps)
        print("by:", min_rps)
        print("norm_weight:", norm_weight)
        return [x*norm_weight for x in rps_list]
    rps_list = rpm_to_rps(rpm)
    
    # TODO: Hardcoded
    # norm_rps_list = normalize(rps_list, 10)
    norm_rps_list = normalize(rps_list, base_rps)
    
    print(min(norm_rps_list))
    wrk_list = list()
    ts = time.time()
    for rps in norm_rps_list:
        wrk_list.append(wrk_g.WorkloadGenerator(req_per_sec=rps, total_sec=60))
        # wrk_list.append(WorkloadGenerator(req_per_sec=cr, total_sec=1))
    print("creating wrk generator object: {}".format(time.time() - ts))
    gen_ts = time.time()
    request_interval = list()
    print("len(wrk_list): ", len(wrk_list))
    for i in range(len(wrk_list)):
        ts = time.time()
        request_interval += wrk_list[i].exponential_distribution()
        print("get exponential_distribution(idx{}, size,{}): {}".format(i, wrk_list[i].total_num_req,  time.time() - ts))
        # request_interval += wrk.constant_distribution()
    print("generating request interval: {}".format(time.time() - gen_ts))
    return msname, request_interval
    

if __name__ == "__main__":
    trace_file_path = sys.argv[1]
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace_file_path", type=str, default=None, help="path to Alibaba MCR_RCT file", required=True)
    parser.add_argument("--target_base_rps", type=int, default=10, help="target base request per second(RPS)", required=True)
    flags, unparsed = parser.parse_known_args()
    
    msname, request_interval = generate_workload_from_alibaba_trace(flags.trace_file_path, flags.target_base_rps)
    request_arrival = wrk_g.interval_to_arrival(request_interval)
    print("len(request_arrival) == total num requests: ", len(request_arrival))

    ts = time.time()
    temp = list()
    for arr_time in request_arrival:
        temp.append(str(arr_time)+"\n")
    file1 = open("request_arrival_time_"+msname+".txt", 'w')
    file1.writelines(temp)
    file1.close()
    print("request arrival file write: {}".format(time.time() - ts))

    ts = time.time()
    plot_workload_histogram_2(request_arrival, msname, 0)
    print("plot: {}".format(time.time() - ts))
