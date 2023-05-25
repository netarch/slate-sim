import argparse
import copy
import heapq
import graphviz
import json
from matplotlib import pyplot as plt
import numpy as np
import math
import os
import pandas as pd
from queue import Queue
import random
from utils import *

np.random.seed(1234)

        
class WorkloadGenerator:
    def __init__ (self, req_per_sec, total_sec):
        self.request_per_sec = req_per_sec
        self.total_seconds = total_sec
        self.total_num_req = self.request_per_sec * self.total_seconds
        # TODO:
        # assert (self.total_num_req - int(self.total_num_req)) == 0
        print_log("DEBUG", "self.total_num_req: " + str(self.total_num_req))
        self.total_num_req = int(self.total_num_req)

    def exponential_distribution(self):
        # scale_ = (1 / self.request_per_sec) * 1000 # millisecond
        scale_ = (1 / self.request_per_sec) # scale parameter is the inverse of the rate parameter \lambda
        print_log("DEBUG", "total_num_req: " + str(self.total_num_req))
        print_log("DEBUG", "scale: " + str(scale_))
        
        exp_dist=np.random.exponential(scale=scale_, size=(self.total_num_req))
        
        exp_dist_sum = sum(exp_dist)
        total_millisecond = self.total_seconds*1000
        weight = total_millisecond / exp_dist_sum
        norm_exp_dist = [ x*weight for x in exp_dist ] # norm + sec->ms
        
        # print_log("DEBUG", "")
        # print_log("DEBUG", "="*40)
        # print_log("DEBUG", "== Exponential workload statistics ==")
        # print_log("DEBUG", "="*40)
        # print_log("DEBUG", "- total num requests: {}".format(self.total_num_req))
        # print_log("DEBUG", "- sum: {}".format(sum(norm_exp_dist)))
        # print_log("DEBUG", "- mean interval: {}".format(sum(norm_exp_dist)/len(norm_exp_dist)))
        # print_log("DEBUG", "- max interval: {}".format(max(norm_exp_dist)))
        # print_log("DEBUG", "- min interval: {}".format(min(norm_exp_dist)))
        # print_log("DEBUG", "="*40)
        return norm_exp_dist
    
    def constant_distribution(self):
        print_log("DEBUG", "total_num_req: ", self.total_num_req)
        dist = [(self.total_seconds/self.total_num_req) * 1000] * self.total_num_req
        print_log("DEBUG", dist)
        print_log("DEBUG", "")
        print_log("DEBUG", "="*40)
        print_log("DEBUG", "== Constant workload statistics ==")
        print_log("DEBUG", "="*40)
        print_log("DEBUG", "- total num requests: {}".format(self.total_num_req))
        print_log("DEBUG", "- sum: {}".format(sum(dist)))
        print_log("DEBUG", "- mean interval: {}".format(sum(dist)/len(dist)))
        print_log("DEBUG", "- max interval: {}".format(max(dist)))
        print_log("DEBUG", "- min interval: {}".format(min(dist)))
        print_log("DEBUG", "="*40)
        return dist


def calc_non_burst_rps(durations_ratio, moment_rps_, target_rps_):
    burst_num_req = 0
    non_burst_duration = 0
    for i in range(len(durations_ratio)):
        if moment_rps_[i] != -1:
            burst_num_req += moment_rps_[i] * durations_ratio[i]
        else:
            non_burst_duration += durations_ratio[i]
    non_burst_rps = (target_rps_ - burst_num_req) / non_burst_duration
    
    # assert (non_burst_rps - int(non_burst_rps)) == 0 # rps must be int
    non_burst_rps = int(non_burst_rps) # TODO:
    
    print_log("DEBUG", "non_burst_rps: ", non_burst_rps)
    for i in range(len(moment_rps_)):
        if moment_rps_[i] == -1:
            moment_rps_[i] = non_burst_rps
    print_log("DEBUG", "moment_rps: ", moment_rps_)
        
    return moment_rps_

def interval_to_arrival(req_intv):
        req_arrival = list()
        for i in range(len(req_intv)):
            if i == 0:
                req_arrival.append(req_intv[i])
            else:
                req_arrival.append(req_arrival[i-1] + req_intv[i])
        return req_arrival

def argparse_add_argument(parser):
    parser.add_argument("--trace_file", type=str, default="", help="path to the target call rate trace file", required=True)
    
    
def generate_workload_from_alibaba_trace(file_path):
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
    norm_rps_list = normalize(rps_list,40)
    print(min(norm_rps_list))
    
    wrk_list = list()
    ts = time.time()
    for rps in norm_rps_list:
        wrk_list.append(WorkloadGenerator(req_per_sec=rps, total_sec=60))
    print("creating wrk generator object: {}".format(time.time() - ts))
    
    gen_ts = time.time()
    request_interval = list()
    print("len(wrk_list): ", len(wrk_list))


    print("len(wrk_list): ", len(wrk_list))
    for i in range(len(wrk_list)):
        ts = time.time()
        request_interval += wrk_list[i].exponential_distribution()
        print("get exponential_distribution(idx{}, size,{}): {}".format(i, wrk_list[i].total_num_req,  time.time() - ts))
        # request_interval += wrk.constant_distribution()
    print("generating request interval: {}".format(time.time() - gen_ts))

    return msname, request_interval

    
    
if __name__ == "__main__":
    program_start_time = time.time()
    
    random.seed(1234) # To reproduce the random load balancing
    
    ##############################################################################
    ############################## ORIGNAL CODE ##################################
    ##############################################################################
    parser = argparse.ArgumentParser()
    argparse_add_argument(parser)
    flags, unparsed = parser.parse_known_args()
    
    trace_file_path = flags.trace_file
    assert trace_file_path != ""
    msname, request_interval = generate_workload_from_alibaba_trace(trace_file_path)
    request_arrival = interval_to_arrival(request_interval)
    print("len(request_arrival) == total num requests: ", len(request_arrival))
    
    ts = time.time()
    temp = list()
    for arr_time in request_arrival:
        temp.append(str(arr_time)+"\n")
        
    fname = "request_arrival_time_"+msname[:8]+"_norm_40.txt"
    file1 = open(fname, 'w')
    file1.writelines(temp)
    file1.close()
    print("request arrival file write: {}".format(time.time() - ts))
    
    plot_workload_histogram_2(request_arrival, msname, 0)
    
    print("DONE!!!")