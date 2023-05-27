import time
import sys
import numpy as np
sys.path.append("/srv/scratch/gangmuk2/clusterdata_with_parser/cluster-trace-microservices-v2021/data/MSRTQps/csv_files/slate-sim/simulator")
from utils import utils

class WorkloadGenerator:
    def __init__ (self, req_per_sec, total_sec):
        self.request_per_sec = req_per_sec
        self.total_seconds = total_sec
        self.total_num_req = self.request_per_sec * self.total_seconds
        # TODO:
        # assert (self.total_num_req - int(self.total_num_req)) == 0
        utils.print_log("DEBUG", "self.total_num_req: " + str(self.total_num_req))
        self.total_num_req = int(self.total_num_req)

    def exponential_distribution(self):
        # scale_ = (1 / self.request_per_sec) * 1000 # millisecond
        scale_ = (1 / self.request_per_sec) # scale parameter is the inverse of the rate parameter \lambda
        utils.print_log("DEBUG", "total_num_req: " + str(self.total_num_req))
        utils.print_log("DEBUG", "scale: " + str(scale_))
        
        ts = time.time()
        exp_dist=np.random.exponential(scale=scale_, size=(self.total_num_req))
        print("np.random.exponential overhead: {}".format(time.time() - ts))
        
        ts = time.time()
        exp_dist_sum = sum(exp_dist)
        total_millisecond = self.total_seconds*1000
        weight = total_millisecond / exp_dist_sum
        norm_exp_dist = [ x*weight for x in exp_dist ] # norm + sec->ms
        # norm_exp_dist = [ (x / sum(exp_dist))*self.total_seconds*1000 for x in exp_dist ] # norm + sec->ms
        print("norm_exp_dist overhead: {}".format(time.time() - ts))
        
        # first_request_start_time = 0.0
        # norm_exp_dist.insert(0, first_request_start_time) # For the very first event, 0 second event arrival is inserted.
        
        
        utils.print_log("DEBUG", "")
        utils.print_log("DEBUG", "="*40)
        utils.print_log("DEBUG", "== Exponential workload statistics ==")
        utils.print_log("DEBUG", "="*40)
        utils.print_log("DEBUG", "- total num requests: {}".format(self.total_num_req))
        utils.print_log("DEBUG", "- sum: {}".format(sum(norm_exp_dist)))
        utils.print_log("DEBUG", "- mean interval: {}".format(sum(norm_exp_dist)/len(norm_exp_dist)))
        utils.print_log("DEBUG", "- max interval: {}".format(max(norm_exp_dist)))
        utils.print_log("DEBUG", "- min interval: {}".format(min(norm_exp_dist)))
        utils.print_log("DEBUG", "="*40)
        return norm_exp_dist
    
    def constant_distribution(self):
        utils.print_log("DEBUG", "total_num_req: ", self.total_num_req)
        dist = [(self.total_seconds/self.total_num_req) * 1000] * self.total_num_req
        utils.print_log("DEBUG", dist)
        utils.print_log("DEBUG", "")
        utils.print_log("DEBUG", "="*40)
        utils.print_log("DEBUG", "== Constant workload statistics ==")
        utils.print_log("DEBUG", "="*40)
        utils.print_log("DEBUG", "- total num requests: {}".format(self.total_num_req))
        utils.print_log("DEBUG", "- sum: {}".format(sum(dist)))
        utils.print_log("DEBUG", "- mean interval: {}".format(sum(dist)/len(dist)))
        utils.print_log("DEBUG", "- max interval: {}".format(max(dist)))
        utils.print_log("DEBUG", "- min interval: {}".format(min(dist)))
        utils.print_log("DEBUG", "="*40)
        return dist
    
def interval_to_arrival(req_intv):
    req_arrival = list()
    for i in range(len(req_intv)):
        if i == 0:
            req_arrival.append(req_intv[i])
        else:
            req_arrival.append(req_arrival[i-1] + req_intv[i])
    return req_arrival
    

def generate_workload(scenario_, base_rps):
    if scenario_ == "exp_burst_8x":
        #NOTE: HARDCODED!!!!
        duration = [120, 60, 60, 30, 60, 60, 300]
        # duration = [60, 30, 30, 30, 30, 30, 60]
        # duration = [10, 10, 10, 10, 10, 10, 10]
        wrk_part_0 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[0])
        wrk_part_1 = WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration[1])
        wrk_part_2 = WorkloadGenerator(req_per_sec=base_rps*4, total_sec=duration[2])
        wrk_part_3 = WorkloadGenerator(req_per_sec=base_rps*8, total_sec=duration[3])
        wrk_part_4 = WorkloadGenerator(req_per_sec=base_rps*4, total_sec=duration[4])
        wrk_part_5 = WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration[5])
        wrk_part_6 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[6])
        request_interval_list = list()
        request_interval_list += wrk_part_0.exponential_distribution()
        request_interval_list += wrk_part_1.exponential_distribution()
        request_interval_list += wrk_part_2.exponential_distribution()
        request_interval_list += wrk_part_3.exponential_distribution()
        request_interval_list += wrk_part_4.exponential_distribution()
        request_interval_list += wrk_part_5.exponential_distribution()
        request_interval_list += wrk_part_6.exponential_distribution()
        # plot_distribution(request_interval_list, "exponential request interval")
    
    elif scenario_ == "exp_normal_8x":
        #NOTE: HARDCODED!!!!
        duration = [120, 60, 60, 30, 60, 60, 300]
        # duration = [60, 30, 30, 30, 30, 30, 60]
        # duration = [10, 10, 10, 10, 10, 10, 10]
        wrk = WorkloadGenerator(req_per_sec=base_rps, total_sec=sum(duration))
        request_interval_list = wrk.exponential_distribution()
        
    elif scenario_ == "exp_burst_steep_8x":
        #NOTE: HARDCODED!!!!
        duration = [120, 60, 30, 60, 300]
        wrk_part_0 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[0])
        wrk_part_1 = WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration[1])
        wrk_part_2 = WorkloadGenerator(req_per_sec=base_rps*8, total_sec=duration[2])
        wrk_part_3 = WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration[3])
        wrk_part_4 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[4])
        request_interval_list = list()
        request_interval_list += wrk_part_0.exponential_distribution()
        request_interval_list += wrk_part_1.exponential_distribution()
        request_interval_list += wrk_part_2.exponential_distribution()
        request_interval_list += wrk_part_3.exponential_distribution()
        request_interval_list += wrk_part_4.exponential_distribution()
        # plot_distribution(request_interval_list, "exponential request interval")
    
    elif scenario_ == "exp_normal_steep_8x":
        #NOTE: HARDCODED!!!!
        duration = [120, 60, 30, 60, 300]
        wrk = WorkloadGenerator(req_per_sec=base_rps, total_sec=sum(duration))
        request_interval_list = wrk.exponential_distribution()
        
    elif scenario_ == "exp_burst_4x":
        #NOTE: HARDCODED!!!!
        duration = [120, 60, 30, 60, 300]
        wrk_part_0 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[0])
        wrk_part_1 = WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration[1])
        wrk_part_2 = WorkloadGenerator(req_per_sec=base_rps*4, total_sec=duration[2])
        wrk_part_3 = WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration[3])
        wrk_part_4 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[4])
        
        request_interval_list = list()
        request_interval_list += wrk_part_0.exponential_distribution()
        request_interval_list += wrk_part_1.exponential_distribution()
        request_interval_list += wrk_part_2.exponential_distribution()
        request_interval_list += wrk_part_3.exponential_distribution()
        request_interval_list += wrk_part_4.exponential_distribution()

    
    elif scenario_ == "exp_normal_4x":
        #NOTE: HARDCODED!!!!
        duration = [120, 60, 30, 60, 300]
        wrk = WorkloadGenerator(req_per_sec=base_rps, total_sec=sum(duration))
        request_interval_list = wrk.exponential_distribution()
    
    elif scenario_ == "exp_burst_2x":
        #NOTE: HARDCODED!!!!
        duration = [5, 5, 5]
        wrk_part_0 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[0])
        wrk_part_1 = WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration[1])
        wrk_part_2 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[2])
        request_interval_list = list()
        request_interval_list += wrk_part_0.exponential_distribution()
        request_interval_list += wrk_part_1.exponential_distribution()
        request_interval_list += wrk_part_2.exponential_distribution()
    
    elif scenario_ == "exp_normal_2x":
        #NOTE: HARDCODED!!!!
        duration = [5, 5, 5]
        wrk = WorkloadGenerator(req_per_sec=base_rps, total_sec=sum(duration))
        request_interval_list = wrk.exponential_distribution()
        
    elif scenario_ == "constant_burst_8x":
        #NOTE: HARDCODED!!!!
        # duration = [120, 60, 60, 30, 60, 60, 300]
        duration = [120, 30, 300]
        wrk_part_0 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[0])
        wrk_part_1 = WorkloadGenerator(req_per_sec=base_rps*10, total_sec=duration[1])
        wrk_part_2 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[2])

        request_interval_list = list()
        request_interval_list += wrk_part_0.constant_distribution()
        request_interval_list += wrk_part_1.constant_distribution()
        request_interval_list += wrk_part_2.constant_distribution()
        # request_interval_list += wrk_part_3.constant_distribution()
        # request_interval_list += wrk_part_4.constant_distribution()
        # request_interval_list += wrk_part_5.constant_distribution()
        # request_interval_list += wrk_part_6.constant_distribution()
        
    elif scenario_ == "constant_normal_8x":
        #NOTE: HARDCODED!!!!
        # duration = [120, 60, 60, 30, 60, 60, 300]
        duration = [120, 30, 300]
        wrk = WorkloadGenerator(req_per_sec=base_rps, total_sec=sum(duration))
        request_interval_list = wrk.constant_distribution()
        
    elif scenario_ == "constant_burst_2x":
        #NOTE: HARDCODED!!!!
        duration = [10, 10, 10]
        wrk_part_0 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[0])
        wrk_part_1 = WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration[1])
        wrk_part_2 = WorkloadGenerator(req_per_sec=base_rps, total_sec=duration[2])
        request_interval_list = list()
        request_interval_list += wrk_part_0.constant_distribution()
        request_interval_list += wrk_part_1.constant_distribution()
        request_interval_list += wrk_part_2.constant_distribution()
    
    elif scenario_ == "constant_normal_2x":
        #NOTE: HARDCODED!!!!
        duration = [10, 10, 10]
        wrk = WorkloadGenerator(req_per_sec=base_rps, total_sec=sum(duration))
        request_interval_list = wrk.constant_distribution()
        
    elif scenario_ == "start_altogether":
        request_interval_list = [0] * 10
    elif scenario_ == "constant_interval":
        request_interval_list = [0]
        request_interval_list += list([25] * 10)
    elif scenario_ == "single_cluster_test":
        duration = 30
        wrk_part = list()
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps*3, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps*4, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps*5, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps*4, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps*3, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps*2, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps, total_sec=duration))
        wrk_part.append(WorkloadGenerator(req_per_sec=base_rps, total_sec=duration))
        request_interval_list = list()
        for wrk in wrk_part:
            request_interval_list += wrk.constant_distribution()
    elif scenario_ == "empty":
        return [0]
    else:
        utils.print_log("ERROR", "Workload scenario " + str(scenario_) + " is not supported.")
    
    request_arrivals = interval_to_arrival(request_interval_list)
    return request_arrivals
