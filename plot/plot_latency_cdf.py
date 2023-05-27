import os
import argparse
from datetime import datetime
import time
import numpy as np
from matplotlib import pyplot as plt
from matplotlib import colors as mcolors
import random
import sys

LCLB = "Local Load Balancing"
MCLB = "Multi-Cluster Load Balancing"
TE = "Service Layer TE"

##########################################################################################
''' Marker list '''
marker_list = ['d', 'o', 'x', 'v', '^', '<', '>', 's', '8', 'p']

''' Linewidth '''
lw = 5

''' Color list '''
## Manually set color list
# color_list = ["blue", "orange", "green", "purple", "brown", "#FBDD7E", "cyan", "#C20078", "#FAC205", "gray",  "#06C2AC", "#FFFF14", "#E6DAA6","olive", "#76FF7B", "pink", "#BC8F6", "#AAA662"]
## Tableau color list
color_dict = mcolors.TABLEAU_COLORS
color_list = list()
for name in color_dict:
    color_list.append(color_dict[name])

''' Line style '''
line_style_list = ["solid", "dashdot", "dashed", 'dotted']
##########################################################################################


def plot_latency_cdf(latency_dict_, cluster_name, path):
    fig = plt.figure()
    color_idx = 0
    ls_idx = 0
    for key in latency_dict_:
        x_0 = np.sort(latency_dict_[key])
        num_data_point_0 = len(latency_dict_[key])
        y_0 = np.arange(num_data_point_0) / float(num_data_point_0)
        plt.plot(x_0, y_0, label=key, color=color_list[color_idx], linewidth=lw, linestyle=line_style_list[ls_idx])
        color_idx += 1
        ls_idx += 1
    # plt.title("cluster", fontsize=30)
    plt.xlim(0, )
    plt.xlabel('Request latency(ms)', fontsize=30)
    plt.ylabel('CDF', fontsize=25)
    # plt.legend(loc="lower right", fontsize=14, frameon = False)
    plt.legend(loc="lower right", fontsize=16)
    plt.xticks(fontsize=20, rotation=45)
    plt.yticks(fontsize=20)
    plt.savefig(path+cluster_name+"-latency_cdf.pdf", dpi=100, bbox_inches='tight')
    plt.show()
    plt.close(fig)
    

def plot_tail_latency_cdf(latency_dict_, cluster_name, path):
    fig = plt.figure()     
    def find_tail_idx(data, tail):
        tail_idx = 0
        for i in range(len(data)):
            if data[i] >= 0.95:
                tail_idx = i
                break
        print("{}, tail_idx: {}".format("???", tail_idx))
        return tail_idx
    color_idx = 0
    ls_idx = 0
    for key in latency_dict_:
        num_data_point_0 = len(latency_dict_[key])
        y_0 = np.arange(num_data_point_0) / float(num_data_point_0)
        tail = 0.95
        tail_idx = find_tail_idx(y_0, tail)
        y_tail = y_0[tail_idx:]
        x_0 = np.sort(latency_dict_[key])
        x_tail = x_0[tail_idx:]
        plt.plot(x_tail, y_tail, label=key, color=color_list[color_idx], linewidth=lw, linestyle=line_style_list[ls_idx])
        color_idx += 1
        ls_idx += 1
    # plt.title("Bursty cluster", fontsize=30)
    plt.xlim(0, )
    plt.xlabel('Request latency(ms)', fontsize=30)
    plt.ylabel('CDF', fontsize=25)
    # plt.legend(loc="lower right", fontsize=14, frameon = False)
    plt.legend(loc="lower right", fontsize=16)
    plt.xticks(fontsize=20, rotation=45)
    plt.yticks(fontsize=20)
    plt.savefig(path+cluster_name+"-tail_latency_cdf.pdf", dpi=100, bbox_inches='tight')
    plt.show()
    plt.close(fig)


def parse_file(li):
    '''
    cluster_0
    three_depth
    6d9c26b9
    RoundRobin
    LCLB
    '''
    cluster_name = li.pop(0).strip()
    app = li.pop(0).strip()
    wrk = li.pop(0).strip()
    lb = li.pop(0).strip()
    routing_algorithm = li.pop(0).strip()
    latency = list()
    for elem in li:
        latency.append(float(elem.strip()))
    print("="*15)
    print(cluster_name)
    print(app)
    print(wrk)
    print(lb)
    print(routing_algorithm)
    print("="*15)
    return latency, cluster_name, app, wrk, lb, routing_algorithm


def print_statistics(latency_dict_):
    for key in latency_dict_:
        print("="*30)
        print(key)
        print("- Avg: {}".format(round(np.mean(latency_dict_[key]), 2)))
        print("- 50p: {}".format(round(np.percentile(latency_dict_[key], 50), 2)))
        print("- 90p: {}".format(round(np.percentile(latency_dict_[key], 90), 2)))
        print("- 95p: {}".format(round(np.percentile(latency_dict_[key], 95), 2)))
        print("- 99p: {}".format(round(np.percentile(latency_dict_[key], 99), 2)))
        print("- 99.9p: {}".format(round(np.percentile(latency_dict_[key], 99.9), 2)))
    

if __name__ == "__main__":
    file_list = list(sys.argv[1:])
    # print("file_list: ", file_list)
    for fname in file_list:
        try:
            assert fname.split(".")[-1] == "txt" and fname.split("/")[-1].split("-")[0] == "latency"
        except:
            print("invalid file name: {}".format(fname))
            exit()
    latency_dict = dict()
    for fname in file_list:
        file_ = open(fname, 'r')
        data = file_.readlines()
        latency, cluster_name, app, wrk, lb, routing_algorithm = parse_file(data)
        key = cluster_name + "-" + routing_algorithm + "-" + lb
        try:
            assert key not in latency_dict
        except:
            print("key already exists in latency_dict_: {}".format(key))
            exit()
        latency_dict[key] = latency
    
    ## PLOT CDF
    plot_latency_cdf(latency_dict, cluster_name, path="./")
    plot_tail_latency_cdf(latency_dict, cluster_name, path="./")
    
    ## PRINT STATISTICS
    print_statistics(latency_dict)