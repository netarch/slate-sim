import os
from datetime import datetime
import time
import numpy as np
from matplotlib import pyplot as plt
from matplotlib import colors as mcolors
import random
random.seed(1234)

def avg(li):
    if len(li) == 0:
        print("avg, !!! Empty list !!! return 0")
        return 0
    return sum(li)/len(li)

def current_epoch():
    return round(time.time()*1000)  # millisecond epoch

def uniform_random(low, high, size=1):
    return np.random.uniform(low, high, size)[0]

log_map = dict()
log_map["VERBOSE"] = 5
log_map["DEBUG"] = 4
log_map["INFO"] = 3
log_map["WARNING"] = 2
log_map["ERROR"] = 1
LOG_LEVEL = 2 # VERBOSE(5), DEBUG(4), INFO(3), WARNING(2), ERROR(1)

LOG_FILE_PATH = os.getcwd() + "/log/log_" + datetime.now().strftime("%Y%m%d_%H%M%S")

OUTPUT_FILE_PATH = os.getcwd() + "/output/output_" + datetime.now().strftime("%Y%m%d_%H%M%S")

def print_log(tag, msg, end="\n"):
    if log_map[tag] <= LOG_LEVEL:
        # print("[" + tag + "]: " + msg)
        print(msg, end=end)
        if tag == "ERROR":
            print("EXIT PROGRAM...")
            # exit()
            assert False
            

## marker list.
marker_list = ['d', 'o', 'x', 'v', '^', '<', '>', 's', '8', 'p']

####################
#### Color list ####
####################
## Method 1. I maunually create this list.
color_list = ["orange", "green", "purple", "brown", "cyan", "#C20078", "#FAC205", "gray","#FBDD7E", "#06C2AC", "#FFFF14", "#E6DAA6","olive", "#76FF7B", "pink", "#BC8F6", "#AAA662"]

## Method 3. tableau color list
# color_dict = mcolors.TABLEAU_COLORS
# color_list = list()
# for name in color_dict:
#     color_list.append(color_dict[name])
# print("len(color_list): ", len(color_list))
# print("type(color_list): ", type(color_list))

## Method 2. Matplot color list generator ##
# colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
# by_hsv = sorted((tuple(mcolors.rgb_to_hsv(mcolors.to_rgba(color)[:3])), name)
#                 for name, color in colors.items())
# sorted_names = [name for hsv, name in by_hsv]
# color_list = list()
# for name in sorted_names:
#     color_list.append(colors[name])
# random.shuffle(color_list)
# print(color_list)


def write_log(msg, log_level):
    f = open(LOG_FILE_PATH, "a")
    f.write(msg)
    f.close() #TODO: Performance issue: file open close everytime you write the log.

output_log = list()
column_names = list()

def print_queueing_time_statistics(dag_):
    percent = [1,5,10,25,50,75,90,95,99]
    print()
    print("queueing time statistics")
    print("replica, ", end="")
    for p_ in percent:
        print("p" + str(p_) + ", ", end="")
    print("avg, min, max")
    for repl in dag_.all_replica:
        li = list(repl.queueing_time.values())
        if len(li) > 0:
            print(repl.to_str() + ", ", end="")
            for p_ in percent:
                print("{}, ".format(round(np.percentile(li, p_)), 2), end="")
            print("{}, ".format(round(np.mean(li), 2)), end="")
            print("{}, ".format(round(np.min(li), 2)), end="")
            print("{}, ".format(round(np.max(li), 2)), end="\n")
    print()


def print_percentiles(li):
    print_log("DEBUG", "- Average: {}".format(round(np.mean(li), 2)))
    print_log("DEBUG", "- Min: {}".format(round(np.min(li), 2)))
    print_log("DEBUG", "- Max: {}".format(round(np.max(li), 2)))
    print_log("DEBUG", "- 50p: {}".format(round(np.percentile(li, 50), 2)))
    print_log("DEBUG", "- 90p: {}".format(round(np.percentile(li, 90), 2)))
    print_log("DEBUG", "- 95p: {}".format(round(np.percentile(li, 95), 2)))
    print_log("DEBUG", "- 99p: {}".format(round(np.percentile(li, 99), 2)))
    print_log("DEBUG", "- 99.9p: {}".format(round(np.percentile(li, 99.9), 2)))
    print_log("DEBUG", "="*30)
    print_log("DEBUG", "len(li): " + str(len(li)))
    print_log("DEBUG", "")

def plot_histogram(li, title):
    percentile = list()
    for i in range(100):
        percentile.append(np.percentile(li, i))
    plt.figure(figsize=(10,7))
    plt.title(title)
    plt.bar(range(len(percentile)), percentile)
    plt.show()
    


def plot_workload_histogram_2(req_arrival, title, ym=0):
    cur = 1000
    cnt_list = list()
    cnt_list.append(0)
    idx = 0
    for arr in req_arrival:
        if arr > cur:
            cur += 1000
            idx += 1
            cnt_list.append(0)
        cnt_list[idx] += 1
    
    plt.figure(figsize=(10,7))
    print_percentiles(cnt_list)
    ymax = max(cnt_list)
    if ym == 0:
        plt.ylim(0, ymax+50)
    else:
        plt.ylim(0, ym+50)
    ts = time.time()
    plt.bar(range(len(cnt_list)), cnt_list)
    print("bar plot: {}".format(time.time() - ts))
    plt.title(title)
    # plt.plot(cnt_list)
    ts = time.time()
    plt.show()
    print("plt.show: {}".format(time.time() - ts))
    return ymax
        
def plot_workload_histogram(req_arrival, title, x_max, y_max):
    # req_arrival = [ x/1000 for x in req_arrival ]
    req_arrival = [ x for x in req_arrival ]
    np_arr = np.array(req_arrival)
    fig, ax = plt.subplots(figsize =(10, 7))
    bins_ = list()
    
    # if len(req_arrival) > 1000000:
    #     num_bin = int(len(req_arrival)/10000)
    # elif len(req_arrival) > 100000:
    #     num_bin = int(len(req_arrival)/1000)
    if len(req_arrival) > 10000:
        # num_bin = int(len(req_arrival)/100)
        num_bin = int(req_arrival[-1]/1000)
        
    elif len(req_arrival) > 1000:
        num_bin = int(len(req_arrival)/10)
    elif len(req_arrival) > 100:
        num_bin = int(len(req_arrival)/5)
        
    bin_size = int(req_arrival[-1]/num_bin)
    bins_ = list()
    cur = req_arrival[0]
    for i in range(num_bin):
        bins_.append(cur + bin_size)
        cur += bin_size
    # ax.hist(np_arr, bins=bins_)
    y, x, _ = plt.hist(np_arr, bins=bins_)
    
    # if x_max > x.max():
    #     plt.xlim(xmin=6.5, xmax=x_max)
    # plt.ylim(ymin=0, ymax=200)
    
    plt.title(title)
    plt.xlabel("Time(second)")
    plt.ylabel("Num of requests")
    plt.show()
    return x.max, y.max

def plot_rps_and_capacity(c0_capa_ts, c0_capa_trend, c1_capa_ts, c1_capa_trend, title):
    # plt.plot(c0_capa_ts, c0_capa_trend, marker='x', color='black', linewidth=5, label='cluster 0 capacity')
    plt.plot(c0_capa_ts, c0_capa_trend, marker='x', label='cluster 0 capacity')
    plt.plot(c1_capa_ts, c1_capa_trend, marker='x', label='cluster 1 capacity')
    plt.show()

def plot_pdf(x0, y0, x1, y1, title, path):
    x0_sec = [ x/1000 for x in x0 ]
    x1_sec = [ x/1000 for x in x1 ]
    fig = plt.figure()
    plt.xlabel('Request in order')
    plt.ylabel('Request latency(ms)')
    plt.title(title)
    if len(y0) > 0:
        plt.plot(x0_sec, y0, color='red', label='bursty cluster')
    else:
        print_log("WARNING", "plot_pdf, y0 is empty")
    if len(y1) > 0:
        plt.plot(x1_sec, y1, color='blue', label='normal cluster')
    else:
        print_log("WARNING", "plot_pdf, y1 is empty")
        
    
def plot_distribution(list_, title):
    plt.figure(figsize=(10,7))
    plt.xlabel('Request arrival order')
    plt.ylabel('Request arrival interval distribution(ms)')
    plt.title(title)
    # plt.plot(list_, color='red', marker='.', label='PDF')
    plt.plot(list_)
    plt.show()
    
    
    
###########################################################################################
def plot_cdf(u0_latency, u1_latency, title, path):
    # X-axis is the actual value in CDF graph.
    x_0 = np.sort(u0_latency)
    x_1 = np.sort(u1_latency)
    
    num_data_point_0 = len(u0_latency)
    num_data_point_1 = len(u1_latency)
    
    y_0 = np.arange(num_data_point_0) / float(num_data_point_0)
    y_1 = np.arange(num_data_point_1) / float(num_data_point_1)
    
    fig = plt.figure()
    plt.xlabel('Request latency(ms)')
    plt.ylabel('CDF')
    plt.title(title)
    # plt.plot(x_1, y_1, color='black' , label='CDF bursty+normal cluster')
    if len(x_0) > 0:
        plt.plot(x_0, y_0, color='red', label='bursty cluster')
    else:
        print_log("WARNING", "plot_cdf, u0_latency is empty")
    if len(x_1) > 0:
        plt.plot(x_1, y_1, color='blue', label='normal cluster')
    else:
        print_log("WARNING", "plot_cdf, u1_latency is empty")
    plt.legend(loc="lower right")
    
    # plt.savefig(path, dpi=100, bbox_inches='tight')
    plt.show()
    
    plt.close(fig)
    
###########################################################################################
def plot_workload_histogram_with_autoscaling(req_arrival, capacity, title, ylim, path):
    capa_ts = dict()
    capa_trend = dict()
    for service in capacity:
        capa_ts[service] = [ x[0]/1000 for x in capacity[service]]
        capa_trend[service] = [ x[1] for x in capacity[service]]
        
    plt.figure(figsize=(12,7))
    ## Mimicing histogram with count!
    # Accumulate the number of arrival every 1000ms (1sec)
    cur = 1000 # 1000ms, 1sec
    cnt_list = list()
    cnt_list.append(0)
    idx = 0
    for arr in req_arrival:
        if arr > cur: # Every 1000ms
            cur += 1000
            idx += 1
            cnt_list.append(0)
        cnt_list[idx] += 1
    plt.bar(range(len(cnt_list)), cnt_list)
    
    plt.ylim(0, ylim)
    
    marekr_idx = 0
    for service in capa_ts:
        if service.name.find("User") >= 0:
            print_log("INFO", "plot_workload_histogram_with_autoscaling, This plot will skip User service.")
        else:
            plt.plot(capa_ts[service], capa_trend[service], marker=marker_list[marekr_idx],  markersize=5, color=color_list[marekr_idx], label="Capacity service " + service.name)
            marekr_idx += 1
    
    plt.legend(loc="upper right")
    plt.title(title)
    # plt.plot(cnt_list)
    plt.savefig(path, dpi=100, bbox_inches='tight')
    # plt.show()
    plt.close()
    # return ymax
###########################################################################################
    

    
###########################################################################################
def plot_pdf_with_autoscaling(x0, y0, x1, y1, c0_autoscale_ts, c1_autoscale_ts, title, path):
    x0_sec = [ x/1000 for x in x0 ]
    x1_sec = [ x/1000 for x in x1 ]
    c0_auto_ts = dict()
    c1_auto_ts = dict()
    for service in c0_autoscale_ts:
        c0_auto_ts[service] = [ x/1000 for x in c0_autoscale_ts[service] ]
        c1_auto_ts[service] = [ x/1000 for x in c1_autoscale_ts[service] ]
    
    fig = plt.figure()
    
    # Cluster 0
    if len(y0) > 0:
        plt.plot(x0_sec, y0, color='red', label='bursty cluster', linewidth=0.5, alpha=0.8)
    else:
        print_log("WARNING", "plot_pdf, y0 is empty")
        
    # Cluster 1
    if len(y1) > 0:
        plt.plot(x1_sec, y1, color='blue', label='normal cluster', linewidth=0.5, alpha=0.8)
    else:
        print_log("WARNING", "plot_pdf, y1 is empty")
        
    # Cluster 0 Autoscaling
    color_idx = 0
    
    for service in c0_auto_ts:
        if service.name.find("User") >= 0:
            print_log("INFO", "plot_pdf_with_autoscaling, This plot will skip User service.")
        elif len(c0_auto_ts[service]) > 0:
            first_label_flag=True
            for ts in c0_auto_ts[service]:
                if first_label_flag:
                    plt.axvline(x = ts, label=service.name+"(cluster 0)", color=color_list[color_idx], linewidth=1)
                else:
                    plt.axvline(x = ts, color=color_list[color_idx], linewidth=1)
                first_label_flag=False
            color_idx += 1
            
    # Cluster 1 Autoscaling
    for service in c1_auto_ts:
        if service.name.find("User") >= 0:
            print_log("INFO", "plot_pdf_with_autoscaling, This plot will skip User service.")
        elif len(c0_auto_ts[service]) > 0:
            first_label_flag=True
            for ts in c1_auto_ts[service]:
                if first_label_flag:
                    plt.axvline(x = ts, label=service.name+"(cluster 1)", color=color_list[color_idx], linewidth=1)
                else:
                    plt.axvline(x = ts, color=color_list[color_idx], linewidth=1)
                first_label_flag=False
            color_idx += 1
        
    plt.xlabel('Request in order')
    plt.ylabel('Request latency(ms)')
    plt.title(title)
    plt.legend(loc="upper right")
    plt.savefig(path, dpi=100, bbox_inches='tight')
    # plt.show()
    plt.close(fig)
###########################################################################################