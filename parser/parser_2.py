import copy
import pandas as pd
import numpy as np
import os
import threading
import sys
import time
from utils import utils
        
def get_stat_list(msname_, li_):
    if np.average(li_) == -1:
        print("[ERROR]: li_ is invalid sum_list.")
    stat_list = list()
    stat_list.append(msname_)
    stat_list.append(msname_[:8])
    stat_list.append(np.average(li_))
    stat_list.append(np.std(li_))
    sorted_li = np.sort(li_, axis = 0)
    min_ = sorted_li[0]
    for elem in sorted_li:
        if elem != -1 and elem != 0:
            min_ = elem
            break
    stat_list.append(max(li_) - min_)
    if min_ == 0:
        stat_list.append(float('inf'))
    else:
        stat_list.append(max(li_)/min_)
    stat_list.append(max(li_)/np.percentile(li_, 1)) # max/p1
    stat_list.append(max(li_)/np.percentile(li_, 75)) # max/p75
    stat_list.append(max(li_)/np.percentile(li_, 50)) # max/p50
    stat_list.append(np.percentile(li_, 99)/np.percentile(li_, 50)) # p99/p50
    stat_list.append(max(li_)/np.average(li_)) # max/avg
    stat_list.append(np.percentile(li_, 99)/np.average(li_)) # p99/avg
    stat_list.append(min_)
    stat_list.append(max(li_))
    stat_list.append(np.percentile(li_, 1))
    stat_list.append(np.percentile(li_, 10))
    stat_list.append(np.percentile(li_, 25))
    stat_list.append(np.percentile(li_, 50))
    stat_list.append(np.percentile(li_, 75))
    stat_list.append(np.percentile(li_, 90))
    stat_list.append(np.percentile(li_, 99))
    stat_list.append(li_)
    return stat_list


if __name__ == "__main__":
    start = time.time()
    input_path = "merged_providerRPC_MCR.csv"
    print("Read merged providerRPC_MCR file, {}".format(input_path))
    result_df = pd.read_csv(input_path)
    unique_timestamp = result_df["timestamp"].unique().tolist()
    unique_timestamp.sort()
    temp_df_list = list()
    msname_list = result_df["msname"].unique()
    col = ["msname", "msname-8", "avg", "std", "max-min", "max/min", "max/p1", "max/p75", "max/p50", "p99/p50", "max/avg", "p99/avg", "min", "max", "p1", "p10", "p25", "p50", "p75", "p90", "p99"]
    col.append("li")
    svc_per_line_df = pd.DataFrame(columns=col)
    for msname in msname_list:
        temp_df = result_df[result_df["msname"] == msname]
        temp_df_list.append(temp_df)
        sum_list = temp_df["sum"].values.tolist()
        if np.average(sum_list) != -1 and max(sum_list) != 0:
            sum_list = [x for x in sum_list if x != 0]
            ms_stat = get_stat_list(msname, sum_list)
            svc_per_line_df.loc[len(svc_per_line_df.index)] = ms_stat
    svc_per_line_df = svc_per_line_df[svc_per_line_df["avg"] > 1]
    svc_per_line_df = svc_per_line_df[svc_per_line_df["min"] > 1]
    svc_per_line_df = svc_per_line_df.reset_index(drop = True)
    svc_per_line_df.to_csv("svc_per_line-x.csv")
    print("runtime: {}".format(time.time() - start))