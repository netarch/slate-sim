import os
import pandas as pd
import sys
import time
import numpy as np

        
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
    ts0 = time.time()
    csv_files_dir = sys.argv[1]
    input_files = os.listdir(csv_files_dir)
    input_files = [x for x in input_files if x.split(".")[-1] == "csv" and  x.split("-")[0] == "providerRPC_MCR"]
    # print(input_files)
    frames = list()
    for inp in  input_files:
        inp_path = csv_files_dir + "/" + inp
        temp_df = pd.read_csv(inp_path)
        frames.append(temp_df)
        print("{} read done".format(inp))
    merged_df = pd.concat(frames)
    merged_df = merged_df.drop('Unnamed: 0', axis=1)
    df_list = list()
    msname_list = merged_df["msname"].unique()
    msname_list.sort()
    for msname in msname_list:
        temp = merged_df[merged_df["msname"] == msname]
        temp = temp.sort_values(by=["timestamp"], ascending=True)
        df_list.append(temp)
    print("sort done: {}s".format(int(time.time() - ts0)))
    result_df = pd.concat(df_list)
    result_df = result_df.reset_index(drop=True)
    msname_list = result_df["msname"].unique()
    result_df.to_csv("merged_providerRPC_MCR.csv")
    print("file write done")
    print("merge done: {}s".format(int(time.time() - ts0)))