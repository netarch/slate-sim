#!/usr/bin/env python
# coding: utf-8

import sys
sys.dont_write_bytecode = True

import time
import numpy as np  
import pandas as pd
import datetime
import graphviz
import gurobipy as gp
from gurobipy import GRB
sys.path.append("simulator")
from simulator import simulator as sim
import random
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.compose import make_column_transformer
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from sklearn.metrics import r2_score
import gurobipy_pandas as gppd
from gurobi_ml import add_predictor_constr
import matplotlib.pyplot as plt
import argparse

OUTPUT_DIR = "./optimizer_output/"

random.seed(1234)

VERBOSITY=0
GRAPHVIZ=False
OUTPUT_WRITE=False

DUMMY_REGRESSOR=True

timestamp_list = list()
temp_timestamp_list = list()
def LOG_TIMESTAMP(event_name):
    timestamp_list.append([event_name, time.time()])
    if len(timestamp_list) > 1:
        print("Finished, " + event_name + ", duration, " + str(round(timestamp_list[-1][1] - timestamp_list[-2][1], 5)))

def TEMP_LOG_TIMESTAMP(event_name):
    temp_timestamp_list.append([event_name, time.time()])
    if len(temp_timestamp_list) > 1:
        print("Finished, " + event_name + ", duration, " + str(round(timestamp_list[-1][1] - timestamp_list[-2][1], 5)))
        
def prettyprint_timestamp():
    print()
    print("*"*30)
    print("** timestamp_list(ms)")
    for i in range(1, len(timestamp_list)):
        print(timestamp_list[i][0], end=",")
    print()    

def print_timestamp():
    for i in range(1, len(timestamp_list)):
        print(round((timestamp_list[i][1] - timestamp_list[i-1][1]), 5), end=",")
    print()
    # print("*"*30)
        
def print_log(msg, obj=None):
    if VERBOSITY >= 1:
        if obj == None:
            print("[LOG] ", end="")
            print(msg)
        else:
            print("[LOG] ", end="")
            print(msg, obj)
        
def print_error(msg):
    exit_time = 5
    print("[ERROR] " + msg)
    print("EXIT PROGRAM in")
    for i in reversed(range(exit_time)) :
        print("{} seconds...".format(i))
        time.sleep(1)
    exit()

def calc_num_svc_of_general_tree(flags):
    def calc_num_svc_within_depth(fod, nochild, dep):
        if dep == 0 or dep == 1:
            return 1
        proliferation_degree = fod - nochild
        return pow(proliferation_degree, dep-2) * fod
    num_svc = list()
    for d in range(flags.depth):
        num_svc.append(calc_num_svc_within_depth(flags.fan_out_degree, flags.no_child_constant, d))
    print("num svc in each depth: ", num_svc)
    print("total num svc per cluster: ", sum(num_svc))

def argparse_add_argument(parser):
    def list_of_ints(arg):
        return list(map(int, arg.split(',')))
    parser.add_argument("--application", type=str, default="three_depth", help="the name of the target app", choices=["linear", "three_depth", "general_tree"], required=True)
    parser.add_argument("--routing_algorithm", type=str, default="SLATE", choices=["LCLB", "MCLB", "heuristic_TE", "SLATE"])
    parser.add_argument("--NUM_CLUSTER", type=int, required=True)
    parser.add_argument('--NUM_REQUEST', type=list_of_ints, default=None)
    parser.add_argument("--depth", type=int, default=3)
    parser.add_argument("--fan_out_degree", type=int, default=3)
    parser.add_argument("--no_child_constant", type=int, default=0)
    
    parser.add_argument("--regressor_degree", type=int, default=1)
    
def check_argument(flags):
    assert len(flags.NUM_REQUEST) == flags.NUM_CLUSTER
    assert flags.no_child_constant <= flags.fan_out_degree
    assert flags.depth >= 2
    
def print_argument(flags):
    print_log("=============================================================")
    print_log("======================= argument ============================")
    print_log("=============================================================")
    for key, value in vars(flags).items():
        print_log("** {}: {}".format(key, str(value)))
    print_log("=============================================================")
    
if __name__ == "__main__":
    # print_error("test")
    
    LOG_TIMESTAMP("main function start")
    parser = argparse.ArgumentParser()
    argparse_add_argument(parser)
    flags, unparsed = parser.parse_known_args()
    if flags.NUM_REQUEST == None:
        flags.NUM_REQUEST = list()
        for _ in range(flags.NUM_CLUSTER):
            flags.NUM_REQUEST.append(10000)
    print_argument(flags)
    check_argument(flags)
    LOG_TIMESTAMP("argparse")
    if flags.application == "linear":
        sim.linear_topology_application()
    elif flags.application == "three_depth":
        sim.three_depth_application(flags.NUM_CLUSTER)
    elif flags.application == "general_tree":
        no_child_constant = 1
        assert(no_child_constant < flags.fan_out_degree)
        replica_for_cluster, total_num_svc_in_each_depth = sim.general_tree_application(flags.fan_out_degree, flags.no_child_constant, flags.depth, flags.NUM_CLUSTER)
        calc_num_svc_of_general_tree(flags)
    else:
        print_error("Unsupported application, ", flags.application)
        exit()
    LOG_TIMESTAMP("load simulator app")
    
    ## print dag
    # sim.dag.graph
    # sim.dag.print_dependency()

    ## print replica by cluster
    # for repl in sim.dag.all_replica:
    #     if repl.service.name == "User":
    #         print()
    #     print(repl.name, end=", ")
    # print()

    svc_name_list = list()
    compute_arc_var_name = list()
    per_service_compute_arc = dict()
    for repl in sim.dag.all_replica:
        # if repl.service.name != "User":
        var_name = repl.name+sim.DELIMITER+"start", repl.name+sim.DELIMITER+"end"
        compute_arc_var_name.append(var_name)
        if repl.service.name not in per_service_compute_arc:
            per_service_compute_arc[repl.service.name] = list()
        per_service_compute_arc[repl.service.name].append(var_name)
        svc_name_list.append(repl.service.name)

    LOG_TIMESTAMP("defining compute_arc_var_name")

    network_arc_loop_cnt = 0
    exist_check_cnt = 0
    ## Define names of the variables for network arc in gurobi
    source_node = "src_*_*"+sim.DELIMITER+"*"+sim.DELIMITER+"*"
    destination_node = "dst_*_*"+sim.DELIMITER+"*"+sim.DELIMITER+"*"
    network_arc_var_name = list()
    var_name_time = 0
    append_time = 0
    exist_time = 0
    for parent_repl in sim.dag.child_replica:
        for child_svc in sim.dag.child_replica[parent_repl]:
            child_repl_list = sim.dag.child_replica[parent_repl][child_svc]
            for child_repl in child_repl_list:
                network_arc_loop_cnt += 1
                if parent_repl.service.name == "User":
                    ts = time.time()
                    var_name = ("src_*_*"+sim.DELIMITER+"*"+sim.DELIMITER+"*", parent_repl.name+sim.DELIMITER+"start")
                    var_name_time += (time.time() - ts)
                    ts = time.time()
                    exist = var_name in network_arc_var_name
                    exist_time += (time.time() - ts)
                    if exist == False:
                    # if var_name not in network_arc_var_name:
                        ts = time.time()
                        network_arc_var_name.append(var_name)
                        append_time += (time.time() - ts)
                    else:
                        exist_check_cnt += 1
                    ts = time.time()
                    var_name = (parent_repl.name+sim.DELIMITER+"end", child_repl.name+sim.DELIMITER+"start")
                    var_name_time += (time.time() - ts)
                    ts = time.time()
                    exist = var_name in network_arc_var_name
                    exist_time += (time.time() - ts)
                    if exist == False:
                    # if var_name not in network_arc_var_name:
                        ts = time.time()
                        network_arc_var_name.append(var_name)
                        append_time += (time.time() - ts)
                    else:
                        exist_check_cnt += 1
                else:
                    ts = time.time()
                    var_name = (parent_repl.name+sim.DELIMITER+"end",  child_repl.name+sim.DELIMITER+"start")
                    var_name_time += (time.time() - ts)
                    ts = time.time()
                    exist = var_name in network_arc_var_name
                    exist_time += (time.time() - ts)
                    if exist == False:
                    # if var_name not in network_arc_var_name:
                        ts = time.time()
                        network_arc_var_name.append(var_name)
                        append_time += (time.time() - ts)
                    else:
                        exist_check_cnt += 1
                if sim.dag.is_leaf(child_repl.service):
                    ts = time.time()
                    var_name = (child_repl.name+sim.DELIMITER+"end", "dst_*_*"+sim.DELIMITER+"*"+sim.DELIMITER+"*")
                    var_name_time += (time.time() - ts)
                    ts = time.time()
                    exist = var_name in network_arc_var_name
                    exist_time += (time.time() - ts)
                    if exist == False:
                    # if var_name not in network_arc_var_name:
                        ts = time.time()
                        network_arc_var_name.append(var_name)
                        append_time += (time.time() - ts)
                    else:
                        exist_check_cnt += 1
    print("APPEND TIME, "+str(append_time))
    print("VAR_NAME TIME, "+str(var_name_time))
    print("EXIST TIME, "+str(exist_time))
    print("EXIST CNT, "+str(exist_check_cnt))
    LOG_TIMESTAMP("defining network_arc_var_name("+str(network_arc_loop_cnt)+")")

    assert len(flags.NUM_REQUEST) == flags.NUM_CLUSTER
    TOTAL_NUM_REQUEST = sum(flags.NUM_REQUEST)
    true_function_degree = 1 # 1: linear, >2: polynomial
    regressor_degree = flags.regressor_degree # 1: linear, >2: polynomial
    
    ###################################################
    # num_data_point = TOTAL_NUM_REQUEST ## It is related to max throughput(service capacity) constraint
    num_data_point = 10 ## NOTE: 10 is not actual number of observed data point.
    ###################################################
    
    x_rps = list()
    x_service_name = list()
    y_compute_time = list()
    x_repl_name = list()
    ts = time.time()
    print("len(compute_arc_var_name): ", len(compute_arc_var_name))
    for i in range(len(compute_arc_var_name)):
        x_rps += list(np.arange(0,num_data_point))
        x_repl_name += [compute_arc_var_name[i]] * num_data_point
        x_service_name += [svc_name_list[i]] * num_data_point
        for j in range(num_data_point):
            svc_name = compute_arc_var_name[i][0].split(sim.DELIMITER)[0]
            slope = abs(hash(svc_name)%10)+1
            y_intercept = 5
            if svc_name == "User":
                y_compute_time.append(0)
            else:
                # y_compute_time.append(pow(x_rps[j],true_function_degree)*slope + y_intercept)
                y_compute_time.append(pow(x_rps[j],true_function_degree)*slope + y_intercept)
    LOG_TIMESTAMP("creating fake observation data point") ## slow
    ts = time.time()
    compute_time_observation = pd.DataFrame(
        data={
            "service_name": x_service_name,
            "rps": x_rps, 
            "compute_time": y_compute_time,
        },
        index=x_repl_name
    )
    LOG_TIMESTAMP("creating compute_time_observation dataframe") ## slow

    ## Per-service rps-to-compute time modeling.
    unique_service_name = list(compute_time_observation["service_name"].unique())
    # unique_service_name.remove("User")
    print_log("unique_service_name: ", unique_service_name)

    max_compute_time = dict()
    regressor_dict = dict()
    
    dummy_svc = unique_service_name[0]
    if DUMMY_REGRESSOR:
        temp_df = compute_time_observation[compute_time_observation["service_name"] == dummy_svc]
        X = temp_df[["rps"]]
        y = temp_df["compute_time"]
        for svc_name in unique_service_name:
            max_compute_time[svc_name] = max(y)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, train_size=0.8, random_state=1
        )
        feat_transform = make_column_transformer(
            (StandardScaler(), ["rps"]),
            # ("passthrough", ["rps"]),
            verbose_feature_names_out=False,
            remainder='drop'
        )
        if regressor_degree == 1:
            regressor_dict[dummy_svc] = make_pipeline(feat_transform, LinearRegression())
            regressor_dict[dummy_svc].fit(X_train, y_train)
        elif regressor_degree > 1:
            poly = PolynomialFeatures(degree=regressor_degree, include_bias=True)
            regressor_dict[dummy_svc] = make_pipeline(feat_transform, poly, LinearRegression())
            regressor_dict[dummy_svc].fit(X_train, y_train)
        ## Run prediction and compare it with the ground truth to see how accurate the trained model is
        y_pred = regressor_dict[dummy_svc].predict(X_test)
    else:
        for svc_name in unique_service_name:
            temp_df = compute_time_observation[compute_time_observation["service_name"] == svc_name]
            # X = temp_df[["service_name", "rps"]]
            X = temp_df[["rps"]]
            y = temp_df["compute_time"]
            max_compute_time[svc_name] = max(y)
            # print_log("max(y): ", max(y))
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, train_size=0.8, random_state=1
            )
            feat_transform = make_column_transformer(
                (StandardScaler(), ["rps"]),
                # ("passthrough", ["rps"]),
                verbose_feature_names_out=False,
                remainder='drop'
            )
            
            if regressor_degree == 1:
                regressor_dict[svc_name] = make_pipeline(feat_transform, LinearRegression())
                regressor_dict[svc_name].fit(X_train, y_train)
            elif regressor_degree > 1:
                poly = PolynomialFeatures(degree=regressor_degree, include_bias=True)
                regressor_dict[svc_name] = make_pipeline(feat_transform, poly, LinearRegression())
                regressor_dict[svc_name].fit(X_train, y_train)
            ## Run prediction and compare it with the ground truth to see how accurate the trained model is
            y_pred = regressor_dict[svc_name].predict(X_test)
            print_log("Service {}".format(svc_name))
            print_log("- Coef: {}".format(regressor_dict[svc_name]["linearregression"].coef_))
            print_log(f"- R^2: {np.round(r2_score(y_test, y_pred),5)}")
    LOG_TIMESTAMP("train regression model")


    idx = 0
    min_rps = 0
    # max_rps = max(x_rps)
    max_rps = sum(flags.NUM_REQUEST)
    min_network_egress_cost = list()
    max_network_egress_cost = list()
    for src_repl, dst_repl in network_arc_var_name:
        src_svc_name = src_repl.split(sim.DELIMITER)[0] # A
        dst_svc_name = dst_repl.split(sim.DELIMITER)[0] # B
        # print_log("src_svc_name:{}, dst_svc_name:{}".format(src_svc_name, dst_svc_name))
        if src_svc_name == "src_*_*":
            min_network_egress_cost.append(0)
            max_network_egress_cost.append(0)
        elif dst_svc_name == "dst_*_*":
            min_network_egress_cost.append(0)
            max_network_egress_cost.append(0)
        else:
            # print_log(src_svc_name)
            # print_log(src_repl.split("-"))
            try:
                # svc_name: [name]_[depth]_[local_id]
                # [svc_name]-[cluster id]_[start or end]svc_0_0-0_start
                src_idx = int(src_repl.split(sim.DELIMITER)[1])
            except:
                print_log(src_svc_name, src_repl, src_repl.split(sim.DELIMITER))
            try:
                dst_idx = int(dst_repl.split(sim.DELIMITER)[1])
            except:
                print_log(dst_svc_name, dst_repl, dst_repl.split(sim.DELIMITER))
            # local routing
            if src_idx % flags.NUM_CLUSTER == dst_idx % flags.NUM_CLUSTER:
                # print_log("local routing: src_svc_name:{}, dst_svc_name:{}".format(src_svc_name, dst_svc_name))
                min_network_egress_cost.append(0) # no egress charge on local routing
                max_network_egress_cost.append(0)
            # remote routing
            else:
                # print_log("remote routing: src_svc_name:{}, dst_svc_name:{}".format(src_svc_name, dst_svc_name))
                min_network_egress_cost.append(sim.dag.weight_graph[src_svc_name][dst_svc_name])
                max_network_egress_cost.append(sim.dag.weight_graph[src_svc_name][dst_svc_name])
    
    network_egress_cost_data = pd.DataFrame(
        data={
            "min_network_egress_cost": min_network_egress_cost,
            "max_network_egress_cost": max_network_egress_cost,
            # "min_rps":[min_rps]*len(network_arc_var_name),
            # "max_rps":[max_rps]*len(network_arc_var_name),
        },
        index=network_arc_var_name
    )

    min_compute_egress_cost = list()
    max_compute_egress_cost = list()
    for src_repl, dst_repl in compute_arc_var_name:
        # compute edge does not involve any networking
        min_compute_egress_cost.append(0)
        max_compute_egress_cost.append(0)
                    
    compute_egress_cost_data = dict()
    for svc_name in unique_service_name:
        compute_egress_cost_data[svc_name] = pd.DataFrame(
            data={
                # "min_rps":[min_rps] * len(per_service_compute_arc[svc_name]),
                # "max_rps":[max_rps] * len(per_service_compute_arc[svc_name]),
                "min_compute_egress_cost": [0] * len(per_service_compute_arc[svc_name]),
                "max_compute_egress_cost": [0] * len(per_service_compute_arc[svc_name]),
            },
            index=per_service_compute_arc[svc_name]
        )

    ## Duplicate
    # min_rps = 0
    # max_rps = max(x_rps)
    compute_time_data = dict()
    for svc_name in unique_service_name:
        compute_time_data[svc_name] = pd.DataFrame(
            data={
                "min_rps":[min_rps] * len(per_service_compute_arc[svc_name]),
                "max_rps":[max_rps] * len(per_service_compute_arc[svc_name]),
                "min_compute_time": [0] * len(per_service_compute_arc[svc_name]),
                "max_compute_time": [max_compute_time[svc_name]] * len(per_service_compute_arc[svc_name]),
            },
            index=per_service_compute_arc[svc_name]
        )


    min_network_latency = list()
    max_network_latency = list()
    for src_repl, dst_repl in network_arc_var_name:
        src_svc_name = src_repl.split(sim.DELIMITER)[0]
        dst_svc_name = dst_repl.split(sim.DELIMITER)[0]
        if src_svc_name == "src_*_*":
            min_network_latency.append(0)
            max_network_latency.append(0)
        elif dst_svc_name == "dst_*_*":
            min_network_latency.append(0)
            max_network_latency.append(0)
        else:
            try:
                src_idx = int(src_repl.split(sim.DELIMITER)[1])
            except:
                print_error(src_svc_name, src_repl.split(sim.DELIMITER))
            try:
                dst_idx = int(dst_repl.split(sim.DELIMITER)[1])
            except:
                print_error(dst_svc_name, dst_repl.split(sim.DELIMITER))
            # Network latency for local routing
            if src_idx % flags.NUM_CLUSTER == dst_idx % flags.NUM_CLUSTER:
                min_network_latency.append(sim.NetworkLatency().same_rack)
                max_network_latency.append(sim.NetworkLatency().same_rack)
            # Network latency for remote routing
            else:
                min_network_latency.append(sim.NetworkLatency().far_inter_region)
                max_network_latency.append(sim.NetworkLatency().far_inter_region)

    network_latency_data = pd.DataFrame(
        data={
            # "service_name":network_arc_var_name,
            "min_rps":[min_rps]*len(network_arc_var_name),
            "max_rps":[max_rps]*len(network_arc_var_name),
            "min_network_latency": min_network_latency,
            "max_network_latency": max_network_latency,
        },
        index=network_arc_var_name
    )
    LOG_TIMESTAMP("creating egress cost and compute/network latency dataframe")

    ## Optimizer runtime timestamp, start time
    optimizer_start_time = time.time()
    model = gp.Model('RequestRouting')
    # Add variables for the regression
    compute_time = dict()
    compute_rps = dict()
    # for svc_name in unique_service_name:
    #     print_log("svc_name, ", svc_name)

    for svc_name in unique_service_name:
        #ValueError: Index contains duplicate entries, cannot create variables
        print_log(svc_name)
        compute_time[svc_name] = gppd.add_vars(model, compute_time_data[svc_name], name="compute_time", lb="min_compute_time", ub="max_compute_time")
        compute_rps[svc_name] = gppd.add_vars(model, compute_time_data[svc_name], name="rps_for_compute_edge", lb="min_rps", ub="max_rps")
    # model.update()

    m_feats = dict()
    for svc_name in unique_service_name:
        m_feats[svc_name] = pd.DataFrame(
            data={
                "rps": compute_rps[svc_name],
            },
            index=per_service_compute_arc[svc_name]
        )
        #################################################################################################3
        ## Currently train linear regressor is the bottleneck.
        ## For now, to avoid it for the faster execution, we will train only one linear regressor and use it for all services.
        if DUMMY_REGRESSOR:
            pred_constr = add_predictor_constr(model, regressor_dict[dummy_svc], m_feats[svc_name], compute_time[svc_name])
        else:
            pred_constr = add_predictor_constr(model, regressor_dict[svc_name], m_feats[svc_name], compute_time[svc_name])
        #################################################################################################3
        if svc_name == "A":
            pred_constr.print_stats()
    # model.update()

    network_latency = gppd.add_vars(model, network_latency_data, name="network_latency", lb="min_network_latency", ub="max_network_latency")
    network_rps = gppd.add_vars(model, network_latency_data, name="rps_for_network_edge", lb="min_rps", ub="max_rps")
    # model.update()

    network_egress_cost = gppd.add_vars(model, network_egress_cost_data, name="network_egress_cost", lb="min_network_egress_cost", ub="max_network_egress_cost")

    compute_egress_cost = dict()
    for svc_name in unique_service_name:
        compute_egress_cost[svc_name] = gppd.add_vars(model, compute_egress_cost_data[svc_name], name="compute_egress_cost", lb="min_compute_egress_cost", ub="max_compute_egress_cost")
    # model.update()

    # egress cost sum
    network_egress_cost_sum = sum(network_egress_cost.multiply(network_rps))
    for svc_name in unique_service_name:
        compute_egress_cost_sum = sum(compute_egress_cost[svc_name].multiply(compute_rps[svc_name]))
    total_egress_sum = network_egress_cost_sum + compute_egress_cost_sum
    print_log("total_egress_sum: ", total_egress_sum)

    # total latency sum
    network_latency_sum = sum(network_latency.multiply(network_rps))
    for svc_name in unique_service_name:
        compute_latency_sum = sum(compute_time[svc_name].multiply(m_feats[svc_name]["rps"])) # m_feats[svc_name]["rps"] is identical to compute_rps[svc_name]
    total_latency_sum = network_latency_sum + compute_latency_sum
    print_log("total_latency_sum: ", total_latency_sum)

    objective = "latency" # latency or egress_cost or multi-objective
    if objective == "latency":
        model.setObjective(total_latency_sum, gp.GRB.MINIMIZE)
    elif objective == "egress_cost":
        model.setObjective(total_egress_sum, gp.GRB.MINIMIZE)
    elif objective == "multi-objective":
        model.setObjective(total_latency_sum*50 + total_egress_sum*50, gp.GRB.MINIMIZE)
    else:
        print_error("unsupported objective, ", objective)
        
    # model.update()
    print_log("model objective")
    print_log(type(model.getObjective()))
    print_log(model.getObjective())

    # arcs is the keys
    # aggregated_rps is dictionary
    concat_compute_rps = pd.Series()
    for svc_name, c_rps in compute_rps.items():
        concat_compute_rps = pd.concat([concat_compute_rps, compute_rps[svc_name]], axis=0)
    arcs, aggregated_rps = gp.multidict(pd.concat([network_rps, concat_compute_rps], axis=0).to_dict())
    
    ###################################################
    node = dict()
    max_tput = TOTAL_NUM_REQUEST
    for repl in sim.dag.all_replica:
        if repl.service.name != "User":
            node[repl.name+sim.DELIMITER+"start"] = max_tput
            node[repl.name+sim.DELIMITER+"end"] = max_tput
    print_log(node)
    LOG_TIMESTAMP("gurobi add_vars and set objective")

    ###################################################
    ###################################################
    constraint_setup_start_time = time.time()
    ###################################################
    # Constraint 1: source
    source = dict()
    source[source_node] = TOTAL_NUM_REQUEST

    destination = dict()
    destination[destination_node] = TOTAL_NUM_REQUEST

    src_keys = source.keys()
    src_flow = model.addConstrs((gp.quicksum(aggregated_rps.select(src, '*')) == source[src] for src in src_keys), name="source")
    # per clustrer load constraint
    for repl in sim.dag.all_replica:
        if repl.service.name == "User":
            start_node = repl.name+sim.DELIMITER+"start"
            end_node = repl.name+sim.DELIMITER+"end"
            cid = int(repl.name.split(sim.DELIMITER)[1])
            print_log("gp.quicksum(aggregated_rps.select('*', start_node): ", gp.quicksum(aggregated_rps.select('*', start_node)))
            print_log("cid: ", cid)
            per_cluster_load_in = model.addConstr((gp.quicksum(aggregated_rps.select('*', start_node)) == flags.NUM_REQUEST[cid]), name="cluster_"+str(cid)+"_load_in")
            per_cluster_load_out = model.addConstr((gp.quicksum(aggregated_rps.select(end_node, '*')) == flags.NUM_REQUEST[cid]), name="cluster_"+str(cid)+"_load_out")
    # model.update()

    ###################################################
    # Constraint 2: destination
    dest_keys = destination.keys()
    num_leaf_svc = 0
    leaf_svc = list()
    for svc in sim.dag.all_service:
        # print_log("iterate ", svc.name)
        if sim.dag.is_leaf(svc):
            num_leaf_svc += 1
            leaf_svc.append(svc.name)
    print_log("leaf_svc: ", leaf_svc)
    print_log("num_leaf_svc: ", num_leaf_svc)
    dst_flow = model.addConstrs((gp.quicksum(aggregated_rps.select('*', dst)) == destination[dst]*num_leaf_svc for dst in dest_keys), name="destination")
    # model.update()

    ###################################################
    # Constraint 3: flow conservation
    for repl in sim.dag.all_replica:
        # if repl.service.name != "User":
        # Start node in-out flow conservation
        st = repl.name+sim.DELIMITER+"start"
        node_flow = model.addConstr((gp.quicksum(aggregated_rps.select('*', st)) == gp.quicksum(aggregated_rps.select(st, '*'))), name="flow_conservation["+st+"]")
        # End node in-out flow conservation
        en = repl.name+sim.DELIMITER+"end"
        if sim.dag.is_leaf(repl.service):
            node_flow = model.addConstr((gp.quicksum(aggregated_rps.select('*', en)) == gp.quicksum(aggregated_rps.select(en, '*'))), name="flow_conservation["+en+"]")
        else:
            for child_svc in repl.child_services:
                out_sum = 0
                for child_repl in child_svc.replicas:
                    child_repl_name = child_repl.name + sim.DELIMITER+"start"
                    out_sum += aggregated_rps.sum(en, child_repl_name)
                node_flow = model.addConstr((gp.quicksum(aggregated_rps.select('*', en)) == out_sum), name="flow_conservation["+en+"]")
    # model.update()

    ###################################################
    # Constraint 4: Tree topology
    for svc in sim.dag.all_service:
        if svc.name != "User":
            svc_repl = svc.replicas
            sum_ = 0
            for repl in svc_repl:
                repl_name = repl.name + sim.DELIMITER+"start"
                sum_ += aggregated_rps.sum('*', repl_name)
            #     print_log("repl_name:", repl_name)
            #     print_log("aggregated_rps.sum('*', repl_name):", aggregated_rps.sum('*', repl_name))
            # print_log("flow sum ({}): {}".format(svc.name, sum_))
            node_flow = model.addConstr(sum_ == TOTAL_NUM_REQUEST, name="tree_topo_conservation")
            # print_log()
    # model.update()

    ###################################################
    # Constraint 5: max throughput of service
    node_key = node.keys()
    throughput = model.addConstrs((gp.quicksum(aggregated_rps.select('*', n_)) <= node[n_] for n_ in node_key), name="service_capacity")
    constraint_setup_end_time = time.time()
    ###################################################
    # Lazy update for model
    model.update()
    LOG_TIMESTAMP("gurobi add constraints and model update")


    ###################################################
    ## File write constraint info
    # constrInfo = [(c.constrName, model.getRow(c), c.Sense, c.RHS) for c in model.getConstrs() ]
    # df_constr = pd.DataFrame(constrInfo)
    # df_constr.columns=['Constraint Name','Constraint equation', 'Sense','RHS']
    # print_log(df_constr.shape)
    # now =datetime .datetime.now()
    # df_constr.to_csv(OUTPUT_DIR + now.strftime("%Y%m%d_%H:%M:%S") + "-light_model_constraint.csv")

    ## Defining objective function
    model.setParam('NonConvex', 2)
    solve_start_time = time.time()
    model.optimize()
    solve_end_time = time.time()
    LOG_TIMESTAMP("MODEL OPTIMIZE")

    ## Not belonging to optimizer critical path
    ts = time.time()
    ## Variable info
    varInfo = [(v.varName, v.LB, v.UB) for v in model.getVars() ] # use list comprehension
    df_var = pd.DataFrame(varInfo) # convert to pandas dataframe
    df_var.columns=['Variable Name','LB','UB'] # Add column headers
    num_var = len(df_var)

    ## Linear constraint info
    constrInfo = [(c.constrName, model.getRow(c), c.Sense, c.RHS) for c in model.getConstrs() ]
    df_constr = pd.DataFrame(constrInfo)
    df_constr.columns=['Constraint Name','Constraint equation', 'Sense','RHS']
    num_constr = len(df_constr)
    substract_time = time.time() - ts
    LOG_TIMESTAMP("get var and constraint")
    
    if model.Status == GRB.INFEASIBLE:
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXX")
        print("XXXX INFEASIBLE MODEL! XXXX")
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXX")
        with pd.option_context('display.max_colwidth', None):
            with pd.option_context('display.max_rows', None):
                # display(df_var)
                print(df_constr)
        
        model.computeIIS()
        model.write("model.ilp")
        print('\nThe following constraints and variables are in the IIS:')
        # for c in model.getConstrs():
        #     if c.IISConstr: print(f'\t{c.constrname}: {model.getRow(c)} {c.Sense} {c.RHS}')
        for v in model.getVars():
            if v.IISLB: print(f'\t{v.varname} ≥ {v.LB}')
            if v.IISUB: print(f'\t{v.varname} ≤ {v.UB}')
    else:
        ## Model solved!
        ## Print out the result
        optimize_end_time = time.time()
        optimizer_runtime = round((optimize_end_time - optimizer_start_time) - substract_time, 5)
        solve_runtime = round(solve_end_time - solve_start_time, 5)
        constraint_setup_time = round(constraint_setup_end_time - constraint_setup_start_time, 5)
        print("*"*50)
        print("** Objective: " + objective)
        print("** Num constraints: ", num_constr)
        print("** Num variables: ", num_var)
        print("** Optimization runtime: {} ms".format(optimizer_runtime))
        print("** model.optimize() runtime: {} ms".format(solve_runtime))
        print("** constraint_setup_time runtime: {} ms".format(constraint_setup_time))
        print("** model.objVal: ", int(model.objVal))
        print("** model.objVal / total num requests: ", int(model.objVal/TOTAL_NUM_REQUEST))
        
        if OUTPUT_WRITE:
            request_flow = pd.DataFrame(columns=["From", "To", "Flow"])
            for arc in arcs:
                if aggregated_rps[arc].x > 1e-6:
                    temp = pd.DataFrame({"From": [arc[0]], "To": [arc[1]], "Flow": [aggregated_rps[arc].x]})
                    request_flow = pd.concat([request_flow, temp], ignore_index=True)
            now = datetime.datetime.now()
            request_flow.to_csv(OUTPUT_DIR + now.strftime("%Y%m%d_%H:%M:%S") + "-" + flags.application + "-light_model_solution.csv")
            LOG_TIMESTAMP("file write model output")
            
        ## Performance log write
        # print("@@, App, num_constr, num_gurobi_var, compute_arc_var_name, network_arc_var_name, NUM_CLUSTER, depth, total_num_svc, fan_out_degree, no_child_constant, regressor_degree,  optimizer_runtime, solve_runtime")
        print("@@, ",end="")
        print(flags.application + "," + \
                str(num_constr) + "," + \
                str(num_var) + "," + \
                str(len(compute_arc_var_name)) + "," + \
                str(len(network_arc_var_name)) + "," + \
                str(flags.NUM_CLUSTER) + "," + \
                str(flags.depth) + "," + \
                str(sum(total_num_svc_in_each_depth)) + "," + \
                str(flags.fan_out_degree) + "," + \
                str(flags.no_child_constant) + "," + \
                str(flags.regressor_degree) + "," + \
                str(optimizer_runtime) + "," + \
                str(solve_runtime) + ",", \
                end="")
                # total_num_svc_in_each_depth, \
                # constraint_setup_time, \
                # flags.NUM_REQUEST, \
        prettyprint_timestamp()
        print_timestamp()

    if GRAPHVIZ:
        g_ = graphviz.Digraph()
        # The node() method takes a name identifier as first argument and an optional label.
        # The edge() method takes the names of start node and end node
        print_all = True
        node_pw = "1"
        edge_pw = "0.5"
        fs = "8"
        edge_fs_0 = "10"
        edge_fs_1 = "5"
        local_routing_edge_color = "black"
        remote_routing_edge_color = "blue"
        fn="times bold italic"
        edge_arrowsize="0.5"
        edge_minlen="1"
        src_and_dst_node_color = "#8587a8" # Gray
        node_color = ["#FFBF00", "#ff6375", "#6973fa", "#AFE1AF"] # yellow, pink, blue, green
        # node_color = ["#ff0000","#ff7f00","#ffff00","#7fff00","#00ff00","#00ff7f","#00ffff","#007fff","#0000ff","#7f00ff"] # rainbow

        total_num_remote_routing = 0
        if print_all:
            for repl_name, v in aggregated_rps.items():
                src_replica_id = repl_name[0].split(sim.DELIMITER)[1]
                dst_replica_id = repl_name[1].split(sim.DELIMITER)[1]
                remote_routing = False
                if src_replica_id == '*' and dst_replica_id == '*':
                    edge_color = "black"
                    src_cid = -1
                    dst_cid = -1
                elif src_replica_id == '*' and dst_replica_id != '*':
                    edge_color = "black"
                    src_cid = -1
                    dst_cid = int(dst_replica_id) % flags.NUM_CLUSTER
                elif src_replica_id != '*' and dst_replica_id == '*':
                    edge_color = "black"
                    src_cid = int(src_replica_id) % flags.NUM_CLUSTER
                    dst_cid = -1
                else:
                    src_cid = int(src_replica_id) % flags.NUM_CLUSTER
                    dst_cid = int(dst_replica_id) % flags.NUM_CLUSTER
                    if src_cid == dst_cid:
                        edge_color =  local_routing_edge_color# local routing
                    else:
                        edge_color = remote_routing_edge_color # remote routing
                        remote_routing = True
                if repl_name[0] in request_flow["From"].to_list() and repl_name[1] in request_flow["To"].to_list():
                    if src_cid == -1:
                        src_node_color = src_and_dst_node_color
                    else:
                        src_node_color = node_color[src_cid]
                    if dst_cid == -1:
                        dst_node_color = src_and_dst_node_color
                    else:
                        dst_node_color = node_color[src_cid]
                    g_.node(name=repl_name[0], label=repl_name[0], shape='circle', style='filled', fillcolor=src_node_color, penwidth=node_pw, fontsize=fs, fontname=fn, fixedsize="True", width="0.5")
                    g_.node(name=repl_name[1], label=repl_name[1], shape='circle', style='filled', fillcolor=dst_node_color, penwidth=node_pw, fontsize=fs, fontname=fn, fixedsize="True", width="0.5")
                # else:
                #     g_.node(name=repl_name[1], label=repl_name[1], shape='circle', style='filled', fillcolor=dst_node_color, penwidth=node_pw, fontsize=fs, fontname=fn, fixedsize="True", width="0.5")
                temp = request_flow[request_flow["From"]==repl_name[0]]
                temp = temp[temp["To"]==repl_name[1]]
                if len(temp) > 0:
                    if remote_routing:
                        total_num_remote_routing += temp["Flow"].to_list()[0]
                    g_.edge(repl_name[0], repl_name[1], label=str(int(temp["Flow"].to_list()[0])), penwidth=edge_pw, style="filled", fontsize=edge_fs_0, fontcolor=edge_color, color=edge_color, arrowsize=edge_arrowsize, minlen=edge_minlen)
        # else:
        #     for repl_name, v in aggregated_rps.items():
        #         if repl_name[0] in request_flow["From"].to_list() and repl_name[1] in request_flow["To"].to_list():
        #             node_color = "#FFBF00"
        #         else:
        #             node_color = "#A7A4A4"
        #         g_.node(name=repl_name[0], label=repl_name[0], shape='circle', style='filled', fillcolor=node_color, penwidth=node_pw, fontsize=fs, fontname=fn, fixedsize="True", width="0.75")
        #         g_.node(name=repl_name[1], label=repl_name[1], shape='circle', style='filled', fillcolor=node_color, penwidth=node_pw, fontsize=fs, fontname=fn, fixedsize="True")
        #         temp = request_flow[request_flow["From"]==repl_name[0]]
        #         temp = temp[temp["To"]==repl_name[1]]
        #         if len(temp) > 0:
        #             # g_.edge(repl_name[0], repl_name[1], label=str(v)+", "+str(temp["Flow"].to_list()[0]), penwidth=edge_pw, style="filled", fontsize=edge_fs_0, color="blue", arrowsize=edge_arrowsize, minlen=edge_minlen)
        #             g_.edge(repl_name[0], repl_name[1], label=str(temp["Flow"].to_list()[0]), penwidth=edge_pw, style="filled", fontsize=edge_fs_0, color="blue", arrowsize=edge_arrowsize, minlen=edge_minlen)
        #         else:
        #             g_.edge(repl_name[0], repl_name[1], label=str(v), penwidth=edge_pw, style="dotted", fontsize=edge_fs_1, arrowhead="none", minlen=edge_minlen)
        print("** total_num_remote_routing: ", total_num_remote_routing)
        print("*"*50)   

        now =datetime .datetime.now()
        g_.render(OUTPUT_DIR + now.strftime("%Y%m%d_%H:%M:%S") + '_call_graph', view = False) # output: call_graph.pdf
        g_