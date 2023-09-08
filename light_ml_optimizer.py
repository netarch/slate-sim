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

OUTPUT_DIR = "./optimizer_output/"
NUM_CLUSTER = 3

random.seed(1234)

def trunc(temp): # utility function for node naming
    st = temp.find("(")
    temp = temp[:st]
    return temp

application = "tree" # "linear", "three_depth", "tree"
if application == "linear":
    sim.linear_topology_application(NUM_CLUSTER)
elif application == "three_depth":
    sim.three_depth_application(NUM_CLUSTER)
elif application == "tree":
    fan_out_degree = 3
    no_child_constant = 0
    depth = 3
    assert(no_child_constant < fan_out_degree)
    sim.general_tree_application(fan_out_degree, no_child_constant, depth, NUM_CLUSTER)
else:
    print("ERROR: Unsupported application, ", application)
print(sim.dag.graph)
sim.dag.print_dependency()



svc_name_list = list()
compute_arc_var_name = list()
per_service_compute_arc = dict()
for repl in sim.dag.all_replica:
    # if repl.service.name != "User":
    var_name = (trunc(repl.to_str())+"_start", trunc(repl.to_str())+"_end")
    compute_arc_var_name.append(var_name)
    if repl.service.name not in per_service_compute_arc:
        per_service_compute_arc[repl.service.name] = list()
    per_service_compute_arc[repl.service.name].append(var_name)
    svc_name_list.append(repl.service.name)

## Define names of the variables for network arc in gurobi
network_arc_var_name = list()
for parent_repl in sim.dag.child_replica:
    for child_svc in sim.dag.child_replica[parent_repl]:
        child_repl_list = sim.dag.child_replica[parent_repl][child_svc]
        for child_repl in child_repl_list:
            if parent_repl.service.name == "User":
                var_name = ("src_*_*", trunc(parent_repl.to_str())+"_start")
                if var_name not in network_arc_var_name:
                    network_arc_var_name.append(var_name)
                var_name = (trunc(parent_repl.to_str())+"_end", trunc(child_repl.to_str())+"_start")
                if var_name not in network_arc_var_name:
                    network_arc_var_name.append(var_name)
            else:
                var_name = (trunc(parent_repl.to_str())+"_end",  trunc(child_repl.to_str())+"_start")
                if var_name not in network_arc_var_name:
                    network_arc_var_name.append(var_name)
            if sim.dag.is_leaf(child_repl.service):
                var_name = (trunc(child_repl.to_str())+"_end", "dst_*_*")
                if var_name not in network_arc_var_name:
                    network_arc_var_name.append(var_name)
print(network_arc_var_name)

NUM_REQUEST = [500, 200, 100]
assert len(NUM_REQUEST) == NUM_CLUSTER
TOTAL_NUM_REQUEST = sum(NUM_REQUEST)
true_function_degree = 1 # 1: linear, >2: polynomial
regressor_degree = 1
num_data_point = TOTAL_NUM_REQUEST
randomize_factor = 100
x_rps = list()
x_service_name = list()
y_compute_time = list()
x_repl_name = list()
for i in range(len(compute_arc_var_name)):
    x_rps += list(np.arange(0,num_data_point))
    x_repl_name += [compute_arc_var_name[i]] * num_data_point
    x_service_name += [svc_name_list[i]] * num_data_point
    for j in range(num_data_point):
        svc_name = compute_arc_var_name[i][0].split("_")[0]
        slope = abs(hash(svc_name)%10)+1
        if svc_name == "User":
            y_compute_time.append(0)
        else:
            y_compute_time.append(pow(x_rps[j],true_function_degree)*slope + 10)
# print(len(x_service_name))
# print(len(x_rps))
# print(len(y_compute_time))
# print(len(x_repl_name))

compute_time_observation = pd.DataFrame(
    data={
        "service_name": x_service_name,
        "rps": x_rps, 
        "compute_time": y_compute_time,
    },
    index=x_repl_name
)

## Per-service rps-to-compute time modeling.
unique_service_name = list(compute_time_observation["service_name"].unique())
# unique_service_name.remove("User")
print("unique_service_name: ", unique_service_name)

max_compute_time = dict()
idx = 0
regressor_dict = dict()
for svc_name in unique_service_name:
    temp_df = compute_time_observation[compute_time_observation["service_name"] == svc_name]
    # X = temp_df[["service_name", "rps"]]
    X = temp_df[["rps"]]
    y = temp_df["compute_time"]
    max_compute_time[svc_name] = max(y)
    # print("max(y): ", max(y))
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, train_size=0.8, random_state=1
    )
    feat_transform = make_column_transformer(
        # (StandardScaler(), ["rps"]),
        ("passthrough", ["rps"]),
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
    print("Service {}".format(svc_name))
    print("- Coef: {}".format(regressor_dict[svc_name]["linearregression"].coef_))
    ## Run prediction and compare it with the ground truth to see how accurate the trained model is
    y_pred = regressor_dict[svc_name].predict(X_test)
    print(f"- R^2: {np.round(r2_score(y_test, y_pred),5)}")

min_rps = 0
max_rps = max(x_rps)
min_network_egress_cost = list()
max_network_egress_cost = list()
for src_repl, dst_repl in network_arc_var_name:
    src_svc = src_repl.split("_")[0] # A
    dst_svc = dst_repl.split("_")[0] # B
    if src_svc == "src":
        min_network_egress_cost.append(0)
        max_network_egress_cost.append(0)
    elif dst_svc == "dst":
        min_network_egress_cost.append(0)
        max_network_egress_cost.append(0)
    else:
        from_idx = int(src_repl.split("_")[1])
        to_idx = int(dst_repl.split("_")[1])
        # local routing
        if from_idx%2 == to_idx%2:
            min_network_egress_cost.append(0) # no egress charge on local routing
            max_network_egress_cost.append(0)
        # remote routing
        else:
            min_network_egress_cost.append(sim.dag.weight_graph[src_svc][dst_svc])
            max_network_egress_cost.append(sim.dag.weight_graph[src_svc][dst_svc])
                
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

min_rps = 0
max_rps = max(x_rps)
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
    src_svc = src_repl.split("_")[0]
    dst_svc = dst_repl.split("_")[0]
    if src_svc == "src":
        min_network_latency.append(0)
        max_network_latency.append(0)
    elif dst_svc == "dst":
        min_network_latency.append(0)
        max_network_latency.append(0)
    else:
        from_idx = int(src_repl.split("_")[1])
        to_idx = int(dst_repl.split("_")[1])
        # Network latency for local routing
        if from_idx%2 == to_idx%2:
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

###################################################
## Optimizer runtime timestamp, start time
optimizer_start_time = time.time()

model = gp.Model('RequestRouting')
# Add variables for the regression
compute_time = dict()
compute_rps = dict()
for svc_name in unique_service_name:
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
    pred_constr = add_predictor_constr(model, regressor_dict[svc_name], m_feats[svc_name], compute_time[svc_name])
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
print("total_egress_sum: ", total_egress_sum)

# total latency sum
network_latency_sum = sum(network_latency.multiply(network_rps))
for svc_name in unique_service_name:
    compute_latency_sum = sum(compute_time[svc_name].multiply(m_feats[svc_name]["rps"])) # m_feats[svc_name]["rps"] is identical to compute_rps[svc_name]
total_latency_sum = network_latency_sum + compute_latency_sum
print("total_latency_sum: ", total_latency_sum)

objective = "latency" # latency or egress_cost or multi-objective
if objective == "latency":
    model.setObjective(total_latency_sum, gp.GRB.MINIMIZE)
elif objective == "egress_cost":
    model.setObjective(total_egress_sum, gp.GRB.MINIMIZE)
elif objective == "multi-objective":
    model.setObjective(total_latency_sum*50 + total_egress_sum*50, gp.GRB.MINIMIZE)
else:
    print("ERROR: unsupported objective, ", objective)
    
# model.update()
print("model objective")
print(type(model.getObjective()))
print(model.getObjective())

# arcs is the keys
# aggregated_rps is dictionary
concat_compute_rps = pd.Series()
for svc_name, c_rps in compute_rps.items():
    concat_compute_rps = pd.concat([concat_compute_rps, compute_rps[svc_name]], axis=0)
arcs, aggregated_rps = gp.multidict(pd.concat([network_rps, concat_compute_rps], axis=0).to_dict())

###################################################
source = dict()
destination = dict()
source["src_*_*"] = TOTAL_NUM_REQUEST
destination["dst_*_*"] = TOTAL_NUM_REQUEST
node = dict()
max_tput = TOTAL_NUM_REQUEST
for repl in sim.dag.all_replica:
    if repl.service.name != "User":
        node[trunc(repl.to_str())+"_start"] = max_tput
        node[trunc(repl.to_str())+"_end"] = max_tput
print(node)

###################################################
# Test print
# print("<network_rps>")
# print(network_rps)
# print()
# print("<aggregated_rps.select>")
# print(aggregated_rps.select("A_0_end", "*"))
# print()
# print("<gp.quicksum(aggregated_rps.select>")
# print(gp.quicksum(aggregated_rps.select("A_0_end", "*")))
# print()

###################################################
# Constraint 1: source
src_keys = source.keys()
src_flow = model.addConstrs((gp.quicksum(aggregated_rps.select(src, '*')) == source[src] for src in src_keys), name="source")
# per clustrer load constraint
for repl in sim.dag.all_replica:
    if repl.service.name == "User":
        start_node = trunc(repl.to_str())+"_start"
        end_node = trunc(repl.to_str())+"_end"
        cid = int(trunc(repl.to_str()).split("_")[1])
        print("gp.quicksum(aggregated_rps.select('*', start_node): ", gp.quicksum(aggregated_rps.select('*', start_node)))
        print("cid: ", cid)
        per_cluster_load_in = model.addConstr((gp.quicksum(aggregated_rps.select('*', start_node)) == NUM_REQUEST[cid]), name="cluster_"+str(cid)+"_load_in")
        per_cluster_load_out = model.addConstr((gp.quicksum(aggregated_rps.select(end_node, '*')) == NUM_REQUEST[cid]), name="cluster_"+str(cid)+"_load_out")
# model.update()

###################################################
# Constraint 2: destination
dest_keys = destination.keys()
num_leaf_svc = 0
leaf_svc = list()
for svc in sim.dag.all_service:
    # print("iterate ", svc.name)
    if sim.dag.is_leaf(svc):
        num_leaf_svc += 1
        leaf_svc.append(svc.name)
print("leaf_svc: ", leaf_svc)
print("num_leaf_svc: ", num_leaf_svc)
dst_flow = model.addConstrs((gp.quicksum(aggregated_rps.select('*', dst)) == destination[dst]*num_leaf_svc for dst in dest_keys), name="destination")
# model.update()

###################################################
# Constraint 3: flow conservation
for repl in sim.dag.all_replica:
    # if repl.service.name != "User":
    # Start node in-out flow conservation
    st = trunc(repl.to_str())+"_start"
    node_flow = model.addConstr((gp.quicksum(aggregated_rps.select('*', st)) == gp.quicksum(aggregated_rps.select(st, '*'))), name="flow_conservation["+st+"]")
    # End node in-out flow conservation
    en = trunc(repl.to_str())+"_end"
    if sim.dag.is_leaf(repl.service):
        node_flow = model.addConstr((gp.quicksum(aggregated_rps.select('*', en)) == gp.quicksum(aggregated_rps.select(en, '*'))), name="flow_conservation["+en+"]")
    else:
        for child_svc in repl.child_services:
            out_sum = 0
            for child_repl in child_svc.replicas:
                child_repl_name = trunc(child_repl.to_str()) + "_start"
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
            repl_name = trunc(repl.to_str()) + "_start"
            sum_ += aggregated_rps.sum('*', repl_name)
        #     print("repl_name:", repl_name)
        #     print("aggregated_rps.sum('*', repl_name):", aggregated_rps.sum('*', repl_name))
        # print("flow sum ({}): {}".format(svc.name, sum_))
        node_flow = model.addConstr(sum_ == TOTAL_NUM_REQUEST, name="tree_topo_conservation")
        # print()
# model.update()

###################################################
# Constraint 5: max throughput of service
node_key = node.keys()
throughput = model.addConstrs((gp.quicksum(aggregated_rps.select('*', n_)) <= node[n_] for n_ in node_key), name="service_capacity")

###################################################
# Lazy update for model
model.update()

###################################################
## File write constraint info
# constrInfo = [(c.constrName, model.getRow(c), c.Sense, c.RHS) for c in model.getConstrs() ]
# df_constr = pd.DataFrame(constrInfo)
# df_constr.columns=['Constraint Name','Constraint equation', 'Sense','RHS']
# print(df_constr.shape)
# now =datetime .datetime.now()
# df_constr.to_csv(OUTPUT_DIR + now.strftime("%Y%m%d_%H:%M:%S") + "-light_model_constraint.csv")

## Defining objective function
model.setParam('NonConvex', 2)
model.optimize()

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
   
if model.Status == GRB.INFEASIBLE:
    print("###########################")
    print("#### INFEASIBLE MODEL! ####")
    print("###########################")
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
    optimizer_runtime = ((optimize_end_time - optimizer_start_time) - substract_time)*100
    print("*"*50)
    print("** Objective: " + objective)
    print("** Num constraints: ", num_constr)
    print("** Num variables: ", num_var)
    print("** Optimization runtime: {} ms".format(round(optimizer_runtime, 2)))
    print("** model.objVal: ", int(model.objVal))
    print("** model.objVal / total num requests: ", int(model.objVal/TOTAL_NUM_REQUEST))
    request_flow = pd.DataFrame(columns=["From", "To", "Flow"])
    for arc in arcs:
        if aggregated_rps[arc].x > 1e-6:
            temp = pd.DataFrame({"From": [arc[0]], "To": [arc[1]], "Flow": [aggregated_rps[arc].x]})
            request_flow = pd.concat([request_flow, temp], ignore_index=True)
    now = datetime.datetime.now()
    request_flow.to_csv(OUTPUT_DIR + now.strftime("%Y%m%d_%H:%M:%S") + "-" + application + "-light_model_solution.csv")

g_ = graphviz.Digraph()
# The node() method takes a name identifier as first argument and an optional label.
# The edge() method takes the names of start node and end node
print_all = True
node_pw = "1"
edge_pw = "0.5"
fs = "8"
edge_fs_0 = "10"
edge_fs_1 = "5"
fn="times bold italic"
edge_arrowsize="0.5"
edge_minlen="1"
c0_node_color = "#FFBF00"
c1_node_color = "#ff6375"
other_node_color = "#6973fa"

total_num_remote_routing = 0
if print_all:
    for repl_name, v in aggregated_rps.items():
        src_replica_id = repl_name[0].split("_")[1]
        dst_replica_id = repl_name[1].split("_")[1]
        remote_routing = False
        if src_replica_id == '*' and dst_replica_id == '*':
            edge_color = "black"
            src_cid = -1
            dst_cid = -1
        elif src_replica_id == '*' and dst_replica_id != '*':
            edge_color = "black"
            src_cid = -1
            dst_cid = int(dst_replica_id)%2
        elif src_replica_id != '*' and dst_replica_id == '*':
            edge_color = "black"
            src_cid = int(src_replica_id)%2
            dst_cid = -1
        else:
            src_cid = int(src_replica_id)%2
            dst_cid = int(dst_replica_id)%2
            if src_cid == dst_cid:
                edge_color = "black" # local routing
            else:
                edge_color = "blue" # remote routing
                remote_routing = True
        if repl_name[0] in request_flow["From"].to_list() and repl_name[1] in request_flow["To"].to_list():
            if src_cid == -1:
                src_node_color = other_node_color
            elif src_cid == 0:
                src_node_color = c0_node_color
            elif src_cid == 1:
                src_node_color = c1_node_color
            else:
                print("ERROR:  unsupported src_cid, ", src_cid)
                break
            if dst_cid == -1:
                dst_node_color = other_node_color
            elif dst_cid == 0:
                dst_node_color = c0_node_color
            elif dst_cid == 1:
                dst_node_color = c1_node_color
            else:
                print("ERROR: unsupported dst_cid, ", dst_cid)
                break
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
else:
    for repl_name, v in aggregated_rps.items():
        if repl_name[0] in request_flow["From"].to_list() and repl_name[1] in request_flow["To"].to_list():
            node_color = "#FFBF00"
        else:
            node_color = "#A7A4A4"
        g_.node(name=repl_name[0], label=repl_name[0], shape='circle', style='filled', fillcolor=node_color, penwidth=node_pw, fontsize=fs, fontname=fn, fixedsize="True", width="0.75")
        g_.node(name=repl_name[1], label=repl_name[1], shape='circle', style='filled', fillcolor=node_color, penwidth=node_pw, fontsize=fs, fontname=fn, fixedsize="True")
        temp = request_flow[request_flow["From"]==repl_name[0]]
        temp = temp[temp["To"]==repl_name[1]]
        if len(temp) > 0:
            # g_.edge(repl_name[0], repl_name[1], label=str(v)+", "+str(temp["Flow"].to_list()[0]), penwidth=edge_pw, style="filled", fontsize=edge_fs_0, color="blue", arrowsize=edge_arrowsize, minlen=edge_minlen)
            g_.edge(repl_name[0], repl_name[1], label=str(temp["Flow"].to_list()[0]), penwidth=edge_pw, style="filled", fontsize=edge_fs_0, color="blue", arrowsize=edge_arrowsize, minlen=edge_minlen)
        else:
            g_.edge(repl_name[0], repl_name[1], label=str(v), penwidth=edge_pw, style="dotted", fontsize=edge_fs_1, arrowhead="none", minlen=edge_minlen)
print("** total_num_remote_routing: ", total_num_remote_routing)
print("*"*50)

now =datetime .datetime.now()
g_.render(now.strftime("%Y%m%d_%H:%M:%S") + '_call_graph', view = False) # output: call_graph.pdf
