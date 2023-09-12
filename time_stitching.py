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
import copy
from pprint import pprint

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


VERBOSITY=1

""" Trace exampe line
2
f85116460cc0c607a484d0521e62fb19 7c30eb0e856124df a484d0521e62fb19 1694378625363 1694378625365
4ef8ed533389d8c9ace91fc1931ca0cd 48fb12993023f618 ace91fc1931ca0cd 1694378625363 1694378625365

<Num requests>
<Trace Id> <Span Id> <Parent Span Id> <Start Time> <End Time>

Root svc will have no parent span id
"""
        
def print_log(msg, obj=None):
    if VERBOSITY >= 1:
        if obj == None:
            print("[LOG] ", end="")
            print(msg)
        else:
            print("[LOG] ", end="")
            print(msg, obj)
        
def print_error(msg):
    exit_time = 1
    print("[ERROR] " + msg)
    print("EXIT PROGRAM in")
    for i in reversed(range(exit_time)) :
        print("{} seconds...".format(i))
        time.sleep(1)
    exit()

def stitch_traces():
    return "Not implemented yet"
    
def file_read(path_):
    f = open(path_, "r")
    lines = f.readlines()
    return lines


class Span:
    def __init__(self, svc, span_id, parent_span_id, st, et, load):
        self.svc_name = svc
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.duration = et - st
        self.st = st
        self.et = et
        self.root = (parent_span_id=="")
        self.load = load
    
    def print(self):
        print("SPAN,{},{}->{},{},{},{},{}".format(self.svc_name, self.parent_span_id, self.span_id, self.st, self.et, self.duration, self.load))

# <Trace Id> <Span Id> <Parent Span Id> <Start Time> <End Time>
# <Parent Span Id> will not exist for the frontend service. e.g., productpage service in bookinfo
# min len of tokens = 4

SPAN_DELIM = " "
SPAN_TOKEN_LEN = 5
def parse_and_create_span(line, svc, load):
    tokens = line.split(SPAN_DELIM)
    if len(tokens) != SPAN_TOKEN_LEN:
        print_error("Invalid token length in span line. len(tokens):{}, line: {}".format(len(tokens), line))
    tid = tokens[0]
    sid = tokens[1]
    psid = tokens[2]
    st = int(tokens[3])
    et = int(tokens[4])
    span = Span(svc, sid, psid, st, et, load)
    return tid, span

DE_in_log=" "
info_kw = "INFO"
info_kw_idx = 2
# def is_info_line(line):
#     if line.split(" ")[info_kw_idx] == info_kw:
#         return True
#     return False

min_len_tokens = 4
svc_kw_idx = -2
load_kw_idx = -1
def parse(lines):
    traces_ = dict() # nested dictionary. key for outer dict: trace_id, key for inner dict: service name, value: Span object
    filtered_lines = list()
    i = 0
    while i < len(lines):
        sp_line = lines[i].split(DE_in_log)
        if len(sp_line) >= min_len_tokens:
            if sp_line[info_kw_idx] == info_kw:
                try:
                    load_per_tick = int(sp_line[load_kw_idx])
                    service_name = sp_line[svc_kw_idx][:-1]
                    #######################################################
                    if load_per_tick > 0:
                        print_log("svc name," + service_name + ", load per tick," + str(load_per_tick))
                        filtered_lines.append(lines[i])
                        while True:
                            i += 1
                            if lines[i+1] == "\n": # or len(lines[i+1]) == 1:
                                break
                            tid, span = parse_and_create_span(lines[i], service_name, load_per_tick)
                            if tid not in traces_:
                                traces_[tid] = dict()
                            if service_name not in traces_[tid]:
                                traces_[tid][service_name] = span
                            else:
                                print_error(service_name + " already exists in trace["+tid+"]")
                            # print(str(span.span_id) + " is added to " + tid + "len, "+ str(len(traces_[tid])))
                            filtered_lines.append(lines[i])
                    #######################################################
                except ValueError:
                    print_error("sp_line["+str(load_kw_idx)+"]: " + sp_line[load_kw_idx] + " is not integer..?\nline: "+lines[i])
                except Exception as error:
                    print(error)
                    print_error("line: " + lines[i])
        i+=1
    return filtered_lines, traces_

# span_to_service_name = dict()

FRONTEND_svc = "productpage-v1"
REVIEW_V1_svc = "reviews-v1"
REVIEW_V2_svc = "reviews-v2"
REVIEW_V3_svc = "reviews-v3"
RATING_svc = "ratings-v1"
DETAIL_svc = "details-v1"
# ratings-v1 and reviews-v1 should not exist in the same trace
min_trace_len = 3
max_trace_len = 4
def remove_incomplete_trace(traces_):
    # ret_traces_ = copy.deepcopy(traces_)
    input_trace_len = len(traces_)
    removed_traces_ = dict()
    what = dict()
    ret_traces_ = dict()
    for tid, spans in traces_.items():
        if FRONTEND_svc not in spans or DETAIL_svc not in spans:
            removed_traces_[tid] = spans
        elif len(spans) < min_trace_len:
            removed_traces_[tid] = spans
        elif len(spans) > max_trace_len:
            removed_traces_[tid] = spans
        elif len(spans) == min_trace_len and (REVIEW_V1_svc not in spans or REVIEW_V2_svc in spans or REVIEW_V3_svc in spans):
            removed_traces_[tid] = spans
        elif len(spans) == max_trace_len and REVIEW_V2_svc not in spans and REVIEW_V3_svc not in spans:
            removed_traces_[tid] = spans
        else:
            ret_traces_[tid] = spans
    
    assert input_trace_len == len(ret_traces_) + len(removed_traces_)
    
    print("#input trace: " + str(input_trace_len))
    print("#filtered trace: " + str(len(ret_traces_)))
    print("#removed_traces trace: " + str(len(removed_traces_)))
    
    return ret_traces_, removed_traces_
    
def change_to_relative_time(traces_):
    input_trace_len = len(traces_)
    for tid, spans in traces_.items():
        try:
            base_t = spans[FRONTEND_svc].st
        except Exception as error:
            # print(tid + " does not have " + FRONTEND_svc + ". Skip this trace")
            # for _, span in spans.items():
            #     span.print()
            print(error)
            exit()
        for _, span in spans.items():
            span.st -= base_t
            span.et -= base_t
            
    assert input_trace_len == len(traces_)
    print("#relative t trace: " + str(len(traces)))
    return traces_

# Using a Python dictionary to act as an adjacency list
'''
e.g.,
{   484fb04e7deab207d8fc0d9d8edc0388 :
    {
        {details-v1: d8fc0d9d8edc0388->db1760fc78c58bff,34,36,2,81}, 
        {productpage-v1: ""->d8fc0d9d8edc0388,0,320,320,96}, 
        {ratings-v1: 34669d6678cb17c7->3d337444c3d30efc,119,120,1,45}, 
        {reviews-v2: d8fc0d9d8edc0388->34669d6678cb17c7,113,124,11,25}
    },
    ...
}

graph = {
  'd8fc0d9d8edc0388(productpage)' : ['34669d6678cb17c7(reviews-v2)','db1760fc78c58bff(details-v1)'],
  '34669d6678cb17c7(reviews-v2)' : ['34669d6678cb17c7(ratings-v1)'],
}
'''


# def dfs(visited_, graph_, span_):  #function for dfs 
#     if node_ not in visited_:
#         print (node_)
#         visited_.add(node_)
#         for neighbour in graph_[node_]:
#             dfs(visited_, graph_, neighbour)
                
def spans_to_graph(spans_):
    visited = set()
    graph = dict()
    # parent_span = spans_[root_svc]
    for _, parent_span in spans_.items():
        graph[parent_span] = list() 
        for _, span in spans_.items():
            if span not in visited:
                if span.parent_span_id == parent_span.span_id:
                    graph[parent_span].append(span)
                    visited.add(span)
                    # ASSUMPTION of visited: there is always only one parent service. If one service is a child service of some parent service, there is any other parent service for this child service.
                    # For example, A->C, B->C is NOT possible.
        if len(graph[parent_span]) == 0:
            del graph[parent_span]
    return graph
        

def print_graph(graph_):
    for parent, children in graph_.items():
        print("parent: " + parent.svc_name + "("+parent.span_id+"), child: " , end="")
        for child in children:
            print(child.svc_name + "("+child.span_id+")," , end="")
        print()

def append_exclusive_time(traces_):
    for tid, spans in traces_.items():
        graph = spans_to_graph(spans)
        print()
        print("="*10)
        print_graph(graph)
        print("="*10)
    return traces_

LOG_PATH = "./trace_and_load_log.txt"

if __name__ == "__main__":
    print_log("time stitching")
    lines = file_read(LOG_PATH)
    filtered_lines, traces = parse(lines)
    traces, removed_traces = remove_incomplete_trace(traces)
    traces = change_to_relative_time(traces)
    traces = append_exclusive_time(traces)
    print("*"*50)
    for tid, spans in traces.items():
        print()
        print("Trace: " + tid)
        for _, span in spans.items():
            span.print()
    
    print()
    print("#final valid traces: " + str(len(traces)))