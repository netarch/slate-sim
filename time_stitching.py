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
    def __init__(self, svc, span_id, pspan_id, st, et):
        self.svc_name = svc
        self.span_id = span_id
        self.pspan_id = pspan_id
        self.duration = et - st
        self.st = st
        self.et = et
        self.root = (pspan_id=="")
    
    def print(self):
        print("SPAN,{},{}->{},{},({}-{})".format(self.svc_name, self.pspan_id, self.span_id, self.duration, self.st, self.et))

# <Trace Id> <Span Id> <Parent Span Id> <Start Time> <End Time>
# <Parent Span Id> will not exist for the frontend service. e.g., productpage service in bookinfo
# min len of tokens = 4

SPAN_DELIM = " "
SPAN_TOKEN_LEN = 5
def parse_and_create_span(line, svc):
    tokens = line.split(SPAN_DELIM)
    if len(tokens) != SPAN_TOKEN_LEN:
        print_error("Invalid token length in span line. len(tokens):{}, line: {}".format(len(tokens), line))
    tid = tokens[0]
    sid = tokens[1]
    psid = tokens[2]
    st = int(tokens[3])
    et = int(tokens[4])
    span = Span(svc, sid, psid, st, et)
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
    traces = dict() # key: trace_id, value: spans
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
                            tid, span = parse_and_create_span(lines[i], service_name)
                            if tid not in traces:
                                traces[tid] = dict()
                            if service_name not in traces[tid]:
                                traces[tid][service_name] = span
                            else:
                                print_error(service_name + " already exists in trace["+tid+"]")
                            # print(str(span.span_id) + " is added to " + tid + "len, "+ str(len(traces[tid])))
                            filtered_lines.append(lines[i])
                    #######################################################
                except ValueError:
                    print_error("sp_line["+str(load_kw_idx)+"]: " + sp_line[load_kw_idx] + " is not integer..?\nline: "+lines[i])
                except Exception as error:
                    print(error)
                    print_error("line: " + lines[i])
        i+=1
    return filtered_lines, traces

# span_to_service_name = dict()

FRONTEND_SVC = "productpage-v1"
def remove_incomplete_trace(traces):
    # ret_traces = copy.deepcopy(traces)
    removed_traces = dict()
    ret_traces = dict()
    for tid, spans in traces.items():
        if FRONTEND_SVC not in spans:
            # print_log(tid + " does not have " + FRONTEND_SVC + ". Skip this trace")
            removed_traces[tid] = spans
        else:
            ret_traces[tid] = spans
    return ret_traces, removed_traces
    
def change_to_relative_time(traces):
    for tid, spans in traces.items():
        try:
            base_t = spans[FRONTEND_SVC].st
        except Exception as error:
            # print(tid + " does not have " + FRONTEND_SVC + ". Skip this trace")
            # for svc, span in spans.items():
            #     span.print()
            print(error)
            exit()
        for svc_name, span in spans.items():
            span.st -= base_t
            span.et -= base_t
    return traces

LOG_PATH = "./logs"

if __name__ == "__main__":
    print_log("time stitching")
    lines = file_read(LOG_PATH)
    filtered_lines, original_traces = parse(lines)
            
    print("="*50)
    print("#original trace: " + str(len(original_traces)))
    
    filtered_traces, removed_traces = remove_incomplete_trace(original_traces)
    assert len(original_traces) == len(filtered_traces) + len(removed_traces)
    print("#filtered trace: " + str(len(filtered_traces)))
    print("#removed_traces trace: " + str(len(removed_traces)))
    
    relative_t_traces = change_to_relative_time(filtered_traces)
    print("#relative t trace: " + str(len(relative_t_traces)))
    assert len(relative_t_traces) == len(filtered_traces)
    
    print("*"*50)
    for tid, spans in relative_t_traces.items():
        print()
        print("Trace: " + tid)
        for svc_name, span in spans.items():
            span.print()