#!/usr/bin/env python
# coding: utf-8

import time
from pprint import pprint

# LOG_PATH = "./call-logs-sept-13.txt"
# LOG_PATH = "./trace_and_load_log.txt"
LOG_PATH = "./modified_trace_and_load_log.txt"
# LOG_PATH = "./call-logs-sept-16.txt"

VERBOSITY=1
intra_cluster_network_rtt = 1
inter_cluster_network_rtt = 20

""" Trace exampe line (Version 1 wo call size)
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
    assert False

def file_read(path_):
    f = open(path_, "r")
    lines = f.readlines()
    return lines


class Span:
    def __init__(self, svc, my_span_id, parent_span_id, st, et, load, cluster_id):
        self.svc_name = svc
        self.child_spans = list()
        self.cluster_id = cluster_id
        self.name_cid = self.svc_name+"#"+str(self.cluster_id)
        self.root = (parent_span_id=="")
        
        self.my_span_id = my_span_id
        self.parent_span_id = parent_span_id
        
        self.request_size_in_bytes = 10 # parent_span to this span
        
        self.st = st
        self.et = et
        self.rt = et - st
        self.xt = 0
        self.load = load
    
    def print(self):
        print("SPAN,{},{},{}->{},{},{},{},{},{}".format(self.svc_name, self.cluster_id, self.parent_span_id, self.my_span_id, self.st, self.et, self.rt, self.xt, self.load))

# <Trace Id> <Span Id> <Parent Span Id> <Start Time> <End Time>
# <Parent Span Id> will not exist for the frontend service. e.g., productpage service in bookinfo
# min len of tokens = 4

SPAN_DELIM = " "

if LOG_PATH == "./call-logs-sept-16.txt":
    SPAN_TOKEN_LEN = 6
else:
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
    span = Span(svc, sid, psid, st, et, load, -1)
    return tid, span

DE_in_log=" "
info_kw = "INFO"
info_kw_idx = 2
min_len_tokens = 4

## New
# svc_kw_idx = -1

## Old
svc_kw_idx = -2
load_kw_idx = -1

NUM_CLUSTER = 2
def parse(lines):
    traces_ = dict() # nested dictionary. key for outer dict: trace_id, key for inner dict: service name, value: Span object
    filtered_lines = list()
    idx = 0
    while idx < len(lines):
        token = lines[idx].split(DE_in_log)
        if len(token) >= min_len_tokens:
            if token[info_kw_idx] == info_kw:
                try:
                    
                    ####################################################
                    ## New
                    # service_name = token[svc_kw_idx][:-2]
                    # idx += 1 # NOTE: load is in the new line...
                    # load_per_tick = int(lines[idx])
                    # print("service_name,{}, load_per_tick,{}".format(service_name, load_per_tick))
                    
                    ## Old
                    load_token = lines[idx].split(DE_in_log)
                    load_per_tick = int(token[load_kw_idx])
                    service_name = token[svc_kw_idx][:-1]
                    #######################################################
                    if load_per_tick > 0:
                        print_log("svc name," + service_name + ", load per tick," + str(load_per_tick))
                        filtered_lines.append(lines[idx])
                        while True:
                            idx += 1
                            if lines[idx+1] == "\n": # or len(lines[i+1]) == 1:
                                break
                            tid, span = parse_and_create_span(lines[idx], service_name, load_per_tick)
                            if tid not in traces_:
                                traces_[tid] = dict()
                            if service_name not in traces_[tid]:
                                traces_[tid][service_name] = span
                            else:
                                if LOG_PATH == "./call-logs-sept-16.txt":
                                    print_log("!!! WARNING !!!, " + service_name + " already exists in trace["+tid+"]")
                                else:
                                    print_error(service_name + " already exists in trace["+tid+"]")
                                
                            # print(str(span.my_span_id) + " is added to " + tid + "len, "+ str(len(traces_[tid])))
                            filtered_lines.append(lines[idx])
                    #######################################################
                except ValueError:
                    print_error("token["+str(load_kw_idx)+"]: " + token[load_kw_idx] + " is not integer..?\nline: "+lines[idx])
                except Exception as error:
                    print(error)
                    print_error("line: " + lines[idx])
        idx+=1
    return filtered_lines, traces_


FRONTEND_svc = "productpage-v1"
span_id_of_FRONTEND_svc = ""
REVIEW_V1_svc = "reviews-v1"
REVIEW_V2_svc = "reviews-v2"
REVIEW_V3_svc = "reviews-v3"
RATING_svc = "ratings-v1"
DETAIL_svc = "details-v1"
###############################
FILTER_REVIEW_V1 = True # False
FILTER_REVIEW_V2 = False # False
FILTER_REVIEW_V3 = True # False
###############################
# ratings-v1 and reviews-v1 should not exist in the same trace
MIN_TRACE_LEN = 3
MAX_TRACE_LEN = 4
def remove_incomplete_trace(traces_):
    # ret_traces_ = copy.deepcopy(traces_)
    input_trace_len = len(traces_)
    removed_traces_ = dict()
    what = [0]*9
    ret_traces_ = dict()
    weird_span_id = 0
    for tid, spans in traces_.items():
        if FRONTEND_svc not in spans or DETAIL_svc not in spans:
            if FRONTEND_svc not in spans:
                print("no frontend")
            if DETAIL_svc not in spans:
                print("no detail")
            removed_traces_[tid] = spans
            for svc, sp in spans.items():
                print(svc, " ")
                sp.print()
            print()
            what[0] += 1
        elif len(spans) < MIN_TRACE_LEN:
            removed_traces_[tid] = spans
            what[1] += 1
        elif len(spans) > MAX_TRACE_LEN:
            removed_traces_[tid] = spans
            what[2] += 1
        elif len(spans) == MIN_TRACE_LEN and (REVIEW_V1_svc not in spans or REVIEW_V2_svc in spans or REVIEW_V3_svc in spans):
            removed_traces_[tid] = spans
            what[3] += 1
        elif len(spans) == MAX_TRACE_LEN and REVIEW_V2_svc not in spans and REVIEW_V3_svc not in spans:
            removed_traces_[tid] = spans
            what[4] += 1
        elif spans[FRONTEND_svc].parent_span_id != span_id_of_FRONTEND_svc:
            removed_traces_[tid] = spans
            weird_span_id += 1
            what[5] += 1
        elif FILTER_REVIEW_V1 and REVIEW_V1_svc in spans:
            if len(spans) != 3:
                print_spans(spans)
            assert len(spans) == 3
            removed_traces_[tid] = spans
            what[6] += 1
        elif FILTER_REVIEW_V2 and REVIEW_V2_svc in spans:
            if len(spans) != 4:
                print_spans(spans)
            assert len(spans) == 4
            removed_traces_[tid] = spans
            what[7] += 1
        elif FILTER_REVIEW_V3 and REVIEW_V3_svc in spans:
            if len(spans) != 4:
                print_spans(spans)
            assert len(spans) == 4
            removed_traces_[tid] = spans
            what[8] += 1
        else:
            ret_traces_[tid] = spans
    print("weird_span_id: ", weird_span_id)
    print("filter stats: ", what)
    assert input_trace_len == len(ret_traces_) + len(removed_traces_)
    
    print("#input trace: " + str(input_trace_len))
    print("#filtered trace: " + str(len(ret_traces_)))
    print("#removed_traces trace: " + str(len(removed_traces_)))
    
    return ret_traces_, removed_traces_


def append_arbitrary_cluster_id_to_spans(traces_):
    i = 0 # NOTE: idx i will be used to give arbitrary cluster id to each span
    for _, spans in traces_.items():
        cluster_id = i%NUM_CLUSTER
        i += 1
        for _, span in spans.items():
            span.cluster_id = cluster_id
    return traces_

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
    print("#relative t trace: " + str(len(traces_)))
    return traces_


def print_spans(spans_):
    for _, span in spans_.items():
        span.print()

def print_dag(single_dag_):
    for parent_span, children in single_dag_.items():
        for child_span in children:
            print("{}({})->{}({})".format(parent_span.svc_name, parent_span.my_span_id, child_span.svc_name, child_span.my_span_id))
            
'''
parallel-1
    ----------------------A
        -----------B
           -----C
parallel-2
    ----------------------A
        --------B
             ---------C
'''
def is_parallel_execution(span_a, span_b):
    assert span_a.parent_span_id == span_b.parent_span_id
    if span_a.st < span_b.st:
        earlier_start_span = span_a
        later_start_span = span_b
    else:
        earlier_start_span = span_b
        later_start_span = span_a
    if earlier_start_span.et > later_start_span.st and later_start_span.et > earlier_start_span.st: # parallel execution
        if earlier_start_span.st < later_start_span.st and earlier_start_span.et > later_start_span.et: # parallel-1
            return 1
        else: # parallel-2
            return 2
    else: # sequential execution
        return 0
                
def spans_to_graph_and_calc_exclusive_time(spans_):
    # visited = set()
    single_dag = dict()
    # parent_span = spans_[root_svc]
    for _, parent_span in spans_.items():
        single_dag[parent_span] = list()
        child_spans = list()
        for _, span in spans_.items():
            # if span not in visited: # visited is redundant currently.
            if span.parent_span_id == parent_span.my_span_id:
                child_spans.append(span)
                single_dag[parent_span].append(span)
                    # visited.add(span)
                    # NOTE: ASSUMPTION of visited: there is always only one parent service. If one service is a child service of some parent service, there is any other parent service for this child service.
                    # For example, A->C, B->C is NOT possible.
        # exhaustive search
        if len(child_spans) == 0:
            continue
        exclude_child_rt = 0
        if  len(child_spans) == 1:
            print("parent: {}, child_1: {}, child_2: None, single child".format(parent_span.svc_name, child_spans[0].svc_name))
            exclude_child_rt = child_spans[0].rt
        else: # else is redundant but still I leave it there to make the if/else logic easy to follow
            for i in range(len(child_spans)):
                for j in range(i+1, len(child_spans)):
                    is_parallel = is_parallel_execution(child_spans[i], child_spans[j])
                    if is_parallel == 1 or is_parallel == 2: # parallel execution
                        # TODO: parallel-1 and parallel-2 should be dealt with individually.
                        exclude_child_rt = max(child_spans[i].rt, child_spans[j].rt)
                        print("parent: {}, child_1: {}, child_2: {}, parallel-{} sibling".format(parent_span.svc_name, child_spans[i].svc_name, child_spans[j].svc_name, is_parallel))
                    else: # sequential execution
                        exclude_child_rt = child_spans[i].rt + child_spans[j].rt
                        print("parent: {}, child_1:{}, child_2: {}, sequential sibling".format(parent_span.svc_name, child_spans[i].svc_name, child_spans[j].svc_name))
        parent_span.xt = parent_span.rt - exclude_child_rt
        if parent_span.xt < 0:
            print("parent_span")
            parent_span.print()
            print("child_spans")
            for span in child_spans:
                span.print()
            print_error("parent_span exclusive time cannot be negative value: {}".format(parent_span.xt))
        if len(single_dag[parent_span]) == 0:
            del single_dag[parent_span]
    return single_dag


def traces_to_graphs_and_calc_exclusive_time(traces_):
    graph_dict = dict()
    for tid, spans in traces_.items():
        single_dag = spans_to_graph_and_calc_exclusive_time(spans)
        graph_dict[tid] = single_dag
    return graph_dict


# def add_child_services(traces_, graph_dict_):
def add_child_services(graph_dict_):
    for tid in graph_dict_:
        # spans = traces_[tid] # { svc_name: span }
        dag = graph_dict_[tid] # { parent_span: list of child spans }
        for parent_span, children in dag.items():
            for child_span in children:
                # spans[parent_span.svc_name].child_spans.append(child_span)
                parent_span.child_spans.append(child_span)
                # print("parent: {} adds child: {}".format(parent_span.svc_name, child_span.svc_name))

def get_unique_dag_list(graph_dict_):
    unique_dags = dict()
    for _, dag in graph_dict_.items():
        temp_list = list()
        for parent_span, children in dag.items():
            for child_span in children:
                temp_list.append((parent_span.svc_name + "," + child_span.svc_name))
        temp_list.sort()
        temp_str = ""
        for elem in temp_list:
            temp_str += elem + ","
        if temp_str not in unique_dags:
            unique_dags[temp_str] = dag
    print(" --- unique dag list --- ")
    i = 0
    for _, dag in unique_dags.items():
        print("unique dag #"+str(i))
        print_dag(dag)
        i += 1
    return unique_dags

def get_unique_svc_names_from_dag(dag_):
    unique_svc_names = dict()
    for parent_span, children in dag_.items():
        for child_span in children:
            unique_svc_names[parent_span.svc_name] = "xxxx"
            unique_svc_names[child_span.svc_name] = "xxxx"
    return unique_svc_names

def stitch_time(log_path):
    print_log("time stitching starts")
    
    lines = file_read(log_path)
    filtered_lines, traces = parse(lines)
    traces, removed_traces = remove_incomplete_trace(traces)
    traces = append_arbitrary_cluster_id_to_spans(traces)
    traces = change_to_relative_time(traces)
    graph_dict = traces_to_graphs_and_calc_exclusive_time(traces)
    unique_dags = get_unique_dag_list(graph_dict)
    # traces = add_child_services(traces, graph_dict)
    add_child_services(graph_dict)
    
    print("*"*50)
    for tid, spans in traces.items():
        print("="*30)
        print("Trace: " + tid)
        print_dag(graph_dict[tid])
        for _, span in spans.items():
            span.print()
        print("="*30)
    print()
    print("#final valid traces: " + str(len(traces)))
    
    print_log("time stitching done")
    return traces, graph_dict, unique_dags

if __name__ == "__main__":
    traces, graph_dict, unique_dags = stitch_time(LOG_PATH)