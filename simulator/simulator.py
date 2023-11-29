import argparse
import copy
import string
import heapq
import graphviz
import json
from matplotlib import pyplot as plt
import math
import numpy as np
import os
import pandas as pd
from queue import Queue
import random
import time
import logging
import sys
sys.path.append("utils")
from utils import utils
import logging
# from memory_profiler import profile


#logging.basicConfig(level=logging.DEBUG)
#logging.basicConfig(filename='logging_test2.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.ERROR)

np.random.seed(1234)

DELIMITER = "#"

NUM_CLUSTER = 2
LOG_MACRO=False # not in CONFIG dictionary for convenience.

CONFIG = dict()
CONFIG["ROUTING_ALGORITHM"]=None

CONFIG["MA_WINDOW_SIZE"]="Uninitialized"
CONFIG["WARMUP_SIZE"]=200
CONFIG["PROCESSING_TIME_SIGMA"]=5

''' Load(RPS) record parameter '''
CONFIG["RPS_WINDOW"]=15000
CONFIG["RPS_UPDATE_INTERVAL"]=1000 # 1sec just for the convenience. 1sec is aligned with RPS.
CONFIG["NUM_BASKET"]=CONFIG["RPS_WINDOW"]/CONFIG["RPS_UPDATE_INTERVAL"]
CONFIG["MILLISECOND_LOAD_UPDATE"]=100 # 100ms


''' Autoscaler parameter '''
CONFIG["SCALEUP_POLICY"]="kubernetes"
CONFIG["SCALEDOWN_POLICY"]="kubernetes" # not being used
CONFIG["SCALE_UP_OVERHEAD"]=5000

# scale-down-recommendations during a fixed interval (default is 5min), and a new number of replicas is set to the maximum of all recommendations in that interval. This is done to prevent a constant fluctuation in the number of pods if the traffic/load fluctuates over a short period.
CONFIG["SCALE_DOWN_OVERHEAD"]=5000
CONFIG["SCALE_DOWN_STABILIZE_WINDOW"]=150000 # 5min
CONFIG["SCALE_DOWN_PERIOD"]=15000
CONFIG["SCALE_DOWN_STABILIZE_WINDOW_SIZE"]=CONFIG["SCALE_DOWN_STABILIZE_WINDOW"]/CONFIG["SCALE_DOWN_PERIOD"] # size: 5
# CONFIG["SCALE_DOWN_STATUS"]=[1,1] # It keeps track of stabilization window status.
## Stabilization window concept in k8s
# While scaling down, we should pick the safest (largest) "desiredReplicas" number during last stabilizationWindowSeconds.
# While scaling up, we should pick the safest (smallest) "desiredReplicas" number during last stabilizationWindowSeconds.

CONFIG["CNT_DELAYED_ROUTING"]=0
CONFIG["CROSS_CLUSTER_ROUTING"]=dict()


'''
"graph" data structure.
{parent service: [{'service': child_service_0, 'weight': data size from parent_service to child_service_0}]}
E.g.,
- example topology: User->A(frontend), A->B, A->C, B->D
    - { Service('User'): [{'service': A, 'weight': 10}] },
    - { Service('A'): [{'service": B, 'weight': 1, {'service": C, 'weight': 2) ] },
    - { Service('B'): [{'service": D, 'weight': 4 ] },
    - { Service('D'): [] },
'''

'''
"child_replica" data structure. Global view of replica level DAG.
mapping from parent replica to child replicas.
E.g.,
    {A1: 
        {B: B1, B2},
        {C: C1, C2, C3}
    },
    {A2:
        {B: B1, B2},
        {C: C1, C2, C3}
    },
    {B1: 
        {D: D1, D2}
    },
    {B2: ...},
    {C1: ...},
    {C2: ...},
    {C3: ...}
    
    # D1, D2 does not exist as a key in child_replica dictionary since it does not have any child service.
'''
class DAG:
    def __init__(self):
        self.graph = dict() # key: parent service, value: list of child services and weights
        self.graph_with_service_name = dict() # key: parent service name, value: list of child service names and weights
        self.reverse_graph = dict()
        self.reverse_graph_with_service_name = dict() # key: parent service name, value: child service name and weight
        self.weight_graph = dict() # it is nested map. will be initialized in add_dependency function. outer map key: source service name, inner map key: child service name, inner map value: weight. e.g., {weight_graph["A"]["B"]: 10}
        self.child_replica = dict() # Multi-cluster aware variable
        self.all_replica = list()
        self.all_service = list()
        

    def is_same_cluster(self, repl_0, repl_1):
        return (repl_0.id%2) == (repl_1.id%2)


    def add_service(self, service):
        self.all_service.append(service)
        
        
    def print_service_and_replica(self):
        print("===== print_service_and_replica =====")
        for service in self.all_service:
            service.print_replica()
        print()
        
        
    def add_dependency(self, parent_service, child_service, weight):
        if parent_service not in self.graph:
            self.graph[parent_service] = list()
        self.graph[parent_service].append(dict({"service": child_service, "weight": weight}))
        if child_service not in self.reverse_graph:
            self.reverse_graph[child_service] = list()
        self.reverse_graph[child_service].append(dict({'service': parent_service, 'weight': weight}))
        # Same as the above but service name as the key in dictionary not service instance
        if parent_service.name not in self.graph_with_service_name:
            self.graph_with_service_name[parent_service.name] = list()
        self.graph_with_service_name[parent_service.name].append(dict({"service": child_service.name, "weight": weight}))
        if child_service.name not in self.reverse_graph_with_service_name:
            self.reverse_graph_with_service_name[child_service.name] = list()
        self.reverse_graph_with_service_name[child_service.name].append(dict({'service': parent_service.name, 'weight': weight}))
        # easy-to-use purpose
        if parent_service.name not in self.weight_graph:
            self.weight_graph[parent_service.name] = dict()
        self.weight_graph[parent_service.name][child_service.name] = weight
        
        
    def register_replica(self, repl):
        self.all_replica.append(repl)
        if self.is_leaf(repl.service) is False:
            # In segregated cluster setup, a replica can only recognize replicas in the local cluster only.
            if CONFIG["ROUTING_ALGORITHM"] == "LCLB":
                if repl not in self.child_replica:
                    self.child_replica[repl] = dict()
                for child_svc in repl.child_services:
                    if child_svc not in self.child_replica[repl]:
                        self.child_replica[repl][child_svc] = list()
                    #########################################################
                    superset_replica = placement.svc_to_repl[repl.cluster_id][child_svc]
                    #########################################################
                    for child_repl in superset_replica:
                        # Hardcoded.
                        # TODO: The number of clusters should be able to be configurable.
                        assert repl.cluster_id == child_repl.cluster_id # local replica check.
                        self.child_replica[repl][child_svc].append(child_repl)
                
            # In Multi-cluster setup, a parent replica has all child replicas both in the local cluster and remote cluster.
            else:
                if repl not in self.child_replica:
                    self.child_replica[repl] = dict()
                for child_svc in repl.child_services:
                    if child_svc not in self.child_replica[repl]:
                        self.child_replica[repl][child_svc] = list()
                    ################################################
                    superset_child_replica = placement.all_svc_to_repl[child_svc]
                    ################################################
                    for child_repl in superset_child_replica:
                        self.child_replica[repl][child_svc].append(child_repl)
                        if LOG_MACRO: utils.print_log("WARNING", "{}'s {} childe: {}".format(repl.to_str(), child_svc.name, child_repl.to_str()))
                        
    def find_replica_by_name(self, target_repl_name):
        self.check_duplicate_replica(target_repl_name)
        for repl in self.all_replica:
            if repl.name == target_repl_name:
                return repl
            
    def count_replica_by_name(self, target_repl_name):
        cnt = 0
        for repl in self.all_replica:
            if repl.name == target_repl_name:
                cnt += 1
        return cnt
                        
    def check_duplicate_replica(self):    
        for repl in self.all_replica:
            cnt = self.count_replica_by_name(repl.name)
            if cnt > 1:
                print("There is duplicate replica name: {}, count: {}".format(repl.name, cnt))
                assert False
                        
    def print_all_replica(self):
        for repl in self.all_replica:
            print(repl.name)
           
    def deregister_replica(self, target_repl):
        if target_repl in self.child_replica:
            del self.child_replica[target_repl]
        assert target_repl not in self.child_replica
        
        # TODO: Shouldn't we delete the target replica from parent replica as a child?
        # for repl in self.child_replica:
        #     for svc in self.child_replica[repl]:
        #         if target_repl in self.child_replica[repl][svc]:
        #             self.child_replica[repl][svc].remove(target_repl)
            
        self.all_replica.remove(target_repl)
        assert target_repl not in self.all_replica
        
        
    # Multi-cluster aware function because child_replica is multi-cluster aware variable!!!
    def get_child_replica(self, parent_repl, child_svc):
        return self.child_replica[parent_repl][child_svc]


    def get_child_services(self, parent_svc):
        # NOTE: Leaf services will not be found in graph data structure's key.
        if parent_svc not in self.graph:
            if LOG_MACRO: utils.print_log("DEBUG", "get_child_services: " + parent_svc.name + " has no child. It is leaf service.")
            return None
        child_services = list()
        li = self.graph[parent_svc]
        for elem in li:
            child_services.append(elem['service'])
        if LOG_MACRO: 
            utils.print_log("DEBUG", parent_svc.name + "'s child services: ", end="")
            for child_svc in child_services:
                utils.print_log("DEBUG", child_svc.name + ", ", end="")
            utils.print_log("DEBUG", "")
        return child_services
    
    
    # Multi-cluster aware function!!!
    def get_parent_replica(self, repl, parent_svc):
        if CONFIG["ROUTING_ALGORITHM"] == "LCLB":
            p_replica = list()
            p_repl_list = list()
            try:
                p_repl_list = placement.svc_to_repl[repl.cluster_id][parent_svc]
            except:
                print("Error, repl.cluster_id:{}, parent_svc: {}".format(repl.cluster_id, parent_svc.name))
            for can_parent_repl in p_repl_list:
                assert can_parent_repl.cluster_id == repl.cluster_id
                p_replica.append(can_parent_repl)
            
            return p_replica
        else:
            return placement.all_svc_to_repl[parent_svc]


    def get_parent_services(self, svc):
        if svc not in self.reverse_graph:
            if LOG_MACRO: utils.print_log("DEBUG", "get_parent_services:" + svc.name + " has no parent service.")
            if svc.name.find("User") == -1:
                utils.error_handling(svc.name + "(non-User service) must have at least one parent service.")
            return None
        parent_services = list()
        li = self.reverse_graph[svc]
        for elem in li:
            parent_services.append(elem['service'])
        if LOG_MACRO:
            utils.print_log("DEBUG", svc.name + "'s parent services: ", end="")
            for parent_svc in parent_services:
                utils.print_log("DEBUG", parent_svc.name + ", ", end="")
            utils.print_log("DEBUG", "")
        return parent_services
    
    
    def size(self):
        return len(self.graph)

    def is_frontend(self, target_svc):
        parent_services = self.reverse_graph[target_svc]
        for svc in parent_services:
            if svc['service'].name.find("User") != -1:
                if LOG_MACRO: utils.print_log("DEBUG", "This("+svc['service'].name+") is the frontend service.")
                return True
        if LOG_MACRO: utils.print_log("DEBUG", "This("+svc['service'].name+") is not a frontend service.")
        return False
        
            
    def get_frontend(self):
        frontend_services = self.get_frontend()
        return frontend_services
    
    def get_all_service(self):
        all_service = list()
        for upstream in self.graph:
            if upstream not in all_service:
                all_service.append(upstream)
            for downstream in self.graph[upstream]:
                if downstream["service"] not in all_service:
                    all_service.append(downstream["service"])
        return all_service
                
    def print_frontend(self):
        frontend_services = self.get_frontend()
        if LOG_MACRO: utils.print_log("DEBUG", "frontend services: ", end="")
        for fend in frontend_services:
            if LOG_MACRO: utils.print_log("DEBUG", fend["service"].name + ", ", end="")
        if LOG_MACRO: utils.print_log("DEBUG", "")
        
    def is_leaf(self, service_):
        return service_ not in self.graph
        
    def print_dependency(self):
        print("="*10 + " DAG " + "="*10)
        for key in self.graph:
            for elem in self.graph[key]:
                print(key.name + "->" + elem["service"].name + ", " + str(elem["weight"]))

        
    ''' Plot graph using Graphviz '''
    def plot_graphviz(self):
        g_ = graphviz.Digraph()
        g_.node(name="U", label="User", shape='circle')
        
        all_service = self.get_all_service()
        for svc in all_service:
            if svc.name.find("User") == -1:
                if LOG_MACRO: utils.print_log("DEBUG", "add node: " + svc.name)
                g_.node(name=svc.name, label=svc.name, shape='circle')
                    
        g_.edge("U", "A", style='solid', color='black')
        for upstream in self.graph:
            if upstream.name.find("User") == -1:
                for elem in self.graph[upstream]:
                    if LOG_MACRO: utils.print_log("DEBUG", "add edge: " + upstream.name + "->" + elem["service"].name)
                    g_.edge(upstream.name, elem["service"].name, style='solid', color='black', label=upstream.lb)
                
        ## Show graph
        # g_.render('call_graph', view = True)
        
    def print_and_plot_processing_time():
        for repl in dag.all_replica:
            li = repl.processing_time_history
            if len(li) > 0:
                # print_percentiles(li, repl.to_str() + " processing_time")
                utils.plot_histogram(li, repl.to_str() + " processing_time")
                break
    
    def print_and_plot_queuing_time():
        ## Print Queueing time.
        if LOG_MACRO: utils.print_log("DEBUG", "Queueing time")
        for repl in dag.all_replica:
            li = list(repl.queueing_time.values())
            if len(li) > 0:
                if LOG_MACRO: utils.print_log("DEBUG", repl.to_str())
                utils.print_percentiles(li)
        # Plot Queueing time to see how queuing time changes.
        for repl in dag.all_replica:
            li = list(repl.queueing_time.values())
            if len(li) > 0:
                utils.plot_histogram(li, repl.to_str() + " queueing time")
    
    def print_replica_num_request():
        ## Print how many request each replica received in total.
        for repl in dag.all_replica:
            if LOG_MACRO: utils.print_log("DEBUG", repl.to_str() + " num_recv_request: " + str(repl.num_recv_request))
        
        
''' make dag variable global '''
dag = DAG()


'''
E.g.
{ Service: [Replica_0, Replica_1] },
{ service_A : [repl_0, repl_1] },
{ service_B : [repl_2, repl_3] },
{ service_C : [repl_1, repl_3] },
{ service_D : [repl_2, repl_4] }
'''
class Placement:
    def __init__(self):
        # TODO: This variable should be moved to class DAG
        self.all_svc_to_repl = dict() # It is not aware of multi-cluster.
        ## Hardcoded. Only two cluster currently.
        self.svc_to_repl = dict() # cluster 0
        self.svc_to_repl[0] = dict() # cluster 0
        self.svc_to_repl[1] = dict() # cluster 1
        self.total_num_replica = 0
        self.node_list = list()
        
    def add_node(self, node_):
        self.node_list.append(node_)

    def place_replica_to_node_and_allocate_core(self, repl_, node_, allocated_mcore_):
        # Deduct num core from Node's available core to allocate them to the replica.
        # if LOG_MACRO: utils.print_log("DEBUG", "Try to place replica " + repl_.name + " to node " + node_.to_str())
        node_.place_replica_and_allocate_core(repl_, allocated_mcore_)
        # Allocate that num core to replica. (Now, replica has dedicated cores.)
        repl_.place_and_allocate(node_, allocated_mcore_)
        
        if repl_.service not in self.all_svc_to_repl:
            self.all_svc_to_repl[repl_.service] = list()
        if repl_.service not in self.svc_to_repl[repl_.cluster_id]:
            self.svc_to_repl[repl_.cluster_id][repl_.service] = list()
            
        ##############################################
        self.all_svc_to_repl[repl_.service].append(repl_)
        self.svc_to_repl[repl_.cluster_id][repl_.service].append(repl_)
        ##############################################
        if LOG_MACRO: utils.print_log("INFO", "place_replica_to_node_and_allocate_core, {} to {}".format(repl_.to_str(), node_.to_str()))
        self.total_num_replica += 1
        
    def evict_replica_and_free_core(self, target_repl):
        for node_ in self.node_list:
            if target_repl in node_.placed_replicas:
                node_.evict_replica_from_node(target_repl)
        if LOG_MACRO: utils.print_log("INFO", "evict_replica_and_free_core, Evict replica {}, service {}".format(target_repl.to_str(), target_repl.service.name))
        self.svc_to_repl[target_repl.cluster_id][target_repl.service].remove(target_repl)
        assert target_repl not in self.svc_to_repl[target_repl.cluster_id][target_repl.service]
        self.total_num_replica -= 1
        
    def get_total_num_replicas(self):
        return self.total_num_replica
        
    def print(self):
        if LOG_MACRO:
            utils.print_log("DEBUG", "")
            utils.print_log("DEBUG", "="*40)
            utils.print_log("DEBUG", "* Placement *")
            for i in [0, 1]:
                for svc in self.svc_to_repl[i]:
                    utils.print_log("DEBUG", svc.name+": ", end="")
                    for repl in self.svc_to_repl[i][svc]:
                        utils.print_log("DEBUG", repl.to_str(), end=", ")
                    utils.print_log("DEBUG", "")
                utils.print_log("DEBUG", "="*40)

''' placement (global variable)'''
placement = Placement()


class Node:
    def __init__(self, region_id_, zone_id_, rack_id_, node_id_, total_mcore_):
        self.region_id = region_id_
        self.zone_id = zone_id_
        self.rack_id = rack_id_
        self.node_id = node_id_
        self.id = self.region_id*1000 + self.zone_id*100 + self.rack_id*10 + self.node_id
        self.total_mcore = total_mcore_
        self.available_mcore = total_mcore_
        self.placed_replicas = dict() # {Replica: Num cores}
        utils.print_log("DEBUG", "Node " + self.to_str() + " is created.")
        
    def place_replica_and_allocate_core(self, repl_, allocated_mcore_):
        if self.available_mcore < allocated_mcore_:
            utils.error_handling("node " + self.to_str() + ", Can't allocate more cpu than the node has. (available mcore: " + str(self.available_mcore) + ", requested mcore: " + str(allocated_mcore_) + ")")
        
        self.placed_replicas[repl_] = allocated_mcore_
        old_available_mcore = self.available_mcore
        self.available_mcore -= allocated_mcore_
        if LOG_MACRO: utils.print_log("DEBUG", "After core allocation, node " + self.to_str() + ", (requested mcore: " + str(allocated_mcore_) + ", available mcore: " + str(old_available_mcore) + " -> " + str(self.available_mcore) + ")")
        
    def evict_replica_from_node(self, target_repl):
        if LOG_MACRO: utils.print_log("INFO", "evict_replica_from_node, replica {} from ME:node {}".format(target_repl.to_str(), self.to_str()))
        core_in_replica = self.placed_replicas[target_repl]
        del self.placed_replicas[target_repl]
        self.available_mcore += core_in_replica
        
        
    def to_str(self):
        return str(self.region_id) + str(self.zone_id) + str(self.rack_id) + str(self.node_id)
    
''' Node (global variable for convenience) '''
# 100,000 core or 1,000*100,000 millicore
# Assumption: infinite number of core
# node_0: cluster 0
# node_1: cluster 1

nodes = list()
## It is moved to app function
# for cid in range(NUM_CLUSTER):
#     n_ = Node(region_id_=cid, zone_id_=0, rack_id_=0, node_id_=0, total_mcore_=1000*100000)
#     nodes.append(n_)
#     placement.add_node(n_)
    
# node_0 = Node(region_id_=0, zone_id_=0, rack_id_=0, node_id_=0, total_mcore_=1000*100000)
# node_1 = Node(region_id_=1, zone_id_=0, rack_id_=0, node_id_=0, total_mcore_=1000*100000)
# placement.add_node(node_0)
# placement.add_node(node_1)


class Service:
    def __init__(self, name_, mcore_per_req_, processing_t_, lb_):
        
        self.interarr_to_queuing = list()
        self.agg_pred_queue_time = [dict(), dict()] # key: replica, value: predicted queue time
        
        self.name = name_
        self.mcore_per_req = mcore_per_req_ # required num of milli-core per request
        self.processing_time = processing_t_
        self.ready_to_process = dict()
        self.ready_to_sendback = dict()
        self.lb = lb_
        self.replicas = list()
        
        if self.processing_time == 0:
            self.capacity_per_replica = 99999999
        else:
            self.capacity_per_replica = 1000 / self.processing_time
        if LOG_MACRO: utils.print_log(self.name + " capacity: " + str(self.capacity_per_replica))
    
    
    def cluster_agg_queue_time(self, metric, cid):
        if len(self.agg_pred_queue_time[cid]) == 0:
            print("{}, {}, live repl,{}".format(self.name, cid, len(self.get_cluster_live_replica(cid))))
            for repl in self.get_cluster_live_replica(cid):
                print("{}, received num request {}".format(repl.to_str(), repl.num_req))
            if cid == 0:
                other_cid = 1
            elif cid == 1:
                other_cid = 0
            else:
                utils.error_handling("Invalid cluster id: {}".format(cid))
            print("other cluster-{}'s live replica: {}".format(other_cid, len(self.get_cluster_live_replica(0))))
            ########
            assert False
            # exit()
            # return 0
            ########
        if metric == "avg":
            agg_v = sum(self.agg_pred_queue_time[cid].values())/len(self.agg_pred_queue_time[cid])
        elif metric == "min":
            min_k, agg_v = min(self.agg_pred_queue_time[cid].items(), key=lambda x: x[1])
        elif metric == "max":
            min_k, agg_v = max(self.agg_pred_queue_time[cid].items(), key=lambda x: x[1])
        elif metric == "sum":
            agg_v = sum(self.agg_pred_queue_time[cid].values())
        else:
            utils.error_handling("Invalid metric for queue time aggregation: {}".format(metric))
        return agg_v
    
    
    def num_schedulable_and_zero_queue_replica(self, cluster_id):
        schedulable_replicas = list()
        zero_queue_replicas = list()
        num_schedulable_cluster_replica = 0
        num_zero_queue_replica = 0
        for repl in self.replicas:
            if repl.cluster_id == cluster_id and repl.is_dead == False:
                if repl.is_schedulable():
                    num_schedulable_cluster_replica += 1
                    schedulable_replicas.append(repl)
                    if LOG_MACRO: utils.print_log("DEBUG", "cluster{}-service {} has schedulable replica {}".format(cluster_id, self.name, repl.to_str()))
                if repl.processing_queue_size == 0:
                    num_zero_queue_replica += 1
                    zero_queue_replicas.append(repl)
                    if LOG_MACRO: utils.print_log("DEBUG", "has_zero_queue_replica, cluster {} has zero processing queue replica for service {}".format(cluster_id, self.name))
        return schedulable_replicas, zero_queue_replicas, num_schedulable_cluster_replica, num_zero_queue_replica
    
    def num_delayed_schedulable_replica(self, cluster_id, t_):
        delayed_schd_cluster_replica = list()
        num_delayed_schd_cluster_replica = 0
        for repl in self.replicas:
            if repl.cluster_id == cluster_id and repl.is_dead == False:
                if repl.is_delayed_schedulable(t_):
                    delayed_schd_cluster_replica.append(repl)
                    num_delayed_schd_cluster_replica += 1
                    if LOG_MACRO: utils.print_log("DEBUG", "num_delayed_schedulable_replica, cluster {} has schedulable replica for service {}".format(cluster_id, self.name))
        return delayed_schd_cluster_replica, num_delayed_schd_cluster_replica
    

    ## Deprecated
    # def should_we_scale_down(self, cluster_id):
    #     desired = self.calc_desired_num_replica(cluster_id)
    #     # cur_tot_num_repl = self.get_cluster_num_replica(cluster_id) # BUG
    #     cur_tot_num_repl = self.count_cluster_live_replica(cluster_id) # BUG fixed
    #     if LOG_MACRO:
    #         utils.print_log("WARNING", "should_we_scale_down service {} cluster {} ?".format(self.name, cluster_id))
    #         utils.print_log("WARNING", "\tcur_tot_num_repl, " + str(cur_tot_num_repl))
    #         utils.print_log("WARNING", "\tdesired, " + str(desired))
    #         utils.print_log("WARNING", "\t{}".format(cur_tot_num_repl > desired))
    #     return cur_tot_num_repl > desired
        
    def add_replica(self, repl):
        self.replicas.append(repl)
        
    def count_cluster_live_replica(self, cluster_id):
        cnt = 0
        for repl in self.get_cluster_replica(cluster_id):
            if repl.is_dead == False:
                cnt += 1
        return cnt
    
    def count_cluster_dead_replica(self, cluster_id):
        cnt = 0
        for repl in self.get_cluster_replica(cluster_id):
            if repl.is_dead:
                cnt += 1
        return cnt
        
    def print_replica(self):
        print(self.name)
        for repl in self.replicas:
            print("\tcluster " + str(repl.cluster_id) + " " + repl.to_str())
            
    def sort_cluster_replica_by_least_request(self, cluster_id):
        replica_in_cluster = self.get_cluster_replica(cluster_id)
        sort_replica = list()
        for repl in replica_in_cluster:
            sort_replica.append([repl, repl.get_total_num_outstanding_response()])
        sort_replica.sort(key=lambda x: x[1])
        if LOG_MACRO: utils.print_log("INFO", "sort_replica_by_least_request")
        ret = list()
        for elem in sort_replica:
            if LOG_MACRO: utils.print_log("INFO", "\t{}, status:{}, is_dead:{}, num_outstanding:{}".format(elem[0].to_str(), elem[0].get_status(), elem[0].is_dead, elem[1]))
            ret.append(elem[0])
        return ret
            
    def find_least_request_replica(self, cluster_id):
        replica_in_cluster = self.get_cluster_replica(cluster_id)
        least_req_replica = replica_in_cluster[0]
        for repl in replica_in_cluster:
            if least_req_replica.get_total_num_outstanding_response() > repl.get_total_num_outstanding_response():
                least_req_replica = repl
        return least_req_replica

    def get_cluster_replica(self, cluster_id):
        replica_in_cluster = list()
        for repl in self.replicas:
            if cluster_id == repl.cluster_id:
                replica_in_cluster.append(repl)
        return replica_in_cluster
    
    def get_cluster_live_replica(self, cluster_id):
        replica_in_cluster = list()
        for repl in self.replicas:
            if cluster_id == repl.cluster_id and repl.is_dead == False:
                replica_in_cluster.append(repl)
        return replica_in_cluster
    
    def get_cluster_num_replica(self, cluster_id):
        return len(self.get_cluster_replica(cluster_id))
    
    
    def calc_desired_num_replica(self, cluster_id):
        if FIXED_AUTOSCALER:
            current_metric_value = self.what_I_was_supposed_to_receive(cluster_id)
        else:
            ## PAPER
            # current_metric_value = self.get_current_metric(cluster_id)
            current_metric_value = self.get_current_live_replica_metric(cluster_id)
        
        # desired_num_replica = math.ceil((current_metric_value / simulator.arg_flags.desired_autoscaler_metric) * self.get_cluster_num_replica(cluster_id))
        desired_num_replica = math.ceil((current_metric_value / simulator.arg_flags.desired_autoscaler_metric) * self.count_cluster_live_replica(cluster_id))
        if LOG_MACRO:
            utils.print_log("WARNING", "Calculate desired_num_replica of " + self.name + " in cluster_" + str(cluster_id))
            utils.print_log("WARNING", "current_metric_value, " + str((current_metric_value)))
            utils.print_log("WARNING", "DESIRED_METRIC_VALUE, " + str((simulator.arg_flags.desired_autoscaler_metric)))
            # utils.print_log("INFO", "current total num replica in cluster " + str(cluster_id) + ", " + str(self.get_cluster_num_replica(cluster_id)))
            utils.print_log("INFO", "(current_metric_value / DESIRED_METRIC_VALUE), " + str((current_metric_value / simulator.arg_flags.desired_autoscaler_metric)))
            # utils.print_log("INFO", "current_num_replica, " + str(self.get_cluster_num_replica(cluster_id)))
            utils.print_log("WARNING", "current_num_replica, " + str(self.count_cluster_live_replica(cluster_id)))
            utils.print_log("WARNING", "desired_num_replica, " + str(desired_num_replica))
        if desired_num_replica == 0:
            if LOG_MACRO: utils.print_log("WARNING", "0 number of replica is not possible. Return minimun num replica(1).")
            return 1
        return desired_num_replica
    
    
    def get_total_capacity(self, cluster_id):
        # total_capa = self.get_cluster_num_replica(cluster_id) *  self.capacity_per_replica
        total_capa = self.count_cluster_live_replica(cluster_id) *  self.capacity_per_replica
        if total_capa <= 0:
            utils.error_handling("get_total_capacity, " + self.name + ", current total live num replica: " + str(self.count_cluster_live_replica(cluster_id)) + ", capacity per replica: " + str(self.capacity_per_replica) )
        return total_capa
    
    
    ## Not being used.
    # def remaining_capacity_based_on_rps_window(self, cluster_id):
    #     remaining_capa = self.get_total_capacity(cluster_id) - self.calc_rps(cluster_id)
    #     if LOG_MACRO: utils.print_log("WARNING", "service {}, cluster:{}, remaining_capacity: {} = self.get_total_capacity: {} - current_rps: {}".format(self.name, cluster_id, remaining_capa, self.get_total_capacity(cluster_id), self.calc_rps(cluster_id)))
    #     return remaining_capa
    
    ## Not being used.
    # def remaining_capacity_based_on_last_sec(self, cluster_id):
    #     cur_capa = self.get_total_capacity(cluster_id)
    #     last_sec_rps = self.calc_last_sec_service_rps(cluster_id)
    #     remaining_capa = cur_capa - last_sec_rps
    #     if LOG_MACRO: utils.print_log("WARNING", "service {}, cluster:{}, remaining_capacity: {} = self.get_total_capacity: {} - last_sec_rps: {}".format(self.name, cluster_id, remaining_capa, cur_capa, last_sec_rps))
    #     return remaining_capa

    # Previously,this was used to calculate desired num replica, which was a bug.
    # def get_current_metric(self, cluster_id):
    #     cur_rps = self.calc_rps(cluster_id)
    #     total_capa = self.get_total_capacity(cluster_id)
    #     metric = cur_rps / total_capa
    #     if LOG_MACRO: utils.print_log("WARNING", "get_current_metric service {} in cluster_{}, metric: {}, calc_rps: {}, total_capacity: {}".format(self.name, cluster_id, metric, cur_rps, total_capa))
    #     return metric
    
    #### Key autoscaler function ####
    ## NOTE: This function represents CPU utilization.
    # It is used by calc_desired_num_replica function
    def get_current_live_replica_metric(self, cluster_id):
        # cur_rps = self.calc_avg_rps_of_live_replica(cluster_id) ## It was the previously used line.
        total_cur_rps = self.calc_last_sec_rps_of_live_replica(cluster_id) ## new line
        total_capa = self.get_total_capacity(cluster_id)
        metric = total_cur_rps / total_capa
        if LOG_MACRO: utils.print_log("WARNING", "get_current_live_replica_metric service {} in cluster_{}, metric: {}, calc_rps: {}, total_capacity: {}".format(self.name, cluster_id, metric, total_cur_rps, total_capa))
        return metric
    
    def what_I_was_supposed_to_receive(self, cluster_id):
        my_local_rps = self.calc_local_rps(cluster_id)
        if cluster_id == 0:
            other_cluster_id = 1
        else:
            other_cluster_id = 0
        what_I_am_deprived_of = self.calc_remote_rps(other_cluster_id)
        total_capa = self.get_total_capacity(cluster_id)
        imaginary_metric = (my_local_rps + what_I_am_deprived_of) / total_capa
        return imaginary_metric
    
    def get_local_rps_current_metric(self, cluster_id):
        cur_rps = self.calc_local_rps(cluster_id)
        total_capa = self.get_total_capacity(cluster_id)
        metric = cur_rps / total_capa
        if LOG_MACRO: utils.print_log("WARNING", "get_current_metric service {} in cluster_{}, metric: {}, calc_rps: {}, total_capacity: {}".format(self.name, cluster_id, metric, cur_rps, total_capa))
        return metric
    
    ## Deprecated
    # It should change the name to "calc_cluster_load_based_on_rps_windows"
    # def calc_rps(self, cluster_id): # This is the actual rps
    #     tot_rps = 0
    #     for repl in self.get_cluster_replica(cluster_id):
    #         tot_rps += repl.get_avg_rps()
    #     return tot_rps
    
    
    # It is used by calc_desired_num_replica function
    # This is the actual RPS!
    # It will be used to calculate current metric and it take the average of the last RPS_WINDOW second
    def calc_avg_rps_of_live_replica(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_live_replica(cluster_id):
            tot_rps += repl.get_avg_rps()
        return tot_rps
    
    def calc_last_sec_rps_of_live_replica(self, cluster_id): # This is the actual rps
        tot_rps = 0
        for repl in self.get_cluster_live_replica(cluster_id):
            tot_rps += repl.get_last_sec_rps() # the most recent rps
        return tot_rps
    
    # It calculates the num of requests that come from replicas having the same cluster id. 
    # Since this is a method in service class, it adds up its all replicas' local rps.
    def calc_local_rps(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_live_replica(cluster_id):
            tot_rps += repl.get_last_sec_local_rps()
        return tot_rps
    
    def calc_remote_rps(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_live_replica(cluster_id):
            tot_rps += repl.get_last_sec_remote_rps()
        return tot_rps
    
    def calc_origin_avg_rps(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_live_replica(cluster_id):
            tot_rps += repl.get_avg_origin_rps()
        return tot_rps
    
    def calc_non_origin_avg_rps(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_live_replica(cluster_id):
            tot_rps += repl.get_avg_remote_rps()
        return tot_rps
    
    ## Deprecated
    # def calc_required_num_replica(self, cluster_id):
    #     tot_rps = self.calc_rps(cluster_id)
    #     tot_required_min_num_replica = math.ceil(tot_rps / self.capacity_per_replica)
    #     return tot_required_min_num_replica
    
    def get_last_replica_id(self, cluster_id):
        biggest_id = -1
        for repl in self.get_cluster_replica(cluster_id):
            if repl.id > biggest_id:
                biggest_id = repl.id
        return biggest_id
    
    def provision(self, how_many_scale_up, cluster_id):
        if LOG_MACRO: utils.print_log("WARNING", "\nStart provision(" + CONFIG["SCALEUP_POLICY"] + ") service" + self.name + "(cluster " + str(cluster_id) + ")")
        prev_tot_num_repl = self.count_cluster_live_replica(cluster_id)
        new_tot_num_repl = prev_tot_num_repl + how_many_scale_up
        if LOG_MACRO: 
            # utils.print_log("WARNING", "\tCurrent total RPS: " + str(self.calc_rps(cluster_id)))
            # if LOG_MACRO: utils.print_log("INFO", "\tCurrent total capacity: " + str(self.capacity_per_replica*self.get_cluster_num_replica(cluster_id)))
            utils.print_log("WARNING", "\tCurrent total live capacity: " + str(self.capacity_per_replica*self.count_cluster_live_replica(cluster_id)))
            utils.print_log("WARNING", "\tprev_tot_num_repl: " + str(prev_tot_num_repl))
            # utils.print_log("WARNING", "\tmin_required_num_repl: " + str(min_required_num_repl))
            utils.print_log("WARNING", "\thow_many_scale_up: " + str(how_many_scale_up))
            utils.print_log("WARNING", "\tnew_tot_num_repl: " + str(new_tot_num_repl))
        next_replica_id = self.get_last_replica_id(cluster_id) + 2            
            
        for _ in range(how_many_scale_up):
            ''' POTENTIAL BUG '''
            # Is it allowed to pass "self"?
            # It is circular referencing each other between replica object and service object.
            new_repl = Replica(service=self, id=next_replica_id)
            if cluster_id == 0:
                target_node = nodes[0]
            else:
                target_node = nodes[1]
            placement.place_replica_to_node_and_allocate_core(new_repl, target_node, 1000)
            if LOG_MACRO: utils.print_log("WARNING", "provision, new replica {} is created. ".format(new_repl.to_str()))
            next_replica_id += 2
            self.add_replica(new_repl)
            dag.register_replica(new_repl)
            #################################################
            # A----------> NEW REPLICA--------->B
            #                     \___________->C
            # It should be multi-cluster aware.
            if dag.is_leaf(new_repl.service) == False:
            # If newly created replica is non-leaf, new replica has to register its child replica.
                for child_service in dag.get_child_services(new_repl.service):
                    for child_repl in dag.get_child_replica(new_repl, child_service): # This is multi-cluster aware child replica!
                        new_repl.register_child_replica_2(child_repl)
                    
            ## DEBUG PRINT
            if LOG_MACRO: 
                for parent_service in dag.get_parent_services(new_repl.service):
                    utils.print_log("INFO", "provision, parent_service {}".format(parent_service.name))
                    for parent_repl in dag.get_parent_replica(new_repl, parent_service):
                        utils.print_log("INFO", "\tprovision, parent_repl {}".format(parent_repl.to_str()))
                    
            for parent_service in dag.get_parent_services(new_repl.service):
                for parent_repl in dag.get_parent_replica(new_repl, parent_service):
                    parent_repl.register_child_replica_2(new_repl)
            #################################################
            if LOG_MACRO: utils.print_log("INFO", "\tprovision, new replica " + new_repl.to_str() + ", cluster " + str(cluster_id))
        if LOG_MACRO: utils.print_log("WARNING", "Finish provision " + self.name + "(cluster " + str(cluster_id) + ") from " + str(prev_tot_num_repl) + " to " + str(new_tot_num_repl))
        if LOG_MACRO: utils.print_log("WARNING", "")
        # return self.get_cluster_num_replica(cluster_id)
        
        ## update capacity
        simulator.service_capacity[cluster_id][self] = (CONFIG["MILLISECOND_LOAD_UPDATE"]/self.processing_time)*new_tot_num_repl
        
        
        return self.count_cluster_live_replica(cluster_id)
    
    ## Core function of scale down
    def remove_target_replica(self, target_repl, cluster_id):
        if LOG_MACRO: utils.print_log("WARNING", "remove_target_replica, ME:{} in cluster {}, target_repl:{}".format(self.name, cluster_id, target_repl.to_str()))
        assert target_repl.is_removed == False
        target_repl.is_removed = True
        assert cluster_id == target_repl.id%2
        assert self == target_repl.service
        
        self.replicas.remove(target_repl)
        # del self.agg_pred_queue_time[cluster_id][target_repl]
        
        if LOG_MACRO: utils.print_log("INFO", "\t\tDeleting replica " + target_repl.to_str())
        # WARNING: parent_replica should be parsed before the target replica gets removed from svc_to_repl.
        # Cluster 0 
        # User replica will not be removed so exclude it from parent service.
        parent_service = dag.get_parent_services(target_repl.service)
        for p_svc in parent_service:
            if cluster_id == 0 and p_svc.name == "User1":
                parent_service.remove(p_svc)
                if LOG_MACRO: utils.print_log("INFO", "\t\tExclude User1 from cluster {} {}'s parent service".format(cluster_id, target_repl.to_str()))
            if cluster_id == 1 and p_svc.name == "User0":
                if LOG_MACRO: utils.print_log("INFO", "\t\tExclude User0 from cluster {} {}'s parent service".format(cluster_id, target_repl.to_str()))
                parent_service.remove(p_svc)
        
        all_parent_replica = list()
        for p_svc in parent_service:
            for p_repl in dag.get_parent_replica(target_repl, p_svc):
                all_parent_replica.append(p_repl)
        dag.deregister_replica(target_repl)
        placement.evict_replica_and_free_core(target_repl)
        if LOG_MACRO: utils.print_log("WARNING", "\t\tReplica " + target_repl.to_str() + " is deleted from dag and placement. ")
        #################################################
        # A----------> TARGET REPLICA--------->B
        for parent_repl in all_parent_replica:
            parent_repl.deregister_child_replica(target_repl) ### Tricky part.
        #################################################
        
        
    def scale_down(self, how_many_scale_down, cluster_id):
        assert how_many_scale_down > 0
        
        # cur_tot_num_repl = self.get_cluster_num_replica(cluster_id)
        cur_tot_num_repl = self.count_cluster_live_replica(cluster_id)
        
        # desired = self.calc_desired_num_replica(cluster_id)
        # how_many_scale_down = cur_tot_num_repl - desired
        new_tot_num_repl = cur_tot_num_repl - how_many_scale_down # == desired
        if LOG_MACRO:
            utils.print_log("WARNING", "\nStart scale down(" + CONFIG["SCALEUP_POLICY"] + ") service" + self.name + "(cluster " + str(cluster_id) + ")")
            utils.print_log("WARNING", "\tCurrent total RPS: " + str(self.calc_rps(cluster_id)))
            utils.print_log("WARNING", "\tCurrent total live capacity: " + str(self.capacity_per_replica*self.count_cluster_live_replica(cluster_id)))
            utils.print_log("WARNING", "\cur_tot_num_repl: " + str(cur_tot_num_repl))
            # utils.print_log("INFO", "\desired: " + str(desired))
            utils.print_log("WARNING", "\tSuggested how_many_scale_down: " + str(how_many_scale_down))
            utils.print_log("WARNING", "\tnew_tot_num_repl: " + str(new_tot_num_repl))
        
        sort_replica_list = self.sort_cluster_replica_by_least_request(cluster_id)
        
        if LOG_MACRO:
            utils.print_log("INFO", "Returned sorted_replica_list:")
            for repl in sort_replica_list:
                utils.print_log("INFO", "\t{}".format(repl.to_str()))
        
        #############################################################################
        ################################ BUG BUG BUG ################################
        #############################################################################
        ## I don't understand why it doesn't work.
        #############################################################################
        # if LOG_MACRO: utils.print_log("INFO", "After filtering out dead replica, sorted_replica_list:")
        # for repl in sort_replica_list:
        #     if repl.is_dead:
        #         sort_replica_list.remove(repl)
        #         if LOG_MACRO: utils.print_log("INFO", "\t{}".format(repl.to_str()))
        #############################################################################
            
        # Filter already killed(dead) replica.
        filtered_replica_list = list()
        for repl in sort_replica_list:
            if repl.is_dead == False:
                filtered_replica_list.append(repl)
            else:
                if LOG_MACRO: utils.print_log("INFO", "Replica "+repl.to_str() + " has been already dead. It will be excluded from the candidate.")
        if LOG_MACRO: utils.print_log("INFO", "")
        
        if LOG_MACRO: 
            utils.print_log("WARNING", "Filtered replica, sorted_replica_list:")
            for repl in filtered_replica_list:
                utils.print_log("WARNING", "\t{}".format(repl.to_str()))
            utils.print_log("WARNING", "")
        
        #############################################################################
        ################################ BUG BUG BUG ################################
        #############################################################################
        ## Corner case:
        ## 1. Autoscaler Check event is executed.
        ## 2. It decides to scale down the service X.
        ## 3. There are currently four replicas for service X.
        ## 4. Autoscaler says that service X should be scaled down to two replica (four->two).
        ## 5. Sort the service X replica by num_oustanding_resnpose.
        ## 6. Remove already dead replica. 
        ##    (Dead replicas are the ones which is Not cleared out yet
        ##        because it has still processing requests which was received before it was picked to be scaled down.)
        ## 7. Choose top two target replica to scale down.
        ## 8. THEN, after removing already dead replica from the candidate replica set, 
        ##        there is only 'one' replica that can be scaled down.
        ## 9. And you want to scale down 'two'.
        ## 10. Boom. Crash. (index out of range).
        ## Solution: how_many_scale_down = min(num scaledown-possible replica, suggested scaledown num replica by autoscaler)
        #############################################################################
        if how_many_scale_down > len(filtered_replica_list):
            if LOG_MACRO: utils.print_log("INFO", "first planed scale down replica({}) > num live replica ({})".format(how_many_scale_down, len(filtered_replica_list)))
            print("INFO", "first planed scale down replica({}) > num live replica ({})".format(how_many_scale_down, len(filtered_replica_list)))
            how_many_scale_down = len(filtered_replica_list) - 1 # BUG fixed. BUG 2: At least one replica should stay alive.
            if LOG_MACRO: utils.print_log("INFO", "Decrease how_many_scale_down to {}".format(len(filtered_replica_list)))
        final_scale_down_replica = list()
        for i in range(how_many_scale_down):
            final_scale_down_replica.append(filtered_replica_list[i])
        
        final_new_tot_num_repl = cur_tot_num_repl - how_many_scale_down
        assert final_new_tot_num_repl > 0
            
        delayed_scale_down_replica = list()
        instant_scale_down_replica = list()
        if LOG_MACRO: utils.print_log("WARNING", "Scale down selected replica. It will become dead.")
        for repl in final_scale_down_replica:
            assert repl.is_dead == False
            ####################################
            ######## BUG BUG BUG ###############
            ####################################
            # if repl.update_status("(scale_down {} in {})".format(self.name, cluster_id)) == "Ready": # BUG
            if repl.get_status() == "Ready":
                instant_scale_down_replica.append(repl)
            # elif repl.update_status("(scale_down {} in {})".format(self.name, cluster_id)) == "Active": # BUG
            elif repl.get_status() == "Active":
                delayed_scale_down_replica.append(repl)
            else:
                utils.error_handling("scale_down, Invalid status, Replica " + repl.to_str())
            ####################################
            repl.is_dead = True
            if repl in repl.service.agg_pred_queue_time[cluster_id]:
                del repl.service.agg_pred_queue_time[cluster_id][repl]
            ####################################
            if LOG_MACRO: utils.print_log("WARNING", "\t{}, becomes dead. {}, num_pending: {}, child_oustanding: {}".format(repl.to_str(), repl.get_status(), repl.num_pending_request, repl.get_total_num_outstanding_response()))
            
        if CONFIG["ROUTING_ALGORITHM"] == "queueing_prediction":
            if len(repl.service.agg_pred_queue_time[cluster_id]) == 0:
                print("empty agg pred queue time: {}-{}, num_live_repl,{}".format(repl.service.name, cluster_id, new_tot_num_repl))
                # exit()
            
        if LOG_MACRO: 
            utils.print_log("WARNING", "Instant scale down of Ready replicas:")
            for repl in instant_scale_down_replica:
                utils.print_log("WARNING", "\t{}, {}, num_pending: {}, child_oustanding: {}".format(repl.to_str(), repl.get_status(), repl.num_pending_request, repl.get_total_num_outstanding_response()))
            
            
        if LOG_MACRO: 
            utils.print_log("WARNING", "Delaying scale down of Active replicas:")
            for repl in delayed_scale_down_replica:
                utils.print_log("WARNING", "\t{}, {}, is_dead:{}, num_pending: {}, child_oustanding: {}".format(repl.to_str(), repl.get_status(), repl.is_dead, repl.num_pending_request, repl.get_total_num_outstanding_response()))
        
        #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        for target_repl in instant_scale_down_replica:
            self.remove_target_replica(target_repl, cluster_id)
            
        # if LOG_MACRO: utils.print_log("INFO", "Finish scale down " + self.name + "(cluster " + str(cluster_id) + ") from " + str(cur_tot_num_repl) + " to " + str(self.get_cluster_num_replica(cluster_id)))
        if LOG_MACRO: 
            utils.print_log("WARNING", "Finish scale down " + self.name + "(cluster " + str(cluster_id) + ") from " + str(cur_tot_num_repl) + " to " + str(self.count_cluster_live_replica(cluster_id)))
            utils.print_log("WARNING", "")
        # return self.get_cluster_num_replica(cluster_id)
        return self.count_cluster_live_replica(cluster_id)

class Replica:
    # def __init__(self, service_, id_, node_):
    def __init__(self, service, id):
        self.ewma_rt = dict()
        self.cur_ewma_intarrtime = 0
        self.most_recent_arrival_time = 0
        self.arr_time_window_size = 20
        self.intarrival_time_list = list()
        self.moving_avg_interarr_time = dict()
        self.queueing_start_time = dict()
        self.ewma_interarrtime = dict()
        self.pred_queue_time = list()
        self.pred_history_size = 20 # for delayed information
        
        self.num_req_per_millisecond = 0
        
        self.service = service
        self.id = id
        self.cluster_id = id%2
        self.node = None
        self.allocated_mcore = 0 # NOTE: Replica has its own dedicated core.
        self.repl_avail_mcore = 0
        self.repl_avail_mcore_history = list()
        self.repl_history_size = 10
        self.name = self.service.name + DELIMITER + str(self.id)
        self.child_services = dag.get_child_services(self.service)
        self.child_replica = dict() # Multi-cluster aware
        self.processing_time_history = list()
        
        self.recent_load = 0
        self.time_window = 0
        
        # WARNING: outstanding_response is not used. We don't need it.
        # self.outstanding_response = dict() # {child_replica(B1):[req1, req3], child_replica(B2):[req2,req4]}
        self.num_outstanding_response_from_child = dict()
        # self.pending_request = dict() # 'MY' pending request
        self.sending_time = dict()
        self.response_time = dict() # Only holds ma_window_size of rt history.
        # self.all_rt_history = dict() # WARNING: Not being used yet. It should be deleted.
        self.is_warm = False
        self.num_recv_request = 0
        self.num_pending_request = 0 # It decrements when it receives the sendback request from the downstream.
        self.num_completed_request = 0
        
        self.processing_queue_size = 0 # Actual queue size. It doesn't count the request that is being processed.
        
        self.num_req = 0
        self.local_num_req = 0
        self.remote_num_req = 0
        self.origin_num_req = 0
        self.non_origin_num_req = 0
        self.req_count_list = list()
        self.local_req_count_list = list()
        self.remote_req_count_list = list()
        self.origin_req_count_list = list()
        self.non_origin_req_count_list = list()
        
        if self.name.find("User") >= 0:
            self.status = "Active"
        else:
            self.status = "Ready"
        self.is_dead = False
        self.is_removed = False
        
        
        # TODO:
        # D: current replica
        # B->D
        # C->D
        # parent_service data structure
        # { B : [req_0, req_2] }
        # { C : [req_1] }
        self.parent_services = dag.get_parent_services(self.service)
        
        
        # self.recv_requests = dict()
        # if self.parent_services != None:
        #     for parent_svc in self.parent_services:
        #         self.recv_requests[parent_svc] = list()
                
        ### List version
        # Assumption: All services have only one parenet.
        self.fixed_recv_requests = list() # FIFO list
        ### Dictionary version
        # To support more than one parent service call graph, dictionary datastructure is needed.
        # self.fixed_recv_requests = dict() # key: src_repl, value: list of recv_req
        
        self.fixed_ready_queue = list() # FIFO: only append and pop(0) are allowed.
        
        # TODO:
        # A: current replica
        # A->B
        # A->C
        # child_service data structure
        # { B : [req_0, req_2] }
        # { C : [req_1] }
        self.sendback_requests = dict()
        if self.child_services != None:
            if LOG_MACRO: utils.print_log("DEBUG", self.name + " child services: ", end=", ")
            for child_svc in self.child_services:
                if LOG_MACRO: utils.print_log("DEBUG", child_svc.name, end=", ")
            if LOG_MACRO: utils.print_log("DEBUG", "")
        if self.child_services != None:
            for child_svc in self.child_services:
                if child_svc not in self.sendback_requests:
                    self.sendback_requests[child_svc] = list()
                else:
                    utils.error_handling(self.to_str() + " has already " + child_svc.name + " in sendback_requests dictionary.")
            if LOG_MACRO: utils.print_log("DEBUG", self.name + " sendback_requests: ", end="")
            for child_svc in self.sendback_requests:
                if LOG_MACRO: utils.print_log("DEBUG", child_svc.name, end=", ")
            if LOG_MACRO: utils.print_log("DEBUG", "")
            
    def calc_moment_latency(self, src_repl):
        if self.cluster_id == src_repl.cluster_id:
            # NOTE: hardcoded. 
            # multiplied by 2 to reflect both send and receive network latency
            network_lat = network_latency.same_rack*2 
        else:
            network_lat = network_latency.far_inter_region*2
        queuing = self.processing_queue_size * self.service.processing_time
        processing_t = self.service.processing_time
        
        est_moment_latency = network_lat + queuing + processing_t
        return est_moment_latency
    
    def get_status(self):
        return self.status
        
    def set_status(self, status_):
        self.status = status_
        
    def is_idle(self):
        return self.num_pending_request == 0 and self.get_total_num_outstanding_response() == 0
        
            
    #         ####################################################################################
    #         ## TODO: BUG
    #         ## We have to check target replica's all parent replicas before update the status.
    #         ## "Ready" status requires all parent replicas to have '0' outstanding_response for the target replica.
    #         ####################################################################################
    #         if self.is_idle():
    #             if self.get_status() != "Ready":
    #                 prev_status = self.get_status()
    #                 self.set_status("Ready")
    #                 if LOG_MACRO: utils.print_log("INFO", "({})Changed the status, Replica {}, {}->{}".format(keyword, self.to_str(), prev_status, self.get_status()))
    #             return "Ready"
    #         else:
    #             if self.get_status() != "Active":
    #                 prev_status = self.get_status()
    #                 self.set_status("Active")
    #                 if LOG_MACRO: utils.print_log("INFO", "({})Changed the status, Replica {}, {}->{}".format(keyword, self.to_str(), prev_status, self.get_status()))
    #             return "Active"
    
    
    # This function will be called every time load balancing event happens.
    # Callee object is dst_replica of the load balancing decision.
    def increment_total_num_req(self):
        self.num_req += 1
        
    def increment_local_num_req(self):
        self.local_num_req += 1
    
    def increment_remote_num_req(self):
        self.remote_num_req += 1
    
    def increment_origin_num_req(self):
        self.origin_num_req += 1
        
    def increment_non_origin_num_req(self):
        self.non_origin_num_req += 1
    
    # This function will be called every RPS_UPDATE_INTERVAL sec by UpdateRPS event.
    def update_request_count_list(self):
        if self.service.name.find("User") == -1:
            ################################################
            ## BUG BUG BUG ##
            ## Hardcoded.
            ## I didn't update the cluster 1!!!
            # if self.cluster_id == 0:
            ################################################
            self.req_count_list.append(self.num_req) ## Main line of this function
            self.local_req_count_list.append(self.local_num_req)
            self.remote_req_count_list.append(self.remote_num_req)
            self.origin_req_count_list.append(self.origin_num_req)
            self.non_origin_req_count_list.append(self.non_origin_num_req)
            
            if len(self.req_count_list) > CONFIG["NUM_BASKET"]:
                self.req_count_list.pop(0) # Remove the oldest basket.
            if len(self.local_req_count_list) > CONFIG["NUM_BASKET"]:
                self.local_req_count_list.pop(0) # Remove the oldest basket.
            if len(self.remote_req_count_list) > CONFIG["NUM_BASKET"]:
                self.remote_req_count_list.pop(0) # Remove the oldest basket.
            if len(self.origin_req_count_list) > CONFIG["NUM_BASKET"]:
                self.origin_req_count_list.pop(0) # Remove the oldest basket.
            if len(self.non_origin_req_count_list) > CONFIG["NUM_BASKET"]:
                self.non_origin_req_count_list.pop(0) # Remove the oldest basket.    
            
            assert len(self.req_count_list) <= CONFIG["NUM_BASKET"]
            assert len(self.local_req_count_list) <= CONFIG["NUM_BASKET"]
            assert len(self.remote_req_count_list) <= CONFIG["NUM_BASKET"]
            assert len(self.origin_req_count_list) <= CONFIG["NUM_BASKET"]
            assert len(self.non_origin_req_count_list) <= CONFIG["NUM_BASKET"]
            
            self.num_req = 0 # reset num request for the next basket (next RPS_UPDATE_INTERVAL)
            self.local_num_req = 0
            self.remote_num_req = 0
            self.origin_num_req = 0
            self.non_origin_num_req = 0
            
            ## DEBUG PRINT
            # if LOG_MACRO: utils.print_log("INFO", "Updated request_count_list: " + self.to_str(), end="")
            # for elem in self.req_count_list:
            #     if LOG_MACRO: utils.print_log("INFO", elem, end=", ")
            # if LOG_MACRO: utils.print_log("INFO", "")
            # if LOG_MACRO: utils.print_log("INFO", "Updated local_request_count_list: " + self.to_str(), end="")
            # for elem in self.local_req_count_list:
            #     if LOG_MACRO: utils.print_log("INFO", elem, end=", ")
            # if LOG_MACRO: utils.print_log("INFO", "")
            # if LOG_MACRO: utils.print_log("INFO", "Updated remote_request_count_list: " + self.to_str(), end="")
            # for elem in self.remote_req_count_list:
            #     if LOG_MACRO: utils.print_log("INFO", elem, end=", ")
            # if LOG_MACRO: utils.print_log("INFO", "")
            # if LOG_MACRO: utils.print_log("INFO", "Updated origin_request_count_list: " + self.to_str(), end="")
            # for elem in self.origin_req_count_list:
            #     if LOG_MACRO: utils.print_log("INFO", elem, end=", ")
            # if LOG_MACRO: utils.print_log("INFO", "")
            # if LOG_MACRO: utils.print_log("INFO", "Updated non_origin_request_count_list: " + self.to_str(), end="")
            # for elem in self.non_origin_req_count_list:
            #     if LOG_MACRO: utils.print_log("INFO", elem, end=", ")
            # if LOG_MACRO: utils.print_log("INFO", "")
            
            # # elif self.cluster_id == 1:
            # #     self.req_count_list.append(self.num_req) ## Main line of this function
                
            # #     if len(self.req_count_list) > CONFIG["NUM_BASKET"]:
            # #         self.req_count_list.pop(0) # Remove the oldest basket.
            # #     assert len(self.req_count_list) <= CONFIG["NUM_BASKET"]
            # #     if LOG_MACRO: utils.print_log("INFO", "update_request_count_list: " + self.to_str(), end="")
            # #     self.num_req = 0 # reset num request for the next basket (next RPS_UPDATE_INTERVAL)
            # #     for elem in self.req_count_list:
            # #         if LOG_MACRO: utils.print_log("INFO", elem, end=", ")
            
            # if LOG_MACRO: utils.print_log("WARNING", self.to_str() + " avg rps: " + str(avg(self.req_count_list)))
            # if LOG_MACRO: utils.print_log("WARNING", self.to_str() + " avg local rps: " + str(avg(self.local_req_count_list)))
            # if LOG_MACRO: utils.print_log("WARNING", self.to_str() + " avg remote rps: " + str(avg(self.remote_req_count_list)))
            # if LOG_MACRO: utils.print_log("WARNING", self.to_str() + " avg origin rps: " + str(avg(self.origin_req_count_list)))
            # if LOG_MACRO: utils.print_log("WARNING", self.to_str() + " avg non_origin rps: " + str(avg(self.non_origin_req_count_list)))
            
    def get_avg_rps(self):
        # assert len(self.req_count_list) == CONFIG["NUM_BASKET"]
        if len(self.req_count_list) == 0:
            return 0
        return sum(self.req_count_list)/len(self.req_count_list)
    
    def get_last_sec_rps(self):
        if len(self.req_count_list) == 0:
            return 0
        return self.req_count_list[-1]
    
    def get_avg_local_rps(self):
        if len(self.local_req_count_list) == 0:
            return 0
        return sum(self.local_req_count_list)/len(self.local_req_count_list)
    
    def get_last_sec_local_rps(self):
        if len(self.local_req_count_list) == 0:
            return 0
        return self.local_req_count_list[-1]
    
    def get_avg_remote_rps(self):
        if len(self.remote_req_count_list) == 0:
            return 0
        return sum(self.remote_req_count_list)/len(self.remote_req_count_list)
    
    def get_last_sec_remote_rps(self):
        if len(self.remote_req_count_list) == 0:
            return 0
        return self.remote_req_count_list[-1]
    
    def get_avg_origin_rps(self):
        if len(self.origin_req_count_list) == 0:
            return 0
        return sum(self.origin_req_count_list)/len(self.origin_req_count_list)
    
    def get_avg_non_origin_rps(self):
        if len(self.non_origin_req_count_list) == 0:
            return 0
        return sum(self.non_origin_req_count_list)/len(self.non_origin_req_count_list)
    
    
    def deregister_child_replica(self, child_repl):
        if LOG_MACRO: utils.print_log("WARNING", "\t\tderegister_child_replica, Me:" + self.to_str() + ", target_child_repl: " + child_repl.to_str())
        self.child_replica[child_repl.service].remove(child_repl)
        del self.sending_time[child_repl]
        del self.response_time[child_repl]
        del self.num_outstanding_response_from_child[child_repl]
        # del self.outstanding_response[child_repl]
        # del self.all_rt_history[child_repl]
        
    
    def get_total_num_outstanding_response(self):
        total_num_outstanding = 0
        for child_repl in self.num_outstanding_response_from_child:
            total_num_outstanding += self.num_outstanding_response_from_child[child_repl]
        return total_num_outstanding
    
    
    # This is multi-cluster aware!!!
    def register_child_replica_2(self, child_repl):
        if child_repl.service not in self.child_replica:
            self.child_replica[child_repl.service] = list()
        self.child_replica[child_repl.service].append(child_repl)
        
        # if child_repl in self.outstanding_response:
        #     utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in outstanding_response")
        # self.outstanding_response[child_repl] = list()
        
        if child_repl in self.num_outstanding_response_from_child:
            utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in num_outstanding_response_from_child")
        self.num_outstanding_response_from_child[child_repl] = 0
        
        if child_repl in self.sending_time:
            utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in sending_time")
        self.sending_time[child_repl] = dict()
        
        if child_repl in self.response_time:
            utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in response_time")
        self.response_time[child_repl] = list()
        
        if LOG_MACRO: utils.print_log("INFO", "register_child_replica_2, Parent replica {} registers child replica {}.".format(self.to_str(), child_repl.to_str()))
        
        # if child_repl in self.all_rt_history:
        #     utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in all_rt_history")
        # self.all_rt_history[child_repl] = list()
    # def register_child_replica(self, child_repl, child_svc):
    #     if child_svc not in self.child_replica:
    #         self.child_replica[child_svc] = list()
    #     self.child_replica[child_svc].append(child_repl)
        
    #     if child_repl in self.outstanding_response:
    #         utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in outstanding_response")
    #     self.outstanding_response[child_repl] = list()
        
    #     if child_repl in self.num_outstanding_response_from_child:
    #         utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in num_outstanding_response_from_child")
    #     self.num_outstanding_response_from_child[child_repl] = 0
        
    #     if child_repl in self.sending_time:
    #         utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in sending_time")
    #     self.sending_time[child_repl] = dict()
        
    #     if child_repl in self.response_time:
    #         utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in response_time")
    #     self.response_time[child_repl] = list()
        
    #     if child_repl in self.all_rt_history:
    #         utils.error_handling(self.to_str() + " has " + child_repl.to_str() + " already in all_rt_history")
    #     self.all_rt_history[child_repl] = list()
        
    def place_and_allocate(self, node_, allocated_mcore_):
        self.node = node_
        self.allocated_mcore = allocated_mcore_
        self.repl_avail_mcore = allocated_mcore_
        
        
    # True from this function guarantees that AT LEAST one request could be scheduled. 
    # And possibly, more than one request could be scheduled.
    def is_schedulable(self):
        if self.repl_avail_mcore >= self.service.mcore_per_req:
            if LOG_MACRO: utils.print_log("DEBUG", self.to_str() + "is schedulable - avail core(" +  str(self.repl_avail_mcore) + ") > " + self.service.name + "(required core: "+ str(self.service.mcore_per_req) +")")
            return True
        else:
            if LOG_MACRO: utils.print_log("DEBUG", self.to_str() + " is NOT schedulable. Not enough resource - avail core(" +  str(self.repl_avail_mcore) + "), " + self.service.name + "(required core: "+ str(self.service.mcore_per_req) +")")
            return False
        
        
    def is_delayed_schedulable(self, t_):
        for elem in self.repl_avail_mcore_history[::-1]:
            ## WARNING: hardcoded delay. assuming inter-region multi-clusters
            if (t_ - elem[0]) > network_latency.far_inter_region: ## NOTE: network latency is hardcoded
                delayed_avail_core = elem[1]
                if elem[1] >= self.service.mcore_per_req:
                    if LOG_MACRO: utils.print_log("DEBUG", self.to_str() + "is delayed schedulable - avail core(" +  str(delayed_avail_core) + " at time " + str(elem[0]) + ") > " + self.service.name + "(required core: "+ str(self.service.mcore_per_req) +")")
                    
                    
                    if self.repl_avail_mcore_history[-1][1] != delayed_avail_core:
                        CONFIG["CNT_DELAYED_ROUTING"] += 1
                        # print("$$ {} is delayed schedulable ({}d vs {} at time {} = {}-{})".format(self.to_str(), str(delayed_avail_core), str(self.repl_avail_mcore_history[-1][1]), str(t_-elem[0]), str(t_), str(elem[0])))
                    return True
                else:
                    if LOG_MACRO: utils.print_log("DEBUG", self.to_str() + " is NOT delayed schedulable. Not enough resource - avail core(" +  str(delayed_avail_core) + "), " + self.service.name + "(required core: "+ str(self.service.mcore_per_req) +")")
                    return False
        return self.is_schedulable()
            

# $$ A_3(node[1000]) is delayed schedulable (1000d vs 0 at time 146.73081412538886 = 14527691.607726466-14527544.87691234)

    # def allocate_resource_to_request(self, req):
    #     if self.service.mcore_per_req > self.repl_avail_mcore:
    #         utils.error_handling(self.to_str() + "request[" + str(req.id) + "], required_mcore(" + str(self.service.mcore_per_req) +  ") > repl_avail_mcore("+str(self.repl_avail_mcore) + ") ")
    #     self.repl_avail_mcore -= self.service.mcore_per_req
    #     if self.repl_avail_mcore < 0:
    #         utils.error_handling("Negative available core is not allowed. " + self.to_str())
    #     if LOG_MACRO: utils.print_log("DEBUG", "Allocate "  + str(self.service.mcore_per_req) + " core to request[" + str(req.id) + "] in " +  self.to_str() + ", ")
    
    def allocate_resource_to_request(self, t_):
        self.repl_avail_mcore -= self.service.mcore_per_req
        self.repl_avail_mcore_history.append([t_, self.repl_avail_mcore])
        if len(self.repl_avail_mcore_history) > 20:
            self.repl_avail_mcore_history.pop(0)
        if self.repl_avail_mcore < 0:
            utils.error_handling(self.to_str() + ", negative available core(" + str(self.repl_avail_mcore) + ")")
        if LOG_MACRO: utils.print_log("DEBUG", self.to_str() + ", Allocate "  + str(self.service.mcore_per_req) + " mcore ")
        
    def free_resource_from_request(self, req, t_):
        self.repl_avail_mcore += self.service.mcore_per_req
        self.repl_avail_mcore_history.append([t_, self.repl_avail_mcore])
        if len(self.repl_avail_mcore_history) > 20:
            self.repl_avail_mcore_history.pop(0)
        if self.repl_avail_mcore > self.allocated_mcore:
            utils.error_handling("Available core in replica " + self.name + " can't be greater than allocated core. " + self.to_str())
        if self.repl_avail_mcore > self.node.total_mcore:
            utils.error_handling("Available core in replica " + self.name + " can't be greater than total num cores in node[" + self.node.to_str() + "].")
        if LOG_MACRO: utils.print_log("DEBUG", self.to_str() + ", free " + str(self.service.mcore_per_req) + " core from request[" + str(req.id) + "] in " + self.to_str())
        
    def add_to_send_back_queue(self, req_, schd_time_, child_svc_):
        if child_svc_ not in self.sendback_requests:
            if LOG_MACRO: utils.print_log("DEBUG", self.to_str() + " doesn't have " + child_svc_.name + " in its sendback_requests.")
            if LOG_MACRO: utils.print_log("DEBUG", "Currently, it has ", end="")
            for svc in self.sendback_requests:
                if LOG_MACRO: utils.print_log("DEBUG", svc.name, end=", ")
            if LOG_MACRO: utils.print_log("DEBUG", "")
        self.sendback_requests[child_svc_].append(req_)
        if LOG_MACRO: utils.print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + self.to_str() + child_svc_.name + " sendback_queue (size: " + str(len(self.sendback_requests[child_svc_])) + ")")
        
    def is_request_ready_to_send_back(self, req_):
        for key in self.sendback_requests:
            if req_ not in self.sendback_requests[key]:
                return False
        return True
    
    def remove_request_from_send_back_queue(self, req_):
        for svc in self.sendback_requests:
            self.sendback_requests[svc].remove(req_)
    
    # Old version
    # Deprecated
    # def add_to_recv_queue(self, req_, schd_time_, parent_svc_):
    #     self.recv_requests[parent_svc_].append(req_)
    #     if LOG_MACRO: utils.print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + self.to_str() + parent_svc_.name + " recv_queue (size: " + str(len(self.recv_requests[parent_svc_])) + ")")
    
    def add_to_recv_queue(self, req_, schd_time_, src_repl):
        # self: dst_replica

        ## Dictionary version
        # if src_repl not in self.fixed_recv_requests:
        #     self.fixed_recv_requests[src_repl] = list()
        # self.fixed_recv_requests[src_repl].append(req_)
        # if LOG_MACRO: utils.print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + src_repl.to_str() + " recv_queue (size: " + str(len(self.fixed_recv_requests[src_repl])) + ")")
        
        ## List version
        # This is possible because all the requests are unique.
        self.fixed_recv_requests.append([req_, src_repl])
        if LOG_MACRO: utils.print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + self.to_str() + " recv_queue (size: " + str(len(self.fixed_recv_requests)) + ")")


    def is_user(self):
        if self.parent_services == None:
            if LOG_MACRO: utils.print_log("DEBUG", self.name + " is a User.")
            return True
        return False
            
    def is_frontend(self):
        for svc in self.parent_services:
            if svc.name.find("User") != -1:
                # if LOG_MACRO: utils.print_log("DEBUG", self.name + " is a frontend service.")
                return True
            
    # TODO: likely buggy
    def is_request_ready_to_process(self, req_):
        # If this is the frontend service(A) replica, it doesn't need to wait since there is always only one parent service which is User.
        # NOTE: I don't know why I separate the cases between frontend service and non-frontend service.
        if self.is_frontend():
            if LOG_MACRO: utils.print_log("DEBUG", self.name + " is frontend, so recv request["+ str(req_.id)+"] is always ready to be processed.")
            return True
        
        ## Dictionary version
        # Check if this replica receives request from "ALL" src_replicas.
        # for src_repl_key in self.fixed_recv_requests:
        #     if req_ not in self.fixed_recv_requests[src_repl_key]:
        #         if LOG_MACRO: utils.print_log("DEBUG", "Reqeust[" + str(req_.id) + "] is waiting for " + src_repl_key.to_str())
        #         return False
        
        
        ## In List version, it is always true. 
        ## Recv is equivalent to Ready.
        return True

    
    def remove_request_from_recv_queue(self, req_):
        # NOTE: I don't know why I separate the cases between frontend service and non-frontend service.
        # if self.is_frontend():
        #     for src_repl in self.fixed_recv_requests:
        #         if req_ in self.fixed_recv_requests[src_repl]:
        #             self.recv_requests[src_repl].remove(req_)
        # else:
        #     for src_repl in self.fixed_recv_requests:
        #         self.recv_requests[src_repl].remove(req_)
        
        # Dictionary version: Still not finished. Kinda buggy.
        # for src_repl in self.fixed_recv_requests:
        #     self.fixed_recv_requests[src_repl].remove(req_)
        # return src_repl
        
        # Remember it assumes that all services have only one parent service.
        cnt = 0
        idx = -1
        for i in range(len(self.fixed_recv_requests)):
            if self.fixed_recv_requests[i][0] == req_:
                idx = i
                cnt +=1
        assert cnt == 1
        assert idx != -1
        ret = self.fixed_recv_requests.pop(idx)
        # request = ret[0]
        src_repl = ret[1]
        return src_repl        
        
    # def add_to_ready_request(self, request_, schd_time_):
    def add_to_ready_request(self, req_, src_replica):
        # self.ready_queue.put({"request": request_, "time": schd_time_})
        # Remember that there is only ONE ready_queue.
        self.fixed_ready_queue.append([req_, src_replica])
        if LOG_MACRO: utils.print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + self.to_str() + " ready queue. (size: " + str(len(self.fixed_ready_queue)) +")")
        
        
    def dequeue_from_ready_queue(self):
        # if self.fixed_ready_queue:
        #     utils.error_handling(self.to_str() + ", ready queue is empty!")
        ret = self.fixed_ready_queue.pop(0) # FIFO. pop the first elem
        request = ret[0]
        src_replica = ret[1]
        
        if LOG_MACRO: utils.print_log("DEBUG", "Dequeue request[" + str(request.id) + "], from"  + self.to_str() + " ready queue. (size: " + str(len(self.fixed_ready_queue)) + ")")
        
        # return req["request"], req["time"]
        return request, src_replica
    
    # DEPRECATED
    # def process_request(self):
    #     req, t = self.dequeue_from_ready_queue()
    #     assert req != None
    #     # TODO: Resource scheduler could be used here
    #     self.allocate_resource_to_request(req, self.service.mcore_per_req)
    #     # self.record_process(req)
    #     return req, t
    
    # def record_process(self, req):
    #     self.process_history.append(req)
    
    # def ready_queue_size(self):
    #     return self.ready_queue.qsize()
    
    # def ready_queue_empty(self):
    #     return self.ready_queue.empty()

    def to_str(self):
        return self.name + "(node[" + self.node.to_str() + "])"
    

class Simulator:
    def __init__(self, req_arr_0, req_arr_1, cur_time, arg_flags):
        self.millisecond_load = [dict(), dict()]
        self.local_routing_weight = [dict(), dict()]
        self.routing_weight_log = list()
        
        self.queueing_function = dict()
        self.num_started_req = 0
        self.warmup = False
        
        self.c0_num_replicas_recommendation = list()
        self.c1_num_replicas_recommendation = list()
        self.program_start_ts = time.time()
        self.request_arr_0 = req_arr_0
        self.request_arr_1 = req_arr_1
        self.app = arg_flags.app
        self.cur_time = cur_time
        self.arg_flags = arg_flags
        self.sim_start_timestamp = time.time()
        assert len(self.request_arr_0) > 0
        assert len(self.request_arr_1) > 0
        self.first_time_flag = True
        self.user0_num_req = len(self.request_arr_0)
        self.user1_num_req = len(self.request_arr_1)
        self.total_num_req = len(self.request_arr_0) + len(self.request_arr_1)
        if LOG_MACRO: utils.print_log("DEBUG", "user0 num_req: " + str(self.user0_num_req))
        if LOG_MACRO: utils.print_log("DEBUG", "user1 num_req: " + str(self.user1_num_req))
        if LOG_MACRO: utils.print_log("DEBUG", "total num_req: " + str(self.total_num_req))
        self.current_time = 0
        self.event_queue = list()
        heapq.heapify(self.event_queue)
        self.request_start_time = dict()
        self.request_end_time = dict()
        self.end_to_end_latency = dict() # key: request, value: latency
        self.user0_latency = list()
        self.user1_latency = list()
        self.cluster0_service_latency = dict()
        for service in dag.all_service:
            self.cluster0_service_latency[service] = dict()
        self.cluster1_service_latency = dict()
        for service in dag.all_service:
            self.cluster1_service_latency[service] = dict()
        
        self.service_capacity = [dict(), dict()]
        
        self.cluster_capacity = dict() # file write friendly data structure
        for service in dag.all_service:
                self.cluster_capacity[service] = list()
                
        # self.cluster0_capacity = dict()
        # self.cluster1_capacity = dict()
        # def create_key_with_service(dic_):
        #     for service in dag.all_service:
        #         dic_[service] = list()
        # create_key_with_service(self.cluster0_capacity)
        # create_key_with_service(self.cluster1_capacity)
    
    def calc_ewma(self, curr, prev, lmda=0.5):
        # EWMA_t = λY_t+(1−λ)EWMA_t−1
        if prev == 0:
            return curr
        return lmda*curr + (1-lmda)*prev
    
    # @profile
    def append_capacity_data_point(self, cluster_id, ts, svc):
        self.cluster_capacity[svc].append([cluster_id, ts, svc.count_cluster_live_replica(cluster_id), svc.capacity_per_replica, svc.get_total_capacity(cluster_id)])
        
        
    def get_latency(self, cluster_id):
        if cluster_id == 0:
            latency = [ x[1] for x in self.user0_latency ]
        elif cluster_id == 1:
            latency = [ x[1] for x in self.user1_latency ]
        else:
            utils.error_handling("Invalid cluster id: {}".format(cluster_id))
        return latency
        
    def print_summary(self):
        if LOG_MACRO: utils.print_log("DEBUG", "="*30)
        if LOG_MACRO: utils.print_log("DEBUG", "* simulator summary *")
        if LOG_MACRO: utils.print_log("DEBUG", "="*30)
        l_li = list(self.end_to_end_latency.values())
        utils.print_percentiles(l_li)
        # p = ", processing time: 10ms"
        if len(self.user0_latency) > 0:
            cluster0_latency = self.get_latency(cluster_id = 0)
            if LOG_MACRO: utils.print_log("DEBUG", "")
            if LOG_MACRO: utils.print_log("DEBUG", "="*30)
            if LOG_MACRO: utils.print_log("DEBUG", "* User group 0 latency summary *")
            utils.print_percentiles(cluster0_latency)
            if LOG_MACRO: utils.print_log("DEBUG", "len(cluster0_latency): " +str(len(cluster0_latency)))
        if len(self.user1_latency) > 0:
            cluster1_latency_record_time = [ x[0] for x in self.user1_latency ]
            cluster1_latency = self.get_latency(cluster_id = 1)
            if LOG_MACRO: utils.print_log("DEBUG", "")
            if LOG_MACRO: utils.print_log("DEBUG", "="*30)
            if LOG_MACRO: utils.print_log("DEBUG", "* User group 1 latency summary *")
            utils.print_percentiles(cluster1_latency)
            if LOG_MACRO: utils.print_log("DEBUG", "len(cluster1_latency): " + str(len(cluster1_latency)))
            if LOG_MACRO: utils.print_log("DEBUG", "")
            
    def get_experiment_title(self):
        return self.arg_flags.app+"-"+self.arg_flags.workload+"-"+self.arg_flags.load_balancer+"-"+self.arg_flags.routing_algorithm
    
    def get_output_dir(self):
        output_path = self.arg_flags.output_dir + "/" + self.arg_flags.workload + "/" + self.arg_flags.app + "/" + self.arg_flags.load_balancer + "-" + self.arg_flags.routing_algorithm
        # dir_list = output_path.split("/")
        if os.path.exists(output_path) == False:
            os.makedirs(output_path)
        return output_path
    
    def write_metadata_file(self):
        output_dir = self.get_output_dir()
        meta_file = open(output_dir+"/metadata.txt", 'w')
        temp_list = list()
        for key, value in vars(self.arg_flags).items():
            temp_list.append(key + " : " + str(value)+"\n")
        for key in CONFIG:
            temp_list.append(key + " : " + str(CONFIG[key])+"\n")
        temp_list.append("start_time : " + str(self.cur_time)+"\n")
        temp_list.append("runtime : " + str(time.time() - self.sim_start_timestamp)+"\n")
        meta_file.writelines(temp_list)
        
        if 0 in CONFIG["CROSS_CLUSTER_ROUTING"]:
            meta_file.write("CROSS CLUSTER {} : {}".format(0, CONFIG["CROSS_CLUSTER_ROUTING"][0]))
        if 1 in CONFIG["CROSS_CLUSTER_ROUTING"]:
            meta_file.write("CROSS CLUSTER {} : {}".format(1, CONFIG["CROSS_CLUSTER_ROUTING"][1]))
        if 0 in CONFIG["CROSS_CLUSTER_ROUTING"] and 1 in CONFIG["CROSS_CLUSTER_ROUTING"]:
            meta_file.write("TOTAL_CROSS_CLUSTER_ROUTING: {}".format(CONFIG["CROSS_CLUSTER_ROUTING"][0] + CONFIG["CROSS_CLUSTER_ROUTING"][1]))
        for key, value in CONFIG["CROSS_CLUSTER_ROUTING"].items():
            if key != 0 and key != 1:
                meta_file.write("CROSS CLUSTER {} : {}".format(key, value))
    
        meta_file.close()
            
    def write_simulation_latency_result(self):
        def write_latency_result(li_, cluster_id, path):
            f_ = open(path, 'w')
            temp_list = list()
            for elem in li_:
                temp_list.append(str(elem)+"\n")
            f_.writelines(temp_list)
            f_.close()
            
        cluster0_latency = self.get_latency(cluster_id=0)
        cluster1_latency = self.get_latency(cluster_id=1)
        output_dir = self.get_output_dir()
        path_to_latency_file_cluster_0 = output_dir + "/latency-cluster_0.txt"
        path_to_latency_file_cluster_1 = output_dir + "/latency-cluster_1.txt"
        write_latency_result(cluster0_latency, cluster_id=0, path=path_to_latency_file_cluster_0)
        write_latency_result(cluster1_latency, cluster_id=1, path=path_to_latency_file_cluster_1)
        
        
    def write_req_arr_time(self):
        def file_write_request_arrival_time(req_arr, cluster_id, path):
            file1 = open(path, 'w')
            temp_list = list()
            for elem in req_arr:
                temp_list.append(str(elem)+"\n")
            file1.writelines(temp_list)
            file1.close()
        path_to_req_arr_cluster_0 = self.get_output_dir()+"/request_arrival-cluster_0.txt"
        path_to_req_arr_cluster_1 = self.get_output_dir()+"/request_arrival-cluster_1.txt"
        assert path_to_req_arr_cluster_0 != path_to_req_arr_cluster_1
        file_write_request_arrival_time(self.request_arr_0, cluster_id=0, path=path_to_req_arr_cluster_0)
        file_write_request_arrival_time(self.request_arr_1, cluster_id=1, path=path_to_req_arr_cluster_1)
        
    def write_resource_provisioning(self):
        path_ = self.get_output_dir() + "/resource_provisioing_log.csv"
        file1 = open(path_, 'w')
        temp_list = list()
        col = "cluster_id,service,processing_time,timestamp,num_replica,rps_per_replica,capacity\n"
        temp_list.append(col)
        for service in self.cluster_capacity:
            for elem in self.cluster_capacity[service]:
                temp_list.append(str(elem[0]) + "," + str(service.name) + "," + str(service.processing_time) + "," + str(elem[1]) + "," + str(elem[2]) + "," + str(elem[3]) + "," + str(elem[4]) + "\n")
        file1.writelines(temp_list)
        file1.close()
            
        # def file_write_autoscaler_timestamp(capa, path):
        #     file1 = open(path, 'w')
        #     temp_list = list()
        #     col = "cluster_id,service,processing_time,timestamp,num_replica,rps_per_replica,capacity\n"
        #     temp_list.append(col)
        #     for service in capa:
        #         for elem in capa[service]:
        #             temp_list.append(str(service.name) + "," + str(service.processing_time) + "," + str(elem[0]) + "," + str(elem[1]) + "," + str(elem[2]) + "," + str(elem[3]) + "\n")
        #     file1.writelines(temp_list)
        #     file1.close()
        # path_to_autoscaler_cluster_0 = self.get_output_dir()+"/resource_provisioing_log-cluster_0.csv"
        # path_to_autoscaler_cluster_1 = self.get_output_dir()+"/resource_provisioing_log-cluster_1.csv"
        # assert path_to_autoscaler_cluster_0 != path_to_autoscaler_cluster_1
        # file_write_autoscaler_timestamp(self.cluster0_capacity, path=path_to_autoscaler_cluster_0)
        # file_write_autoscaler_timestamp(self.cluster1_capacity, path=path_to_autoscaler_cluster_1)

    
    def write_interarr_to_queuing_list(self):
        for svc in dag.all_service:
            fname = "queuing_list-"+svc.name+".txt"
            path_ = self.get_output_dir() + "/" + fname
            file1 = open(path_, 'w')
            col = "ewma_inter_arrival_time,moving_avg_inter_arrival_time_20,moving_avg_inter_arrival_time_15,moving_avg_inter_arrival_time_12,moving_avg_inter_arrival_time_10,moving_avg_inter_arrival_time_5,queuing_time\n"
            file1.write(col)
            for elem in svc.interarr_to_queuing:
                line = str()
                for e in elem:
                    line += (str(e) + ",")
                file1.write(line[:-1]+'\n')
            file1.close()
            
    def load_queueing_function(self, fname, svc):
        # path_ = self.get_output_dir() + "/" + fname
        path_ = "log/sample-latencyfunc2-test/three_depth/RoundRobin-heuristic_TE/" + fname
        temp_iat = list()
        qt = list()
        with open(path_, "r") as f_:
            li = f_.readlines()
            for elem in li:
                elem = elem.split(",")
                temp_iat.append(float(elem[0]))
                qt.append(float(elem[1]))
        iat = list()
        for i in range(len(temp_iat)):
            if i == 0:
                prev = 0
            else:
                prev = temp_iat[i-1]
            curr = temp_iat[i]
            iat.append([prev, curr])
        self.queueing_function[svc] = [iat, qt]
    
    def binary_search(self, arr, low, high, target):
        if high >= low:
            mid = (high + low) // 2
            if target >= arr[mid][0]  and  target <= arr[mid][1]:
                return mid
            elif target < arr[mid][0]:
                return self.binary_search(arr, low, mid - 1, target)
            else:
                return self.binary_search(arr, mid + 1, high, target)
        else:
            utils.error_handling("binary search fail: {}, {}".format(svc.name, target))
    
    def predict_queueing_time(self, svc, target_iat):
        iat = self.queueing_function[svc][0]
        qt = self.queueing_function[svc][1]
        if target_iat > iat[-1][1]:
            idx = -1
        else:
            idx = self.binary_search(iat, 0, len(iat)-1, target_iat)
        # print("target_iat,{}, iat[{}-{}], prediction,{}".format(target_iat, iat[idx][0], iat[idx][1], qt[idx]))
        return qt[idx]
        
    def schedule_event(self, event):
        # if LOG_MACRO: utils.print_log("DEBUG", "Scheduled: " + event.name + " event at " + str(event.scheduled_time))
        #heapq.heappush(self.event_queue, (event.scheduled_time, np.random.uniform(0, 1, 1)[0], event))
        heapq.heappush(self.event_queue, (event.scheduled_time, random.random(), event))
        # for elem in self.event_queue:
        #     print(str(elem[0]) + ", " + elem[2].name)
            # print(str(elem[0]) + ", " + str(elem[1]) + ", " + elem[2].name)
        # NOTE: heap generates the following error when two different items in heap have the same event.scheduled_time. Heap compares second item in tuple to do min/max heap push operation.
        # TypeError: '<' not supported between instances of 'TryToProcessRequest' and 'SendBackRequest'\
        # Solution: Add a random number as a second argument to heap. It will be the comparator when two event.scheduled_time items have the same value.

    def start_simulation(self):
        if LOG_MACRO: utils.print_log("DEBUG", "========== Start simulation ==========")
        if LOG_MACRO: utils.print_log("DEBUG", "- total num request: " + str(len(self.event_queue)))
        while len(self.event_queue) > 0:
            next_event = heapq.heappop(self.event_queue)[2] # [2]: event
            # print("Next event: " + next_event.name)
            # print("Next event: " + next_event.name + ", request[" + str(next_event.request.id) + "]")
            self.current_time = next_event.scheduled_time
            next_event.execute_event()
        if LOG_MACRO: utils.print_log("DEBUG", "========== Finish simulation ==========")
        print("="*40)
        print("program run time: {}".format(time.time() - self.program_start_ts))
        print("="*40)
        print()
        
class Event:
    def execute_event(self):
        utils.error_handling("You must implement execute_event function for child class of Event class.")


class Request:
    def __init__(self, id_, origin_cluster):
        self.id = id_
        self.origin_cluster = origin_cluster
        self.history = list() # Stack
        
        self.queue_arrival_time = 0
        
        
    def push_history(self, src_repl, dst_repl):
        self.history.append([src_repl, dst_repl])
        if LOG_MACRO: utils.print_log("DEBUG", "[Push] [" + src_repl.to_str() + ", " + dst_repl.to_str() + "]")
        self.print_history()
        # print("History of request[" + str(self.id) + "]")
        # for elem in self.history:
        #     print("[" + elem[0].to_str() + ", " + elem[1].to_str() + "]")
    
    #TODO: [IMPORTANT] Strong assumption: all services have only one parents.
    def pop_history(self, dst_repl):
        self.print_history()
        for elem in self.history:
            ###############################
            # TODO: IS BUG HERE ? not confirmed yet...
            ###############################
            if elem[1] == dst_repl:
                if LOG_MACRO: utils.print_log("DEBUG", "Pop [" + elem[0].to_str() + ", " + elem[1].to_str() + "]")
                ret = elem
                self.history.remove(elem)
                return ret
        self.print_history()
        utils.error_handling("Couldn't find the " + dst_repl.to_str() + " in request[" + str(self.id) + "] history")
        
    def print_history(self):
        if LOG_MACRO: utils.print_log("DEBUG", "Request[" + str(self.id) + "] History: ")
        for record in self.history:
            if LOG_MACRO: utils.print_log("DEBUG", "\t" + record[0].to_str() + "->" + record[1].to_str())
            # record[0]: src, record[1]: dst

######################################################################################################
## Under construction, 100ms time window load update
###########################
class UpdateRoutingWeight(Event):
    def __init__(self, schd_time_):
        self.scheduled_time = schd_time_
        self.name = "UpdateRoutingWeight"

    def event_latency(self):
        return 0

    def execute_event(self):
        e_latency = self.event_latency()
        if LOG_MACRO: utils.print_log("INFO", "Execute: UpdateRoutingWeight during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        for cid in [0,1]:
            for svc in dag.all_service:
                if svc.name.find("User") == -1:
                    simulator.millisecond_load[cid][svc] = 0 # initialize to 0.
                    # replicas of a service in cluster i.
                    for repl in svc.get_cluster_live_replica(cid):
                    # for repl in placement.svc_to_repl[cid][svc]:
                        simulator.millisecond_load[cid][svc] += repl.num_req_per_millisecond # add up loads of all replicas of svc
                        repl.num_req_per_millisecond = 0 # initialize to zero again for the next XX ms
                    
        for svc in dag.all_service:
            if svc.name.find("User") == -1:
                total_capa = simulator.service_capacity[0][svc] + simulator.service_capacity[1][svc]
                total_load = simulator.millisecond_load[1][svc] + simulator.millisecond_load[1][svc]
                total_over = total_capa - total_load
                c0_remaining_capa = simulator.service_capacity[0][svc] - simulator.millisecond_load[0][svc]
                c1_remaining_capa = simulator.service_capacity[1][svc] - simulator.millisecond_load[1][svc]
                c0_cross_cluster_routing_weight = 0
                c1_cross_cluster_routing_weight = 0
                #################################################
                if c0_remaining_capa < 0 and c1_remaining_capa > 0:
                # if c1_remaining_capa > c0_remaining_capa:
                    # simulator.local_routing_weight[0][svc] = 0.0 # This is local routing weight
                    
                    c0_cross_cluster_routing_weight = min(abs(c0_remaining_capa), c1_remaining_capa) ## Original code
                    # c0_cross_cluster_routing_weight = max(abs(c0_remaining_capa), c1_remaining_capa)
                    simulator.local_routing_weight[0][svc] = (simulator.millisecond_load[0][svc] - c0_cross_cluster_routing_weight)/simulator.millisecond_load[0][svc]
                    simulator.local_routing_weight[1][svc] = 1.0
                elif c0_remaining_capa > 0 and c1_remaining_capa < 0:
                # elif c0_remaining_capa > c1_remaining_capa:
                    # simulator.local_routing_weight[1][svc] = 0.0
                    
                    c1_cross_cluster_routing_weight = min(c0_remaining_capa, abs(c1_remaining_capa)) ## Original code
                    # c1_cross_cluster_routing_weight = max(c0_remaining_capa, abs(c1_remaining_capa))
                    simulator.local_routing_weight[1][svc] = (simulator.millisecond_load[1][svc] - c1_cross_cluster_routing_weight)/simulator.millisecond_load[1][svc]
                    simulator.local_routing_weight[0][svc] = 1.0
                else:
                    simulator.local_routing_weight[0][svc] = 1.0
                    simulator.local_routing_weight[1][svc] = 1.0
                #################################################
                simulator.routing_weight_log.append("{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(\
                        svc.name, \
                        simulator.local_routing_weight[0][svc], \
                        simulator.local_routing_weight[1][svc], \
                        c0_cross_cluster_routing_weight, \
                        c1_cross_cluster_routing_weight, \
                        simulator.service_capacity[0][svc], \
                        simulator.millisecond_load[0][svc], \
                        simulator.service_capacity[1][svc], \
                        simulator.millisecond_load[1][svc], \
                        total_capa, \
                        total_load, \
                        c0_remaining_capa, \
                        c1_remaining_capa, \
                        total_over))
                # print(simulator.routing_weight_log[-1])
                
        if len(simulator.local_routing_weight) > 1000:
            path = simulator.get_output_dir() + "/routing_weight_log.csv"
            if os.path.exists(path) == False:
                with open(path, 'w') as file1:
                    file1.write("service,\
                                c0_local_routing_weight,\
                                c1_local_routing_weight,\
                                c0_cross_cluster_routing_weight,\
                                c1_cross_cluster_routing_weight,\
                                c0_capacity,\
                                c0_millisecond_load,\
                                c1_capacity,\
                                c1_millisecond_load,\
                                total_capacity,\
                                total_millisecond_load,\
                                c0_capa-load,\
                                c1_capa-load,\
                                total_capa-load\n")
            else:
                with open(path, 'a') as file1:
                    file1.writelines(simulator.routing_weight_log)
            simulator.routing_weight_log.clear()
######################################################################################################
                

class UpdateRPS(Event):
    def __init__(self, schd_time_):
        self.scheduled_time = schd_time_
        self.name = "UpdateRPS"

    def event_latency(self):
        return 0

    def execute_event(self):
        e_latency = self.event_latency()
        if LOG_MACRO: utils.print_log("INFO", "Execute: UpdateRPS during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        ## Update the request count for ALL replica.
        for repl in dag.all_replica:
            repl.update_request_count_list()
            


# class FinishScaleDownStabilization(Event):
#     def __init__(self, schd_time_, cluster_id):
#         self.scheduled_time = schd_time_
#         self.cluster_id = cluster_id
#         self.name = "FinishScaleDownStabilization"

#     def event_latency(self):
#         return 0

#     def execute_event(self):
#         e_latency = self.event_latency()
#         if LOG_MACRO: utils.print_log("INFO", "Execute: FinishScaleDownStabilization cluster " + str(self.cluster_id) + " can scale down again. " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
#         assert CONFIG["SCALE_DOWN_STATUS"][self.cluster_id] == 0
#         CONFIG["SCALE_DOWN_STATUS"][self.cluster_id] = 1
            
            
class ProgressPrint(Event):
    def __init__(self, schd_time_, cur_req_arr_idx, max_req_arr_idx, progress_percentage):
        self.scheduled_time = schd_time_
        self.name = "ProgressPrint"
        self.progress_percentage = progress_percentage
        self.cur_req_arr_idx = cur_req_arr_idx
        self.max_req_arr_idx = max_req_arr_idx
        
    def execute_event(self):
        print("$$ ProgressPrint: progress,{}%, elapsed_time,{}s, heap size,{}, {}, {}-{}".format(self.progress_percentage, int(time.time()-simulator.program_start_ts), len(simulator.event_queue), simulator.arg_flags.workload, simulator.arg_flags.load_balancer, simulator.arg_flags.routing_algorithm))


class CheckScaleUp(Event):
    def __init__(self, schd_time_):
        self.scheduled_time = schd_time_
        self.name = "CheckScaleUp"

    def event_latency(self):
        return 0

    def execute_event(self):
        if LOG_MACRO: utils.print_log("INFO", "Execute: CheckScaleUp during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time)))
        for service in dag.all_service:
            if service.name.find("User") == -1:
                simulator.append_capacity_data_point(0, self.scheduled_time, service)
                simulator.append_capacity_data_point(1, self.scheduled_time, service)
                ## Cluster 0
                cluster_0_desired = service.calc_desired_num_replica(0)
                cluster_0_cur_tot_num_repl = service.count_cluster_live_replica(0)
                if  cluster_0_desired > cluster_0_cur_tot_num_repl: # overloaded
                    how_many_scale_up = cluster_0_desired - cluster_0_cur_tot_num_repl
                    assert how_many_scale_up > 0
                    scale_up_event = ScaleUp(self.scheduled_time + CONFIG["SCALE_UP_OVERHEAD"], service, how_many_scale_up, cluster_id=0)
                    simulator.schedule_event(scale_up_event)
                    if LOG_MACRO: utils.print_log("WARNING", "Schedule ScaleUp, cluster{}-service {} from {} to {}".format(0, service.name, cluster_0_cur_tot_num_repl, cluster_0_desired))
                ## Cluster 1
                cluster_1_desired = service.calc_desired_num_replica(1)
                cluster_1_cur_tot_num_repl = service.count_cluster_live_replica(1)
                if cluster_1_desired > cluster_1_cur_tot_num_repl:
                    how_many_scale_up = cluster_1_desired - cluster_1_cur_tot_num_repl
                    assert how_many_scale_up > 0
                    scale_up_event = ScaleUp(self.scheduled_time + CONFIG["SCALE_UP_OVERHEAD"], service, how_many_scale_up, cluster_id=1)
                    simulator.schedule_event(scale_up_event)
                    if LOG_MACRO: utils.print_log("WARNING", "Schedule ScaleUp, cluster{}-service {} from {} to {}".format(1, service.name, cluster_1_cur_tot_num_repl, cluster_1_desired))


class CheckScaleDown(Event):
    def __init__(self, schd_time_):
        self.scheduled_time = schd_time_
        self.name = "CheckScaleDown"

    def event_latency(self):
        return 0

    def execute_event(self):
        if LOG_MACRO: utils.print_log("INFO", "Execute: CheckScaleDown during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time )))
        for service in dag.all_service:
            if service.name.find("User") == -1:
                
                c0_cur_num_repl = service.count_cluster_live_replica(0)
                c0_desired = service.calc_desired_num_replica(0)
                c1_cur_num_repl = service.count_cluster_live_replica(1)
                c1_desired = service.calc_desired_num_replica(1)
                if CONFIG["SCALE_DOWN_STABILIZE_WINDOW"] == 0: # no stabilization window
                    ## Cluster 0
                    if c0_cur_num_repl > c0_desired:
                        how_many_scale_down = c0_cur_num_repl - c0_desired
                        scale_down_event = ScaleDown(self.scheduled_time + CONFIG["SCALE_DOWN_OVERHEAD"], service, how_many_scale_down, cluster_id=0)
                        simulator.schedule_event(scale_down_event)
                    ## Cluster 1
                    if c1_cur_num_repl > c1_desired:
                        how_many_scale_down = c1_cur_num_repl - c1_desired
                        scale_down_event = ScaleDown(self.scheduled_time + CONFIG["SCALE_DOWN_OVERHEAD"], service, how_many_scale_down, cluster_id=1)
                        simulator.schedule_event(scale_down_event)
                else:
                    # Cluster 0
                    simulator.c0_num_replicas_recommendation.append(c0_desired)
                    if len(simulator.c0_num_replicas_recommendation) > CONFIG["SCALE_DOWN_STABILIZE_WINDOW_SIZE"]:
                        simulator.c0_num_replicas_recommendation.pop(0)
                    if c0_cur_num_repl > max(simulator.c0_num_replicas_recommendation):
                        how_many_scale_down = c0_cur_num_repl - max(simulator.c0_num_replicas_recommendation)
                        scale_down_event = ScaleDown(self.scheduled_time + CONFIG["SCALE_DOWN_OVERHEAD"], service, how_many_scale_down, cluster_id=0)
                        simulator.schedule_event(scale_down_event)
                    simulator.c1_num_replicas_recommendation.append(c1_desired)
                    # Cluster 1
                    if len(simulator.c1_num_replicas_recommendation) > CONFIG["SCALE_DOWN_STABILIZE_WINDOW_SIZE"]:
                        simulator.c1_num_replicas_recommendation.pop(0)
                    if c1_cur_num_repl > max(simulator.c1_num_replicas_recommendation):
                        how_many_scale_down = c1_cur_num_repl - max(simulator.c1_num_replicas_recommendation)
                        scale_down_event = ScaleDown(self.scheduled_time + CONFIG["SCALE_DOWN_OVERHEAD"], service, how_many_scale_down, cluster_id=1)
                        simulator.schedule_event(scale_down_event)
            

## Deprecated
# class AutoscalerCheck(Event):
#     def __init__(self, schd_time_):
#         self.scheduled_time = schd_time_
#         self.name = "AutoscalerCheck"

#     def event_latency(self):
#         return 0

#     def execute_event(self):
#         # CONFIG["SCALE_UP_OVERHEAD"]=5000 # 5sec
#         # CONFIG["SCALE_DOWN_OVERHEAD"]=5000 # 5sec
#         e_latency = self.event_latency()
#         if LOG_MACRO: utils.print_log("INFO", "Execute: AutoscalerCheck during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
#         for service in dag.all_service:
#             if service.name.find("User") == -1:
#                 simulator.append_capacity_data_point(0, self.scheduled_time, service)
#                 simulator.append_capacity_data_point(1, self.scheduled_time, service)
#                 # simulator.cluster0_capacity[service].append([self.scheduled_time, service.count_cluster_live_replica(0), service.capacity_per_replica, service.get_total_capacity(0)])
#                 # simulator.cluster1_capacity[service].append([self.scheduled_time, service.count_cluster_live_replica(1), service.capacity_per_replica, service.get_total_capacity(1)])
                
#                 ## Cluster 1
#                 cluster_0_desired = service.calc_desired_num_replica(0)
#                 cluster_0_cur_tot_num_repl = service.count_cluster_live_replica(0)
#                 overloaded_cluster_0 = cluster_0_desired > cluster_0_cur_tot_num_repl
#                 # Scale up check
#                 if overloaded_cluster_0: # overloaded
#                     if LOG_MACRO: utils.print_log("WARNING", "Overloaded! service {} in cluster {}, desired: {}, current: {}".format(service.name, 0, cluster_0_desired, cluster_0_cur_tot_num_repl))
#                     how_many_scale_up = cluster_0_desired - cluster_0_cur_tot_num_repl
#                     assert how_many_scale_up > 0
#                     if LOG_MACRO: utils.print_log("WARNING", "Schedule ScaleUp, cluster{}-service {} from {} to {}".format(0, service.name, cluster_0_cur_tot_num_repl, cluster_0_desired))
#                     scale_up_event = ScaleUp(self.scheduled_time + CONFIG["SCALE_UP_OVERHEAD"] + e_latency, service, how_many_scale_up, cluster_id=0)
#                     simulator.schedule_event(scale_up_event)
#                 # If not overloaded, then check if scale down is needed.
#                 elif CONFIG["SCALE_DOWN_STATUS"][0]==1 and service.should_we_scale_down(cluster_id=0):
#                     scale_down_schd_time = self.scheduled_time + CONFIG["SCALE_DOWN_OVERHEAD"] + e_latency
#                     desired = service.calc_desired_num_replica(0)
#                     # cur_num_repl = service.get_cluster_num_replica(0)
#                     cur_num_repl = service.count_cluster_live_replica(0)
#                     how_many_scale_down = cur_num_repl - desired
#                     if LOG_MACRO: utils.print_log("WARNING", "Schedule ScaleDown service {} (cluster_{}) by {} at {}".format(service.name, 0, how_many_scale_down, scale_down_schd_time))
#                     scale_down_event = ScaleDown(scale_down_schd_time, service, how_many_scale_down, cluster_id=0)
#                     simulator.schedule_event(scale_down_event)
                    
#                     CONFIG["SCALE_DOWN_STATUS"][0]=0
#                     finish_stabilization_event = FinishScaleDownStabilization(self.scheduled_time + e_latency + CONFIG["SCALE_DOWN_STABILIZE_WINDOW"], 0)
#                     simulator.schedule_event(finish_stabilization_event)
                
                
#                 ## Cluster 1
#                 cluster_1_desired = service.calc_desired_num_replica(1)
#                 cluster_1_cur_tot_num_repl = service.count_cluster_live_replica(1)
#                 overloaded_cluster_1 = cluster_1_desired > cluster_1_cur_tot_num_repl
#                 # Scale up check
#                 if overloaded_cluster_1:
#                     if LOG_MACRO: utils.print_log("WARNING", "Overloaded! service {} in cluster {}, desired: {}, current: {}".format(service.name, 1, cluster_1_desired, cluster_1_cur_tot_num_repl))
#                     how_many_scale_up = cluster_1_desired - cluster_1_cur_tot_num_repl
#                     assert how_many_scale_up > 0
#                     scale_up_event = ScaleUp(self.scheduled_time + CONFIG["SCALE_UP_OVERHEAD"] + e_latency, service, how_many_scale_up, cluster_id=1)
#                     simulator.schedule_event(scale_up_event)
#                 # If not overloaded, then check if scale down is needed.
#                 elif CONFIG["SCALE_DOWN_STATUS"][1]==1 and service.should_we_scale_down(cluster_id=1):
#                     scale_down_schd_time = self.scheduled_time + CONFIG["SCALE_DOWN_OVERHEAD"] + e_latency
#                     desired = service.calc_desired_num_replica(1)
#                     # cur_num_repl = service.get_cluster_num_replica(1)
#                     cur_num_repl = service.count_cluster_live_replica(1)
#                     how_many_scale_down = cur_num_repl - desired
#                     if LOG_MACRO: utils.print_log("WARNING", "Schedule ScaleDown service {} (cluster_{}) by {} at {}".format(service.name, 1, how_many_scale_down, scale_down_schd_time))
#                     scale_down_event = ScaleDown(self.scheduled_time + CONFIG["SCALE_DOWN_OVERHEAD"] + e_latency, service, how_many_scale_down, cluster_id=1)
#                     simulator.schedule_event(scale_down_event)
                    
#                     CONFIG["SCALE_DOWN_STATUS"][1]=0
#                     finish_stabilization_event = FinishScaleDownStabilization(self.scheduled_time + e_latency + CONFIG["SCALE_DOWN_STABILIZE_WINDOW"], 1)
#                     simulator.schedule_event(finish_stabilization_event)
            
class ScaleUp(Event):
    def __init__(self, schd_time_, service, how_many, cluster_id):
        self.scheduled_time = schd_time_
        self.service = service
        self.how_many = how_many
        self.cluster_id = cluster_id
        self.name = "ScaleUp"

    def event_latency(self):
        return 0 # There is no next event. Hence, event latency is 0.

    def execute_event(self):
        e_latency = self.event_latency()
        # prev_total_num_replica = self.service.get_cluster_num_replica(self.cluster_id)
        prev_total_num_replica = self.service.count_cluster_live_replica(self.cluster_id)
        ###############################################################
        new_total_num_replica = self.service.provision(self.how_many, self.cluster_id)
        assert new_total_num_replica == self.how_many + prev_total_num_replica
        ###############################################################
        if LOG_MACRO: utils.print_log("INFO", "Execute: ScaleUp " + self.service.name + " from " + str(prev_total_num_replica) + " to " + str(new_total_num_replica) + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        simulator.append_capacity_data_point(self.cluster_id, self.scheduled_time + e_latency, self.service)
            

class ScaleDown(Event):
    def __init__(self, schd_time_, service, how_many_scale_down, cluster_id):
        self.scheduled_time = schd_time_
        self.service = service
        self.how_many_scale_down = how_many_scale_down
        self.cluster_id = cluster_id
        self.name = "ScaleDown"

    def event_latency(self):
        return 0 # There is no next event. Hence, event latency is 0.

    def execute_event(self):
        e_latency = self.event_latency()
        # prev_total_num_replica = self.service.get_cluster_num_replica(self.cluster_id)
        prev_total_num_replica = self.service.count_cluster_live_replica(self.cluster_id)
        ###############################################################
        new_total_num_replica = self.service.scale_down(self.how_many_scale_down, self.cluster_id)
        ###############################################################
        if LOG_MACRO: utils.print_log("WARNING", "Execute: ScaleDown " + self.service.name + " from " + str(prev_total_num_replica) + " to " + str(new_total_num_replica) + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        simulator.append_capacity_data_point(self.cluster_id, self.scheduled_time + e_latency, self.service)


######################################################################################################
######################################################################################################
        
        
class LoadBalancing(Event):
    def __init__(self, schd_time_, req_, src_replica_, dst_service_):
        self.scheduled_time = schd_time_
        self.request = req_
        self.src_replica = src_replica_ # Replica
        self.dst_service = dst_service_ # string
        self.policy = self.src_replica.service.lb
        self.name = "LoadBalancing"

    def event_latency(self):
        return 0
        # return uniform(1,2)
    ###################################################################
    def heuristic_TE(self, cid, other_cid, dst_svc, local_can, remote_can, schd_t):
        local_schd_replicas, local_zq_replicas, num_schd_cluster_repl, num_zero_queue_cluster_repl = dst_svc.num_schedulable_and_zero_queue_replica(cid)
        if num_schd_cluster_repl > 0: # local routing. local cluster has enough resource.
            superset_candidates = local_schd_replicas
        if num_zero_queue_cluster_repl > 0: # local routing. local cluster has enough resource.
            superset_candidates = local_zq_replicas
        else: # check if the remote cluster has available resource
            if simulator.arg_flags.delayed_information:
                remote_schd_replicas, remote_num_schd_cluster_repl = dst_svc.num_delayed_schedulable_replica(other_cid, schd_t + 0)
            else:
                remote_schd_replicas, remote_zq_replicas, remote_num_schd_cluster_repl, remote_num_zero_queue_cluster_repl = dst_svc.num_schedulable_and_zero_queue_replica(other_cid)
            if remote_num_schd_cluster_repl > 0:
                superset_candidates = remote_schd_replicas
            else:
                superset_candidates = local_can
        # verifying_superset_candidates = placement.svc_to_repl[cid][dst_svc]
        # for repl in verifying_superset_candidates:
        #     if repl not in local_can and repl not in remote_can:
        #         utils.error_handling("replica {} is not included neither in local candidate nor in remote candidate.".format(repl.to_str()))
        dst_replica_candidates = list()
        for repl in superset_candidates:
            if repl.is_dead == False:
                dst_replica_candidates.append(repl)
            else:
                if LOG_MACRO: utils.print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
        return dst_replica_candidates
    ###################################################################
    
    # @profile
    def execute_event(self):
        if LOG_MACRO: utils.print_log("INFO", "Start LB event! (src_replica: {})".format(self.src_replica.to_str()))
        if simulator.first_time_flag:
            simulator.append_capacity_data_point(0, 0, self.dst_service)
            simulator.append_capacity_data_point(1, 0, self.dst_service)
            # simulator.cluster0_capacity[self.dst_service].append([0, self.dst_service.get_total_capacity(0)])
            # simulator.cluster1_capacity[self.dst_service].append([0, self.dst_service.get_total_capacity(1)])
            simulator.first_time_flag = False
            
        e_latency = self.event_latency()
        
        if  simulator.warmup == False:
            simulator.num_started_req += 1
            if simulator.num_started_req > 2000: # NOTE: hardcoded
                simulator.warmup = True
            
        # Step 1: Find replica candidates
        # Step 2: Pick one replica from the candidates.
        
        # Step 1 
        # If the static_lb rule is defined for User. static_lb is only for User.
        if CONFIG["ROUTING_ALGORITHM"] == "LCLB": # No multi-cluster
            superset_candidates = placement.svc_to_repl[self.src_replica.cluster_id][self.dst_service]
            dst_replica_candidates = list()
            
            # if self.dst_service.name not in simulator.dummy[0]:
            #     simulator.dummy[0][self.dst_service.name] = list()
            # if self.dst_service.name not in simulator.dummy[1]:
            #     simulator.dummy[1][self.dst_service.name] = list()
            
            # Cluster 0
            if self.src_replica.cluster_id == 0:
                # simulator.dummy[0][self.dst_service.name].append([self.scheduled_time , len(superset_candidates)])
                #########################################################
                # TODO: overhead
                #########################################################
                for repl in superset_candidates:
                     if repl.cluster_id == 0:
                        if repl.is_dead == False:
                            dst_replica_candidates.append(repl)
                        else:
                            if LOG_MACRO: utils.print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
                #########################################################
            # Cluster 1
            elif self.src_replica.cluster_id == 1:
                # simulator.dummy[1][self.dst_service.name].append([self.scheduled_time, len(superset_candidates)])
                for repl in superset_candidates:
                     if repl.cluster_id == 1:
                        if repl.is_dead == False:
                            dst_replica_candidates.append(repl)
                        else:
                            if LOG_MACRO: utils.print_log("INFO", "Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
                            
        elif CONFIG["ROUTING_ALGORITHM"] == "heuristic_TE":
            ## This is naive implementation of request routing.
            ## Assumption: it assumes that you know the available resource of the remote cluster magically.
            ## Heuristic: the requests will be routed when the load becomes soaring up and the local cluster cannot accomodate it with the current capacity AND when the remote cluster has some rooms to process more request.
            if self.src_replica.cluster_id == 0:
                other_cluster = 1
            else:
                other_cluster = 0
            local_candidate = self.dst_service.get_cluster_replica(self.src_replica.cluster_id)
            remote_candidate = self.dst_service.get_cluster_replica(other_cluster)
            dst_replica_candidates = self.heuristic_TE(self.src_replica.cluster_id, other_cluster, self.dst_service, local_candidate, remote_candidate, self.scheduled_time)
        
        elif CONFIG["ROUTING_ALGORITHM"] == "queueing_prediction":
            if self.src_replica.cluster_id == 0:
                other_cluster = 1
            else:
                other_cluster = 0
            local_replica = self.dst_service.get_cluster_replica(self.src_replica.cluster_id)
            remote_replica = self.dst_service.get_cluster_replica(other_cluster)
            if simulator.warmup:
                ####################################################################################
                ## 1. queueing time gradient based cut
                # inverse_latency = list()
                # for repl in self.dst_service.replicas:
                #     if repl.is_dead == False:
                #         if len(repl.pred_queue_time) < 2:
                #             pred_q_t = 1
                #         else:
                #             pred_q_t = repl.pred_queue_time[-1]
                #         if self.src_replica.cluster_id == repl.cluster_id:
                #             pred_latency = pred_q_t + self.dst_service.processing_time + network_latency.same_rack*2
                #         else:
                #             pred_latency = pred_q_t + self.dst_service.processing_time + network_latency.far_inter_region*2
                #         inverse_latency.append([repl, pred_latency, 1/pred_latency])
                # sum_of_inverse = sum(x[2] for x in inverse_latency)
                # for i in range(len(inverse_latency)):
                #     w_ = inverse_latency[i][2]/sum_of_inverse
                #     inverse_latency[i].append(w_)
                # # inverse_latency.sort(key=lambda x: x[3])
                # inverse_latency.sort(key=lambda x: x[1])
                # i = 0
                # gradient = 0
                # prev = inverse_latency[0][1]
                # for elem in inverse_latency:
                #     curr = elem[1]
                #     gradient = (curr - prev)/prev
                #     elem.append(gradient)
                #     # prev = elem[1]
                #     i += 1
                # dst_replica_candidates = list()
                # gradient_threshold = 0.1
                # for elem in inverse_latency:
                #     if elem[4] > gradient_threshold:
                #         break
                #     dst_replica_candidates.append(elem)
                # dst_replica_candidates = [x[0] for x in dst_replica_candidates]
                # # print("{}, {}".format(self.dst_service.name, len(dst_replica_candidates)))
                # # for elem in inverse_latency:
                # #     print("src,{}, dst_can,{}, pred,{}, gradient,{}, inverse_pred,{}, weight,{}".format(self.src_replica.to_str(), elem[0].to_str(), elem[1], elem[4], elem[2], elem[3]*100))
                # # print()
                ####################################################################################
                
                ####################################################################################
                # ## 2. avg/min/max of aggregated queueing time in cluster-wise + weight
                agg_metric = "min" # avg, min, max
                local_cluster_queueing_metric = self.dst_service.cluster_agg_queue_time(agg_metric, self.src_replica.cluster_id)
                remote_cluster_queueing_metric = self.dst_service.cluster_agg_queue_time(agg_metric, other_cluster)
                local_agg_pred_latency = local_cluster_queueing_metric \
                                        + self.dst_service.processing_time \
                                        + network_latency.same_rack*2 ## NOTE: network latency is hardcoded
                remote_agg_pred_latency = remote_cluster_queueing_metric \
                                        + self.dst_service.processing_time \
                                        + network_latency.far_inter_region*2 ## NOTE: network latency is hardcoded
                if local_agg_pred_latency < remote_agg_pred_latency:
                    dst_replica_candidates = local_replica
                else:
                    dst_replica_candidates = remote_replica
                inverse_latency = list()
                for repl in dst_replica_candidates:
                    if repl.is_dead == False:
                        if len(repl.pred_queue_time) == 0:
                            pred_q_t = 1
                        else:
                            pred_q_t = repl.pred_queue_time[-1]
                        if self.src_replica.cluster_id == repl.cluster_id:
                            pred_latency = pred_q_t + self.dst_service.processing_time + network_latency.same_rack*2
                        else:
                            pred_latency = pred_q_t + self.dst_service.processing_time + network_latency.far_inter_region*2
                        inverse_latency.append([repl, pred_latency, 1/pred_latency])
                sum_of_inverse = sum(x[2] for x in inverse_latency)
                for i in range(len(inverse_latency)):
                    w_ = inverse_latency[i][2]/sum_of_inverse
                    inverse_latency[i].append(w_)
                dst_replica_candidates = random.choices([x[0] for x in inverse_latency], weights=[x[3] for x in inverse_latency], k=1)
                ####################################################################################
                
                
                ####################################################################################
                ## 3. no cross-cluster routing. local routing only with queueing based routing 
                # dst_replica_candidates = local_replica
                # inverse_latency = list()
                # for repl in dst_replica_candidates:
                #     if repl.is_dead == False:
                #         if len(repl.pred_queue_time) == 0:
                #             pred_q_t = 1
                #         else:
                #             pred_q_t = repl.pred_queue_time[-1]
                #         pred_latency = pred_q_t + self.dst_service.processing_time + network_latency.same_rack*2
                #         inverse_latency.append([repl, pred_latency, 1/pred_latency, pred_q_t, repl.cur_ewma_intarrtime])
                # inverse_latency.sort(key=lambda x: x[1])
                # # for elem in inverse_latency:
                # #     print("{}, {}, {}, {}, {}".format(elem[0].to_str(), elem[1], elem[2], elem[3], elem[4]))
                # # print("---")
                # # Top 5 local instances by queueing time.
                # num_can = 5
                # if len(inverse_latency) >= num_can:
                #     inverse_latency = inverse_latency[:num_can]
                #     dst_replica_candidates = [x[0] for x in inverse_latency]
                # else:
                #     dst_replica_candidates = [x[0] for x in inverse_latency]
                # # for elem in dst_replica_candidates:
                # #     print("{}".format(elem.to_str()))
                # # print()
                ####################################################################################
                
                # sum_of_inverse = sum(x[2] for x in inverse_latency)
                # for i in range(len(inverse_latency)):
                #     w_ = inverse_latency[i][2]/sum_of_inverse
                #     inverse_latency[i].append(w_)
                # dst_replica_candidates = random.choices([x[0] for x in inverse_latency], weights=[x[3] for x in inverse_latency], k=1)
            else:
                dst_replica_candidates = self.heuristic_TE(self.src_replica.cluster_id, other_cluster, self.dst_service, local_replica, remote_replica, self.scheduled_time)
                
                
        elif CONFIG["ROUTING_ALGORITHM"] == "capacity_TE":
            if self.src_replica.cluster_id == 0: other_cluster = 1
            else: other_cluster = 0
            local_candidate = self.dst_service.get_cluster_live_replica(self.src_replica.cluster_id)
            remote_candidate = self.dst_service.get_cluster_live_replica(other_cluster)
            
            # routing_weight is local routing weight
            if self.dst_service in simulator.local_routing_weight[self.src_replica.cluster_id]:
                ran = random.random()
                if ran <= simulator.local_routing_weight[self.src_replica.cluster_id][self.dst_service]:
                    superset_candidates = local_candidate
                else:
                    superset_candidates = remote_candidate
            else:
                superset_candidates = local_candidate
                
            dst_replica_candidates = list()
            for repl in superset_candidates:
                if repl.is_dead == False:
                    dst_replica_candidates.append(repl)
                else:
                    if LOG_MACRO: utils.print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
        #####$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
        elif CONFIG["ROUTING_ALGORITHM"] == "moment_response_time":
            # It routes requests to a replica who has the shortest expected response time at the time(moment) a request is decided to be rounted to a child service.
            # MOMENT LATENCY = (Network latency RTT) + (Queue size*processing time) + (Processing time)
            # E.g., 
            #   - local replica, queue size 4: (1*2) + (4*100) + 100 = 512
            #   - remote replica, queue size 3: (40*2) + (3*100) + 100 = 480
            #   In this case, the request will be routed to the remote replica.
            superset_candidates = placement.all_svc_to_repl[self.dst_service]
            moment_latency_list = list()
            for i in range(len(superset_candidates)):
                mmt_latency = self.src_replica.calc_moment_latency(superset_candidates[i])
                moment_latency_list.append([mmt_latency, i, superset_candidates[i]])
            moment_latency_list.sort(key=lambda x:x[0])
            if LOG_MACRO:
                for elem in moment_latency_list:
                    utils.print_log("WARNING", "moment_latency, from {} to {}, est_latency: {}, idx: {}".format(self.src_replica.to_str(), elem[2].to_str(), elem[0], elem[1]))
            sorted_replica_list = [x[2] for x in moment_latency_list]
            dst_replica_candidates = list()
            for repl in sorted_replica_list:
                if repl.is_dead == False:
                    dst_replica_candidates.append(repl)
            num_can = 10
            if  len(dst_replica_candidates) > num_can:
                dst_replica_candidates = dst_replica_candidates[:num_can]
                    
        elif CONFIG["ROUTING_ALGORITHM"] == "MCLB": # multi-cluster load balancing
            # ################################################################
            # ######################### BUG BUG BUG ##########################
            # ################################################################
            ## placement.svc_to_repl[self.dst_service] returns the actual reference of svc_to_repl[self.dst_service] not copy.
            ## dst_replica_candidates.remove() actually removes the replica element in svc_to_repl[self.dst_service].
            ###################################################################
            # dst_replica_candidates = placement.svc_to_repl[self.dst_service]
            # for repl in dst_replica_candidates:
            #     if repl.is_dead:
            #         dst_replica_candidates.remove(repl) ## BUG BUG BUG
            #         if LOG_MACRO: utils.print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
            ###################################################################
            ## BUG FIXED VERSION
            # It treats all replicas in the local and remote clusters equally.
            # It use configured base load balancer for every replicas in all clusters.
            # In other words, no smart routing.
            superset_candidates = placement.all_svc_to_repl[self.dst_service]
            
            ## DEBUG PRINT
            if LOG_MACRO: 
                utils.print_log("INFO", "(LB) Dst superset:")
                for repl in superset_candidates:
                    utils.print_log("INFO", repl.to_str())
            
            dst_replica_candidates = list()
            for repl in superset_candidates:
                if repl.is_dead == False:
                    dst_replica_candidates.append(repl)
                else:
                    if LOG_MACRO: utils.print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
        else:
            utils.error_handling("Invalid ROUTING_ALGORITHM({}).".format(CONFIG["ROUTING_ALGORITHM"]))
                        
                         
            # DEBUG PRINT
            # if LOG_MACRO: utils.print_log("DEBUG", "self.src_replica: " + self.src_replica.to_str())
            # if LOG_MACRO: utils.print_log("DEBUG", "self.dst_service.name: " + self.dst_service.name)
            # if LOG_MACRO: utils.print_log("DEBUG", "superset_candidates: ",end="")
            # for repl in superset_candidates:
            #     if LOG_MACRO: utils.print_log("DEBUG", repl.to_str(), end=",")
            # if LOG_MACRO: utils.print_log("DEBUG", "")

        # DEBUG PRINT
        # if LOG_MACRO: utils.print_log("DEBUG", "LB, " + self.src_replica.to_str() + "=> dst_replica_candidates: ", end="")
        # for repl in dst_replica_candidates:
        #     # if LOG_MACRO: utils.print_log("DEBUG", repl.to_str())
        #     if LOG_MACRO: utils.print_log("DEBUG", repl.to_str(), end=", ")
        # if LOG_MACRO: utils.print_log("DEBUG", "")
        
        
        if LOG_MACRO: 
            utils.print_log("DEBUG", self.src_replica.to_str())
            for repl in dst_replica_candidates:
                utils.print_log("DEBUG", "\t" + repl.to_str() + " outstanding queue size: " + str(self.src_replica.num_outstanding_response_from_child[repl]))
            
        assert len(dst_replica_candidates) > 0
        
        # Step 2 
        if self.policy == "RoundRobin":
            dst_replica = self.roundrobin(dst_replica_candidates)
        elif self.policy == "Random":
            dst_replica = self.random(dst_replica_candidates)
        elif self.policy == "LeastRequest":
            dst_replica = self.least_outstanding_request(dst_replica_candidates)
        elif self.policy == "MovingAvg":
            dst_replica = self.moving_average(dst_replica_candidates)
        elif self.policy == "EWMA_ResponseTime":
            dst_replica = self.ewma_response_time(dst_replica_candidates)
        else:
            utils.error_handling(self.policy + " load balancing policy is not supported.")

        if self.src_replica.cluster_id != dst_replica.cluster_id:
            if self.src_replica.cluster_id not in CONFIG["CROSS_CLUSTER_ROUTING"]:
                CONFIG["CROSS_CLUSTER_ROUTING"][self.src_replica.cluster_id] = 0
            CONFIG["CROSS_CLUSTER_ROUTING"][self.src_replica.cluster_id] += 1
            key = self.src_replica.service.name +"-"+str(self.src_replica.cluster_id)
            if key not in CONFIG["CROSS_CLUSTER_ROUTING"]:
                CONFIG["CROSS_CLUSTER_ROUTING"][key] = 0
            CONFIG["CROSS_CLUSTER_ROUTING"][key] += 1
        dst_replica.num_pending_request += 1
        dst_replica.processing_queue_size += 1

        if LOG_MACRO: utils.print_log("INFO", "Execute: LoadBalancing " + self.policy + " (request[" + str(self.request.id) + "]), " + self.src_replica.to_str() + "=>" + dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        
        assert self.src_replica.get_status() == "Active"
        
        if dst_replica.get_status() == "Ready":
            prev_status = dst_replica.get_status()
            dst_replica.set_status("Active")
            if LOG_MACRO: utils.print_log("INFO", "(LB) Replica {} changed the status {}->{}".format(dst_replica.to_str(), prev_status, dst_replica.get_status()))
        
        
        #################################################################################
        ## Increment total num req that actually the dst replica receives
        ## - local request:
        ## - remote request:
        ## - origin request:
        ## - non_origin request:
        ## - What I am supposed to receive:
        ## - What I am deprived of:
        #################################################################################
        dst_replica.increment_total_num_req()
        dst_replica.num_req_per_millisecond += 1
        
        ## Record whether this request comes from the local upstream replica
        if self.src_replica.cluster_id == dst_replica.cluster_id:
            dst_replica.increment_local_num_req()
        else:
            dst_replica.increment_remote_num_req()
            
        ## Record whether this request originates from original cluster ingress
        if self.request.origin_cluster == dst_replica.cluster_id:
            dst_replica.increment_origin_num_req()
        else:
            dst_replica.increment_non_origin_num_req()
        #################################################################################
        
        # send_to_server_event = SendRequest(self.scheduled_time + e_latency, self.request, self.src_replica, dst_replica, self.request_history_stack)
        send_to_server_event = SendRequest(self.scheduled_time + e_latency, self.request, self.src_replica, dst_replica)
        
        simulator.schedule_event(send_to_server_event)

    def roundrobin(self, replicas):
        idx = self.request.id % len(replicas)
        return replicas[idx]
    
    
    def random(self, replicas):
        idx = random.randint(0, len(replicas)-1)
        return replicas[idx]
    
    
    def least_outstanding_request(self, replicas):
        least_request_repl = replicas[0]
        for repl in replicas:
            if self.src_replica.num_outstanding_response_from_child[least_request_repl] > self.src_replica.num_outstanding_response_from_child[repl]:
                least_request_repl = repl
            # if LOG_MACRO: utils.print_log("DEBUG", repl.to_str() + "'s outstanding queue size: " + str(self.src_replica.num_outstanding_response_from_child[repl]))
        return least_request_repl
        

    def ewma_response_time(self, replicas):
        if self.src_replica.is_warm == False:
            return self.roundrobin(replicas)
        new_repl = list()
        ordered_repl_list = list()
        for repl in replicas:
            if repl.is_dead == False:
                if repl in self.src_replica.ewma_rt:
                    outstanding = self.src_replica.num_outstanding_response_from_child[repl]
                    ewma_rt = self.src_replica.ewma_rt[repl]
                    ordered_repl_list.append([repl, ewma_rt, outstanding])
                else:
                    new_repl.append(repl)
        ordered_repl_list.sort(key=lambda x: x[1])
        for repl in ordered_repl_list:
            print("{}->{}, ewma_rt: {}".format(self.src_replica.to_str(), repl[0].to_str(), repl[1]))
        print()
        ordered_repl_list = [x[0] for x in ordered_repl_list]
        ordered_repl_list = ordered_repl_list[:3]
        ordered_repl_list += new_repl
        random_pick = self.random(ordered_repl_list)
        return random_pick

        
    def moving_average(self, replicas):
        def get_avg(li):
            if len(li) == 0:
                return -1
            return sum(li)/len(li)
        
        # if LOG_MACRO: utils.print_log("DEBUG", self.src_replica.to_str() + " response_time: ")
        # for repl in replicas:
        #     avg = get_avg(self.src_replica.response_time[repl])
        #     if LOG_MACRO: utils.print_log("DEBUG", "\t" + repl.to_str() + ": ", end="")
        # if LOG_MACRO: utils.print_log("DEBUG", "")
        # for repl in replicas:
        #     if repl.is_warm == False:
        #         return self.least_outstanding_request(replicas)
        
        least_repl = replicas[0]
        least_avg = get_avg(self.src_replica.response_time[replicas[0]])
        for repl in replicas:
            avg = get_avg(self.src_replica.response_time[repl])
            if avg == -1:
                return self.least_outstanding_request(replicas)
            if least_avg > avg:
                least_repl = repl
                least_avg = avg
        if LOG_MACRO: 
            utils.print_log("DEBUG", "moving_average(" + str(least_avg) + "): " + self.src_replica.to_str() + "->" + least_repl.to_str())
            for repl in replicas:
                utils.print_log("DEBUG", "\t" + repl.to_str() + ": ", end="")
                for rt in self.src_replica.response_time[repl]:
                    utils.print_log("DEBUG", rt, end=", ")
                utils.print_log("DEBUG", "")
        return least_repl

        
class NetworkLatency:
    def __init__(self, same_rack=0.5, inter_rack=1.0, inter_zone=5.0, close_inter_region=15.0, far_inter_region=30.0):
        self.same_rack = same_rack
        self.inter_rack = inter_rack
        self.inter_zone = inter_zone
        self.close_inter_region = close_inter_region
        self.far_inter_region = far_inter_region
        self.std = 5
        
    def get_latency(self, src_replica_, dst_replica_):
        # Inter region (east-west)
        if src_replica_.node.region_id != dst_replica_.node.region_id:
            return self.far_inter_region
        # Inter region (east-east, west-west)
        elif src_replica_.node.zone_id != dst_replica_.node.zone_id:
            return self.close_inter_region
        # Inter zone
        elif src_replica_.node.rack_id != dst_replica_.node.rack_id:
            return self.inter_zone
        # Inter rack
        elif src_replica_.node.rack_id == dst_replica_.node.rack_id and src_replica_.node.node_id != dst_replica_.node.node_id:
            return self.inter_rack
        # Within the same rack
        else:
            return self.same_rack
        
    def sample_latency(self, src_replica_, dst_replica_):
        def normal_dist(mean, std):
            sample = np.random.normal(mean, std, 1)
            return sample[0]
            
        # Inter region (east-west)
        if src_replica_.node.region_id != dst_replica_.node.region_id:
            return normal_dist(self.far_inter_region, self.std) # 50~60
        
        # Inter region (east-east, west-west)
        elif src_replica_.node.zone_id != dst_replica_.node.zone_id:
            return normal_dist(self.close_inter_region, self.std) # 15~20ms
        
        # Inter zone
        elif src_replica_.node.rack_id != dst_replica_.node.rack_id:
            return normal_dist(self.inter_zone, self.std) # 2~8ms
        
        # Inter rack
        elif src_replica_.node.rack_id == dst_replica_.node.rack_id and src_replica_.node.node_id != dst_replica_.node.node_id:
            return normal_dist(self.inter_rack, self.std) # 0.5
        
        # Within the same rack
        else:
            return self.same_rack

class SendRequest(Event):
    # def __init__(self, schd_time_, req_, src_replica_, dst_replica_, req_history_stack_):
    def __init__(self, schd_time_, req_, src_replica_, dst_replica_):
        self.scheduled_time = schd_time_
        self.request = req_
        self.src_replica = src_replica_ # Sender
        self.dst_replica = dst_replica_ # Receiver
        # self.request_history_stack = req_history_stack_
        self.name = "SendRequest"

    def event_latency(self):
        return network_latency.sample_latency(self.src_replica, self.dst_replica)

    def execute_event(self):
        e_latency = self.event_latency()
        assert self.src_replica.num_outstanding_response_from_child[self.dst_replica] >= 0
        self.src_replica.num_outstanding_response_from_child[self.dst_replica] += 1
        if simulator.arg_flags.load_balancer == "MovingAvg" or simulator.arg_flags.load_balancer == "EWMA_ResponseTime":
            if self.request in self.src_replica.sending_time[self.dst_replica]:
                utils.error_handling("request[" + str(self.request.id) + "] already exists in " + self.src_replica.to_str() + " sending_time.\nDiamond shape call graph is not supported yet.")
            assert self.request.id not in self.src_replica.sending_time[self.dst_replica]
            self.src_replica.sending_time[self.dst_replica][self.request.id] = self.scheduled_time
        
        # output_log[self.request.id].append(self.schd_time_)
        if LOG_MACRO: utils.print_log("INFO", "Execute: SendRequest(request[" + str(self.request.id) + "]), " + self.src_replica.to_str() + "=>" + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        queuing_event = RecvRequest(self.scheduled_time + e_latency, self.request, self.src_replica, self.dst_replica)
        simulator.schedule_event(queuing_event)


class RecvRequest(Event):
    def __init__(self, schd_time_, req_, src_replica_, dst_replica_):
        self.scheduled_time = schd_time_
        self.request = req_
        self.src_replica = src_replica_ # Sender
        self.dst_replica = dst_replica_ # Receiver
        # self.request_history_stack = req_history_stack_
        self.name = "RecvRequest"
        # self.queuing_time = queuing_time_
    
    def event_latency(self):
        # return self.queuing_time
        return 0
    
    def execute_event(self):
        e_latency = self.event_latency()    
        if LOG_MACRO: utils.print_log("INFO", "Execute: RecvRequest(request[" + str(self.request.id) + "]) in " + self.dst_replica.to_str() + "'s " + self.src_replica.service.name + " recv queue" + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        
        ##########################################################
        if CONFIG["ROUTING_ALGORITHM"] == "queueing_prediction":
            intarrtime = self.scheduled_time - self.dst_replica.most_recent_arrival_time
            ewma = simulator.calc_ewma(intarrtime, self.dst_replica.cur_ewma_intarrtime)
            self.dst_replica.intarrival_time_list.append(intarrtime)
            if len(self.dst_replica.intarrival_time_list) > self.dst_replica.arr_time_window_size:
                self.dst_replica.intarrival_time_list.pop(0)
            m_avg_arr_t = sum(self.dst_replica.intarrival_time_list)/len(self.dst_replica.intarrival_time_list)
            m_avg_arr_t_2 = sum(self.dst_replica.intarrival_time_list[-15:])/len(self.dst_replica.intarrival_time_list[-15:])
            m_avg_arr_t_3 = sum(self.dst_replica.intarrival_time_list[-12:])/len(self.dst_replica.intarrival_time_list[-12:])
            m_avg_arr_t_4 = sum(self.dst_replica.intarrival_time_list[-10:])/len(self.dst_replica.intarrival_time_list[-10:])
            m_avg_arr_t_5 = sum(self.dst_replica.intarrival_time_list[-5:])/len(self.dst_replica.intarrival_time_list[-5:])
            self.dst_replica.moving_avg_interarr_time[self.request] = [m_avg_arr_t, m_avg_arr_t_2, m_avg_arr_t_3, m_avg_arr_t_4, m_avg_arr_t_5]
            # self.request.ewma_interarrtime = ewma
            self.dst_replica.cur_ewma_intarrtime = ewma
            
            prediction = simulator.predict_queueing_time(self.dst_replica.service, ewma) # UPDATE QUEUEING TIME PREDICTION 
            # prediction = simulator.predict_queueing_time(self.dst_replica.service, m_avg_arr_t_4) # UPDATE QUEUEING TIME PREDICTION 
            
            self.dst_replica.pred_queue_time.append(prediction)
            self.dst_replica.service.agg_pred_queue_time[self.dst_replica.cluster_id][self.dst_replica] = prediction
            # if self.dst_replica.service.name == "D" and self.dst_replica.cluster_id == 0:
            #     print("Insert {}".format(self.dst_replica.to_str()))
            if len(self.dst_replica.pred_queue_time) > self.dst_replica.pred_history_size:
                self.dst_replica.pred_queue_time.pop(0)
            self.dst_replica.most_recent_arrival_time = self.scheduled_time
            # self.request.queue_arrival_time = self.scheduled_time
            assert self.request not in self.dst_replica.queueing_start_time
            self.dst_replica.ewma_interarrtime[self.request] = ewma
            self.dst_replica.queueing_start_time[self.request] = self.scheduled_time
        ##########################################################
        
        ## Comment out to reduce memory consumption
        ### Record start timestamp for this service
        # if self.dst_replica.cluster_id == 0:
        #     ####################################################################
        #     ## It will be asserted if there this replica(service) has two parents.
        #     ## For example, A->C, B->C
        #     assert self.request not in simulator.cluster0_service_latency[self.dst_replica.service]
        #     simulator.cluster0_service_latency[self.dst_replica.service][self.request] = self.scheduled_time 
        # elif self.dst_replica.cluster_id == 1:
        #     simulator.cluster1_service_latency[self.dst_replica.service][self.request] = self.scheduled_time

        # if self.dst_replica.get_status() != "Active":
        #     prev_status = self.dst_replica.get_status()
        #     self.dst_replica.set_status("Active")
        #     if LOG_MACRO: utils.print_log("INFO", "(RecvRequest) Replica {} changed the status {}->{}".format(self.dst_replica.to_str(), prev_status, self.dst_replica.get_status()))
        
        self.dst_replica.num_recv_request += 1
        #######################################################################
        
        
        # TODO: This part could have some bugs.
        self.dst_replica.add_to_recv_queue(self.request, self.scheduled_time, self.src_replica)
        ###########################################################################
        ## IMPORTANT: src_replica will not be passed to the next event from here.
        ## Rather, src_replica will be stored in recv_queue and it will be fetched by CheckRequestIsReady event.
        check_ready_event = CheckRequestIsReady(self.scheduled_time + e_latency, self.request, self.dst_replica)
        simulator.schedule_event(check_ready_event)
        

''' When can you schedule a request process event? '''
# There are two cases where pending requests could be scheduled to be processed.
# - When a new request is received from a parent.
# - When another request finishes its process and so some resources become free.
#   - EndProcess => TryProcess
#
# Two conditions to start processing a request.
# - The replica cannot process start process until it receives results from all parents.
# - Available resource in replica > required resource to process a request
# Request ready ≠ able to be processed
# * [Request is ready] + [Sufficient resource is available]
#
# recv a request---> recv_queue ---recv from all parents---> ready_queue ---if resource is available---> process
class CheckRequestIsReady(Event):
    # def __init__(self, schd_time_, req_, src_replica_, dst_replica_):
    def __init__(self, schd_time_, req_, dst_replica_):
        self.scheduled_time = schd_time_
        self.request = req_
        #######################################################################
        # self.src_replica = src_replica_ # Sender #BUG: WHY DO WE NEED IT!!!?!!
        #######################################################################
        self.dst_replica = dst_replica_ # Receiver, *Process target
        self.name = "CheckRequestIsReady"
    
    def event_latency(self):
        return 0

    def execute_event(self):
        e_latency = self.event_latency()
        if LOG_MACRO: utils.print_log("INFO", "Execute: CheckRequestIsReady in " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        if self.dst_replica.is_request_ready_to_process(self.request):
            ############################################################################
            # BUG FIXED
            src_replica_of_this_req = self.dst_replica.remove_request_from_recv_queue(self.request)
            # self.dst_replica.add_to_ready_request(self.request, self.scheduled_time, self.src_replica, self.dst_replica) # BUG code
            self.dst_replica.add_to_ready_request(self.request, src_replica_of_this_req)

            ############################################################################
            # NOTE: Remember CheckRequestIsReady event does NOT pass the request argument to TryProcess event.
            # BUG !!!!!!!!!!!!!
            # self.src_replica does not need to be passed to TryToProcessRequest
            # try_process_event = TryToProcessRequest(self.scheduled_time + e_latency, self.src_replica, self.dst_replica)
            #############################################################################
            try_process_event = TryToProcessRequest(self.scheduled_time + e_latency, self.dst_replica)
            simulator.schedule_event(try_process_event)

# NOTE: TryProcess should try to schedule as many requests as it can. That's why it needs(?) to call TryToProcessRequest multiple times until there is no more resource to schedule a new request.
# In other words, 'multiple' requests could be scheduled with the resources released from 'one' finished request.
# One assumption here is each request may require different amount of resources.
# NOTE: Request argument is not passed to TryProcess event because it is going to try to schedule request processing events from the ready queue not the request previous event(CheckRequest, RecvRequest, SendRequest event).
class TryToProcessRequest(Event):
    # def __init__(self, schd_time_, req_, src_replica_, dst_replica_):
    # def __init__(self, schd_time_, src_replica_, dst_replica_):
    def __init__(self, schd_time_, dst_replica_):
        self.scheduled_time = schd_time_
        #############################################################################
        # BUG !!!!!!!!!!!!!
        # self.request = req_
        # self.src_replica is not used here.
        # self.src_replica = src_replica_ # TODO: Why do we need src_replica here?
        #############################################################################
        self.dst_replica = dst_replica_ # NOTE: the current replica which processs the given request
        self.name = "TryToProcessRequest"
        
    def event_latency(self):
        return 0
    
    def execute_event(self):
        e_latency = self.event_latency()
        if LOG_MACRO: utils.print_log("INFO", "Execute: TryToProcessRequest in " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        if self.dst_replica.is_schedulable() and (len(self.dst_replica.fixed_ready_queue) > 0):
            self.dst_replica.allocate_resource_to_request(self.scheduled_time + e_latency)
            self.dst_replica.processing_queue_size -= 1
            
            ############################################################################
            # BUG !!!!!!!!!!!!!
            # self.src_replica is not used here.
            # process_request_event = ProcessRequest(self.scheduled_time + e_latency, self.src_replica, self.dst_replica)
            process_request_event = ProcessRequest(self.scheduled_time + e_latency, self.dst_replica)
            ############################################################################
            simulator.schedule_event(process_request_event)
            
            ##################################################################################################
            # IMPORTANT
            # TODO: Why do we need to call TryToProcessRequest recursively?
            # Even if the req is ready, available resource might not be sufficient to schedule the req.
            # Multiple request could be scheduled after ONE request is completed and releases the resources.
            # THIS IS TRICKY.
            # For now, all the req coming into the replica requires the same num cores.
            # one_time_unit = 0.001
            # one_time_unit = 0
            # try_process_event = TryToProcessRequest(self.scheduled_time + e_latency+one_time_unit, self.src_replica, self.dst_replica)
            # simulator.schedule_event(try_process_event)
            ##################################################################################################
            
    
class ProcessRequest(Event):
    # def __init__(self, schd_time_, src_replica_, dst_replica_):
    def __init__(self, schd_time_, dst_replica_):
        self.scheduled_time = schd_time_
        # self.src_replica = src_replica_
        self.dst_replica = dst_replica_ # NOTE: the current replica which processs the given request
        self.name = "ProcessRequest"
        
    def event_latency(self):
        mu = self.dst_replica.service.processing_time
        sigma = CONFIG["PROCESSING_TIME_SIGMA"]
        sample = np.random.normal(mu, sigma, 1)
        return sample[0]
        # return self.dst_replica.service.processing_time
        
        
    def execute_event(self):
        e_latency = self.event_latency()
        self.dst_replica.processing_time_history.append(e_latency)
        # target_req, queued_time = self.dst_replica.process_request()
        target_req, src_repl = self.dst_replica.dequeue_from_ready_queue()
        ## BUG: You are supposed to allocate the resource to the request once it decides to execute the request which is TryToProcess.
        ## allocate_resource_to_request call was moved to TryToProcess. ## BUG Fixed
        # self.dst_replica.allocate_resource_to_request(target_req) ## BUG
        
        # self.dst_replica.service.func_interarr_to_queuing[target_req.ewma_interarrtime] = queuing_time
        # self.dst_replica.service.interarr_to_queuing.append([target_req.ewma_interarrtime, queuing_time, self.scheduled_time, target_req.queue_arrival_time])
        # queuing_time = self.scheduled_time - target_req.queue_arrival_time # queuing time of the target request
        
        if CONFIG["ROUTING_ALGORITHM"] == "queueing_prediction":
            assert target_req in self.dst_replica.queueing_start_time
            assert target_req in self.dst_replica.ewma_interarrtime
            queueing_time = self.scheduled_time - self.dst_replica.queueing_start_time[target_req]
            if self.dst_replica.is_warm:
                self.dst_replica.service.interarr_to_queuing.append([self.dst_replica.ewma_interarrtime[target_req] \
                                                                    , self.dst_replica.moving_avg_interarr_time[target_req][0] \
                                                                    , self.dst_replica.moving_avg_interarr_time[target_req][1] \
                                                                    , self.dst_replica.moving_avg_interarr_time[target_req][2] \
                                                                    , self.dst_replica.moving_avg_interarr_time[target_req][3] \
                                                                    , self.dst_replica.moving_avg_interarr_time[target_req][4] \
                                                                    , queueing_time])
            del self.dst_replica.moving_avg_interarr_time[target_req]
            del self.dst_replica.queueing_start_time[target_req]
            del self.dst_replica.ewma_interarrtime[target_req]
        
        if LOG_MACRO: utils.print_log("INFO", "Execute: ProcessRequest(request[" + str(target_req.id) + "]), " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)) + ", " + str(e_latency))
        # Process request function
        # Dequeue a request from ready queue -> allocate resource to dequeued request 
        # The replica(dst_replica) who processed this request will be recorded by the current process target request.
        #########
        ## BUG ##
        #########
        # target_req is obtained from history queue and src_replica and dst_replica come from caller event(TryToProcessRequest event).
        # TryToProcessRequest event could be called by two different events.
        # (1): Send -> Recv -> CheckRequestIsReady -> "TryToProcessRequest"
        # (2): EndProcess -> "TryToProcessRequest"
        # In (1) scenario, [src_replica, dst_replica] matches [actual target_req's src, actual target_req's dst]
        # This is wrong. 
        # They are not matched.
        # target_req.push_history(self.src_replica, self.dst_replica) # BUG line
        target_req.push_history(src_repl, self.dst_replica)
        ## It could be useful later.
        # queuing_delay = self.scheduled_time - queued_time # TODO: here is queuing delay.
        # print("Sampled processing latency: {}".format(e_latency))
        end_exec_event = EndProcessRequest(self.scheduled_time + e_latency, target_req, self.dst_replica)
        simulator.schedule_event(end_exec_event)
                
class EndProcessRequest(Event):
    # def __init__(self, schd_time_, req_, src_replica_, dst_replica_):
    def __init__(self, schd_time_, req_, dst_replica_):
        self.scheduled_time = schd_time_
        self.request = req_
        # self.src_replica = src_replica_
        self.dst_replica = dst_replica_ # this replica ends processing the request.
        self.name = "EndProcessRequest"

    def event_latency(self):
        return 0
    
    def execute_event(self):
        e_latency = self.event_latency()
        if LOG_MACRO: utils.print_log("INFO", "Execute: EndProcessRequest(request[" + str(self.request.id) + "]) in " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        
        self.dst_replica.free_resource_from_request(self.request, self.scheduled_time + e_latency)
        
        if dag.is_leaf(self.dst_replica.service):
            if LOG_MACRO: utils.print_log("DEBUG", "request[" + str(self.request.id) + "] reached leaf service " + self.dst_replica.service.name)
            # TODO: potentially buggy and apparently it was BUG BUG BUG!!!!!!!
            # Hopefully this is fixing the bug.
            # src = self.dst_replica
            ret_elem = self.request.pop_history(self.dst_replica)
            elem_src_repl = ret_elem[0]
            elem_dst_repl = ret_elem[1]
            assert elem_dst_repl == self.dst_replica
            # print("elem_src_repl,"+elem_src_repl.to_str()+",elem_dst_repl,"+elem_dst_repl.to_str()+",self.dst_replica,"+self.dst_replica.to_str())
            
            # send_back_event = SendBackRequest(self.scheduled_time + e_latency, self.request, src, dst)
            # NOTE: we will send back so src and dst should be flipped over each other.
            # FLIP~
            src = elem_dst_repl
            dst = elem_src_repl
            send_back_event = SendBackRequest(self.scheduled_time + e_latency, self.request, src, dst)
            simulator.schedule_event(send_back_event)
            ######################
            # BUG: It WAS a bug. 
            # TryToProcess event should be scheduled regardless of whether it is leaf or not.
            ######################
            # # TODO: EndProcess event will free the resource allocated to the request. A new request could be scheduled to be processed with the freed resource. That's why we need to call TryToProcessRequest.
            # try_to_process_event = TryToProcessRequest(self.scheduled_time + e_latency, self.src_replica, self.dst_replica)
            # simulator.schedule_event(try_to_process_event)
        else:
            child_services = dag.graph[self.dst_replica.service]
            src = self.dst_replica
            for dst_service in child_services:
                lb_event  = LoadBalancing(self.scheduled_time + e_latency, self.request, src, dst_service["service"])
                simulator.schedule_event(lb_event)

        # BUG fixed
        # TODO: EndProcess event will free the resource allocated to the request. A new request could be scheduled to be processed with the freed resource. That's why we need to call TryToProcessRequest.
        # try_to_process_event = TryToProcessRequest(self.scheduled_time + e_latency, self.src_replica, self.dst_replica)
        try_to_process_event = TryToProcessRequest(self.scheduled_time + e_latency, self.dst_replica)
        simulator.schedule_event(try_to_process_event)
            
class SendBackRequest(Event):
    def __init__(self, schd_time_, req_, src_replica_, dst_replica_):
        self.scheduled_time = schd_time_
        self.request = req_
        self.src_replica = src_replica_ #NOTE: This is the replica who sent the sendback.
        self.dst_replica = dst_replica_ #NOTE: This is the replica who receives the sendback.
        self.name = "SendBackRequest"
        
    def event_latency(self):
        return network_latency.sample_latency(self.src_replica, self.dst_replica)
        
    def execute_event(self):
        e_latency = self.event_latency()
        if LOG_MACRO: utils.print_log("INFO", "Execute: SendBackRequest(request[" + str(self.request.id) + "]), " + self.src_replica.to_str() + " -> " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        
        ## Comment out to reduce memory consumption
        # if self.src_replica.cluster_id == 0:
        #     assert self.request in simulator.cluster0_service_latency[self.src_replica.service]
        #     start_time = simulator.cluster0_service_latency[self.src_replica.service][self.request]
        #     simulator.cluster0_service_latency[self.src_replica.service][self.request] = self.scheduled_time - start_time
        # elif self.src_replica.cluster_id == 1:
        #     assert self.request in simulator.cluster1_service_latency[self.src_replica.service]
        #     start_time = simulator.cluster1_service_latency[self.src_replica.service][self.request]
        #     simulator.cluster1_service_latency[self.src_replica.service][self.request] = self.scheduled_time - start_time
        
        
        ######################################################
        ################## BUG BUG BUG #######################
        ######################################################
        ## It should be moved to to RecvSendBack event side.
        # self.src_replica.num_pending_request -= 1
        # assert self.src_replica.num_pending_request >= 0
        
        ########################################################################
        ############################ BUG BUG BUG ###############################
        ### There could be network latency between sendback event and recv_sendback event.
        ### The status has to be updated by the recv_sendback event not sendback event.
        ### Corner case: 
        ### 1. sendback request event. (sender side)
        ###    At the same time, change the sender replica status from active to "ready".
        ### 2. Network latency.
        ### 3. Scale down kicks in.
        ###    Terminate "sender" replica since it is "ready".
        ### 4. Recv sendback event. (receiver side)
        ###    Check the "sender" replica status again and the sender is "ready".
        ###    Terminate the "sender" replica AGAIN. 
        ###    Then Boom, crash.
        ########################################################################
        # self.src_replica.update_status("(SendBackRequest {}->{})".format(self.src_replica.to_str(), self.dst_replica.to_str()))
        
        # if self.src_replica.num_pending_request == 0 and self.src_replica.get_status() != "Ready":
        #     prev_status = self.src_replica.get_status()
        #     self.src_replica.set_status("Ready")
        #     if LOG_MACRO: utils.print_log("INFO", "(SendBackRequest) Replica {} changed the status {}->{}".format(self.src_replica.to_str(), prev_status, self.src_replica.get_status()))
            
        recv_sendback_event = RecvSendBack(self.scheduled_time + e_latency, self.request, self.src_replica, self.dst_replica)
        simulator.schedule_event(recv_sendback_event)
        
            
class RecvSendBack(Event):
    def __init__(self, schd_time_, req_, src_replica_, dst_replica_):
        self.scheduled_time = schd_time_
        self.request = req_
        self.src_replica = src_replica_ #NOTE: This is the replica who sent the sendback.
        self.dst_replica = dst_replica_ #NOTE: This is the replica who receives the sendback.
        self.name = "RecvSendBack"
        
    def event_latency(self):
        return 0
    
    def execute_event(self):
        e_latency = self.event_latency()
        assert e_latency == 0
        if LOG_MACRO: utils.print_log("INFO", "Execute: RecvSendBack(request[" + str(self.request.id) + "])" + self.dst_replica.to_str() + " from " + self.src_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        
        self.dst_replica.num_completed_request += 1
        ##############################################################
        if self.dst_replica.is_warm == False and self.dst_replica.num_completed_request >= CONFIG["WARMUP_SIZE"]:
            self.dst_replica.is_warm = True
            if LOG_MACRO: utils.print_log("DEBUG", "WARMED UP! " + self.dst_replica.to_str())
        ##########################################################33##
        
        if simulator.arg_flags.load_balancer == "MovingAvg" or simulator.arg_flags.load_balancer == "EWMA_ResponseTime":
            if self.src_replica not in self.dst_replica.sending_time:
                utils.error_handling("(RecvSendBack) {}(child replica) does not exist in {}(parent replica)'s sending_time data structure.".format(self.src_replica.to_str(), self.dst_replica.to_str()))
            sending_ts = self.dst_replica.sending_time[self.src_replica][self.request.id]
            del self.dst_replica.sending_time[self.src_replica][self.request.id]
            # self.dst_replica.response_time[self.src_replica].append(self.scheduled_time + e_latency - sending_ts)
            # if len(self.dst_replica.response_time[self.src_replica]) > CONFIG["MA_WINDOW_SIZE"]:
                # self.dst_replica.response_time[self.src_replica].pop(0)
                
            cur_rt = self.scheduled_time + e_latency - sending_ts
            if self.src_replica not in self.dst_replica.ewma_rt:
                prev_ewma = 0
                # print("the very first response from {} to {}".format(self.src_replica.to_str(), self.dst_replica.to_str()))
            else:
                prev_ewma = self.dst_replica.ewma_rt[self.src_replica]
            new_ewma = simulator.calc_ewma(cur_rt, prev_ewma, 0.7)
            self.dst_replica.ewma_rt[self.src_replica] = new_ewma
            
        assert self.src_replica in self.dst_replica.num_outstanding_response_from_child
        self.dst_replica.num_outstanding_response_from_child[self.src_replica] -= 1
        self.src_replica.num_pending_request -= 1 ### BUG fixed
        assert self.src_replica.num_pending_request >= 0
                
        ###########################################################################
        # This is receiver (dst_replica)
        assert self.dst_replica.get_status() == "Active" # Previous status
        # self.dst_replica.update_status("(RecvSendBack {}->{})".format(self.src_replica.to_str(),self.dst_replica.to_str()))
        
        if self.src_replica.get_status() == "Active" and self.src_replica.is_idle():
            if LOG_MACRO: utils.print_log("INFO", "RecvSendBack, sender({}) is idle. num_pending_request: {}, total_num_outstanding_response: {}".format(self.src_replica.to_str(), self.src_replica.num_pending_request, self.src_replica.get_total_num_outstanding_response()))
            self.src_replica.set_status("Ready")
        ###########################################################################
        # if self.dst_replica.get_total_num_outstanding_response() == 0 and self.dst_replica.num_pending_request == 0:
        #     assert self.dst_replica.get_status() == "Active" # Previous status
        #     if self.dst_replica.is_user(): # User is always active! 
        #         if LOG_MACRO: utils.print_log("DEBUG", "User is always active.")
        #     else: # Non User replica
        #         prev_status = self.dst_replica.get_status()
        #         self.dst_replica.set_status("Ready")
        #         if LOG_MACRO: utils.print_log("INFO", "(RecvSendBack) Replica {} changed the status {}->{}".format(self.dst_replica.to_str(), prev_status, self.dst_replica.get_status()))
           
        ### Delayed Kill
        # This is sender (src_replica)
        # Parent replica kills delayed scale down child replica.
        if self.src_replica.is_dead and self.src_replica.get_status() == "Ready":
            if (self.dst_replica.is_user() == False) or (self.dst_replica.is_user() and self.src_replica.cluster_id == self.dst_replica.cluster_id):
                assert self.src_replica.is_user() == False
                if LOG_MACRO: utils.print_log("WARNING", "RecvSendBack, Delayed scale down {} by {}!".format(self.src_replica.to_str(), self.dst_replica.to_str()))
                if LOG_MACRO: utils.print_log("INFO", "Sender replica(" + self.src_replica.to_str() + ") was dead and it finally becomes Ready.")
                
                self.src_replica.service.remove_target_replica(self.src_replica, self.src_replica.cluster_id)
                
                if LOG_MACRO: utils.print_log("INFO", "sender replica " + self.src_replica.to_str() + " is now deleted!")
        self.dst_replica.add_to_send_back_queue(self.request, self.scheduled_time, self.src_replica.service)
        
        if self.dst_replica.is_request_ready_to_send_back(self.request):
            self.dst_replica.remove_request_from_send_back_queue(self.request)
            if self.dst_replica.service.name.find("User") != -1:
                completion_event = CompleteRequest(self.scheduled_time + e_latency, self.request, self.dst_replica)
                simulator.schedule_event(completion_event)
            else:
                #TODO: [IMPORTANT] Strong assumption: all services have only one parents.
                #TODO: Otherwise, we need to do SendBackRequest for the num of parents service times.
                elem_ret = self.request.pop_history(self.dst_replica)
                elem_ret_src_repl = elem_ret[0]
                elem_ret_dst_repl = elem_ret[1]
                assert elem_ret_dst_repl == self.dst_replica
                src = elem_ret_dst_repl #NOTE: Flip over.
                dst = elem_ret_src_repl
                send_back_event = SendBackRequest(self.scheduled_time + e_latency, self.request, src, dst)
                simulator.schedule_event(send_back_event)
        
    
class CompleteRequest(Event):
    def __init__(self, schd_time_, req_, last_processing_replica):
        self.scheduled_time = schd_time_
        self.request = req_
        self.last_processing_replica = last_processing_replica
        self.name = "CompleteRequest"
    
    def execute_event(self):
        simulator.request_end_time[self.request] = self.scheduled_time
        latency = simulator.request_end_time[self.request] - simulator.request_start_time[self.request]
        simulator.end_to_end_latency[self.request] = latency
        
        #####################################################################
        # TODO: This is currently hardcoded. 
        # Be careful. It is ERROR PRONE!!
        # Assumption: wrk generates user0's request first and assigns request id incrementally. Therefore, the requests generated for user1 strickly come after the request generated user0.
        #####################################################################
        # if self.request.id < simulator.user0_num_req: # Hardcoded version
        
        # last_processing_replica is always User.
        if self.last_processing_replica.cluster_id == 0:
            simulator.user0_latency.append([self.scheduled_time, latency])
        elif self.last_processing_replica.cluster_id == 1:
            simulator.user1_latency.append([self.scheduled_time, latency])
        else:
            utils.error_handling("Invalid cluster id {}".format(self.last_processing_replica.cluster_id))
            
        if LOG_MACRO: utils.print_log("DEBUG", "Completed: request[" + str(self.request.id) + "], end-to-end latency: " + str(simulator.end_to_end_latency[self.request]))
        # comp = input("Complete the request[" + str(self.request.id) + "]")
        

#######################################################################################
######################### It is the end of event classes ##############################
#######################################################################################

def test4(a=0):
    print("test2")

# User->A->B->C
def linear_topology_application(load_balancer="RoundRobin", c0_req_arr=list(), c1_req_arr=list()):
    user = Service(name_="User", mcore_per_req_=0, processing_t_=0, lb_=load_balancer)
    # user_1 = Service(name_="User1", mcore_per_req_=0, processing_t_=0, lb_=load_balancer)
    core_per_request = 1000
    svc_a = Service(name_="A", mcore_per_req_=core_per_request, processing_t_=20, lb_=load_balancer) # Frontend
    svc_b = Service(name_="B", mcore_per_req_=core_per_request, processing_t_=30, lb_=load_balancer) # Calculation
    svc_c = Service(name_="C", mcore_per_req_=core_per_request, processing_t_=5, lb_=load_balancer) # DB
    # Currently it is assumed that cluster 0 and cluster 1 have the same application topology
    service_map = {"User":user, "A":svc_a, "B":svc_b, "C":svc_c}
    
    dag.add_service(user)
    dag.add_service(svc_a)
    dag.add_service(svc_b)
    dag.add_service(svc_c)
    
    # DAG
    # Heavy data transfer in the tail.
    dag.add_dependency(parent_service=user, child_service=svc_a, weight=20)
    dag.add_dependency(parent_service=svc_a, child_service=svc_b, weight=10)
    dag.add_dependency(parent_service=svc_b, child_service=svc_c, weight=100)
    
    num_replica_for_cluster_0 = dict()
    num_replica_for_cluster_1 = dict()
    for svc in service_map:
        # Cluster 0
        if len(c0_req_arr) != 0:
            num_replica_for_cluster_0[svc] = calc_initial_num_replica(c0_req_arr, service_map[svc])
        else:
            if svc == "User":
                num_replica_for_cluster_0[svc] = 1 # NOTE: hardcoded
            else:
                num_replica_for_cluster_0[svc] = 2 # NOTE: hardcoded
        # Cluster 1
        if len(c1_req_arr) != 0:
            num_replica_for_cluster_1[svc] = calc_initial_num_replica(c1_req_arr, service_map[svc])
        else:
            if svc == "User":
                num_replica_for_cluster_1[svc] = 1 # NOTE: hardcoded
            ########################################################
            elif svc == "C":
                num_replica_for_cluster_1[svc] = 0 # NOTE: C in cluster 2 is not available
            ########################################################
            else:
                num_replica_for_cluster_1[svc] = 2 # NOTE: hardcoded
    print("- num_replica_for_cluster_0")
    for svc in num_replica_for_cluster_0:
        print("\t- {}: {}".format(svc, num_replica_for_cluster_0[svc]))
    print("- num_replica_for_cluster_1")
    for svc in num_replica_for_cluster_1:
        print("\t- {}: {}".format(svc, num_replica_for_cluster_1[svc]))
        
    replica_for_cluster_0 = dict()
    for svc in service_map:
        replica_for_cluster_0[svc] = list()
    replica_for_cluster_1 = dict()
    for svc in service_map:
        replica_for_cluster_1[svc] = list()
        
    # Replica
    temp_all_replica = list()
    # Replica for cluster 0
    for svc in service_map:
        for i in range(num_replica_for_cluster_0[svc]):
            repl = Replica(service=service_map[svc], id=i*2)
            service_map[svc].add_replica(repl)
            temp_all_replica.append(repl)
            replica_for_cluster_0[svc].append(repl) # replica id: 0,2,4,6
    # Replica for cluster 1
    for svc in service_map:
        for i in range(num_replica_for_cluster_1[svc]):
            repl = Replica(service=service_map[svc], id=i*2+1)
            service_map[svc].add_replica(repl)
            temp_all_replica.append(repl)
            replica_for_cluster_1[svc].append(repl) # replica id: 1,3,5,7
    
    # Placement
    for svc in replica_for_cluster_0:
        for repl in replica_for_cluster_0[svc]:
            if svc == "User":
                placement.place_replica_to_node_and_allocate_core(repl, node_0, 0)
            else:
                placement.place_replica_to_node_and_allocate_core(repl, node_0, 1000)
    for svc in replica_for_cluster_1:
        for repl in replica_for_cluster_1[svc]:
            if svc == "User":
                placement.place_replica_to_node_and_allocate_core(repl, node_1, 1)
            else:
                placement.place_replica_to_node_and_allocate_core(repl, node_1, 1000)
    # static_lb["User0"] = [0, 2, 4, 6] # dst replica id of cluster 0 frontend service
    # static_lb["User1"] = [1, 3, 5, 7]
    dag.print_service_and_replica()
    for repl in temp_all_replica:
        dag.register_replica(repl)
    print("replica_for_cluster_0[User],", replica_for_cluster_0["User"][0].to_str())
    print("replica_for_cluster_1[User],", replica_for_cluster_1["User"][0].to_str())
    # replica_for_cluster_0["User"][0] since there is only one user replica for each cluster. 
    return replica_for_cluster_0["User"][0], replica_for_cluster_1["User"][0], svc_a

# Tree topology application
def three_depth_application(load_balancer="RoundRobin", c0_req_arr=list(), c1_req_arr=list()):
    
    user = Service(name_="User", mcore_per_req_=0, processing_t_=0, lb_=load_balancer)
    # user_1 = Service(name_="User1", mcore_per_req_=0, processing_t_=0, lb_=load_balancer)
    core_per_request = 1000
    svc_a = Service(name_="A", mcore_per_req_=core_per_request, processing_t_=30, lb_=load_balancer) # H
    svc_b = Service(name_="B", mcore_per_req_=core_per_request, processing_t_=10, lb_=load_balancer) # L
    svc_c = Service(name_="C", mcore_per_req_=core_per_request, processing_t_=25, lb_=load_balancer) # M
    svc_d = Service(name_="D", mcore_per_req_=core_per_request, processing_t_=40, lb_=load_balancer) # H
    svc_e = Service(name_="E", mcore_per_req_=core_per_request, processing_t_=30, lb_=load_balancer) # H
    svc_f = Service(name_="F", mcore_per_req_=core_per_request, processing_t_=10, lb_=load_balancer) # L
    # Currently it is assumed that cluster 0 and cluster 1 have the same application topology
    service_map = {"User":user, "A":svc_a, "B":svc_b, "C":svc_c, "D":svc_d, "E":svc_e, "F":svc_f}
    
    dag.add_service(user)
    # dag.add_service(user_1)
    dag.add_service(svc_a)
    dag.add_service(svc_b)
    dag.add_service(svc_c)
    dag.add_service(svc_d)
    dag.add_service(svc_e)
    dag.add_service(svc_f)
    
    # DAG
    # Heavy data transfer in the tail.
    dag.add_dependency(parent_service=user, child_service=svc_a, weight=10)
    # dag.add_dependency(parent_service=user_1, child_service=svc_a, weight=10)
    dag.add_dependency(parent_service=svc_a, child_service=svc_b, weight=10)
    dag.add_dependency(parent_service=svc_a, child_service=svc_c, weight=10)
    dag.add_dependency(parent_service=svc_a, child_service=svc_d, weight=10)
    dag.add_dependency(parent_service=svc_d, child_service=svc_e, weight=10)
    dag.add_dependency(parent_service=svc_d, child_service=svc_f, weight=10)
    
    
    num_replica_for_cluster = list()
    for _ in range(NUM_CLUSTER):
        num_replica_for_cluster.append(dict())
    for i in range(NUM_CLUSTER):
        for svc_name in service_map:
            if len(c0_req_arr) != 0:
                num_replica_for_cluster[i][svc_name] = calc_initial_num_replica(c0_req_arr, service_map[svc_name])
            else:
                if svc_name == "User":
                    num_replica_for_cluster[i][svc_name] = 1 # NOTE: hardcoded
                else:
                    num_replica_for_cluster[i][svc_name] = 2 # NOTE: hardcoded
    for i in range(NUM_CLUSTER):
        print("- num_replica_for_cluster_" + str(i))
        for svc in num_replica_for_cluster[i]:
            print("\t- {}: {}".format(svc, num_replica_for_cluster[i][svc]))

    replica_for_cluster = list()
    for _ in range(NUM_CLUSTER):
        replica_for_cluster.append(dict())
    for cid in range(NUM_CLUSTER):
        for svc_name in service_map:
            replica_for_cluster[cid][svc_name] = list()
            # print(svc_name)
        
    # Replica
    temp_all_replica = list()
    for i in range(NUM_CLUSTER):
        for svc_name in service_map:
            for j in range(num_replica_for_cluster[i][svc_name]):
                replica_id = i+(j*NUM_CLUSTER)
                repl = Replica(service=service_map[svc_name], id=replica_id)
                service_map[svc_name].add_replica(repl)
                temp_all_replica.append(repl)
                replica_for_cluster[i][svc_name].append(repl) # replica id: 0,2,4,6
    
    global nodes
    assert len(nodes) == 0
    for cid in range(NUM_CLUSTER):
        n_ = Node(region_id_=cid, zone_id_=0, rack_id_=0, node_id_=0, total_mcore_=1000*100000)
        nodes.append(n_)
        placement.add_node(n_)
    
    # Placement
    print(len(nodes))
    for i in range(NUM_CLUSTER):
        for svc in replica_for_cluster[i]:
            for repl in replica_for_cluster[i][svc]:
                if svc == "User":
                    placement.place_replica_to_node_and_allocate_core(repl, nodes[i], 0)
                else:
                    placement.place_replica_to_node_and_allocate_core(repl, nodes[i], 1000)
    # static_lb["User0"] = [0, 2, 4, 6] # dst replica id of cluster 0 frontend service
    # static_lb["User1"] = [1, 3, 5, 7]
    dag.print_service_and_replica()
    for repl in temp_all_replica:
        dag.register_replica(repl)
    dag.check_duplicate_replica()
    # return replica_for_cluster
    # return replica_for_cluster_0["User"][0], replica_for_cluster_1["User"][0], svc_a
    return replica_for_cluster[0]["User"][0], replica_for_cluster[1]["User"][0], svc_a


def general_tree_application(fan_out_degree=3, no_child_constant=1, depth=3, num_cluster=2, load_balancer="RoundRobin", c0_req_arr=list(), c1_req_arr=list()):
    ts_list = list()
    ts_list.append(time.time())
    alphabet = string.ascii_uppercase
    NUM_CLUSTER = num_cluster
    proliferation_degree = fan_out_degree - no_child_constant
    assert(no_child_constant < fan_out_degree)
    svc_list = list()
    core_per_request = 1000
    alpha_idx = 0
    total_num_svc_in_each_depth = list()
    for d_ in range(depth):
        local_idx = 0 # local_idx: id within the same depth. starting from 0 within the same depth
        svc_list.append(list())
        if d_ == 0:
            svc_list[d_].append(Service(name_="User", mcore_per_req_=0, processing_t_=0, lb_=load_balancer))
            if LOG_MACRO: utils.print_log("DEBUG", "depth,{}, num_svc: {}".format(d_, 0))
            local_idx += 1
            total_num_svc_in_each_depth.append(1)
        else:
            if d_ == 1:
                num_svc_within_depth = 1 # Frontend service in depth 1
                total_num_svc_in_each_depth.append(1)
            else:
                num_svc_within_depth = pow(proliferation_degree, d_-2) * fan_out_degree ## IMPORTANT
                total_num_svc_in_each_depth.append(num_svc_within_depth)
            # assert num_svc_within_depth <= len(alphabet)
            for i in range(num_svc_within_depth):
                service_name = "svc_"+str(d_)+"_"+str(local_idx)
                # service_name = alphabet[alpha_idx]+"_"+str(d_)+"_"+str(local_idx)
                svc_list[d_].append(Service(name_=service_name, mcore_per_req_=core_per_request, processing_t_=30, lb_=load_balancer))
                local_idx += 1
                alpha_idx += 1
            
    ts_list.append(time.time())
    print(ts_list[-1] - ts_list[-2])
    
    ## debug print
    # for i in range(len(svc_list)):
    #     for j in range(len(svc_list[i])):
    #         print("depth,{}, svc, {}".format(i, svc_list[i][j].name))

    service_map = dict()
    for i in range(len(svc_list)):
        for j in range(len(svc_list[i])):
            service_map[svc_list[i][j].name] = svc_list[i][j]
    
    for i in range(len(svc_list)):
        for j in range(len(svc_list[i])):
            dag.add_service(svc_list[i][j])
    ts_list.append(time.time())
    print(ts_list[-1] - ts_list[-2])
    
    # DAG
    for i in range(len(svc_list)):
        if i == 0:
            continue
        for j in range(len(svc_list[i])):
            parent_idx = int(j / (fan_out_degree))
            if LOG_MACRO: utils.print_log("DEBUG", "parent_idx,{}, child_idx,{}".format(parent_idx, j))
            dag.add_dependency(parent_service=svc_list[i-1][parent_idx], child_service=svc_list[i][j], weight = i*2+10)
    ts_list.append(time.time())
    print(ts_list[-1] - ts_list[-2])
    
    if LOG_MACRO:
        dag.print_dependency()
        
    num_replica_for_cluster = list()
    for _ in range(NUM_CLUSTER):
        num_replica_for_cluster.append(dict())
    for i in range(NUM_CLUSTER):
        for svc_name in service_map:
            if len(c0_req_arr) != 0:
                num_replica_for_cluster[i][svc_name] = calc_initial_num_replica(c0_req_arr, service_map[svc_name])
            else:
                if svc_name == "User":
                    num_replica_for_cluster[i][svc_name] = 1 # NOTE: hardcoded
                else:
                    #############################################################
                    num_replica_for_cluster[i][svc_name] = 1 # NOTE: hardcoded
                    #############################################################
    if LOG_MACRO:
        for i in range(NUM_CLUSTER):
            print("- num_replica_for_cluster_" + str(i))
            for svc in num_replica_for_cluster[i]:
                print("\t- {}: {}".format(svc, num_replica_for_cluster[i][svc]))

    replica_for_cluster = list()
    for cid in range(NUM_CLUSTER):
        replica_for_cluster.append(dict())
    for cid in range(NUM_CLUSTER):
        for svc_name in service_map:
            replica_for_cluster[cid][svc_name] = list()
            # print(svc_name)
        
    # Replica
    temp_all_replica = list()
    for i in range(NUM_CLUSTER):
        for svc_name in service_map:
            for j in range(num_replica_for_cluster[i][svc_name]):
                replica_id = i+(j*NUM_CLUSTER)
                repl = Replica(service=service_map[svc_name], id=replica_id)
                service_map[svc_name].add_replica(repl)
                temp_all_replica.append(repl)
                replica_for_cluster[i][svc_name].append(repl) # replica id: 0,2,4,6
    
    global nodes
    assert len(nodes) == 0
    for cid in range(NUM_CLUSTER):
        n_ = Node(region_id_=cid, zone_id_=0, rack_id_=0, node_id_=0, total_mcore_=1000*100000)
        nodes.append(n_)
        placement.add_node(n_)
    
    ts_list.append(time.time())
    print(ts_list[-1] - ts_list[-2])
    
    # Placement
    # print("total num nodes: ", len(nodes))
    for i in range(NUM_CLUSTER):
        for svc in replica_for_cluster[i]:
            for repl in replica_for_cluster[i][svc]:
                if svc == "User":
                    placement.place_replica_to_node_and_allocate_core(repl, nodes[i], 0)
                else:
                    placement.place_replica_to_node_and_allocate_core(repl, nodes[i], 1000)
    ts_list.append(time.time())
    print(ts_list[-1] - ts_list[-2])
      
    ## print all service in all cluster     
    # dag.print_service_and_replica()
    
    ts_list.append(time.time())
    print(ts_list[-1] - ts_list[-2])
    
    for repl in temp_all_replica:
        dag.register_replica(repl)
    ts_list.append(time.time())
    print(ts_list[-1] - ts_list[-2])
    
    ## too slow
    # dag.check_duplicate_replica()
    # ts_list.append(time.time())
    # print(ts_list[-1] - ts_list[-2])
    
    ## timestamp print
    # print("$"*30)
    # for i in range(1, len(ts_list)):
    #     print(ts_list[i] - ts_list[i-1], end=", ")
    # print()
    # print("$"*30)
    
    return replica_for_cluster, total_num_svc_in_each_depth

# def calc_non_burst_rps(durations_ratio, moment_rps_, target_rps_):
#     burst_num_req = 0
#     non_burst_duration = 0
#     for i in range(len(durations_ratio)):
#         if moment_rps_[i] != -1:
#             burst_num_req += moment_rps_[i] * durations_ratio[i]
#         else:
#             non_burst_duration += durations_ratio[i]
#     non_burst_rps = (target_rps_ - burst_num_req) / non_burst_duration
#     # assert (non_burst_rps - int(non_burst_rps)) == 0 # rps must be int
#     non_burst_rps = int(non_burst_rps) # TODO:
#     if LOG_MACRO: utils.print_log("DEBUG", "non_burst_rps: ", non_burst_rps)
#     for i in range(len(moment_rps_)):
#         if moment_rps_[i] == -1:
#             moment_rps_[i] = non_burst_rps
#     if LOG_MACRO: utils.print_log("DEBUG", "moment_rps: ", moment_rps_)
#     return moment_rps_


def argparse_add_argument(parser):
    parser.add_argument("--app", type=str, default=None, help="the name of the target app", choices=["one_service", "three_depth"], required=True)
    
    parser.add_argument("--load_balancer", type=str, default=None, help="load balancing policy", choices=["Random", "RoundRobin", "LeastRequest", "MovingAvg", "EWMA_ResponseTime"], required=True)
    
    parser.add_argument("--routing_algorithm", type=str, default=None, choices=["LCLB", "MCLB", "heuristic_TE", "moment_response_time", "capacity_TE", "queueing_prediction"], help="routing algorithm when multi-cluster is enabled.", required=True)
    
    parser.add_argument("--c0_request_arrival_file", type=str, default=None, help="path to the cluster 0 request arrival time file", required=True)
    
    parser.add_argument("--c1_request_arrival_file", type=str, default=None, help="path to the cluster 1 request arrival time file", required=True)
    
    parser.add_argument("--workload", type=str, default=None, help="workload type or msname", required=True)
    
    parser.add_argument("--fixed_autoscaler", type=int, default=None, choices=[0, 1], help="fixed autoscaler", required=True)
    
    parser.add_argument("--autoscaler_period", type=int, default=None, help="autoscaler_period", required=True)
    
    parser.add_argument("--desired_autoscaler_metric", type=float, default=None, help="desired_autoscaler_metric", required=True)
    
    parser.add_argument("--delayed_information", type=int, default=None, choices=[0, 1], help="delayed information between clusters", required=True)
    
    parser.add_argument("--output_dir", type=str, default="log", help="base directory where output log will be written.", required=True)
    
    parser.add_argument("--response_time_window_size", type=int, default=200, help="window size of response time for moving average and ewma", required=False)
    
    parser.add_argument("--same_rack_network_latency", type=float, default=0.5, help="same_rack_network_latency cluster", required=False)
    
    parser.add_argument("--inter_rack_network_latency", type=float, default=1, help="inter_rack_network_latency cluster", required=False)
    
    parser.add_argument("--inter_zone_network_latency", type=float, default=5, help="inter_zone_network_latency cluster", required=False)
    
    parser.add_argument("--close_inter_region_network_latency", type=float, default=15, help="close_inter_region_network_latency cluster", required=False)
    
    parser.add_argument("--far_inter_region_network_latency", type=float, default=40, help="far_inter_region_network_latency cluster", required=False)
    
    
    
def print_argument(flags):
    if LOG_MACRO: utils.print_log("INFO", "=============================================================")
    if LOG_MACRO: utils.print_log("INFO", "======================= argument ============================")
    if LOG_MACRO: utils.print_log("INFO", "=============================================================")
    for key, value in vars(flags).items():
        if LOG_MACRO: utils.print_log("INFO", "{}: {}".format(key, str(value)))
    if LOG_MACRO: utils.print_log("INFO", "=============================================================")

def calc_initial_num_replica(req_arr, svc):
    if svc.name == "User":
        return 1
    def request_arrival_to_rps(req_arr):
        rps_list = list()
        rps = 0
        window = 0
        for i in range(len(req_arr)):
            rps += 1
            if req_arr[i] >= 1000*window:
                rps_list.append(rps)                    
                rps = 0
                window += 1
        return rps_list
    rps_list = request_arrival_to_rps(req_arr)
    initial_rps = rps_list[:60]
    avg_init_rps = sum(initial_rps)/len(initial_rps)
    temp = 0.5
    initial_num_replica = int((avg_init_rps/svc.capacity_per_replica) * (1/temp))
    # print("rps_list: ", rps_list)
    # print("per_replica_capacity: ", per_replica_capacity)
    # print("avg_init_rps: ", avg_init_rps)
    # print("initial_rps: ", initial_rps)
    # print("initial_num_replica: ", initial_num_replica)
    
    # if initial_num_replica <= 1:
    if initial_num_replica < int(1/temp):
        initial_num_replica = int(1/temp)    
    return initial_num_replica
  
if __name__ == "__main__":
    program_start_time = time.time()
    cur_time = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    random.seed(1234) # To reproduce the random load balancing
    
    parser = argparse.ArgumentParser()
    argparse_add_argument(parser)
    flags, unparsed = parser.parse_known_args()
                
    ''' multi-cluster on/off '''
    CONFIG["MA_WINDOW_SIZE"] = flags.response_time_window_size
    
    ''' multi-cluster routing algorithm '''
    FIXED_AUTOSCALER = flags.fixed_autoscaler
    CONFIG["ROUTING_ALGORITHM"] = flags.routing_algorithm
    
    ''' Configure network latency '''
    same_rack = flags.same_rack_network_latency
    inter_rack = flags.inter_rack_network_latency
    inter_zone = flags.inter_zone_network_latency
    close_inter_region = flags.close_inter_region_network_latency
    far_inter_region = flags.far_inter_region_network_latency
    network_latency = NetworkLatency(same_rack, inter_rack, inter_zone, close_inter_region, far_inter_region)
    
    ''' Read request arrival time file '''
    f1 = open(flags.c0_request_arrival_file, 'r')
    lines = f1.readlines()
    c0_request_arrival = list()
    for i in range(len(lines)):
        c0_request_arrival.append(float(lines[i]))
    f1.close()
    
    f2 = open(flags.c1_request_arrival_file, 'r')
    lines = f2.readlines()
    c1_request_arrival = list()
    for i in range(len(lines)):
        c1_request_arrival.append(float(lines[i]))
    f2.close()
    
    print("c0_request_arrival[0]: ", c0_request_arrival[0])
    print("c0_request_arrival[-1]: ", c0_request_arrival[-1])
    print("c1_request_arrival[0]: ", c1_request_arrival[0])
    print("c1_request_arrival[-1]: ", c1_request_arrival[-1])
    print("len(c0_request_arrival): ", len(c0_request_arrival))
    print("len(c1_request_arrival): ", len(c1_request_arrival))
    
    ## Deprecated: Configure initial num replica

    ''' Application '''
    # if flags.app == "one_service":
    #     init_num_repl_c0 = calc_initial_num_replica(c0_request_arrival, 2)
    #     init_num_repl_c1 = calc_initial_num_replica(c1_request_arrival, 2)
    #     print("init_num_repl_cluster_0: ", init_num_repl_c0)
    #     print("init_num_repl_cluster_1: ", init_num_repl_c1)
    #     user_group_0, user_group_1, svc_a = one_service_application(flags.load_balancer, init_num_repl_c0, init_num_repl_c1)
    if flags.app == "three_depth":
        repl_user_0, repl_user_1, svc_a = three_depth_application(flags.load_balancer, c0_request_arrival, c1_request_arrival)
    else:
        utils.error_handling("Application scenario " + flags.app + " is not supported.")
    ''' Replica level DAG '''
    def create_replica_dependency():
        for repl in dag.all_replica:
            if dag.is_leaf(repl.service) is False:
                for child_svc in repl.child_services:
                    child_repl = dag.get_child_replica(repl, child_svc)
                    for c_repl in child_repl:
                        repl.register_child_replica_2(c_repl)
                        if LOG_MACRO: utils.print_log("WARNING", "{} -> {}".format(repl.to_str(), c_repl.to_str()))
    create_replica_dependency()
    
    ## DEBUG PRINT 
    for repl in dag.all_replica:
        if LOG_MACRO: utils.print_log("DEBUG", repl.to_str() + " child_replica: ", end="")
        if dag.is_leaf(repl.service):
            if LOG_MACRO: utils.print_log("DEBUG", "None", end="")
        else:
            for svc in repl.child_services:
                if LOG_MACRO: utils.print_log("DEBUG", svc.name + ": ", end="")
                for child_repl in repl.child_replica[svc]:
                    if LOG_MACRO: utils.print_log("DEBUG", child_repl.to_str(), end=", ")
        if LOG_MACRO: utils.print_log("DEBUG", "")
            
        
    dag.print_dependency()
    # dag.plot_graphviz()
    placement.print()
    # ym1 = plot_workload_histogram_2(request_arrivals_0, "Cluster 0 - Workload distribution", 0)
    # ym2 = plot_workload_histogram_2(request_arrivals_1, "Cluster 1 - Workload distribution", ym1)
    simulator = Simulator(c0_request_arrival, c1_request_arrival, cur_time, flags)
    
    for svc in dag.all_service:
        if svc.name.find("User") == -1:
            simulator.service_capacity[0][svc] = (CONFIG["MILLISECOND_LOAD_UPDATE"]/svc.processing_time)*svc.count_cluster_live_replica(0)
            simulator.service_capacity[1][svc] = (CONFIG["MILLISECOND_LOAD_UPDATE"]/svc.processing_time)*svc.count_cluster_live_replica(1)
    
    req_id = 0
    origin_cluster_0 = 0
    for req_arr in c0_request_arrival:
        request = Request(req_id, origin_cluster_0)
        simulator.request_start_time[request] = req_arr
        # service A is frontend service
        simulator.schedule_event(LoadBalancing(req_arr, request, repl_user_0, svc_a)) 
        req_id += 1
    origin_cluster_1 = 1
    for req_arr in c1_request_arrival:
            request = Request(req_id, origin_cluster_1)
            simulator.request_start_time[request] = req_arr
            simulator.schedule_event(LoadBalancing(req_arr, request, repl_user_1, svc_a))
            req_id += 1
      
    max_arrival_time = max(c0_request_arrival[-1], c1_request_arrival[-1])
    num_scaleup_check = int(max_arrival_time/simulator.arg_flags.autoscaler_period)
    for i in range(num_scaleup_check):
        simulator.schedule_event(CheckScaleUp(simulator.arg_flags.autoscaler_period*i))
        
    num_scaledown_check = int(max_arrival_time/CONFIG["SCALE_DOWN_PERIOD"])
    for i in range(num_scaledown_check):
        if i > 5:
            simulator.schedule_event(CheckScaleDown(CONFIG["SCALE_DOWN_PERIOD"]*i+3000))
      
    ## Deprecated      
    # # Schedule autoscaling check event every 30 sec
    # max_arrival_time = max(c0_request_arrival[-1], c1_request_arrival[-1])
    # num_autoscale_check = int(max_arrival_time/simulator.arg_flags.autoscaler_period)
    # for i in range(num_autoscale_check):
    #     if i > 5:
    #         simulator.schedule_event(AutoscalerCheck(simulator.arg_flags.autoscaler_period*i))
    
    if len(c0_request_arrival) > len(c1_request_arrival):
        longer_request_arr = c0_request_arrival
    else:
        longer_request_arr = c1_request_arrival
    num_progress_check = 10
    for i in range(1, num_progress_check+1):
        idx = int(len(longer_request_arr)/num_progress_check)*i - 1
        e_time = longer_request_arr[idx]
        # print("progress check: {} of out {}, {}\%, req_arr_time {}".format(idx, len(longer_request_arr), i*10, e_time))
        simulator.schedule_event(ProgressPrint(e_time, idx, len(longer_request_arr), i*10))
            
    # # Schedule UpdateRPS every RPS_UPDATE_INTERVAL sec
    num_rps_update = int(max_arrival_time/CONFIG["RPS_UPDATE_INTERVAL"])
    for i in range(num_rps_update):
        simulator.schedule_event(UpdateRPS(CONFIG["RPS_UPDATE_INTERVAL"]*i))
        
        
    ### millisecond level capacity update for capacity based TE
    if CONFIG["ROUTING_ALGORITHM"] == "capacity_TE":
        num_millisecond_load_update = int(max_arrival_time/CONFIG["MILLISECOND_LOAD_UPDATE"])
        print("num_millisecond_load_update: ", num_millisecond_load_update)
        for i in range(int(100*(100/CONFIG["MILLISECOND_LOAD_UPDATE"])), num_millisecond_load_update):
            if i == int(100*(100/CONFIG["MILLISECOND_LOAD_UPDATE"])):
                for repl in dag.all_replica:
                    repl.num_req_per_millisecond = 0
            simulator.schedule_event(UpdateRoutingWeight(CONFIG["MILLISECOND_LOAD_UPDATE"]*i))

    ### queueing function load for queueing prediction based TE
    if CONFIG["ROUTING_ALGORITHM"] == "queueing_prediction":
        for svc in dag.all_service:
            if svc.name.find("User") == -1:
                # simulator.load_queueing_function("postprocessed_queuing_function-"+svc.name+".txt", svc)
                # simulator.load_queueing_function("moving_avg_inter_arrival_time_10_queuing_function-"+svc.name+".txt", svc)
                # simulator.load_queueing_function("moving_avg_inter_arrival_time_5_queuing_function-"+svc.name+".txt", svc)
                simulator.load_queueing_function("ewma_inter_arrival_time_queuing_function-"+svc.name+".txt", svc)
            
    simulator.start_simulation()
    simulator.print_summary()
    simulator.write_resource_provisioning()
    simulator.write_simulation_latency_result()
    simulator.write_metadata_file()
    if simulator.arg_flags.routing_algorithm == "LCLB":
        simulator.write_req_arr_time()
    # simulator.write_interarr_to_queuing_list()
    
    # simulator.plot_and_save_resource_provisioning()
    # dag.print_and_plot_processing_time()
    # dag.print_and_plot_queuing_time()
    # dag.print_replica_num_request
    
    print("CNT_DELAYED_ROUTING: ", CONFIG["CNT_DELAYED_ROUTING"])
    
    if 0 in CONFIG["CROSS_CLUSTER_ROUTING"]:
        print("CROSS CLUSTER {} : {}".format(0, CONFIG["CROSS_CLUSTER_ROUTING"][0]))
    if 1 in CONFIG["CROSS_CLUSTER_ROUTING"]:
        print("CROSS CLUSTER {} : {}".format(1, CONFIG["CROSS_CLUSTER_ROUTING"][1]))
    if 0 in CONFIG["CROSS_CLUSTER_ROUTING"] and 1 in CONFIG["CROSS_CLUSTER_ROUTING"]:
        print("TOTAL_CROSS_CLUSTER_ROUTING: {}",format(CONFIG["CROSS_CLUSTER_ROUTING"][0] + CONFIG["CROSS_CLUSTER_ROUTING"][1]))
    for key, value in CONFIG["CROSS_CLUSTER_ROUTING"].items():
        if key != 0 and key != 1:
            print("CROSS CLUSTER {} : {}".format(key, value))
    
    # ts0 = list()
    # sc_repl0 = list()
    # for svc in simulator.dummy[0]:
    #     for elem in simulator.dummy[0][svc]:
    #         ts0.append(elem[0])
    #         sc_repl0.append(elem[1])
    # ts1 = list()
    # sc_repl1 = list()
    # for svc in simulator.dummy[1]:
    #     for elem in simulator.dummy[1][svc]:
    #         ts1.append(elem[0])
    #         sc_repl1.append(elem[1])
    # for svc in simulator.dummy[0]:
    #     plt.plot(ts0, sc_repl0, label=svc+" cluster0", marker='x')
    # for svc in simulator.dummy[1]:
    #     plt.plot(ts1, sc_repl1, label=svc+" cluster1", marker='x')
    # plt.legend(bbox_to_anchor=(1, 1), loc='upper left')
    # plt.show()