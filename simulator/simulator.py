import argparse
import copy
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
from time import gmtime, strftime
from utils import *
import workload_generator as wrk_g

np.random.seed(1234)

ROUTING_ALGORITHM=None

MA_WINDOW_SIZE="Uninitialized"
WARMUP_SIZE=200
PROCESSING_TIME_SIGMA=5

''' Load(RPS) record parameter '''
AUTOSCALER_INTRERVAL=15000 # 15sec
MOST_RECENT_RPS_PERIOD=15000 # 15sec
RPS_UPDATE_INTERVAL=1000 # 1sec just for the convenience. 1sec is aligned with RPS.
NUM_BASKET=MOST_RECENT_RPS_PERIOD/RPS_UPDATE_INTERVAL

''' Autoscaler parameter '''
SCALEUP_POLICY="kubernetes"
SCALEDOWN_POLICY="kubernetes"
DESIRED_METRIC_VALUE=0.5 # You want to keep the cpu util to 50% basically.
SCALE_UP_OVERHEAD=5000 # 5sec
SCALE_DOWN_OVERHEAD=5000 # 5sec
SCALE_DOWN_STABILIZE_WINDOW=300000 # kubernetes default: 300,000ms, 300sec, 5min
SCALE_DOWN_STATUS=[1,1] # It keeps track of stabilization window status.


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
        self.graph = dict()
        self.reverse_graph = dict()
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
        
        
    def register_replica(self, repl):
        self.all_replica.append(repl)
        if self.is_leaf(repl.service) is False:
            # In segregated cluster setup, a replica can only recognize replicas in the local cluster only.
            if ROUTING_ALGORITHM == "LCLB":
                if repl not in self.child_replica:
                    self.child_replica[repl] = dict()
                for child_svc in repl.child_services:
                    if child_svc not in self.child_replica[repl]:
                        self.child_replica[repl][child_svc] = list()
                    superset_replica = placement.svc_to_repl[child_svc]
                    for child_repl in superset_replica:
                        # Hardcoded.
                        # TODO: The number of clusters should be able to be configurable.
                        if repl.id%2 == 0 and child_repl.id%2 == 0: # local replica check.
                            self.child_replica[repl][child_svc].append(child_repl)
                        elif repl.id%2 == 1 and child_repl.id%2 == 1:
                            self.child_replica[repl][child_svc].append(child_repl)
                
            # In Multi-cluster setup, a parent replica has all child replicas both in the local cluster and remote cluster.
            else:
                if repl not in self.child_replica:
                    self.child_replica[repl] = dict()
                for child_svc in repl.child_services:
                    if child_svc not in self.child_replica[repl]:
                        self.child_replica[repl][child_svc] = list()
                    superset_replica = placement.svc_to_repl[child_svc]
                    for child_repl in superset_replica:
                        self.child_replica[repl][child_svc].append(child_repl)
                            
                            
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
            print_log("DEBUG", "get_child_services: " + parent_svc.name + " has no child. It is leaf service.")
            return None
        child_services = list()
        li = self.graph[parent_svc]
        for elem in li:
            child_services.append(elem['service'])
        print_log("DEBUG", parent_svc.name + "'s child services: ", end="")
        for child_svc in child_services:
            print_log("DEBUG", child_svc.name + ", ", end="")
        print_log("DEBUG", "")
        return child_services
    
    
    # Multi-cluster aware function!!!
    def get_parent_replica(self, repl, parent_svc):
        if ROUTING_ALGORITHM == "LCLB":
            p_replica = list()
            for can_parent_repl in placement.svc_to_repl[parent_svc]:
                if can_parent_repl.cluster_id == repl.cluster_id:
                    p_replica.append(can_parent_repl)
            return p_replica
        else:
            return placement.svc_to_repl[parent_svc]
            


    def get_parent_services(self, svc):
        if svc not in self.reverse_graph:
            print_log("DEBUG", "get_parent_services:" + svc.name + " has no parent service.")
            if svc.name.find("User") == -1:
                print_log("ERROR", svc.name + "(non-User service) must have at least one parent service.")
            return None
        parent_services = list()
        li = self.reverse_graph[svc]
        for elem in li:
            parent_services.append(elem['service'])
        print_log("DEBUG", svc.name + "'s parent services: ", end="")
        for parent_svc in parent_services:
            print_log("DEBUG", parent_svc.name + ", ", end="")
        print_log("DEBUG", "")
        
        return parent_services
    
    
    def size(self):
        return len(self.graph)

    def is_frontend(self, target_svc):
        parent_services = self.reverse_graph[target_svc]
        for svc in parent_services:
            if svc['service'].name.find("User") != -1:
                print_log("DEBUG", "This("+svc['service'].name+") is the frontend service.")
                return True
        print_log("DEBUG", "This("+svc['service'].name+") is not a frontend service.")
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
        print_log("DEBUG", "frontend services: ", end="")
        for fend in frontend_services:
            print_log("DEBUG", fend["service"].name + ", ", end="")
        print_log("DEBUG", "")
        
    def is_leaf(self, service_):
        return service_ not in self.graph
        
    def print_dependency(self):
        print_log("DEBUG", "="*10 + " DAG " + "="*10)
        for key in self.graph:
            for elem in self.graph[key]:
                print_log("DEBUG", key.name + "->" + elem["service"].name + ", " + str(elem["weight"]))
        
        # for p_c_pair, lb in self.lb_dictionary.items():
        #     print_log("DEBUG", p_c_pair + ": " + lb)
        print_log("DEBUG", "="*25)
        
    ''' Plot graph using Graphviz '''
    def plot_graphviz(self):
        g_ = graphviz.Digraph()
        g_.node(name="U", label="User", shape='circle')
        
        all_service = self.get_all_service()
        for svc in all_service:
            if svc.name.find("User") == -1:
                print_log("DEBUG", "add node: " + svc.name)
                g_.node(name=svc.name, label=svc.name, shape='circle')
                    
        g_.edge("U", "A", style='solid', color='black')
        for upstream in self.graph:
            if upstream.name.find("User") == -1:
                for elem in self.graph[upstream]:
                    print_log("DEBUG", "add edge: " + upstream.name + "->" + elem["service"].name)
                    g_.edge(upstream.name, elem["service"].name, style='solid', color='black', label=upstream.lb)
                
        # s = graphviz.Source.from_file(os.getcwd() + "/call_graph.dot")
        # s.render('abcd.gv', format='jpg',view=True)
        # # s.view()
        
        ## Show graph
        # g_.format = 'png'
        # g_.render('call_graph', view = True)
        
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
        self.svc_to_repl = dict() # It is not aware of multi-cluster. 
        self.total_num_replica = 0
        self.node_list = list()
        
    def add_node(self, node_):
        self.node_list.append(node_)

    def place_replica_to_node_and_allocate_core(self, repl_, node_, allocated_mcore_):
        # Deduct num core from Node's available core to allocate them to the replica.
        # print_log("DEBUG", "Try to place replica " + repl_.name + " to node " + node_.to_str())
        node_.place_replica_and_allocate_core(repl_, allocated_mcore_)
        # Allocate that num core to replica. (Now, replica has dedicated cores.)
        repl_.place_and_allocate(node_, allocated_mcore_)
        if repl_.service not in self.svc_to_repl:
            self.svc_to_repl[repl_.service] = list()
            print_log("INFO", "place_replica_to_node_and_allocate_core, first time seen service {}".format(repl_.service.name))
        self.svc_to_repl[repl_.service].append(repl_)
        print_log("INFO", "place_replica_to_node_and_allocate_core, {} to {}".format(repl_.to_str(), node_.to_str()))
        self.total_num_replica += 1
        
    def evict_replica_and_free_core(self, target_repl):
        for node_ in self.node_list:
            if target_repl in node_.placed_replicas:
                node_.evict_replica_from_node(target_repl)
                
        ## DEBUG PRINT
        # for service in self.svc_to_repl:
        #     print_log("INFO", "svc_to_repl[{}]:".format(service.name))
        #     for repl in self.svc_to_repl[service]:
        #         print_log("INFO", "replica {}".format(repl.to_str()))
                
        print_log("INFO", "evict_replica_and_free_core, Evict replica {}, service {}".format(target_repl.to_str(), target_repl.service.name))
        self.svc_to_repl[target_repl.service].remove(target_repl)
        assert target_repl not in self.svc_to_repl[target_repl.service]
        self.total_num_replica -= 1
        
    def get_total_num_services(self):
        return len(self.svc_to_repl)
    
    def get_number_replica(self, service_):
        return len(self.svc_to_repl[service_])
    
    def get_total_num_replicas(self):
        return self.total_num_replica
        
    def print(self):
        print_log("DEBUG", "")
        print_log("DEBUG", "="*40)
        print_log("DEBUG", "* Placement *")
        for svc in self.svc_to_repl:
            print_log("DEBUG", svc.name+": ", end="")
            for repl in self.svc_to_repl[svc]:
                print_log("DEBUG", repl.to_str(), end=", ")
            print_log("DEBUG", "")
        print_log("DEBUG", "="*40)

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
        
    def place_replica_and_allocate_core(self, repl_, allocated_mcore_):
        if self.available_mcore < allocated_mcore_:
            print_log("ERROR", "node " + self.to_str() + ", Can't allocate more cpu than the node has. (available mcore: " + str(self.available_mcore) + ", requested mcore: " + str(allocated_mcore_) + ")")
        
        self.placed_replicas[repl_] = allocated_mcore_
        old_available_mcore = self.available_mcore
        self.available_mcore -= allocated_mcore_
        print_log("DEBUG", "After core allocation, node " + self.to_str() + ", (requested mcore: " + str(allocated_mcore_) + ", available mcore: " + str(old_available_mcore) + " -> " + str(self.available_mcore) + ")")
        
    def evict_replica_from_node(self, target_repl):
        print_log("INFO", "evict_replica_from_node, replica {} from ME:node {}".format(target_repl.to_str(), self.to_str()))
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
node_0 = Node(region_id_=0, zone_id_=0, rack_id_=0, node_id_=0, total_mcore_=1000*100000)
node_1 = Node(region_id_=1, zone_id_=0, rack_id_=0, node_id_=0, total_mcore_=1000*100000)
placement.add_node(node_0)
placement.add_node(node_1)


class Service:
    def __init__(self, name_, mcore_per_req_, processing_t_, lb_):
        self.name = name_
        self.mcore_per_req = mcore_per_req_ # required num of milli-core per request
        self.processing_time = processing_t_
        self.ready_to_process = dict()
        self.ready_to_sendback = dict()
        self.lb = lb_
        self.replicas = list()
        if self.processing_time == 0:
            self.capacity_per_replica = 9999
        else:
            self.capacity_per_replica = 1000 / self.processing_time
        print(self.name + " capacity: " + str(self.capacity_per_replica))
    
    
    
   
    ## DEPRECATED     
    # def is_overloaded(self, cluster_id):
    #     if SCALEUP_POLICY == "conservative" or SCALEUP_POLICY == "doubleup":
    #         required_min_num_repl = self.calc_required_num_replica(cluster_id)
    #         #########################################################
    #         # cur_num_repl = self.get_cluster_num_replica(cluster_id)
    #         cur_num_repl = self.count_cluster_live_replica(cluster_id)
    #         #########################################################
    #         if  required_min_num_repl > cur_num_repl:
    #             print_log("WARNING", "overloaded " + self.name + ", required_min_num_repl,"+str(required_min_num_repl) + ", current_num_repl," + str(cur_num_repl))
    #             return True
    #         return False
    #     elif SCALEUP_POLICY == "kubernetes":
    #         desired = self.calc_desired_num_replica(cluster_id)
    #         #########################################################
    #         # cur_tot_num_repl = self.get_cluster_num_replica(cluster_id)
    #         cur_tot_num_repl = self.count_cluster_live_replica(cluster_id)
    #         #########################################################
    #         print_log("WARNING", "is_overloaded?, service {} in cluster {}, desired: {}, current: {}, Answer: {}".format(self.name, cluster_id, desired, cur_tot_num_repl, desired > cur_tot_num_repl))
    #         if desired > cur_tot_num_repl:
    #             print_log("WARNING", "Overloaded! service {} in cluster {}, desired: {}, current: {}".format(self.name, cluster_id, desired, cur_tot_num_repl))
    #             return True
    #         else:
    #             return False
    #     else:
    #         print_log("ERROR", SCALEUP_POLICY + " is not supported.")
    
    def has_schedulable_replica(self, cluster_id):
        for repl in self.replicas:
            if repl.cluster_id == cluster_id and repl.is_dead == False:
                if repl.is_schedulable():
                    print_log("WARNING", "has_schedulable_replica, cluster {} has schedulable replica for service {}".format(cluster_id, self.name))
                    return True
        return False
    
    def has_zero_queue_replica(self, cluster_id):
        for repl in self.replicas:
            if repl.cluster_id == cluster_id and repl.is_dead == False:
                if repl.processing_queue_size == 0:
                    print_log("WARNING", "has_zero_queue_replica, cluster {} has zero processing queue replica for service {}".format(cluster_id, self.name))
                    return True
        return False
            
    def should_we_scale_down(self, cluster_id):
        desired = self.calc_desired_num_replica(cluster_id)
        # cur_tot_num_repl = self.get_cluster_num_replica(cluster_id) # BUG
        cur_tot_num_repl = self.count_cluster_live_replica(cluster_id) # BUG fixed
        
        print_log("WARNING", "should_we_scale_down service {} cluster {} ?".format(self.name, cluster_id))
        print_log("WARNING", "\tcur_tot_num_repl, " + str(cur_tot_num_repl))
        print_log("WARNING", "\tdesired, " + str(desired))
        print_log("WARNING", "\t{}".format(cur_tot_num_repl > desired))
        return cur_tot_num_repl > desired
        
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
        print_log("INFO", "sort_replica_by_least_request")
        ret = list()
        for elem in sort_replica:
            print_log("INFO", "\t{}, status:{}, is_dead:{}, num_outstanding:{}".format(elem[0].to_str(), elem[0].get_status(), elem[0].is_dead, elem[1]))
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
        
        # desired_num_replica = math.ceil((current_metric_value / DESIRED_METRIC_VALUE) * self.get_cluster_num_replica(cluster_id))
        desired_num_replica = math.ceil((current_metric_value / DESIRED_METRIC_VALUE) * self.count_cluster_live_replica(cluster_id))
        print_log("WARNING", "Calculate desired_num_replica of " + self.name + " in cluster_" + str(cluster_id))
        print_log("WARNING", "current_metric_value, " + str((current_metric_value)))
        print_log("WARNING", "DESIRED_METRIC_VALUE, " + str((DESIRED_METRIC_VALUE)))
        # print_log("INFO", "current total num replica in cluster " + str(cluster_id) + ", " + str(self.get_cluster_num_replica(cluster_id)))

        print_log("INFO", "(current_metric_value / DESIRED_METRIC_VALUE), " + str((current_metric_value / DESIRED_METRIC_VALUE)))
        # print_log("INFO", "current_num_replica, " + str(self.get_cluster_num_replica(cluster_id)))
        print_log("WARNING", "current_num_replica, " + str(self.count_cluster_live_replica(cluster_id)))
        print_log("WARNING", "desired_num_replica, " + str(desired_num_replica))
        if desired_num_replica == 0:
            print_log("WARNING", "0 number of replica is not possible. Return minimun num replica(1).")
            return 1
        print_log("WARNING", "")
        return desired_num_replica
    
    
    def get_total_capacity(self, cluster_id):
        # total_capa = self.get_cluster_num_replica(cluster_id) *  self.capacity_per_replica
        total_capa = self.count_cluster_live_replica(cluster_id) *  self.capacity_per_replica
        if total_capa <= 0:
            print_log("ERROR", "get_total_capacity, " + self.name + ", current total live num replica: " + str(self.count_cluster_live_replica(cluster_id)) + ", capacity per replica: " + str(self.capacity_per_replica) )
        return total_capa
    
    
    ## Not being used.
    def remaining_capacity_based_on_rps_window(self, cluster_id):
        remaining_capa = self.get_total_capacity(cluster_id) - self.calc_rps(cluster_id)
        print_log("WARNING", "service {}, cluster:{}, remaining_capacity: {} = self.get_total_capacity: {} - current_rps: {}".format(self.name, cluster_id, remaining_capa, self.get_total_capacity(cluster_id), self.calc_rps(cluster_id)))
        return remaining_capa
    
    ## Not being used.
    def remaining_capacity_based_on_last_sec(self, cluster_id):
        cur_capa = self.get_total_capacity(cluster_id)
        last_sec_rps = self.calc_last_sec_service_rps(cluster_id)
        remaining_capa = cur_capa - last_sec_rps
        print_log("WARNING", "service {}, cluster:{}, remaining_capacity: {} = self.get_total_capacity: {} - last_sec_rps: {}".format(self.name, cluster_id, remaining_capa, cur_capa, last_sec_rps))
        return remaining_capa

    # Previously,this was used to calculate desired num replica, which was a bug.
    def get_current_metric(self, cluster_id):
        cur_rps = self.calc_rps(cluster_id)
        total_capa = self.get_total_capacity(cluster_id)
        metric = cur_rps / total_capa
        print_log("WARNING", "get_current_metric service {} in cluster_{}, metric: {}, calc_rps: {}, total_capacity: {}".format(self.name, cluster_id, metric, cur_rps, total_capa))
        return metric
    
    # It is used by calc_desired_num_replica function
    def get_current_live_replica_metric(self, cluster_id):
        cur_rps = self.calc_live_replica_rps(cluster_id)
        total_capa = self.get_total_capacity(cluster_id)
        metric = cur_rps / total_capa
        print_log("WARNING", "get_current_live_replica_metric service {} in cluster_{}, metric: {}, calc_rps: {}, total_capacity: {}".format(self.name, cluster_id, metric, cur_rps, total_capa))
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
        print_log("WARNING", "get_current_metric service {} in cluster_{}, metric: {}, calc_rps: {}, total_capacity: {}".format(self.name, cluster_id, metric, cur_rps, total_capa))
        return metric
    
    # It should change the name to "calc_cluster_load_based_on_rps_windows"
    def calc_rps(self, cluster_id): # This is the actual rps
        tot_rps = 0
        for repl in self.get_cluster_replica(cluster_id):
            tot_rps += repl.get_avg_rps()
        return tot_rps
    
    # It is used by calc_desired_num_replica function
    # This is the actual RPS!
    def calc_live_replica_rps(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_live_replica(cluster_id):
            tot_rps += repl.get_avg_rps()
        return tot_rps
    
    def calc_last_sec_service_rps(self, cluster_id): # This is the actual rps
        tot_rps = 0
        for repl in self.get_cluster_replica(cluster_id):
            tot_rps += repl.get_last_sec_rps()
        return tot_rps
    
    # It calculates the num of requests that come from replicas having the same cluster id. 
    # Since this is a method in service class, it adds up its all replicas' local rps.
    def calc_local_rps(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_replica(cluster_id):
            tot_rps += repl.get_most_recent_local_rps()
        return tot_rps
    
    def calc_remote_rps(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_replica(cluster_id):
            tot_rps += repl.get_most_recent_remote_rps()
        return tot_rps
    
    def calc_origin_rps(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_replica(cluster_id):
            tot_rps += repl.get_most_recent_origin_rps()
        return tot_rps
    
    def calc_non_origin_rps(self, cluster_id):
        tot_rps = 0
        for repl in self.get_cluster_replica(cluster_id):
            tot_rps += repl.get_most_recent_non_origin_rps()
        return tot_rps
    
    def calc_required_num_replica(self, cluster_id):
        tot_rps = self.calc_rps(cluster_id)
        tot_required_min_num_replica = math.ceil(tot_rps / self.capacity_per_replica)
        return tot_required_min_num_replica
    
    def get_last_replica_id(self, cluster_id):
        biggest_id = -1
        for repl in self.get_cluster_replica(cluster_id):
            if repl.id > biggest_id:
                biggest_id = repl.id
        return biggest_id
    
    def provision(self, how_many, cluster_id):
        print_log("WARNING", "\nStart provision(" + SCALEUP_POLICY + ") service" + self.name + "(cluster " + str(cluster_id) + ")")
        # prev_tot_num_repl = self.get_cluster_num_replica(cluster_id)
        prev_tot_num_repl = self.count_cluster_live_replica(cluster_id)
        min_required_num_repl = self.calc_required_num_replica(cluster_id)
        if SCALEUP_POLICY == "kubernetes":
            ### BUG: There is a gap between the time that overloaded condition was tested and the time that provision calculate the desired num replica with more recent RPS.
            # # Desired metric based calculation
            # # Usually it is more aggressive than doubleup.
            # desired = self.calc_desired_num_replica(cluster_id)
            # how_many_scale_up = desired - prev_tot_num_repl
            # assert how_many_scale_up > 0
            how_many_scale_up = how_many # NOTE: provision already received Kubernetes policy based how_many parameter. 
        else:
            print_log("ERROR", SCALEUP_POLICY  + " is not supported.")
            
        new_tot_num_repl = prev_tot_num_repl + how_many_scale_up
        
        print_log("WARNING", "\tCurrent total RPS: " + str(self.calc_rps(cluster_id)))
        # print_log("INFO", "\tCurrent total capacity: " + str(self.capacity_per_replica*self.get_cluster_num_replica(cluster_id)))
        print_log("WARNING", "\tCurrent total live capacity: " + str(self.capacity_per_replica*self.count_cluster_live_replica(cluster_id)))
        print_log("WARNING", "\tprev_tot_num_repl: " + str(prev_tot_num_repl))
        print_log("WARNING", "\tmin_required_num_repl: " + str(min_required_num_repl))
        print_log("WARNING", "\thow_many_scale_up: " + str(how_many_scale_up))
        print_log("WARNING", "\tnew_tot_num_repl: " + str(new_tot_num_repl))
        next_replica_id = self.get_last_replica_id(cluster_id) + 2            
            
        for _ in range(how_many_scale_up):
            ''' POTENTIAL BUG '''
            # Is it allowed to pass "self"?
            # It is circular referencing each other between replica object and service object.
            new_repl = Replica(service=self, id=next_replica_id)
            if cluster_id == 0:
                target_node = node_0
            else:
                target_node = node_1
            placement.place_replica_to_node_and_allocate_core(new_repl, target_node, 1000)
            print_log("WARNING", "provision, new replica {} is created. ".format(new_repl.to_str()))
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
            for parent_service in dag.get_parent_services(new_repl.service):
                print_log("INFO", "provision, parent_service {}".format(parent_service.name))
                for parent_repl in dag.get_parent_replica(new_repl, parent_service):
                    print_log("INFO", "\tprovision, parent_repl {}".format(parent_repl.to_str()))
                    
            for parent_service in dag.get_parent_services(new_repl.service):
                for parent_repl in dag.get_parent_replica(new_repl, parent_service):
                    parent_repl.register_child_replica_2(new_repl)
            #################################################
            print_log("INFO", "\tprovision, new replica " + new_repl.to_str() + ", cluster " + str(cluster_id))
        print_log("WARNING", "Finish provision " + self.name + "(cluster " + str(cluster_id) + ") from " + str(prev_tot_num_repl) + " to " + str(new_tot_num_repl))
        print_log("WARNING", "")
        # return self.get_cluster_num_replica(cluster_id)
        return self.count_cluster_live_replica(cluster_id)
    
    def remove_target_replica(self, target_repl, cluster_id):
        print_log("WARNING", "remove_target_replica, ME:{} in cluster {}, target_repl:{}".format(self.name, cluster_id, target_repl.to_str()))
        assert target_repl.is_removed == False
        target_repl.is_removed = True
        assert cluster_id == target_repl.id%2
        assert self == target_repl.service
        self.replicas.remove(target_repl)
        print_log("INFO", "\t\tDeleting replica " + target_repl.to_str())
        # WARNING: parent_replica should be parsed before the target replica gets removed from svc_to_repl.
        # Cluster 0 
        parent_service = dag.get_parent_services(target_repl.service)
        for svc in parent_service:
            if cluster_id == 0 and svc.name == "User1":
                parent_service.remove(svc)
                print_log("INFO", "\t\tExclude User1 from cluster {} {}'s parent service".format(cluster_id, target_repl.to_str()))
            if cluster_id == 1 and svc.name == "User0":
                print_log("INFO", "\t\tExclude User0 from cluster {} {}'s parent service".format(cluster_id, target_repl.to_str()))
                parent_service.remove(svc)
        
        all_parent_replica = list()
        for p_svc in parent_service:
            for p_repl in dag.get_parent_replica(target_repl, p_svc):
                all_parent_replica.append(p_repl)
                
        # print_log("DEBUG", "\tall_parent_replica of " + self.name + " cluster " + str(cluster_id))
        # for repl in all_parent_replica:
        #     print_log("DEBUG", "\t\t" + repl.to_str())
        
        dag.deregister_replica(target_repl)
        placement.evict_replica_and_free_core(target_repl)
        print_log("WARNING", "\t\tReplica " + target_repl.to_str() + " is deleted from dag and placement. ")
        #################################################
        # A----------> TARGET REPLICA--------->B
        for parent_repl in all_parent_replica:
            parent_repl.deregister_child_replica(target_repl) ### Tricky part.
        #################################################
        
        
    def scale_down(self, how_many_scale_down, cluster_id):
        assert how_many_scale_down > 0
        
        # prev_tot_num_repl = self.get_cluster_num_replica(cluster_id)
        prev_tot_num_repl = self.count_cluster_live_replica(cluster_id)
        
        # desired = self.calc_desired_num_replica(cluster_id)
        # how_many_scale_down = prev_tot_num_repl - desired
        new_tot_num_repl = prev_tot_num_repl - how_many_scale_down # == desired
        print_log("WARNING", "\nStart scale down(" + SCALEUP_POLICY + ") service" + self.name + "(cluster " + str(cluster_id) + ")")
        print_log("WARNING", "\tCurrent total RPS: " + str(self.calc_rps(cluster_id)))
        # print_log("INFO", "\tCurrent total capacity: " + str(self.capacity_per_replica*self.get_cluster_num_replica(cluster_id)))
        print_log("WARNING", "\tCurrent total live capacity: " + str(self.capacity_per_replica*self.count_cluster_live_replica(cluster_id)))
        print_log("WARNING", "\tprev_tot_num_repl: " + str(prev_tot_num_repl))
        # print_log("INFO", "\desired: " + str(desired))
        print_log("WARNING", "\tSuggested how_many_scale_down: " + str(how_many_scale_down))
        print_log("WARNING", "\tnew_tot_num_repl: " + str(new_tot_num_repl))
        
        sort_replica_list = self.sort_cluster_replica_by_least_request(cluster_id)
        print_log("INFO", "Returned sorted_replica_list:")
        for repl in sort_replica_list:
            print_log("INFO", "\t{}".format(repl.to_str()))
        
        #############################################################################
        ################################ BUG BUG BUG ################################
        #############################################################################
        ## I don't understand why it doesn't work.
        #############################################################################
        # print_log("INFO", "After filtering out dead replica, sorted_replica_list:")
        # for repl in sort_replica_list:
        #     if repl.is_dead:
        #         sort_replica_list.remove(repl)
        #         print_log("INFO", "\t{}".format(repl.to_str()))
        #############################################################################
            
        # Filter already killed(dead) replica.
        filtered_replica_list = list()
        for repl in sort_replica_list:
            if repl.is_dead:
                print_log("INFO", "Replica "+repl.to_str() + " has been already dead. It will be excluded from the candidate.")
            else:
                filtered_replica_list.append(repl)
        print_log("INFO", "")
        
        print_log("WARNING", "Filtered replica, sorted_replica_list:")
        for repl in filtered_replica_list:
            print_log("WARNING", "\t{}".format(repl.to_str()))
        print_log("WARNING", "")
        
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
            print_log("INFO", "first planed scale down replica({}) > num live replica ({})".format(how_many_scale_down, len(filtered_replica_list)))
            how_many_scale_down = len(filtered_replica_list) - 1 # BUG fixed. BUG 2: At least one replica should stay alive.
            print_log("INFO", "Decrease how_many_scale_down to {}".format(len(filtered_replica_list)))
        final_scale_down_replica = list()
        for i in range(how_many_scale_down):
            final_scale_down_replica.append(filtered_replica_list[i])
        
        final_new_tot_num_repl = prev_tot_num_repl - how_many_scale_down
        assert final_new_tot_num_repl > 0
            
        delayed_scale_down_replica = list()
        instant_scale_down_replica = list()
        print_log("WARNING", "Scale down selected replica. It will become dead.")
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
                print_log("ERROR", "scale_down, Invalid status, Replica " + repl.to_str())
            ####################################
            ####################################
            repl.is_dead = True
            print_log("WARNING", "\t{}, becomes dead. {}, num_pending: {}, child_oustanding: {}".format(repl.to_str(), repl.get_status(), repl.num_pending_request, repl.get_total_num_outstanding_response()))
        print_log("WARNING", "")
        
        
        print_log("WARNING", "Instant scale down of Ready replicas:")
        for repl in instant_scale_down_replica:
            print_log("WARNING", "\t{}, {}, num_pending: {}, child_oustanding: {}".format(repl.to_str(), repl.get_status(), repl.num_pending_request, repl.get_total_num_outstanding_response()))
        print_log("WARNING", "")
            
            
        print_log("WARNING", "Delaying scale down of Active replicas:")
        for repl in delayed_scale_down_replica:
            print_log("WARNING", "\t{}, {}, is_dead:{}, num_pending: {}, child_oustanding: {}".format(repl.to_str(), repl.get_status(), repl.is_dead, repl.num_pending_request, repl.get_total_num_outstanding_response()))
        print_log("WARNING", "")
            
        for target_repl in instant_scale_down_replica:
            self.remove_target_replica(target_repl, cluster_id)
            
        # print_log("INFO", "Finish scale down " + self.name + "(cluster " + str(cluster_id) + ") from " + str(prev_tot_num_repl) + " to " + str(self.get_cluster_num_replica(cluster_id)))
        print_log("WARNING", "Finish scale down " + self.name + "(cluster " + str(cluster_id) + ") from " + str(prev_tot_num_repl) + " to " + str(self.count_cluster_live_replica(cluster_id)))
        print_log("WARNING", "")
        # return self.get_cluster_num_replica(cluster_id)
        return self.count_cluster_live_replica(cluster_id)

class Replica:
    # def __init__(self, service_, id_, node_):
    def __init__(self, service, id):
        self.service = service
        self.id = id
        self.cluster_id = id%2
        self.node = None
        self.allocated_mcore = 0 # NOTE: Replica has its own dedicated core.
        self.available_mcore = 0
        self.name = self.service.name + "_" + str(self.id)
        self.child_services = dag.get_child_services(self.service)
        self.child_replica = dict() # Multi-cluster aware
        self.processing_time_history = list()
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
        self.queueing_time = dict()
        
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
            print_log("DEBUG", self.name + " child services: ", end=", ")
            for child_svc in self.child_services:
                print_log("DEBUG", child_svc.name, end=", ")
            print_log("DEBUG", "")
        if self.child_services != None:
            for child_svc in self.child_services:
                if child_svc not in self.sendback_requests:
                    self.sendback_requests[child_svc] = list()
                else:
                    print_log("ERROR", self.to_str + " has already " + child_svc.name + " in sendback_requests dictionary.")
            print_log("DEBUG", self.name + " sendback_requests: ", end="")
            for child_svc in self.sendback_requests:
                print_log("DEBUG", child_svc.name, end=", ")
            print_log("DEBUG", "")
            
    def calc_moment_latency(self, src_repl):
        if self.cluster_id == src_repl.cluster_id:
            network_lat = 0
        else:
            network_lat = 40
        queuing = self.processing_queue_size * self.service.processing_time
        processing_t = self.service.processing_time
        
        est_moment_latency = network_lat*2 + queuing + processing_t
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
    #                 print_log("INFO", "({})Changed the status, Replica {}, {}->{}".format(keyword, self.to_str(), prev_status, self.get_status()))
    #             return "Ready"
    #         else:
    #             if self.get_status() != "Active":
    #                 prev_status = self.get_status()
    #                 self.set_status("Active")
    #                 print_log("INFO", "({})Changed the status, Replica {}, {}->{}".format(keyword, self.to_str(), prev_status, self.get_status()))
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
            
            if len(self.req_count_list) > NUM_BASKET:
                self.req_count_list.pop(0) # Remove the oldest basket.
            if len(self.local_req_count_list) > NUM_BASKET:
                self.local_req_count_list.pop(0) # Remove the oldest basket.
            if len(self.remote_req_count_list) > NUM_BASKET:
                self.remote_req_count_list.pop(0) # Remove the oldest basket.
            if len(self.origin_req_count_list) > NUM_BASKET:
                self.origin_req_count_list.pop(0) # Remove the oldest basket.
            if len(self.non_origin_req_count_list) > NUM_BASKET:
                self.non_origin_req_count_list.pop(0) # Remove the oldest basket.    
            
            assert len(self.req_count_list) <= NUM_BASKET
            assert len(self.local_req_count_list) <= NUM_BASKET
            assert len(self.remote_req_count_list) <= NUM_BASKET
            assert len(self.origin_req_count_list) <= NUM_BASKET
            assert len(self.non_origin_req_count_list) <= NUM_BASKET
            
            self.num_req = 0 # reset num request for the next basket (next RPS_UPDATE_INTERVAL)
            self.local_num_req = 0
            self.remote_num_req = 0
            self.origin_num_req = 0
            self.non_origin_num_req = 0
            
            ## DEBUG PRINT
            # print_log("INFO", "Updated request_count_list: " + self.to_str(), end="")
            # for elem in self.req_count_list:
            #     print_log("INFO", elem, end=", ")
            # print_log("INFO", "")
            # print_log("INFO", "Updated local_request_count_list: " + self.to_str(), end="")
            # for elem in self.local_req_count_list:
            #     print_log("INFO", elem, end=", ")
            # print_log("INFO", "")
            # print_log("INFO", "Updated remote_request_count_list: " + self.to_str(), end="")
            # for elem in self.remote_req_count_list:
            #     print_log("INFO", elem, end=", ")
            # print_log("INFO", "")
            # print_log("INFO", "Updated origin_request_count_list: " + self.to_str(), end="")
            # for elem in self.origin_req_count_list:
            #     print_log("INFO", elem, end=", ")
            # print_log("INFO", "")
            # print_log("INFO", "Updated non_origin_request_count_list: " + self.to_str(), end="")
            # for elem in self.non_origin_req_count_list:
            #     print_log("INFO", elem, end=", ")
            # print_log("INFO", "")
            
            # # elif self.cluster_id == 1:
            # #     self.req_count_list.append(self.num_req) ## Main line of this function
                
            # #     if len(self.req_count_list) > NUM_BASKET:
            # #         self.req_count_list.pop(0) # Remove the oldest basket.
            # #     assert len(self.req_count_list) <= NUM_BASKET
            # #     print_log("INFO", "update_request_count_list: " + self.to_str(), end="")
            # #     self.num_req = 0 # reset num request for the next basket (next RPS_UPDATE_INTERVAL)
            # #     for elem in self.req_count_list:
            # #         print_log("INFO", elem, end=", ")
            
            # print_log("WARNING", self.to_str() + " avg rps: " + str(avg(self.req_count_list)))
            # print_log("WARNING", self.to_str() + " avg local rps: " + str(avg(self.local_req_count_list)))
            # print_log("WARNING", self.to_str() + " avg remote rps: " + str(avg(self.remote_req_count_list)))
            # print_log("WARNING", self.to_str() + " avg origin rps: " + str(avg(self.origin_req_count_list)))
            # print_log("WARNING", self.to_str() + " avg non_origin rps: " + str(avg(self.non_origin_req_count_list)))
        
    def get_avg_rps(self):
        # assert len(self.req_count_list) == NUM_BASKET
        if len(self.req_count_list) == 0:
            return 0
        return sum(self.req_count_list)/len(self.req_count_list)
    
    def get_last_sec_rps(self):
        if len(self.req_count_list) == 0:
            return 0
        return self.req_count_list[-1]
    
    def get_most_recent_local_rps(self):
        if len(self.local_req_count_list) == 0:
            return 0
        return sum(self.local_req_count_list)/len(self.local_req_count_list)
    
    def get_most_recent_remote_rps(self):
        if len(self.remote_req_count_list) == 0:
            return 0
        return sum(self.remote_req_count_list)/len(self.remote_req_count_list)
    
    def get_most_recent_origin_rps(self):
        if len(self.origin_req_count_list) == 0:
            return 0
        return sum(self.origin_req_count_list)/len(self.origin_req_count_list)
    
    def get_most_recent_non_origin_rps(self):
        if len(self.non_origin_req_count_list) == 0:
            return 0
        return sum(self.non_origin_req_count_list)/len(self.non_origin_req_count_list)
    
    
    def deregister_child_replica(self, child_repl):
        print_log("INFO", "\t\tderegister_child_replica, Me:" + self.to_str() + ", target_child_repl: " + child_repl.to_str())
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
        #     print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in outstanding_response")
        # self.outstanding_response[child_repl] = list()
        
        if child_repl in self.num_outstanding_response_from_child:
            print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in num_outstanding_response_from_child")
        self.num_outstanding_response_from_child[child_repl] = 0
        
        if child_repl in self.sending_time:
            print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in sending_time")
        self.sending_time[child_repl] = dict()
        
        if child_repl in self.response_time:
            print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in response_time")
        self.response_time[child_repl] = list()
        
        print_log("INFO", "register_child_replica_2, Parent replica {} registers child replica {}.".format(self.to_str(), child_repl.to_str()))
        
        # if child_repl in self.all_rt_history:
        #     print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in all_rt_history")
        # self.all_rt_history[child_repl] = list()
    # def register_child_replica(self, child_repl, child_svc):
    #     if child_svc not in self.child_replica:
    #         self.child_replica[child_svc] = list()
    #     self.child_replica[child_svc].append(child_repl)
        
    #     if child_repl in self.outstanding_response:
    #         print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in outstanding_response")
    #     self.outstanding_response[child_repl] = list()
        
    #     if child_repl in self.num_outstanding_response_from_child:
    #         print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in num_outstanding_response_from_child")
    #     self.num_outstanding_response_from_child[child_repl] = 0
        
    #     if child_repl in self.sending_time:
    #         print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in sending_time")
    #     self.sending_time[child_repl] = dict()
        
    #     if child_repl in self.response_time:
    #         print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in response_time")
    #     self.response_time[child_repl] = list()
        
    #     if child_repl in self.all_rt_history:
    #         print_log("ERROR", self.to_str() + " has " + child_repl.to_str() + " already in all_rt_history")
    #     self.all_rt_history[child_repl] = list()
        
    def place_and_allocate(self, node_, allocated_mcore_):
        self.node = node_
        self.allocated_mcore = allocated_mcore_
        self.available_mcore = allocated_mcore_
    
    # True from this function guarantees that AT LEAST one request could be scheduled. 
    # And possibly, more than one request could be scheduled.
    def is_schedulable(self):
        # print(self.to_str()+ " is_schedulable: ", end="")
        if self.available_mcore >= self.service.mcore_per_req:
            print_log("DEBUG", self.to_str() + "is schedulable - avail core(" +  str(self.available_mcore) + ") > " + self.service.name + "(required core: "+ str(self.service.mcore_per_req) +")")
            return True
        else:
            print_log("DEBUG", self.to_str() + " is NOT schedulable. Not enough resource - avail core(" +  str(self.available_mcore) + "), " + self.service.name + "(required core: "+ str(self.service.mcore_per_req) +")")
            return False
        
    # def allocate_resource_to_request(self, req):
    #     if self.service.mcore_per_req > self.available_mcore:
    #         print_log("ERROR", self.to_str() + "request[" + str(req.id) + "], required_mcore(" + str(self.service.mcore_per_req) +  ") > available_mcore("+str(self.available_mcore) + ") ")
    #     self.available_mcore -= self.service.mcore_per_req
    #     if self.available_mcore < 0:
    #         print_log("ERROR", "Negative available core is not allowed. " + self.to_str())
    #     print_log("DEBUG", "Allocate "  + str(self.service.mcore_per_req) + " core to request[" + str(req.id) + "] in " +  self.to_str() + ", ")
    
    def allocate_resource(self):
        if self.service.mcore_per_req > self.available_mcore:
            print_log("ERROR", self.to_str() + ", required_mcore(" + str(self.service.mcore_per_req) +  ") > available_mcore("+str(self.available_mcore) + ") ")
            
        self.available_mcore -= self.service.mcore_per_req
        
        if self.available_mcore < 0:
            print_log("ERROR", self.to_str() + ", negative available core(" + str(self.available_mcore) + ")")
        print_log("DEBUG", self.to_str() + ", Allocate "  + str(self.service.mcore_per_req) + " mcore ")
        
    def free_resource_from_request(self, req):
        self.available_mcore += self.service.mcore_per_req
        if self.available_mcore > self.allocated_mcore:
            print_log("ERROR", "Available core in replica " + self.name + " can't be greater than allocated core. " + self.to_str())
        if self.available_mcore > self.node.total_mcore:
            print_log("ERROR", "Available core in replica " + self.name + " can't be greater than total num cores in node[" + self.node.to_str() + "].")
        print_log("DEBUG", self.to_str() + ", free " + str(self.service.mcore_per_req) + " core from request[" + str(req.id) + "] in " + self.to_str())
        
    def add_to_send_back_queue(self, req_, schd_time_, child_svc_):
        if child_svc_ not in self.sendback_requests:
            print_log("DEBUG", self.to_str() + " doesn't have " + child_svc_.name + " in its sendback_requests.")
            print_log("DEBUG", "Currently, it has ", end="")
            for svc in self.sendback_requests:
                print_log("DEBUG", svc.name, end=", ")
            print_log("DEBUG", "")
        self.sendback_requests[child_svc_].append(req_)
        print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + self.to_str() + child_svc_.name + " sendback_queue (size: " + str(len(self.sendback_requests[child_svc_])) + ")")
        
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
    #     print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + self.to_str() + parent_svc_.name + " recv_queue (size: " + str(len(self.recv_requests[parent_svc_])) + ")")
    
    def add_to_recv_queue(self, req_, schd_time_, src_repl):
        # self: dst_replica

        ## Dictionary version
        # if src_repl not in self.fixed_recv_requests:
        #     self.fixed_recv_requests[src_repl] = list()
        # self.fixed_recv_requests[src_repl].append(req_)
        # print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + src_repl.to_str() + " recv_queue (size: " + str(len(self.fixed_recv_requests[src_repl])) + ")")
        
        ## List version
        # This is possible because all the requests are unique.
        self.fixed_recv_requests.append([req_, src_repl])
        print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + self.to_str() + " recv_queue (size: " + str(len(self.fixed_recv_requests)) + ")")


    def is_user(self):
        if self.parent_services == None:
            print_log("DEBUG", self.name + " is a User.")
            return True
        return False
            
    def is_frontend(self):
        for svc in self.parent_services:
            if svc.name.find("User") != -1:
                # print_log("DEBUG", self.name + " is a frontend service.")
                return True
            
    # TODO: likely buggy
    def is_request_ready_to_process(self, req_):
        # If this is the frontend service(A) replica, it doesn't need to wait since there is always only one parent service which is User.
        # NOTE: I don't know why I separate the cases between frontend service and non-frontend service.
        if self.is_frontend():
            print_log("DEBUG", self.name + " is frontend, so recv request["+ str(req_.id)+"] is always ready to be processed.")
            return True
        
        ## Dictionary version
        # Check if this replica receives request from "ALL" src_replicas.
        # for src_repl_key in self.fixed_recv_requests:
        #     if req_ not in self.fixed_recv_requests[src_repl_key]:
        #         print_log("DEBUG", "Reqeust[" + str(req_.id) + "] is waiting for " + src_repl_key.to_str())
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
        print_log("DEBUG", "Add request[" + str(req_.id) + "] to " + self.to_str() + " ready queue. (size: " + str(len(self.fixed_ready_queue)) +")")
        
        
    def dequeue_from_ready_queue(self):
        # if self.fixed_ready_queue:
        #     print_log("ERROR", self.to_str() + ", ready queue is empty!")
        ret = self.fixed_ready_queue.pop(0) # FIFO. pop the first elem
        request = ret[0]
        src_replica = ret[1]
        
        print_log("DEBUG", "Dequeue request[" + str(request.id) + "], from"  + self.to_str() + " ready queue. (size: " + str(len(self.fixed_ready_queue)) + ")")
        
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
    def __init__(self, req_arrival_0, req_arrival_1, app, cur_time, req_arr_0, req_arr_1, arg_flags):
        assert len(req_arrival_0) > 0
        assert len(req_arrival_1) > 0
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
            
        self.cluster0_capacity = dict()
        self.cluster1_capacity = dict()
        self.cluster0_autoscaling_timestamp = dict()
        self.cluster1_autoscaling_timestamp = dict()
        def create_key_with_service(dic_):
            for service in dag.all_service:
                dic_[service] = list()
        create_key_with_service(self.cluster0_capacity)
        create_key_with_service(self.cluster1_capacity)
        create_key_with_service(self.cluster0_autoscaling_timestamp)
        create_key_with_service(self.cluster1_autoscaling_timestamp)
        
        self.user0_num_req = len(req_arrival_0)
        self.user1_num_req = len(req_arrival_1)
        self.total_num_req = len(req_arrival_0) + len(req_arrival_1)
        print_log("DEBUG", "user0 num_req: " + str(self.user0_num_req))
        print_log("DEBUG", "user1 num_req: " + str(self.user1_num_req))
        print_log("DEBUG", "total num_req: " + str(self.total_num_req))
        self.first_time_flag = True
        self.app = app
        self.cur_time = cur_time
        self.request_arr_0 = req_arr_0
        self.request_arr_1 = req_arr_1
        self.arg_flags = arg_flags
        
    def get_latency(self, cluster_id):
        if cluster_id == 0:
            latency = [ x[1] for x in self.user0_latency ]
        elif cluster_id == 1:
            latency = [ x[1] for x in self.user1_latency ]
        else:
            print_log("ERROR", "Invalid cluster id: {}".format(cluster_id))
        return latency
        
    def print_summary(self):
        print_log("DEBUG", "="*30)
        print_log("DEBUG", "* Simulator summary *")
        print_log("DEBUG", "="*30)
        l_li = list(self.end_to_end_latency.values())
        print_percentiles(l_li)
        # p = ", processing time: 10ms"
        
        if len(self.user0_latency) > 0:
            u0_latency_record_time = [ x[0] for x in self.user0_latency ]
            u0_latency = self.get_latency(cluster_id = 0)
            print_log("DEBUG", "")
            print_log("DEBUG", "="*30)
            print_log("DEBUG", "* User group 0 latency summary *")
            print_percentiles(u0_latency)
            print_log("DEBUG", "len(u0_latency): " +str(len(u0_latency)))
        
        if len(self.user1_latency) > 0:
            u1_latency_record_time = [ x[0] for x in self.user1_latency ]
            u1_latency = self.get_latency(cluster_id = 1)
            print_log("DEBUG", "")
            print_log("DEBUG", "="*30)
            print_log("DEBUG", "* User group 1 latency summary *")
            print_percentiles(u1_latency)
            print_log("DEBUG", "len(u1_latency): " + str(len(u1_latency)))
            print_log("DEBUG", "")
            
    def get_experiment_title(self):
        return self.arg_flags.app+"-"+self.arg_flags.workload+"-"+self.arg_flags.load_balancer+"-"+self.arg_flags.routing_algorithm
    
    def get_output_dir(self):
        if os.path.exists(self.arg_flags.output_dir) == False:
            os.mkdir(self.arg_flags.output_dir)
        dir = self.arg_flags.output_dir + "/" + str(self.cur_time) + "-" + self.get_experiment_title()
        # Example: ./log/20230314_201145-three_depth-b694fb-RoundRobin-heuristic_TE
        if os.path.exists(dir) == False:
            os.mkdir(dir)
        return dir
    
    def write_simulation_latency_result(self):
        def write_metadata_file():
            output_dir = self.get_output_dir()
            meta_file = open(output_dir+"/metadata.txt", 'w')
            temp_list = list()
            for key, value in vars(self.arg_flags).items():
                temp_list.append(key + " : " + str(value)+"\n")
            meta_file.writelines(temp_list)
            meta_file.close()
        
        def write_latency_result(li_, cluster_id, path):
            f_ = open(path, 'w')
            temp_list = list()
            temp_list.append("cluster_"+str(cluster_id)+"\n")
            temp_list.append(self.arg_flags.app+"\n")
            temp_list.append(self.arg_flags.workload+"\n")
            temp_list.append(self.arg_flags.load_balancer+"\n")
            temp_list.append(self.arg_flags.routing_algorithm+"\n")
            for elem in li_:
                temp_list.append(str(elem)+"\n")
            f_.writelines(temp_list)
            f_.close()
            
        write_metadata_file()
        u0_latency = self.get_latency(cluster_id=0)
        u1_latency = self.get_latency(cluster_id=1)
        output_dir = self.get_output_dir()
        path_to_latency_file_cluster_0 = output_dir + "/latency-cluster_0.txt"
        path_to_latency_file_cluster_1 = output_dir + "/latency-cluster_1.txt"
        write_latency_result(u0_latency, cluster_id=0, path=path_to_latency_file_cluster_0)
        write_latency_result(u1_latency, cluster_id=1, path=path_to_latency_file_cluster_1)
    
    def plot_and_save_resource_provisioning(self):
        def get_ylim(req_arr_0, req_arr_1, capa_0, capa_1):
            def get_max_rps_ylim(reqarr):
                cur = 1000 # 1000ms, 1sec
                cnt_list = list()
                cnt_list.append(0)
                idx = 0
                for arr in reqarr:
                    if arr > cur: # Every 1000ms
                        cur += 1000
                        idx += 1
                        cnt_list.append(0)
                    cnt_list[idx] += 1
                max_rps = max(cnt_list) # Max number of request/sec which will be the max y value in the plot
                return max_rps
            max_rps_cluster_0 = get_max_rps_ylim(req_arr_0)
            max_rps_cluster_1 = get_max_rps_ylim(req_arr_1)
            max_rps = max(max_rps_cluster_0, max_rps_cluster_1)\
                
            cluster_0_capa_trend = dict()
            cluster_1_capa_trend = dict()
            for service in capa_0:
                cluster_0_capa_trend[service] = [ x[1] for x in capa_0[service]]
                print("cluster_0_capa_trend,{}".format(service.name))
                print(cluster_0_capa_trend[service])
            for service in capa_1:
                cluster_1_capa_trend[service] = [ x[1] for x in capa_1[service]]
                print("cluster_1_capa_trend[service]: ", cluster_1_capa_trend[service])
            max_capa_cluster_0 = 0
            for service in cluster_0_capa_trend:
                if len(cluster_0_capa_trend[service]) > 0:
                    max_capa_cluster_0 = max(max_capa_cluster_0, max(cluster_0_capa_trend[service]))
            max_capa_cluster_1 = 0
            for service in cluster_1_capa_trend:
                if len(cluster_1_capa_trend[service]) > 0:
                    max_capa_cluster_1 = max(max_capa_cluster_1, max(cluster_1_capa_trend[service]))
            max_capa = max(max_capa_cluster_0, max_capa_cluster_1)
            ymax = max(max_rps, max_capa) + 10
            # print_log("DEBUG", "max_rps_cluster_0: ", max_rps_cluster_0)
            # print_log("DEBUG", "max_rps_cluster_1: ", max_rps_cluster_1)
            # print_log("DEBUG", "max_capa_cluster_0: ", max_capa_cluster_0)
            # print_log("DEBUG", "max_capa_cluster_1: ", max_capa_cluster_1)
            # print_log("DEBUG", "ymax: ", ymax)
            return ymax
            
        ylim = get_ylim(self.request_arr_0, self.request_arr_1, self.cluster0_capacity, self.cluster1_capacity)
        title_cluster_0 = "cluster_0-" + self.get_experiment_title()
        title_cluster_1 = "cluster_1-" + self.get_experiment_title()
        path_to_autoscaler_cluster_0 = self.get_output_dir()+"/resource_provisioing_trend-cluster_0.pdf"
        path_to_autoscaler_cluster_1 = self.get_output_dir()+"/resource_provisioing_trend-cluster_1.pdf"
        plot_workload_histogram_with_autoscaling(self.request_arr_0, self.cluster0_capacity, title_cluster_0, ylim, path_to_autoscaler_cluster_0)
        plot_workload_histogram_with_autoscaling(self.request_arr_1, self.cluster1_capacity, title_cluster_1, ylim, path_to_autoscaler_cluster_1)
        
    def write_req_arr_time(self):
        def file_write_request_arrival_time(req_arr, cluster_id, path):
            file1 = open(path, 'w')
            temp_list = list()
            temp_list.append("cluster_"+str(cluster_id)+"\n")
            temp_list.append(self.arg_flags.app+"\n")
            temp_list.append(self.arg_flags.workload+"\n")
            temp_list.append(self.arg_flags.load_balancer+"\n")
            temp_list.append(str(self.arg_flags.routing_algorithm)+"\n")
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
        def file_write_autoscaler_timestamp(req_arr, cluster_id, path):
            file1 = open(path, 'w')
            temp_list = list()
            temp_list.append("cluster_"+str(cluster_id)+"\n")
            temp_list.append(self.arg_flags.app+"\n")
            temp_list.append(self.arg_flags.workload+"\n")
            temp_list.append(self.arg_flags.load_balancer+"\n")
            temp_list.append(str(self.arg_flags.routing_algorithm)+"\n")
            for service in req_arr:
                for elem in req_arr[service]:
                    temp_list.append(str(service.name) + "," + str(service.processing_time) + "," + str(elem[0]) + "," + str(elem[1])+"\n")
            file1.writelines(temp_list)
            file1.close()
        path_to_autoscaler_cluster_0 = self.get_output_dir()+"/resource_provisioing_log-clsuter_0.txt"
        path_to_autoscaler_cluster_1 = self.get_output_dir()+"/resource_provisioing_log-clsuter_1.txt"
        assert path_to_autoscaler_cluster_0 != path_to_autoscaler_cluster_1
        file_write_autoscaler_timestamp(self.cluster0_capacity, cluster_id=0, path=path_to_autoscaler_cluster_0)
        file_write_autoscaler_timestamp(self.cluster1_capacity, cluster_id=1, path=path_to_autoscaler_cluster_1)
        

    def schedule_event(self, event):
        # print_log("DEBUG", "Scheduled: " + event.name + " event at " + str(event.scheduled_time))
        heapq.heappush(self.event_queue, (event.scheduled_time, np.random.uniform(0, 1, 1)[0], event))
        # for elem in self.event_queue:
        #     print(str(elem[0]) + ", " + elem[2].name)
            # print(str(elem[0]) + ", " + str(elem[1]) + ", " + elem[2].name)
        # NOTE: heap generates the following error when two different items in heap have the same event.scheduled_time. Heap compares second item in tuple to do min/max heap push operation.
        # TypeError: '<' not supported between instances of 'TryToProcessRequest' and 'SendBackRequest'\
        # Solution: Add a random number as a second argument to heap. It will be the comparator when two event.scheduled_time items have the same value.

    def start_simulation(self, program_start_ts):
        print_log("DEBUG", "========== Start simulation ==========")
        print_log("DEBUG", "- total num request: " + str(len(self.event_queue)))
        while len(self.event_queue) > 0:
            next_event = heapq.heappop(self.event_queue)[2] # [2]: event
            # print("Next event: " + next_event.name)
            # print("Next event: " + next_event.name + ", request[" + str(next_event.request.id) + "]")
            self.current_time = next_event.scheduled_time
            next_event.execute_event()
        print_log("DEBUG", "========== Finish simulation ==========")
        print("="*40)
        print("program run time: {}".format(time.time() - program_start_ts))
        print("="*40)
        print()
        self.print_summary()
        self.write_simulation_latency_result()
        self.plot_and_save_resource_provisioning()
        self.write_req_arr_time()
        self.write_resource_provisioning()
        # for repl in dag.all_replica:
        #     li = repl.processing_time_history
        #     if len(li) > 0:
        #         # print_percentiles(li, repl.to_str() + " processing_time")
        #         plot_histogram(li, repl.to_str() + " processing_time")
        #         break
        
        
        ## Print Queueing time
        print_log("DEBUG", "Queueing time")
        for repl in dag.all_replica:
            li = list(repl.queueing_time.values())
            if len(li) > 0:
                # print_percentiles(li, repl.to_str() + " queueing time")
                print_log("DEBUG", repl.to_str())
                print_percentiles(li)
        ## Plot Queueing time PDF (to see how queuing time changes.)
        # for repl in dag.all_replica:
        #     li = list(repl.queueing_time.values())
        #     if len(li) > 0:
        #         plot_histogram(li, repl.to_str() + " queueing time")
        
        ## Print how many request each replica received in total.
        for repl in dag.all_replica:
            print_log("DEBUG", repl.to_str() + " num_recv_request: " + str(repl.num_recv_request))
        
class Event:
    def execute_event(self):
        print_log("ERROR", "You must implement execute_event function for child class of Event class.")


class Request:
    def __init__(self, id_, origin_cluster):
        self.id = id_
        self.origin_cluster = origin_cluster
        self.history = list() # Stack
        
    def push_history(self, src_repl, dst_repl):
        self.history.append([src_repl, dst_repl])
        print_log("DEBUG", "[Push] [" + src_repl.to_str() + ", " + dst_repl.to_str() + "]")
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
                print_log("DEBUG", "Pop [" + elem[0].to_str() + ", " + elem[1].to_str() + "]")
                ret = elem
                self.history.remove(elem)
                return ret
        self.print_history()
        print_log("ERROR", "Couldn't find the " + dst_repl.to_str() + " in request[" + str(self.id) + "] history")
        
    def print_history(self):
        print_log("DEBUG", "Request[" + str(self.id) + "] History: ")
        for record in self.history:
            print_log("DEBUG", "\t" + record[0].to_str() + "->" + record[1].to_str())
            # record[0]: src, record[1]: dst

######################################################################################################
######################################################################################################
class UpdateRPS(Event):
    def __init__(self, schd_time_):
        self.scheduled_time = schd_time_
        self.name = "UpdateRPS"

    def event_latency(self):
        return 0

    def execute_event(self):
        e_latency = self.event_latency()
        print_log("INFO", "Execute: UpdateRPS during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        ## Update the request count for ALL replica.
        for repl in dag.all_replica:
            repl.update_request_count_list()
            

class FinishScaleDownStabilization(Event):
    def __init__(self, schd_time_, cluster_id):
        self.scheduled_time = schd_time_
        self.cluster_id = cluster_id
        self.name = "FinishScaleDownStabilization"

    def event_latency(self):
        return 0

    def execute_event(self):
        e_latency = self.event_latency()
        print_log("INFO", "Execute: FinishScaleDownStabilization cluster " + str(self.cluster_id) + " can scale down again. " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        assert SCALE_DOWN_STATUS[self.cluster_id] == 0
        SCALE_DOWN_STATUS[self.cluster_id] = 1
            


class AutoscalerCheck(Event):
    def __init__(self, schd_time_):
        self.scheduled_time = schd_time_
        self.name = "AutoscalerCheck"

    def event_latency(self):
        return 0

    def execute_event(self):
        # SCALE_UP_OVERHEAD=5000 # 5sec
        # SCALE_DOWN_OVERHEAD=5000 # 5sec
        e_latency = self.event_latency()
        print_log("INFO", "Execute: AutoscalerCheck during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        for service in dag.all_service:
            if service.name.find("User") == -1:
                # if simulator.app == "one_service":
                simulator.cluster0_capacity[service].append([self.scheduled_time, service.get_total_capacity(0)])
                simulator.cluster1_capacity[service].append([self.scheduled_time, service.get_total_capacity(1)])
                    
                # Scale up check
                cluster_0_desired = service.calc_desired_num_replica(0)
                cluster_0_cur_tot_num_repl = service.count_cluster_live_replica(0)
                overloaded_cluster_0 = cluster_0_desired > cluster_0_cur_tot_num_repl
                print_log("WARNING", "Is cluster {} overloaded?, desired: {}, current: {}".format(0, cluster_0_desired, cluster_0_cur_tot_num_repl))
                if overloaded_cluster_0: # overloaded
                    print_log("WARNING", "Overloaded! service {} in cluster {}, desired: {}, current: {}".format(self.name, 0, cluster_0_desired, cluster_0_cur_tot_num_repl))
                    how_many_scale_up = cluster_0_desired - cluster_0_cur_tot_num_repl
                    assert how_many_scale_up > 0
                    scale_up_event = ScaleUp(self.scheduled_time + SCALE_UP_OVERHEAD + e_latency, service, how_many_scale_up, cluster_id=0)
                    simulator.schedule_event(scale_up_event)
                elif SCALE_DOWN_STATUS[0]==1 and service.should_we_scale_down(cluster_id=0): # If not overloaded, then check if scale down is needed.
                    scale_down_schd_time = self.scheduled_time + SCALE_DOWN_OVERHEAD + e_latency
                    desired = service.calc_desired_num_replica(0)
                    # cur_num_repl = service.get_cluster_num_replica(0)
                    cur_num_repl = service.count_cluster_live_replica(0)
                    how_many_scale_down = cur_num_repl - desired
                    print_log("INFO", "Schedule ScaleDown! service {} (cluster_{}) by {} at {}".format(service.name, 0, how_many_scale_down, scale_down_schd_time))
                    scale_down_event = ScaleDown(scale_down_schd_time, service, how_many_scale_down, cluster_id=0)
                    simulator.schedule_event(scale_down_event)
                    
                    SCALE_DOWN_STATUS[0]=0
                    finish_stabilization_event = FinishScaleDownStabilization(self.scheduled_time + e_latency + SCALE_DOWN_STABILIZE_WINDOW, 0)
                    simulator.schedule_event(finish_stabilization_event)
                    
                cluster_1_desired = service.calc_desired_num_replica(1)
                cluster_1_cur_tot_num_repl = service.count_cluster_live_replica(1)
                overloaded_cluster_1 = cluster_1_desired > cluster_1_cur_tot_num_repl
                print_log("WARNING", "Is cluster {} overloaded?, desired: {}, current: {}".format(1, cluster_1_desired, cluster_1_cur_tot_num_repl))
                # if service.is_overloaded(cluster_id=1):
                if overloaded_cluster_1: # overloaded
                    print_log("WARNING", "Overloaded! service {} in cluster {}, desired: {}, current: {}".format(self.name, 1, cluster_1_desired, cluster_1_cur_tot_num_repl))
                    how_many_scale_up = cluster_1_desired - cluster_1_cur_tot_num_repl
                    assert how_many_scale_up > 0
                    scale_up_event = ScaleUp(self.scheduled_time + SCALE_UP_OVERHEAD + e_latency, service, how_many_scale_up, cluster_id=1)
                    simulator.schedule_event(scale_up_event)
                elif SCALE_DOWN_STATUS[1]==1 and service.should_we_scale_down(cluster_id=1):
                    scale_down_schd_time = self.scheduled_time + SCALE_DOWN_OVERHEAD + e_latency
                    desired = service.calc_desired_num_replica(1)
                    # cur_num_repl = service.get_cluster_num_replica(1)
                    cur_num_repl = service.count_cluster_live_replica(1)
                    how_many_scale_down = cur_num_repl - desired
                    print_log("INFO", "Schedule ScaleDown! service {} (cluster_{}) by {} at {}".format(service.name, 1, how_many_scale_down, scale_down_schd_time))
                    scale_down_event = ScaleDown(self.scheduled_time + SCALE_DOWN_OVERHEAD + e_latency, service, how_many_scale_down, cluster_id=1)
                    simulator.schedule_event(scale_down_event)
                    
                    SCALE_DOWN_STATUS[1]=0
                    finish_stabilization_event = FinishScaleDownStabilization(self.scheduled_time + e_latency + SCALE_DOWN_STABILIZE_WINDOW, 1)
                    simulator.schedule_event(finish_stabilization_event)
            
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
        print_log("INFO", "Execute: ScaleUp " + self.service.name + " from " + str(prev_total_num_replica) + " to " + str(new_total_num_replica) + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        if self.cluster_id == 0:
            # if simulator.app == "one_service":
            simulator.cluster0_autoscaling_timestamp[self.service].append(self.scheduled_time + e_latency)
            simulator.cluster0_capacity[self.service].append([self.scheduled_time + e_latency, self.service.get_total_capacity(self.cluster_id)])
        if self.cluster_id == 1:
            # if simulator.app == "one_service":
            simulator.cluster1_autoscaling_timestamp[self.service].append(self.scheduled_time + e_latency)
            simulator.cluster1_capacity[self.service].append([self.scheduled_time + e_latency, self.service.get_total_capacity(self.cluster_id)])
            

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
        print_log("WARNING", "Execute: ScaleDown " + self.service.name + " from " + str(prev_total_num_replica) + " to " + str(new_total_num_replica) + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        if self.cluster_id == 0:
            # if simulator.app == "one_service":
            simulator.cluster0_autoscaling_timestamp[self.service].append(self.scheduled_time + e_latency)
            simulator.cluster0_capacity[self.service].append([self.scheduled_time + e_latency, self.service.get_total_capacity(self.cluster_id)])
        if self.cluster_id == 1:
            # if simulator.app == "one_service":
            simulator.cluster1_autoscaling_timestamp[self.service].append(self.scheduled_time + e_latency)
            simulator.cluster1_capacity[self.service].append([self.scheduled_time + e_latency, self.service.get_total_capacity(self.cluster_id)])


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
        
    # def find_dst_candidate_replica_set(self):

    def execute_event(self):
        print_log("INFO", "Start LB event! (src_replica: {})".format(self.src_replica.to_str()))
        if simulator.first_time_flag:
            # if simulator.app == "one_service":
            simulator.cluster0_capacity[self.dst_service].append([0, self.dst_service.get_total_capacity(0)])
            simulator.cluster1_capacity[self.dst_service].append([0, self.dst_service.get_total_capacity(1)])
            simulator.first_time_flag = False
            
        e_latency = self.event_latency()
        
        # Step 1: Find replica candidates
        # Step 2: Pick one replica from the candidates.
        
        # Step 1 
        # If the static_lb rule is defined for User. static_lb is only for User.
        # if self.src_replica.service.name.find("User") != -1:
        #     if len(static_lb) != 0:
        #         dst_replicas = placement.svc_to_repl[self.dst_service]
        #         dst_replica_candidates = list()
        #         dst_repl_idx_list = static_lb[self.src_replica.service.name]
        #         # print(dst_repl_idx_list)
        #         for repl_idx in dst_repl_idx_list:
        #             dst_replica_candidates.append(dst_replicas[repl_idx])
        #         # print_log("DEBUG", "Static LB from "+self.src_replica.service.name + " to ")
        #         # for repl in dst_replica_candidates:
        #         #     print(repl.to_str())
        #     # Otherwise, all replicas of the dst service will be included in lb candidate pool.
        #     else:
        #         dst_replica_candidates = placement.svc_to_repl[self.dst_service]
            
        # No multi-cluster
        if ROUTING_ALGORITHM == "LCLB":
            superset_candidates = placement.svc_to_repl[self.dst_service]
            dst_replica_candidates = list()
            # Cluster 0
            if self.src_replica.id%2 == 0:
                for repl in superset_candidates:
                     if repl.id%2 == 0:
                        if repl.is_dead == False:
                            dst_replica_candidates.append(repl)
                        else:
                            print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
            # Cluster 1
            elif self.src_replica.id%2 == 1:
                for repl in superset_candidates:
                     if repl.id%2 == 1:
                        if repl.is_dead == False:
                            dst_replica_candidates.append(repl)
                        else:
                            print_log("INFO", "Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
        
        elif ROUTING_ALGORITHM == "heuristic_TE":
            # This is naive implementation of request routing.
            # First, it assumes that you know the available resource of the remote cluster magically.
            # Second, the requests will be routed if the load becomes soaring up and the local cluster cannot accomodate it with the current capacity AND if the remote cluster has some rooms to process more request other than its local load.
            if self.src_replica.cluster_id == 0:
                other_cluster = 1
            else:
                other_cluster = 0
            local_candidate = self.dst_service.get_cluster_replica(self.src_replica.cluster_id)
            remote_candidate = self.dst_service.get_cluster_replica(other_cluster)
            
            #############################################################################################
            ## NOTE: hardcode. Allow the request route only from the cluster 0(bursty) to the cluster 1(non-bursty)
            if self.src_replica.cluster_id == 0: 
                ''' Fixed portion routing '''
                local_cluster_remainig_capacity = self.dst_service.has_schedulable_replica(self.src_replica.cluster_id)
                remote_cluster_remaining_capacity = self.dst_service.has_schedulable_replica(other_cluster)
                
                if local_cluster_remainig_capacity == False:
                    does_local_has_zero_queue_replica = self.dst_service.has_zero_queue_replica(self.src_replica.cluster_id)
                    if does_local_has_zero_queue_replica:
                        local_cluster_remainig_capacity = True
                
                print_log("WARNING", "local_cluster_remaining_capacity: {}, remote_cluster_remaining_capacity: {}".format(local_cluster_remainig_capacity, remote_cluster_remaining_capacity))
                if local_cluster_remainig_capacity > 0:
                    superset_candidates = local_candidate
                    print_log("WARNING", "(LB) local free, cluster {}({}) local routing {}".format(self.src_replica.cluster_id, self.src_replica.to_str(), self.src_replica.cluster_id))
                elif remote_cluster_remaining_capacity > 0: # Route the request to the remote cluster ONLY when there 
                    print_log("WARNING", "(LB) local overloaded and remote free, remote routing from cluster {}({}) to cluster{}".format(self.src_replica.cluster_id, self.src_replica.to_str(), other_cluster))
                    superset_candidates = remote_candidate
                else:
                    print_log("WARNING", "(LB) both overloaded, local routing from cluster {}({}) to cluster{}".format(self.src_replica.cluster_id, self.src_replica.to_str(), other_cluster))
                    superset_candidates = local_candidate
                
                ## ERROR Handling    
                verifying_superset_candidates = placement.svc_to_repl[self.dst_service]
                for repl in verifying_superset_candidates:
                    if repl not in local_candidate and repl not in remote_candidate:
                        print_log("ERROR", "replica {} is not included neither in local candidate nor in remote candidate.".format(repl.to_str()))
            else:
                # TODO: Strong assumption: Cluster 1 is non-bursty. Hence, it always routes request to the local replica only.
                superset_candidates = local_candidate
                print_log("WARNING", "(LB) cluster 1 local routing only !!!")
            #############################################################################################
            
            dst_replica_candidates = list()
            for repl in superset_candidates:
                if repl.is_dead == False:
                    dst_replica_candidates.append(repl)
                else:
                    print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
        elif ROUTING_ALGORITHM == "moment_response_time":
            # It routes requests to a replica who has the shortest expected response time at the time(moment) a request is decided to be rounted to a child service.
            # MOMENT LATENCY = (Network latency RTT) + (Queue size*processing time) + (Processing time)
            # E.g., 
            #   - local replica, queue size 4: (1*2) + (4*100) + 100 = 512
            #   - remote replica, queue size 3: (40*2) + (3*100) + 100 = 480
            #   In this case, the request will be routed to the remote replica.
            
            superset_candidates = placement.svc_to_repl[self.dst_service] # It returns every replica in the local and remote clusters.
            moment_latency_list = list()
            for i in range(len(superset_candidates)):
                mmt_latency = self.src_replica.calc_moment_latency(superset_candidates[i])
                moment_latency_list.append([mmt_latency, i, superset_candidates[i]])
            moment_latency_list.sort(key=lambda x:x[0])
            for elem in moment_latency_list:
                print_log("WARNING", "moment_latency, from {} to {}, est_latency: {}, idx: {}".format(self.src_replica.to_str(), elem[2].to_str(), elem[0], elem[1]))
            
            sorted_replica_list = list()
            for elem in moment_latency_list:
                sorted_replica_list.append(elem[2])
            superset_candidates = sorted_replica_list
            
            dst_replica_candidates = list()
            for repl in superset_candidates:
                if repl.is_dead == False:
                    dst_replica_candidates.append(repl)
                else:
                    print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
                    
        elif ROUTING_ALGORITHM == "MCLB": # multi-cluster load balancing
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
            #         print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
            ###################################################################
            ## BUG FIXED VERSION
            # It treats all replicas in the local and remote clusters equally.
            # It use configured base load balancer for every replicas in all clusters.
            # In other words, no smart routing.
            superset_candidates = placement.svc_to_repl[self.dst_service]
            
            ## DEBUG PRINT
            print_log("INFO", "(LB) Dst superset:")
            for repl in superset_candidates:
                print_log("INFO", repl.to_str())
            
            dst_replica_candidates = list()
            for repl in superset_candidates:
                if repl.is_dead == False:
                    dst_replica_candidates.append(repl)
                else:
                    print_log("INFO", "(LB), Replica "+repl.to_str()+" was dead. It will be excluded from lb dst candidate.")
        else:
            print_log("ERROR", "Invalid ROUTING_ALGORITHM({}).".format(ROUTING_ALGORITHM))
                        
                         
            # DEBUG PRINT
            # print_log("DEBUG", "self.src_replica: " + self.src_replica.to_str())
            # print_log("DEBUG", "self.dst_service.name: " + self.dst_service.name)
            # print_log("DEBUG", "superset_candidates: ",end="")
            # for repl in superset_candidates:
            #     print_log("DEBUG", repl.to_str(), end=",")
            # print_log("DEBUG", "")

        # DEBUG PRINT
        # print_log("DEBUG", "LB, " + self.src_replica.to_str() + "=> dst_replica_candidates: ", end="")
        # for repl in dst_replica_candidates:
        #     # print_log("DEBUG", repl.to_str())
        #     print_log("DEBUG", repl.to_str(), end=", ")
        # print_log("DEBUG", "")
        
        
        print_log("DEBUG", self.src_replica.to_str())
        for repl in dst_replica_candidates:
            print_log("DEBUG", "\t" + repl.to_str() + " outstanding queue size: " + str(self.src_replica.num_outstanding_response_from_child[repl]))
            
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
        elif self.policy == "EWMA":
            dst_replica = self.ewma(dst_replica_candidates)
        else:
            print_log("ERROR", self.policy + " load balancing policy is not supported.")

        dst_replica.num_pending_request += 1
        dst_replica.processing_queue_size += 1

        print_log("DEBUG", "")
        print_log("INFO", "Execute: LoadBalancing " + self.policy + " (request[" + str(self.request.id) + "]), " + self.src_replica.to_str() + "=>" + dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        
        assert self.src_replica.get_status() == "Active"
        
        if dst_replica.get_status() == "Ready":
            prev_status = dst_replica.get_status()
            dst_replica.set_status("Active")
            print_log("INFO", "(LB) Replica {} changed the status {}->{}".format(dst_replica.to_str(), prev_status, dst_replica.get_status()))
        
        
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
        
        ## Record whether this request comes from the local upstream replica
        if dst_replica.cluster_id == self.src_replica.cluster_id:
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
            # print_log("DEBUG", repl.to_str() + "'s outstanding queue size: " + str(self.src_replica.num_outstanding_response_from_child[repl]))
        return least_request_repl
        

    def ewma(self, replicas):
        for repl in replicas:
            if repl.is_warm == False:
                return self.least_outstanding_request(replicas)
        def get_avg_2(li):
            if len(li) == 0:
                return 0
            return sum(li)/len(li)
        ret = replicas[0]
        avg_0 = get_avg_2(self.src_replica.response_time[repl])
        # outstanding_0 = len(self.src_replica.outstanding_response[replicas[0]])
        outstanding_0 = self.src_replica.num_outstanding_response_from_child[replicas[0]]
        min_wma = avg_0 * outstanding_0
        print_log("DEBUG", "EWMA, "+ self.src_replica.to_str())
        for repl in replicas:
            avg = get_avg_2(self.src_replica.response_time[repl])
            # wma = avg * len(self.src_replica.outstanding_response[repl])
            wma = avg * self.src_replica.num_outstanding_response_from_child[repl]
            print_log("DEBUG", "\t" + repl.to_str() + ": moving avg rt," + str(avg) + ",num_outstanding." + str(self.src_replica.num_outstanding_response_from_child[repl]) + ",wma,"+str(wma))
            if min_wma > wma:
                min_wma = wma
                ret = repl
        return ret        
        
        # 0.632
        # 0.233 = (1 – 0.632) * 0.632
        # def calc_ewma(li):
        #     weight = 0.632
        #     data = list()
        #     for i in range(len(li)):
        #         elem = li[len(li) - i]
        #         if i == 0:
        #             weight = 0.632
        #         else:
        #             weight = (1 - weight)*weight
        #         data.append(elem*weight)

        
    def moving_average(self, replicas):
        def get_avg(li):
            if len(li) == 0:
                return -1
            return sum(li)/len(li)
        
        print_log("DEBUG", self.src_replica.to_str() + " response_time: ")
        for repl in replicas:
            avg = get_avg(self.src_replica.response_time[repl])
            print_log("DEBUG", "\t" + repl.to_str() + ": ", end="")
        print_log("DEBUG", "")
        for repl in replicas:
            if repl.is_warm == False:
                return self.least_outstanding_request(replicas)
        
        least_repl = replicas[0]
        least_avg = get_avg(self.src_replica.response_time[replicas[0]])
        # if least_avg == -1:
        #     return replicas[0]
        for repl in replicas:
            avg = get_avg(self.src_replica.response_time[repl])
            if avg == -1:
                # print_log("ERROR", repl.to_str() + " has no response time.")
                return self.least_outstanding_request(replicas)
            if least_avg > avg:
                least_repl = repl
                least_avg = avg
        print_log("DEBUG", "moving_average(" + str(least_avg) + "): " + self.src_replica.to_str() + "->" + least_repl.to_str())
        for repl in replicas:
            print_log("DEBUG", "\t" + repl.to_str() + ": ", end="")
            for rt in self.src_replica.response_time[repl]:
                print_log("DEBUG", round(rt,2) , end=", ")
            print_log("DEBUG", "")
        return least_repl

        
class NetworkLatency:
    def __init__(self, same_rack, inter_rack, inter_zone, close_inter_region, far_inter_region):
        self.same_rack = same_rack
        self.inter_rack = inter_rack
        self.inter_zone = inter_zone
        self.close_inter_region = close_inter_region
        self.far_inter_region = far_inter_region
        self.std = 5
        
    def calc_latency(self, src_replica_, dst_replica_):
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
        return network_latency.calc_latency(self.src_replica, self.dst_replica)

    def execute_event(self):
        e_latency = self.event_latency()
        assert self.src_replica.num_outstanding_response_from_child[self.dst_replica] >= 0
        self.src_replica.num_outstanding_response_from_child[self.dst_replica] += 1
        if self.request in self.src_replica.sending_time[self.dst_replica]:
            print_log("ERROR", "request[" + str(self.request.id) + "] already exists in " + self.src_replica.to_str() + " sending_time.\nDiamond shape call graph is not supported yet.")
        assert self.request.id not in self.src_replica.sending_time[self.dst_replica]
        self.src_replica.sending_time[self.dst_replica][self.request.id] = self.scheduled_time
        # output_log[self.request.id].append(self.schd_time_)
        print_log("INFO", "Execute: SendRequest(request[" + str(self.request.id) + "]), " + self.src_replica.to_str() + "=>" + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
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
        print_log("INFO", "Execute: RecvRequest(request[" + str(self.request.id) + "]) in " + self.dst_replica.to_str() + "'s " + self.src_replica.service.name + " recv queue" + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        assert self.request not in self.dst_replica.queueing_time
        self.dst_replica.queueing_time[self.request] = self.scheduled_time
        
        ### Record start timestamp for this service
        if self.dst_replica.cluster_id == 0:
            ####################################################################
            ## It will be asserted if there this replica(service) has two parents.
            ## For example, A->C, B->C
            assert self.request not in simulator.cluster0_service_latency[self.dst_replica.service]
            simulator.cluster0_service_latency[self.dst_replica.service][self.request] = self.scheduled_time 
        elif self.dst_replica.cluster_id == 1:
            simulator.cluster1_service_latency[self.dst_replica.service][self.request] = self.scheduled_time

        # if self.dst_replica.get_status() != "Active":
        #     prev_status = self.dst_replica.get_status()
        #     self.dst_replica.set_status("Active")
        #     print_log("INFO", "(RecvRequest) Replica {} changed the status {}->{}".format(self.dst_replica.to_str(), prev_status, self.dst_replica.get_status()))
        
        self.dst_replica.num_recv_request += 1
        if self.dst_replica.is_warm == False:
            if self.dst_replica.num_recv_request >= WARMUP_SIZE:
                self.dst_replica.is_warm = True
                print_log("DEBUG", "WARMED UP! " + self.dst_replica.to_str())
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
        print_log("INFO", "Execute: CheckRequestIsReady in " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
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
        print_log("INFO", "Execute: TryToProcessRequest in " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        if self.dst_replica.is_schedulable() and (len(self.dst_replica.fixed_ready_queue) > 0):
            self.dst_replica.allocate_resource()
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
        sigma = PROCESSING_TIME_SIGMA
        sample = np.random.normal(mu, sigma, 1)
        return sample[0]
        # return self.dst_replica.service.processing_time
        
        
    def execute_event(self):
        e_latency = self.event_latency()
        self.dst_replica.processing_time_history.append(e_latency)
        # target_req, queued_time = self.dst_replica.process_request()
        target_req, src_repl = self.dst_replica.dequeue_from_ready_queue()
        ## BUG: You are supposed to allocate the resource to the request once it decides to execute the request which is TryToProcess.
        ## allocate_resource call was moved to TryToProcess. ## BUG Fixed
        # self.dst_replica.allocate_resource_to_request(target_req) ## BUG
        assert target_req in self.dst_replica.queueing_time
        queueing_start_time = self.dst_replica.queueing_time[target_req]
        queueing_end_time = self.scheduled_time
        self.dst_replica.queueing_time[target_req] = queueing_end_time - queueing_start_time
        print_log("INFO", "Execute: ProcessRequest(request[" + str(target_req.id) + "]), " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)) + ", " + str(round(e_latency, 2)))
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
        print_log("INFO", "Execute: EndProcessRequest(request[" + str(self.request.id) + "]) in " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        
        self.dst_replica.free_resource_from_request(self.request)
        
        if dag.is_leaf(self.dst_replica.service):
            print_log("DEBUG", "request[" + str(self.request.id) + "] reached leaf service " + self.dst_replica.service.name)
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
        return network_latency.calc_latency(self.src_replica, self.dst_replica)
        
    def execute_event(self):
        e_latency = self.event_latency()
        print_log("INFO", "Execute: SendBackRequest(request[" + str(self.request.id) + "]), " + self.src_replica.to_str() + " -> " + self.dst_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        
        if self.src_replica.cluster_id == 0:
            assert self.request in simulator.cluster0_service_latency[self.src_replica.service]
            start_time = simulator.cluster0_service_latency[self.src_replica.service][self.request]
            simulator.cluster0_service_latency[self.src_replica.service][self.request] = self.scheduled_time - start_time
        elif self.src_replica.cluster_id == 1:
            assert self.request in simulator.cluster1_service_latency[self.src_replica.service]
            start_time = simulator.cluster1_service_latency[self.src_replica.service][self.request]
            simulator.cluster1_service_latency[self.src_replica.service][self.request] = self.scheduled_time - start_time
        
        
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
        #     print_log("INFO", "(SendBackRequest) Replica {} changed the status {}->{}".format(self.src_replica.to_str(), prev_status, self.src_replica.get_status()))
            
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
        print_log("INFO", "Execute: RecvSendBack(request[" + str(self.request.id) + "])" + self.dst_replica.to_str() + " from " + self.src_replica.to_str() + " during " + str(int(self.scheduled_time)) + "-" + str(int(self.scheduled_time + e_latency)))
        if self.src_replica not in self.dst_replica.sending_time:
            print_log("ERROR", "(RecvSendBack) {}(child replica) does not exist in {}(parent replica)'s sending_time data structure.".format(self.src_replica.to_str(), self.dst_replica.to_str()))
        sending_st = self.dst_replica.sending_time[self.src_replica][self.request.id]
        del self.dst_replica.sending_time[self.src_replica][self.request.id]
        self.dst_replica.response_time[self.src_replica].append(self.scheduled_time + e_latency - sending_st)
        if len(self.dst_replica.response_time[self.src_replica]) > MA_WINDOW_SIZE:
            self.dst_replica.response_time[self.src_replica].pop(0)
        assert self.src_replica in self.dst_replica.num_outstanding_response_from_child
        self.dst_replica.num_outstanding_response_from_child[self.src_replica] -= 1
        self.src_replica.num_pending_request -= 1 ### BUG fixed
        assert self.src_replica.num_pending_request >= 0
                
        ###########################################################################
        # This is receiver (dst_replica)
        assert self.dst_replica.get_status() == "Active" # Previous status
        # self.dst_replica.update_status("(RecvSendBack {}->{})".format(self.src_replica.to_str(),self.dst_replica.to_str()))
        
        if self.src_replica.get_status() == "Active" and self.src_replica.is_idle():
            print_log("INFO", "RecvSendBack, sender({}) is idle. num_pending_request: {}, total_num_outstanding_response: {}".format(self.src_replica.to_str(), self.src_replica.num_pending_request, self.src_replica.get_total_num_outstanding_response()))
            self.src_replica.set_status("Ready")
        ###########################################################################
        # if self.dst_replica.get_total_num_outstanding_response() == 0 and self.dst_replica.num_pending_request == 0:
        #     assert self.dst_replica.get_status() == "Active" # Previous status
        #     if self.dst_replica.is_user(): # User is always active! 
        #         print_log("DEBUG", "User is always active.")
        #     else: # Non User replica
        #         prev_status = self.dst_replica.get_status()
        #         self.dst_replica.set_status("Ready")
        #         print_log("INFO", "(RecvSendBack) Replica {} changed the status {}->{}".format(self.dst_replica.to_str(), prev_status, self.dst_replica.get_status()))
           
        ### Delayed Kill
        # This is sender (src_replica)
        # Parent replica kills delayed scale down child replica.
        if self.src_replica.is_dead and self.src_replica.get_status() == "Ready":
            if (self.dst_replica.is_user() == False) or (self.dst_replica.is_user() and self.src_replica.cluster_id == self.dst_replica.cluster_id):
                assert self.src_replica.is_user() == False
                print_log("WARNING", "RecvSendBack, Delayed scale down {} by {}!".format(self.src_replica.to_str(), self.dst_replica.to_str()))
                print_log("INFO", "Sender replica(" + self.src_replica.to_str() + ") was dead and it finally becomes Ready.")
                self.src_replica.service.remove_target_replica(self.src_replica, self.src_replica.cluster_id)
                print_log("INFO", "sender replica " + self.src_replica.to_str() + " is now deleted!")
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
        if self.last_processing_replica.cluster_id == 0:
            simulator.user0_latency.append([self.scheduled_time, latency])
        elif self.last_processing_replica.cluster_id == 1:
            simulator.user1_latency.append([self.scheduled_time, latency])
        else:
            print_log("ERROR", "Invalid cluster id {}".format(self.last_processing_replica.cluster_id))
            
        print_log("DEBUG", "Completed: request[" + str(self.request.id) + "], end-to-end latency: " + str(simulator.end_to_end_latency[self.request]))
        # comp = input("Complete the request[" + str(self.request.id) + "]")
        

#######################################################################################
######################### It is the end of event classes ##############################
#######################################################################################


def one_service_application(load_balancer, init_num_repl_c0, init_num_repl_c1):
    user_0 = Service(name_="User0", mcore_per_req_=0, processing_t_=0, lb_=load_balancer)
    user_1 = Service(name_="User1", mcore_per_req_=0, processing_t_=0, lb_=load_balancer)
    svc_a = Service(name_="A", mcore_per_req_=1000, processing_t_=100, lb_=load_balancer)
    dag.add_service(user_0)
    dag.add_service(user_1)
    dag.add_service(svc_a)
    
    service_map = {"A":svc_a}
    
    # DAG
    dag.add_dependency(parent_service=user_0, child_service=svc_a, weight=10)
    dag.add_dependency(parent_service=user_1, child_service=svc_a, weight=10)
    
    
    # Replica
    temp_all_replica = list()
    # Two users represent two clusters(multi-cluster).
    u_0 = Replica(service=user_0, id=0)
    u_1 = Replica(service=user_1, id=1)
    user_0.add_replica(u_0)
    user_1.add_replica(u_1)
        
    temp_all_replica.append(u_0)
    temp_all_replica.append(u_1)
    
    # Four replicas of the service A.
    # num_replica_for_cluster_0 = {"A":8}
    # num_replica_for_cluster_1 = {"A":8}
    print("init_num_repl_c0: ", init_num_repl_c0)
    print("init_num_repl_c1: ", init_num_repl_c1)
    num_replica_for_cluster_0 = {"A":init_num_repl_c0}
    num_replica_for_cluster_1 = {"A":init_num_repl_c1}
    
    # replica_for_cluster_0 = {"A":list(), "B":list(), "C":list(), "D":list(), "E":list(), "F":list()}
    replica_for_cluster_0 = dict()
    for service in service_map:
        replica_for_cluster_0[service] = list()
    # replica_for_cluster_1 = {"A":list(), "B":list(), "C":list(), "D":list(), "E":list(), "F":list()}
    replica_for_cluster_1 = dict()
    for service in service_map:
        replica_for_cluster_1[service] = list()
        
    # Replica for cluster 0
    for service in service_map:
        for i in range(num_replica_for_cluster_0[service]):
            repl = Replica(service=service_map[service], id=i*2)
            service_map[service].add_replica(repl)
            temp_all_replica.append(repl)
            replica_for_cluster_0[service].append(repl) # replica id: 0,2,4,6
    # Replica for cluster 1
    for service in service_map:
        for i in range(num_replica_for_cluster_1[service]):
            repl = Replica(service=service_map[service], id=i*2+1)
            service_map[service].add_replica(repl)
            temp_all_replica.append(repl)
            replica_for_cluster_1[service].append(repl) # replica id: 1,3,5,7
    
    # Placement
    # Cluster 0
    placement.place_replica_to_node_and_allocate_core(u_0, node_0, 0)
    for service in replica_for_cluster_0:
        for repl in replica_for_cluster_0[service]:
            placement.place_replica_to_node_and_allocate_core(repl, node_0, 1000)
    # Cluster 1
    placement.place_replica_to_node_and_allocate_core(u_1, node_1, 0)
    for service in replica_for_cluster_1:
        for repl in replica_for_cluster_1[service]:
            placement.place_replica_to_node_and_allocate_core(repl, node_1, 1000)
    
    # static_lb["User0"] = [0, 2, 4, 6] # dst replica id of cluster 0 frontend service
    # static_lb["User1"] = [1, 3, 5, 7]
    
    dag.print_service_and_replica()
    
    for repl in temp_all_replica:
        dag.register_replica(repl) # Multi-cluster aware registration!
    
    return u_0, u_1, svc_a

def three_depth_application(load_balancer):
    user_0 = Service(name_="User0", mcore_per_req_=0, processing_t_=0, lb_=load_balancer)
    user_1 = Service(name_="User1", mcore_per_req_=0, processing_t_=0, lb_=load_balancer)
    svc_a = Service(name_="A", mcore_per_req_=1000, processing_t_=30, lb_=load_balancer) # H
    svc_b = Service(name_="B", mcore_per_req_=1000, processing_t_=10, lb_=load_balancer) # L
    svc_c = Service(name_="C", mcore_per_req_=1000, processing_t_=25, lb_=load_balancer) # M
    svc_d = Service(name_="D", mcore_per_req_=1000, processing_t_=40, lb_=load_balancer) # H
    svc_e = Service(name_="E", mcore_per_req_=1000, processing_t_=30, lb_=load_balancer) # H
    svc_f = Service(name_="F", mcore_per_req_=1000, processing_t_=10, lb_=load_balancer) # L
    dag.add_service(user_0)
    dag.add_service(user_1)
    dag.add_service(svc_a)
    dag.add_service(svc_b)
    dag.add_service(svc_c)
    dag.add_service(svc_d)
    dag.add_service(svc_e)
    dag.add_service(svc_f)
    service_map = {"A":svc_a, "B":svc_b, "C":svc_c, "D":svc_d, "E":svc_e, "F":svc_f}
    
    # DAG
    dag.add_dependency(parent_service=user_0, child_service=svc_a, weight=10)
    dag.add_dependency(parent_service=user_1, child_service=svc_a, weight=10)
    dag.add_dependency(parent_service=svc_a, child_service=svc_b, weight=10)
    dag.add_dependency(parent_service=svc_a, child_service=svc_c, weight=10)
    dag.add_dependency(parent_service=svc_a, child_service=svc_d, weight=10)
    dag.add_dependency(parent_service=svc_d, child_service=svc_e, weight=10)
    dag.add_dependency(parent_service=svc_d, child_service=svc_f, weight=10)
    
    # Replica
    temp_all_replica = list()
    # Two users represent two clusters(multi-cluster).
    u_0 = Replica(service=user_0, id=0)
    u_1 = Replica(service=user_1, id=1)
    temp_all_replica.append(u_0)
    temp_all_replica.append(u_1)
    
    # TODO: Initial num of replicas is hardcoded.
    # Four replicas of the service A.
    num_replica_for_cluster_0 = {"A":3, "B":1, "C":2, "D":4, "E":3, "F":1}
    num_replica_for_cluster_1 = {"A":3, "B":1, "C":2, "D":4, "E":3, "F":1}
    # replica_for_cluster_0 = {"A":list(), "B":list(), "C":list(), "D":list(), "E":list(), "F":list()}
    replica_for_cluster_0 = dict()
    for service in service_map:
        replica_for_cluster_0[service] = list()
    # replica_for_cluster_1 = {"A":list(), "B":list(), "C":list(), "D":list(), "E":list(), "F":list()}
    replica_for_cluster_1 = dict()
    for service in service_map:
        replica_for_cluster_1[service] = list()
        
    # Replica for cluster 0
    for service in service_map:
        for i in range(num_replica_for_cluster_0[service]):
            repl = Replica(service=service_map[service], id=i*2)
            service_map[service].add_replica(repl)
            temp_all_replica.append(repl)
            replica_for_cluster_0[service].append(repl) # replica id: 0,2,4,6
    # Replica for cluster 1
    for service in service_map:
        for i in range(num_replica_for_cluster_1[service]):
            repl = Replica(service=service_map[service], id=i*2+1)
            service_map[service].add_replica(repl)
            temp_all_replica.append(repl)
            replica_for_cluster_1[service].append(repl) # replica id: 1,3,5,7
    
    # Placement
    # Cluster 0
    placement.place_replica_to_node_and_allocate_core(u_0, node_0, 0)
    for service in replica_for_cluster_0:
        for repl in replica_for_cluster_0[service]:
            placement.place_replica_to_node_and_allocate_core(repl, node_0, 1000)
    # Cluster 1
    placement.place_replica_to_node_and_allocate_core(u_1, node_1, 0)
    for service in replica_for_cluster_1:
        for repl in replica_for_cluster_1[service]:
            placement.place_replica_to_node_and_allocate_core(repl, node_1, 1000)
    # static_lb["User0"] = [0, 2, 4, 6] # dst replica id of cluster 0 frontend service
    # static_lb["User1"] = [1, 3, 5, 7]
    dag.print_service_and_replica()
    for repl in temp_all_replica:
        dag.register_replica(repl)
    return u_0, u_1, svc_a


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
#     print_log("DEBUG", "non_burst_rps: ", non_burst_rps)
#     for i in range(len(moment_rps_)):
#         if moment_rps_[i] == -1:
#             moment_rps_[i] = non_burst_rps
#     print_log("DEBUG", "moment_rps: ", moment_rps_)
#     return moment_rps_


def argparse_add_argument(parser):
    parser.add_argument("--app", type=str, default=None, help="the name of the target app", choices=["one_service", "three_depth"], required=True)
    parser.add_argument("--load_balancer", type=str, default=None, help="load balancing policy", choices=["Random", "RoundRobin", "LeastRequest", "MovingAvg", "EWMA"], required=True)
    parser.add_argument("--routing_algorithm", type=str, default=None, choices=["LCLB", "MCLB", "heuristic_TE", "moment_response_time"], help="routing algorithm when multi-cluster is enabled.", required=True)
    parser.add_argument("--experiment", type=str, default=None, choices=["Microbenchmark", "Alibaba_trace"], help="workload type", required=False)
    parser.add_argument("--request_arrival_file", type=str, default=None, help="path to the cahced request arrival time file", required=False)
    parser.add_argument("--workload", type=str, default=None, help="workload type or msname", required=True)
    parser.add_argument("--base_rps", type=int, default=None, help="base request per second", required=True)
    parser.add_argument("--fixed_autoscaler", type=int, default=None, choices=[0, 1], help="fixed autoscaler", required=True)
    parser.add_argument("--timeshift", type=int, default=0, help="path to the target call rate trace file", required=False)
    parser.add_argument("--response_time_window_size", type=int, default=200, help="window size of response time for moving average and ewma", required=False)
    
    # network_latency = NetworkLatency(0, 0.5, 2, 15, 40)
    parser.add_argument("--same_rack_network_latency", type=float, default=0, help="same_rack_network_latency cluster", required=False)
    parser.add_argument("--inter_rack_network_latency", type=float, default=0.5, help="inter_rack_network_latency cluster", required=False)
    parser.add_argument("--inter_zone_network_latency", type=float, default=2, help="inter_zone_network_latency cluster", required=False)
    parser.add_argument("--close_inter_region_network_latency", type=float, default=15, help="close_inter_region_network_latency cluster", required=False)
    parser.add_argument("--far_inter_region_network_latency", type=float, default=40, help="far_inter_region_network_latency cluster", required=False)
    
    parser.add_argument("--output_dir", type=str, default="log", help="base directory where output log will be written.")
    
    
def print_argument(flags):
    print_log("INFO", "=============================================================")
    print_log("INFO", "======================= argument ============================")
    print_log("INFO", "=============================================================")
    for key, value in vars(flags).items():
        print_log("INFO", "{}: {}".format(key, str(value)))
    print_log("INFO", "=============================================================")


def create_request_arrival_time_from_cached_file(request_arrival_file, flags):
    assert request_arrival_file != None
    file1 = open(request_arrival_file, 'r')
    lines = file1.readlines()
    file1.close()
    request_arrival = list()
    for i in range(len(lines)):
        request_arrival.append(float(lines[i]))
    # print("req_arr_time", request_arrival)
    # print("request_arrival[-1]: ", request_arrival[-1])


    # DEPRECATED
    # def divide_req_arr_into_two_randomly(req_arr):
    #     # req_arr_0 =  set(random.sample(req_arr, len(req_arr)/2))
    #     # req_arr_1 = req_arr - req_arr_0
    #     deepcopied_req_arr = copy.deepcopy(req_arr)
    #     random.shuffle(deepcopied_req_arr)
    #     half = int(len(deepcopied_req_arr)/2)
    #     req_arr_0 = deepcopied_req_arr[:half]
    #     req_arr_1 = deepcopied_req_arr[half:]
    #     req_arr_0.sort()
    #     req_arr_1.sort()
    #     print("len(req_arr): ", len(req_arr))
    #     print("len(req_arr_0): ", len(req_arr_0))
    #     print("len(req_arr_1): ", len(req_arr_1))
    #     return req_arr_0, req_arr_1
    # request_arrivals_0, request_arrivals_1 = divide_req_arr_into_two_randomly(request_arrival)
    
    req_arr_0 = copy.deepcopy(request_arrival)
    req_arr_1 = copy.deepcopy(request_arrival)

    def timeshift(req_arr_0, req_arr_1, hour):
        temp_0 = list()
        temp_1 = list()
        for elem in req_arr_0:
            if elem < hour*1000:
                # req_arr_0.remove(elem)
                a=1
            else:
                temp_0.append(elem)
        last_arr = req_arr_1[-1]
        for elem in req_arr_1:
            if elem > last_arr - hour*1000:
                a=1
                # req_arr_1.remove(elem)
            else:
                temp_1.append(elem)
        # timeshifted_req_arr_0 = [x-req_arr_0[0] for x in req_arr_0]
        # timeshifted_req_arr_1 = [x-req_arr_1[0] for x in req_arr_1]
        timeshifted_req_arr_0 = [x-temp_0[0] for x in temp_0]
        timeshifted_req_arr_1 = [x-temp_1[0] for x in temp_1]
        return timeshifted_req_arr_0, timeshifted_req_arr_1

    if flags.timeshift == 1:
        timeshift_by = 3600
        req_arr_0, req_arr_1 = timeshift(req_arr_0, req_arr_1, timeshift_by)

    def extract_rps_from_request_arrival(li):
        rps_list = list()
        rps = 0
        window = 0
        for i in range(1, len(li)):
            rps += 1
            if li[i] >= 1000*window:
                rps_list.append(rps)                    
                rps = 0
                window += 1
        return rps_list

    def thin_the_request_arrival_list(req_arr, target_rps):
        def calc_norm_factor(request_arr, target_base_rps):
            rps_list = extract_rps_from_request_arrival(request_arr)
            # TODO: Hardcoded. Apparently, 2 percentile is not p50.
            p50_rps = np.percentile(rps_list, 2)
            norm_weight = p50_rps/target_base_rps
            print("target_base_rps: ", target_base_rps)
            print("p50_rps: ", p50_rps)
            print("norm_weight: ", norm_weight)
            return norm_weight
        
        thin_req_arr = list()
        norm_factor = calc_norm_factor(req_arr, target_rps)
        for i in range(len(req_arr)):
            if i%norm_factor==0:
                thin_req_arr.append(req_arr[i])
        return thin_req_arr

    def generate_non_bursty_cluster_workload(duration_in_sec, target_rps):
        wrk = wrk_g.WorkloadGenerator(req_per_sec=target_rps, total_sec=duration_in_sec)
        print("wrk")
        ts = time.time()
        non_bursty_req_interval = wrk.exponential_distribution()
        print("exp function overhead: ", time.time() - ts)
        req_arr = wrk_g.interval_to_arrival(non_bursty_req_interval)
        return req_arr

    # TODO: Hardcoded.
    target_rps = 8
    print(len(req_arr_0))
    thin_request_arrival_0 = thin_the_request_arrival_list(req_arr_0, target_rps)
    bursty_cluster_rps_list = extract_rps_from_request_arrival(thin_request_arrival_0)
    p50_rps_of_bursty_cluster = np.percentile(bursty_cluster_rps_list, 50)
    total_seconds = len(bursty_cluster_rps_list)
    print("p50_rps_of_bursty_cluster: ", p50_rps_of_bursty_cluster)
    print("target_rps: ", target_rps)
    print("total_seconds: ", total_seconds)
    if flags.timeshift == 1:
        thin_request_arrival_1 = thin_the_request_arrival_list(req_arr_1, target_rps)
    else:
        thin_request_arrival_1 = generate_non_bursty_cluster_workload(total_seconds, p50_rps_of_bursty_cluster)
    request_arrivals_0 = thin_request_arrival_0
    request_arrivals_1 = thin_request_arrival_1
    # rps_0 = extract_rps_from_request_arrival(request_arrivals_0)
    # rps_1 = extract_rps_from_request_arrival(request_arrivals_1)
    def calc_initial_one_service_num_replica(rps_list, factor):
        first_five_minute_rps = rps_list[:1]
        avg_rps = sum(first_five_minute_rps)/len(first_five_minute_rps)
        per_replica_capacity = 10 # 100ms processing time
        initial_num_replica = int((avg_rps/per_replica_capacity) * factor)
        return initial_num_replica
    num_replica_factor = 2
    init_num_repl_cluster_0 = calc_initial_one_service_num_replica(request_arrivals_0, num_replica_factor)
    init_num_repl_cluster_1 = calc_initial_one_service_num_replica(request_arrivals_1, num_replica_factor)
    print("init_num_repl_cluster_0: ", init_num_repl_cluster_0)
    print("init_num_repl_cluster_1: ", init_num_repl_cluster_1)
    return request_arrivals_0, request_arrivals_1, init_num_repl_cluster_0, init_num_repl_cluster_1
    
    
if __name__ == "__main__":
    program_start_time = time.time()
    cur_time = strftime("%Y%m%d_%H%M%S", gmtime())
    random.seed(1234) # To reproduce the random load balancing
    
    parser = argparse.ArgumentParser()
    argparse_add_argument(parser)
    flags, unparsed = parser.parse_known_args()
                
    ''' multi-cluster on/off '''
    MA_WINDOW_SIZE = flags.response_time_window_size
    
    ''' multi-cluster routing algorithm '''
    FIXED_AUTOSCALER = flags.fixed_autoscaler
    ROUTING_ALGORITHM = flags.routing_algorithm
    
    ''' Configure network latency '''
    same_rack = flags.same_rack_network_latency
    inter_rack = flags.inter_rack_network_latency
    inter_zone = flags.inter_zone_network_latency
    close_inter_region = flags.close_inter_region_network_latency
    far_inter_region = flags.far_inter_region_network_latency
    network_latency = NetworkLatency(same_rack, inter_rack, inter_zone, close_inter_region, far_inter_region)
    # network_latency = NetworkLatency(0, 0.5, 2, 15, 40)
    
    ''' Check configuration '''
    def check_flags(flags):
        if flags.experiment == "Alibaba_trace":
            assert flags.request_arrival_file != None
            if flags.request_arrival_file.find(flags.workload) == -1:
                print_log("ERROR", "request_arrival_file and workload(msname) are not consistent. ({} // {})".format(flags.request_arrival_file, flags.workload))
    check_flags(flags)
    
    ''' Worklaod distribution '''
    ## Microbenchmark (synthesized workload distribution)
    if flags.experiment == "Microbenchmark":
        workload_0 = flags.workload
        if workload_0 == "exp_burst_8x":
            workload_1 = "exp_normal_8x"
        elif workload_0 == "exp_burst_steep_8x":
            workload_1 = "exp_normal_steep_8x"
        elif workload_0 == "exp_burst_4x":
            workload_1 = "exp_normal_4x"
        elif workload_0 == "constant_burst_8x":
            workload_1 = "constant_normal_8x"
        elif workload_0 == "single_cluster_test":
            workload_1 = "empty"
        request_arrivals_0 = wrk_g.generate_workload(workload_0, flags.base_rps)
        request_arrivals_1 = wrk_g.generate_workload(workload_1, flags.base_rps)
    
    ## Read existing request arrival file (real trace)
    elif flags.experiment == "Alibaba_trace":
        request_arrivals_0, request_arrivals_1, init_num_repl_c0, init_num_repl_c1 = create_request_arrival_time_from_cached_file(flags.request_arrival_file, flags)
    
    ''' Application '''
    if flags.app == "one_service":
        if flags.experiment == "Alibaba_trace":
            user_group_0, user_group_1, svc_a = one_service_application(flags.load_balancer, init_num_repl_c0, init_num_repl_c1)
        else:
            user_group_0, user_group_1, svc_a = one_service_application(flags.load_balancer, 2, 2)
    elif flags.app == "three_depth":
        user_group_0, user_group_1, svc_a = three_depth_application(flags.load_balancer)
    else:
        print_log("ERROR", "Application scenario " + flags.app + " is not supported.")
    
    ''' Replica level DAG '''
    def create_replica_dependency():
        for repl in dag.all_replica:
            if dag.is_leaf(repl.service) is False: ###
                for child_svc in repl.child_services:
                    child_repl = dag.get_child_replica(repl, child_svc)
                    for c_repl in child_repl:
                        # repl.register_child_replica(c_repl, child_svc)
                        repl.register_child_replica_2(c_repl)
    create_replica_dependency()
    
    ## DEBUG PRINT 
    for repl in dag.all_replica:
        print_log("DEBUG", repl.to_str() + " child_replica: ", end="")
        if dag.is_leaf(repl.service):
            print_log("DEBUG", "None", end="")
        else:
            for svc in repl.child_services:
                print_log("DEBUG", svc.name + ": ", end="")
                for child_repl in repl.child_replica[svc]:
                    print_log("DEBUG", child_repl.to_str(), end=", ")
        print_log("DEBUG", "")
            
        
    dag.print_dependency()
    dag.plot_graphviz()
    placement.print()
    # ym1 = plot_workload_histogram_2(request_arrivals_0, "Cluster 0 - Workload distribution", 0)
    # ym2 = plot_workload_histogram_2(request_arrivals_1, "Cluster 1 - Workload distribution", ym1)
    simulator = Simulator(request_arrivals_0, request_arrivals_1, flags.app, cur_time, request_arrivals_0, request_arrivals_1, flags)
    req_id = 0
    origin_cluster_0 = 0
    for req_arr in request_arrivals_0:
        request = Request(req_id, origin_cluster_0)
        simulator.request_start_time[request] = req_arr
        # service A is frontend service
        simulator.schedule_event(LoadBalancing(req_arr, request, user_group_0, svc_a)) 
        req_id += 1
    origin_cluster_1 = 1
    for req_arr in request_arrivals_1:
            request = Request(req_id, origin_cluster_1)
            simulator.request_start_time[request] = req_arr
            simulator.schedule_event(LoadBalancing(req_arr, request, user_group_1, svc_a))
            req_id += 1
            
    # Schedule autoscaling check event every 30 sec
    max_arrival_time = max(request_arrivals_0[-1], request_arrivals_1[-1])
    num_autoscale_check = int(max_arrival_time/AUTOSCALER_INTRERVAL)
    for i in range(num_autoscale_check):
        if i > 5:
            simulator.schedule_event(AutoscalerCheck(AUTOSCALER_INTRERVAL*i))
            
    # Schedule UpdateRPS every RPS_UPDATE_INTERVAL sec
    num_rps_update = int(max_arrival_time/RPS_UPDATE_INTERVAL)
    for i in range(num_rps_update):
        simulator.schedule_event(UpdateRPS(RPS_UPDATE_INTERVAL*i))

    simulator.start_simulation(program_start_time)
