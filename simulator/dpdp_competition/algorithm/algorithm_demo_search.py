# Copyright (C) 2021. Huawei Technologies Co., Ltd. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE

import copy
import numpy as np
from src.common.node import Node
from src.common.route import Map
from src.conf.configs import Configs
from src.utils.input_utils import get_factory_info, get_route_map
from src.utils.json_tools import convert_nodes_to_json
from src.utils.json_tools import get_vehicle_instance_dict, get_order_item_dict
from src.utils.json_tools import read_json_from_file, write_json_to_file
from src.utils.logging_engine import logger
from algorithm.wolrd import World
from collections import defaultdict
import signal


ids = np.load("algorithm/factory2id.npy")
paths = np.load("algorithm/shortest_path.npy")
factory2id = {}
for i in range(len(ids)):
    factory2id[ids[i]] = i
world = World("benchmark/route_info.csv", "benchmark/factory_info.csv")

# Not working since for now the simulator do not support redesign route.
def get_shortest_path(start, end):
    path = paths[factory2id[start]][factory2id[end]]
    shortest_path = []
    distance = 0
    time = 0
    i = 0
    while ids[int(path[i])] not in shortest_path:
        if int(path[i]) == 0 and ids[int(path[i])] != end and int(path[i + 1]) == 0:
            break
        shortest_path.append(ids[int(path[i])])
        if len(shortest_path) == 1:
            i += 1
            continue
        distance += world.id2factory[shortest_path[-1]
                                     ].origins[shortest_path[-2]].distance
        time += world.id2factory[shortest_path[-1]
                                 ].origins[shortest_path[-2]].time
        i += 1
    return shortest_path, distance, time


# naive dispatching method
def dispatch_orders_to_vehicles(id_to_unallocated_order_item: dict, id_to_vehicle: dict, id_to_factory: dict):
    """
    :param id_to_unallocated_order_item: item_id ——> OrderItem object(state: "GENERATED")
    :param id_to_vehicle: vehicle_id ——> Vehicle object
    :param id_to_factory: factory_id ——> factory object
    """
    vehicle_id_to_destination = {}
    vehicle_id_to_planned_route = {}

    # dealing with the carrying items of vehicles (处理车辆身上已经装载的货物)
    for vehicle_id, vehicle in id_to_vehicle.items():
        unloading_sequence_of_items = vehicle.get_unloading_sequence()
        vehicle_id_to_planned_route[vehicle_id] = []
        if len(unloading_sequence_of_items) > 0:
            delivery_item_list = []
            factory_id = unloading_sequence_of_items[0].delivery_factory_id
            for item in unloading_sequence_of_items:
                if item.delivery_factory_id == factory_id:
                    delivery_item_list.append(item)
                else:
                    factory = id_to_factory.get(factory_id)
                    node = Node(factory_id, factory.lng, factory.lat,
                                [], copy.copy(delivery_item_list))
                    vehicle_id_to_planned_route[vehicle_id].append(
                        node)     # 优化参数
                    delivery_item_list = [item]
                    factory_id = item.delivery_factory_id
            if len(delivery_item_list) > 0:
                factory = id_to_factory.get(factory_id)
                node = Node(factory_id, factory.lng, factory.lat,
                            [], copy.copy(delivery_item_list))
                vehicle_id_to_planned_route[vehicle_id].append(node)

    # for the empty vehicle, it has been allocated to the order, but have not yet arrived at the pickup factory
    pre_matching_item_ids = []
    for vehicle_id, vehicle in id_to_vehicle.items():
        if vehicle.carrying_items.is_empty() and vehicle.destination is not None:
            pickup_items = vehicle.destination.pickup_items
            nodelist = __create_pickup_and_delivery_nodes_of_items(
                pickup_items, id_to_factory)
            for node in nodelist:
                vehicle_id_to_planned_route[vehicle.id].append(node)
            pre_matching_item_ids.extend([item.id for item in pickup_items])

    # get capacity of vehicles
    capacity = __get_capacity_of_vehicle(id_to_vehicle)

    # get vehicles
    vehicles = [vehicle for vehicle in id_to_vehicle.values()]

    vehicle_id_to_planned_route = greedy_dispatch(id_to_unallocated_order_item, pre_matching_item_ids, capacity, id_to_factory, vehicles, vehicle_id_to_planned_route)


    # Running Dispatch Algorithms
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(120)   # 2min
    try:
        # search top 3
        vehicle_id_to_planned_route = search_dispatch_topn_vehicles(id_to_unallocated_order_item, pre_matching_item_ids, capacity, id_to_factory, vehicles, vehicle_id_to_planned_route)
        print("Search Topn Dispatch Finished")
    except Exception as e:
        print(e)
        vehicle_id_to_planned_route = greedy_dispatch(id_to_unallocated_order_item, pre_matching_item_ids, capacity, id_to_factory, vehicles, vehicle_id_to_planned_route)
        print("Greedy Dispatch Finished")


    # create the output of the algorithm
    for vehicle_id, vehicle in id_to_vehicle.items():
        origin_planned_route = vehicle_id_to_planned_route.get(vehicle_id)

        destination = None
        planned_route = []
        # determine the destination
        if vehicle.destination is not None:
            if len(origin_planned_route) == 0:
                logger.error(f"Planned route of vehicle {vehicle_id} is wrong")
            else:
                destination = origin_planned_route[0]
                destination.arrive_time = vehicle.destination.arrive_time
                planned_route = [origin_planned_route[i]
                                 for i in range(1, len(origin_planned_route))]
        elif len(origin_planned_route) > 0:
            destination = origin_planned_route[0]
            planned_route = [origin_planned_route[i]
                             for i in range(1, len(origin_planned_route))]

        vehicle_id_to_destination[vehicle_id] = destination
        vehicle_id_to_planned_route[vehicle_id] = planned_route

    return vehicle_id_to_destination, vehicle_id_to_planned_route


# 搜索算法入口 (Top N Vehicle)
def search_dispatch_topn_vehicles(id_to_unallocated_order_item, pre_matching_item_ids, capacity, id_to_factory, vehicles, vehicle_id_to_planned_route):
    
    # order id to items divided by demand(capacity=15)
    # Reorgnize and Divide orders by demand of capacity, an order demand > capacity would be divided to several orders, eg: an order of demand 33 woulb be devided into sub-orders[15, 15, 3]
    order_id_to_demands = defaultdict(float)
    order_id_to_items_divided = defaultdict(list)
    for item_id, item in id_to_unallocated_order_item.items():
        if item_id in pre_matching_item_ids:
            continue
        order_id = item.order_id
        if order_id_to_demands[order_id] % capacity + item.demand < capacity:
            new_order_id = order_id + "-" + str(order_id_to_demands[order_id] // capacity)
        else:
            new_order_id = order_id + "-" + str(order_id_to_demands[order_id] // capacity + 1)
        order_id_to_demands[order_id] += item.demand
        order_id_to_items_divided[new_order_id].append(item)


    # 搜索最优分类
    dispatch = __dispatch_orders_to_vehicles(order_id_to_items_divided, vehicles)
    
    # create pickup and delivery node for all orders
    order_ids_to_pickup_delivery_nodes = defaultdict(list)
    for order_id, order_items in order_id_to_items_divided.items():
        pickup_node, delivery_node = __create_pickup_and_delivery_nodes_of_items(
            order_items, id_to_factory)
        order_ids_to_pickup_delivery_nodes[order_id] = [
            pickup_node, delivery_node]

    # 生成车辆路径
    # Todo: 合并同一工厂出发的订单
    for vehicle_id, order_ids in dispatch.items():
        for order_id in order_ids:
            vehicle_id_to_planned_route[vehicle_id].extend(order_ids_to_pickup_delivery_nodes[order_id])

    return vehicle_id_to_planned_route


# 搜索最优分配
def __dispatch_orders_to_vehicles(orders, vehicles):
    # order_ids_to_demands = __calculate_order_ids_to_demands(orders)
    order_ids_to_vehicles_choice_list = __select_top_n_vehicles_for_orders(vehicles, orders)
    # vehicle_capacity_ocupied = defaultdict(float)

    # init
    cur_dispatch = defaultdict(list)
    min_cost = float("inf")
    min_cost_dispatch = {}
    cnt = 0


    def backtrack(cur_dispatch, order_index):
        if order_index == len(orders):
            
            # evaluate
            cur_dispatch_cost = __evaluator_simplified(cur_dispatch, orders)
            nonlocal min_cost, min_cost_dispatch
            if min_cost > cur_dispatch_cost:
                min_cost = cur_dispatch_cost
                min_cost_dispatch = copy.deepcopy(cur_dispatch)

            return

        current_order_vehicle_list = order_ids_to_vehicles_choice_list[list(orders.keys())[order_index]]
        for v in current_order_vehicle_list:
            # if vehicle_capacity_ocupied[v.id] + order_ids_to_demands[list(orders.keys())[order_index]] < CAPACITY:
            cur_dispatch[v.id].append(list(orders.keys())[order_index])
            # vehicle_capacity_ocupied[v.id] += order_ids_to_demands[list(orders.keys())[order_index]]

            # print("order_index " + str(order_index))
            backtrack(cur_dispatch, order_index+1)

            cur_dispatch[v.id].pop()
            # vehicle_capacity_ocupied[v.id] -= order_ids_to_demands[list(orders.keys())[order_index]]
        
        nonlocal cnt
        cnt += 1
        print(cnt)

    backtrack(cur_dispatch, 0)

    print("Search Finished!")

    return min_cost_dispatch

# 评估完整dispatch的cost
def __evaluator_simplified(cur_dispatch, orders):

    total_cost = 0
    for v_id in cur_dispatch.keys():          # planned_route
        nodes = []
        for order_id in cur_dispatch[v_id]:
            nodes.append(orders[order_id][0].delivery_factory_id)
            nodes.append(orders[order_id][0].pickup_factory_id)

        pre_node = None
        for node in nodes:
            if not pre_node:
                pre_node = node
                continue

            # .distance,.time
            total_cost += world.id2factory[pre_node].destinations[node].time
            pre_node = node

    return total_cost


# 为每个订单选择TopN的vehicle，n为3时，返回车辆的数量 >= 3
def __select_top_n_vehicles_for_orders(vehicles, orders):
    # N = 3
    orders_id_to_top_n_vehicles = defaultdict(list)

    # 定位所有 vehicle 的位置
    factory_id_to_vehicles = defaultdict(list)
    for v in vehicles:
        v_loc = v.destination.id if v.destination else v.cur_factory_id
        factory_id_to_vehicles[v_loc].append(v)

    for order_id, items in orders.items():
        order_loc = items[0].pickup_factory_id

        # 按距离order所在工厂的距离排序，这部分数据存起来更好
        factory_id_list_by_distance = sorted(world.id2factory[order_loc].destinations, key=lambda x: world.id2factory[order_loc].destinations[x].time)
        for f_id in factory_id_list_by_distance:
            if factory_id_to_vehicles[f_id]:
                orders_id_to_top_n_vehicles[order_id].append(factory_id_to_vehicles[f_id][0])
                # orders_id_to_top_n_vehicles[order_id].extend(factory_id_to_vehicles[f_id])
            
            # N = 2
            if len(orders_id_to_top_n_vehicles[order_id]) >= 1:
                break

    return orders_id_to_top_n_vehicles

# retired
def __calculate_order_ids_to_demands(orders):
    order_ids_to_demands = {}
    for order_id, items in orders.items():
        order_ids_to_demands[order_id]=__calculate_demand(items)
    return order_ids_to_demands



def greedy_dispatch(id_to_unallocated_order_item, pre_matching_item_ids, capacity, id_to_factory, vehicles, vehicle_id_to_planned_route):
        # order id to items
    order_id_to_items = {}
    for item_id, item in id_to_unallocated_order_item.items():
        if item_id in pre_matching_item_ids:
            continue
        order_id = item.order_id
        if order_id not in order_id_to_items:
            order_id_to_items[order_id] = []
        order_id_to_items[order_id].append(item)


    for order_id, items in order_id_to_items.items():
        demand = __calculate_demand(items)
        if demand > capacity:
            cur_demand = 0
            tmp_items = []
            for item in items:                             
                if cur_demand + item.demand > capacity:
                    nodelist = __create_pickup_and_delivery_nodes_of_items(
                        tmp_items, id_to_factory)
                    if len(nodelist) < 2:
                        continue
                    vehicle = select_vehicle_for_orders(vehicles, tmp_items)
                    for node in nodelist:
                        vehicle_id_to_planned_route[vehicle.id].append(node)
                    tmp_items = []
                    cur_demand = 0

                tmp_items.append(item)
                cur_demand += item.demand

            if len(tmp_items) > 0:
                nodelist = __create_pickup_and_delivery_nodes_of_items(
                    tmp_items, id_to_factory)
                if len(nodelist) < 2:
                    continue
                vehicle = select_vehicle_for_orders(vehicles, tmp_items)
                for node in nodelist:
                    vehicle_id_to_planned_route[vehicle.id].append(node)
        else:
            nodelist = __create_pickup_and_delivery_nodes_of_items(
                items, id_to_factory)
            if len(nodelist) < 2:
                continue
            vehicle = select_vehicle_for_orders(vehicles, items)
            for node in nodelist:
                vehicle_id_to_planned_route[vehicle.id].append(node)
    
    return vehicle_id_to_planned_route
    

def select_vehicle_for_orders(vehicles, items):
    min_time = 2147483648
    vehicle = None
    for v in vehicles:
        v_loc = ""
        if len(v.carrying_items.items) == 0:               # 没携带物品的车辆
            v_loc = v.cur_factory_id                       # 携带了物品的，v_loc为空，没携带物品v_loc为所在工厂
        if v_loc == "":   # 非空闲车辆，携带了物品的，或者当前要去拿物品的
            if not v.destination:                          # ？
                v_loc = items[0].pickup_factory_id
            else:                                          # v_loc为目的地
                v_loc = v.destination.id
        v_loc = v.cur_factory_id

        # 目的地为item的pickup_factory
        destination = items[0].pickup_factory_id
        tim = world.id2factory[v_loc].destinations[destination].time    #
        if len(v.carrying_items.items) == 0:
            tim -= 100000
        if tim < min_time:
            min_time = tim
            vehicle = v
    return vehicle

def __calculate_demand(item_list: list):
    demand = 0
    for item in item_list:
        demand += item.demand
    return demand


def __get_capacity_of_vehicle(id_to_vehicle: dict):
    for vehicle_id, vehicle in id_to_vehicle.items():
        return vehicle.board_capacity


def __create_pickup_and_delivery_nodes_of_items(items: list, id_to_factory: dict):
    pickup_factory_id = __get_pickup_factory_id(items)
    delivery_factory_id = __get_delivery_factory_id(items)
    if len(pickup_factory_id) == 0 or len(delivery_factory_id) == 0:
        return None, None

    pickup_factory = id_to_factory.get(pickup_factory_id)
    delivery_factory = id_to_factory.get(delivery_factory_id)
    pickup_node = Node(pickup_factory.id, pickup_factory.lng,
                       pickup_factory.lat, copy.copy(items), [])
    nodelist = [pickup_node]

    delivery_items = []
    last_index = len(items) - 1
    for i in range(len(items)):
        delivery_items.append(items[last_index - i])
    delivery_node = Node(delivery_factory.id, delivery_factory.lng,
                         delivery_factory.lat, [], copy.copy(delivery_items))
    nodelist.append(delivery_node)
    return nodelist


def __get_pickup_factory_id(items):
    if len(items) == 0:
        logger.error("Length of items is 0")
        return ""

    factory_id = items[0].pickup_factory_id
    for item in items:
        if item.pickup_factory_id != factory_id:
            logger.error("The pickup factory of these items is not the same")
            return ""

    return factory_id


def __get_delivery_factory_id(items):
    if len(items) == 0:
        logger.error("Length of items is 0")
        return ""

    factory_id = items[0].delivery_factory_id
    for item in items:
        if item.delivery_factory_id != factory_id:
            logger.error("The delivery factory of these items is not the same")
            return ""

    return factory_id


def signal_handler(signum, frame):
    raise Exception("Timed out!")

"""

Main body
# Note
# This is the demo to show the main flowchart of the algorithm

"""


def scheduling():
    # read the input json, you can design your own classes
    id_to_factory, id_to_unallocated_order_item, id_to_ongoing_order_item, id_to_vehicle = __read_input_json()

    # dispatching algorithm
    vehicle_id_to_destination, vehicle_id_to_planned_route = dispatch_orders_to_vehicles(
        id_to_unallocated_order_item,
        id_to_vehicle,
        id_to_factory)

    # output the dispatch result
    __output_json(vehicle_id_to_destination, vehicle_id_to_planned_route)


def __read_input_json():
    # read the factory info
    id_to_factory = get_factory_info(Configs.factory_info_file_path)

    # read the input json, you can design your own classes
    unallocated_order_items = read_json_from_file(
        Configs.algorithm_unallocated_order_items_input_path)
    id_to_unallocated_order_item = get_order_item_dict(
        unallocated_order_items, 'OrderItem')

    ongoing_order_items = read_json_from_file(
        Configs.algorithm_ongoing_order_items_input_path)
    id_to_ongoing_order_item = get_order_item_dict(
        ongoing_order_items, 'OrderItem')

    id_to_order_item = {**id_to_unallocated_order_item,
                        **id_to_ongoing_order_item}

    vehicle_infos = read_json_from_file(
        Configs.algorithm_vehicle_input_info_path)
    id_to_vehicle = get_vehicle_instance_dict(
        vehicle_infos, id_to_order_item, id_to_factory)

    return id_to_factory, id_to_unallocated_order_item, id_to_ongoing_order_item, id_to_vehicle


def __output_json(vehicle_id_to_destination, vehicle_id_to_planned_route):
    write_json_to_file(Configs.algorithm_output_destination_path,
                       convert_nodes_to_json(vehicle_id_to_destination))
    write_json_to_file(Configs.algorithm_output_planned_route_path,
                       convert_nodes_to_json(vehicle_id_to_planned_route))
