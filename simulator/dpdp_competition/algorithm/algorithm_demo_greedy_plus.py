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

ids = np.load("algorithm/factory2id.npy")
paths = np.load("algorithm/shortest_path.npy")
factory2id = {}
for i in range(len(ids)):
    factory2id[ids[i]] = i
world = World("algorithm/route_info.csv", "algorithm/factory_info.csv")


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
        distance += world.id2factory[shortest_path[-1]].origins[shortest_path[-2]].distance
        time += world.id2factory[shortest_path[-1]].origins[shortest_path[-2]].time
        i += 1
    return shortest_path, distance, time


def optimize_route(route):
    r = []
    for node in route:
        if len(r) == 0:
            r.append(copy.deepcopy(node))
        else:
            if node.id == r[-1].id:
                # if len(node.pickup_items) == 0 and len(r[-1].pickup_items) == 0:
                #     for item in node.delivery_items:
                #         r[-1].delivery_items.append(item)
                # elif len(node.delivery_items) == 0 and len(r[-1].delivery_items) == 0:
                #     for item in node.pickup_items:
                #         r[-1].pickup_items.append(item)
                # elif len(node.delivery_items) == 0:
                for item in node.pickup_items:
                    r[-1].pickup_items.append(item)
                for item in node.delivery_items:
                    r[-1].delivery_items.append(item)
                # else:
                #     r.append(copy.deepcopy(node))
            else:
                r.append(copy.deepcopy(node))
    return r


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
                    # if not vehicle_id_to_planned_route[vehicle_id]:
                    #     path = get_shortest_path(vehicle.destination.id, factory_id)[0]
                    #     for i in range(len(path) - 1):
                    #         node = Node(path[i], world.id2factory[path[i]].lon, world.id2factory[path[i]].lat, [], [])
                    #         vehicle_id_to_planned_route[vehicle_id].append(node)
                    node = Node(factory_id, factory.lng, factory.lat, [], copy.copy(delivery_item_list))
                    vehicle_id_to_planned_route[vehicle_id].append(node)
                    delivery_item_list = [item]
                    factory_id = item.delivery_factory_id
            if len(delivery_item_list) > 0:
                factory = id_to_factory.get(factory_id)
                # if not vehicle_id_to_planned_route[vehicle_id]:
                #     path = get_shortest_path(vehicle.destination.id, factory_id)[0]
                #     for i in range(len(path) - 1):
                #         node = Node(path[i], world.id2factory[path[i]].lon, world.id2factory[path[i]].lat, [], [])
                #         vehicle_id_to_planned_route[vehicle_id].append(node)
                node = Node(factory_id, factory.lng, factory.lat, [], copy.copy(delivery_item_list))
                vehicle_id_to_planned_route[vehicle_id].append(node)

    # for the empty vehicle, it has been allocated to the order, but have not yet arrived at the pickup factory
    pre_matching_item_ids = []
    for vehicle_id, vehicle in id_to_vehicle.items():
        if (vehicle.carrying_items.is_empty() or len(
                vehicle.destination.pickup_items) > 0) and vehicle.destination is not None:
            pickup_items = vehicle.destination.pickup_items
            # pickup_node, delivery_node = __create_pickup_and_delivery_nodes_of_items(pickup_items, id_to_factory)
            # vehicle_id_to_planned_route[vehicle_id].append(pickup_node)
            # vehicle_id_to_planned_route[vehicle_id].append(delivery_node)
            nodelist = __create_pickup_and_delivery_nodes_of_items(pickup_items, id_to_factory)
            pickup_node = nodelist[0]
            if vehicle.carrying_items.is_empty():
                vehicle_id_to_planned_route[vehicle.id].append(pickup_node)
                for node in nodelist[1:]:
                    vehicle_id_to_planned_route[vehicle.id].append(node)
            else:
                c = -1
                d_list = vehicle_id_to_planned_route[vehicle_id]
                for j in range(len(d_list)):
                    if d_list[j].id == pickup_node.id and len(d_list[j].delivery_items) > 0 and len(
                            vehicle.destination.delivery_items) > 0 and d_list[j].delivery_items[0].id == \
                            vehicle.destination.delivery_items[0].id:
                        c = j
                        break
                # vehicle_id_to_planned_route[vehicle.id].insert(c + 1, pickup_node)
                for i in range(len(nodelist)):
                    # if i == 0:
                    #     continue
                    vehicle_id_to_planned_route[vehicle.id].insert(c + 1 + i, nodelist[i])
            pre_matching_item_ids.extend([item.id for item in pickup_items])

    for vehicle_id, _ in id_to_vehicle.items():
        vehicle_id_to_planned_route[vehicle_id] = optimize_route(vehicle_id_to_planned_route[vehicle_id])
    # dispatch unallocated orders to vehicles
    capacity = __get_capacity_of_vehicle(id_to_vehicle)

    order_id_to_items = {}
    for item_id, item in id_to_unallocated_order_item.items():
        if item_id in pre_matching_item_ids:
            continue
        order_id = item.order_id
        if order_id not in order_id_to_items:
            order_id_to_items[order_id] = []
        order_id_to_items[order_id].append(item)

    # vehicle_index = 0
    upcoming_items = []
    vehicles = [vehicle for vehicle in id_to_vehicle.values()]
    for order_id, items in order_id_to_items.items():
        demand = __calculate_demand(items)
        if demand > capacity:
            cur_demand = 0
            tmp_items = []
            for item in items:
                if cur_demand + item.demand > capacity:
                    nodelist = __create_pickup_and_delivery_nodes_of_items(tmp_items, id_to_factory)
                    if len(nodelist) < 2:
                        continue
                    pickup_node = nodelist[0]
                    delivery_node = nodelist[1]
                    if len(pickup_node.pickup_items) > 0:
                        upcoming_items.append([tmp_items, pickup_node, delivery_node])
                    tmp_items = []
                    cur_demand = 0

                tmp_items.append(item)
                cur_demand += item.demand

            if len(tmp_items) > 0:
                nodelist = __create_pickup_and_delivery_nodes_of_items(tmp_items, id_to_factory)
                if len(nodelist) < 2:
                    continue
                pickup_node = nodelist[0]
                delivery_node = nodelist[1]
                if len(pickup_node.pickup_items) > 0:
                    upcoming_items.append([tmp_items, pickup_node, delivery_node])
        else:
            nodelist = __create_pickup_and_delivery_nodes_of_items(items, id_to_factory)
            if len(nodelist) < 2:
                continue

            pickup_node = nodelist[0]
            delivery_node = nodelist[1]
            if len(pickup_node.pickup_items) > 0:
                upcoming_items.append([items, pickup_node, delivery_node])
    N = 3
    max_Orders = 300
    best_planned = None
    best_delay = 2147483648000
    best_redundancy = 0
    for _ in range(N):
        pre_planned = copy.deepcopy(vehicle_id_to_planned_route)
        perm = np.random.permutation(len(upcoming_items))
        cost_list = {}
        check_list = {}
        # length = min(len(upcoming_items), max_Orders)
        length = len(upcoming_items)
        for i in range(length):
            # print(i)
            # item = upcoming_items[i][0]
            # pickup_node = upcoming_items[i][1]
            # delivery_node = upcoming_items[i][2]
            item = upcoming_items[perm[i]][0]
            pickup_node = upcoming_items[perm[i]][1]
            delivery_node = upcoming_items[perm[i]][2]
            best_vehicle, best_planned_route, best_cost = select_vehicle_for_orders(vehicles, item, pre_planned, pickup_node,
                                                                              delivery_node, cost_list)
            pre_planned[best_vehicle.id] = best_planned_route
            cost_list[best_vehicle.id] = best_cost
            check_list[best_vehicle.id] = best_planned_route
        delay = 0
        redundancy = 0
        for v in vehicles:
            cost, r = route_cost(v, pre_planned[v.id])
            if cost >= 0:
                delay += cost
                if delay > best_delay:
                    break
                redundancy += r
            else:
                delay = 21474836480000
                break
        if 0 <= delay < best_delay or (delay == best_delay and redundancy > best_redundancy):
            best_delay = delay
            best_redundancy = redundancy
            best_planned = pre_planned
    vehicle_id_to_planned_route = best_planned

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
                planned_route = [origin_planned_route[i] for i in range(1, len(origin_planned_route))]
        elif len(origin_planned_route) > 0:
            destination = origin_planned_route[0]
            planned_route = [origin_planned_route[i] for i in range(1, len(origin_planned_route))]

        vehicle_id_to_destination[vehicle_id] = destination
        vehicle_id_to_planned_route[vehicle_id] = planned_route

    return vehicle_id_to_destination, vehicle_id_to_planned_route


def route_cost(vehicle, route):
    if len(route) == 0:
        return 0, 0
    if vehicle.destination is not None:
        time = vehicle.destination.arrive_time
        v_loc = vehicle.destination.id
    else:
        time = vehicle.gps_update_time
        v_loc = vehicle.cur_factory_id
    # todo : more accurate starting time

    capacity_limit = vehicle.board_capacity
    capacity = 0
    for item in vehicle.carrying_items.items:
        capacity += item.demand
        if capacity > capacity_limit:
            return -1, 0
    cost = 0
    redundancy = 0
    for node in route:
        time += world.id2factory[v_loc].destinations[node.id].time
        for item in node.delivery_items:
            time += item.unload_time
            cost += max(0, time - item.committed_completion_time) ** 2
            redundancy += item.committed_completion_time - time
            capacity -= item.demand
            # todo : cost is per item or per order?
        for item in node.pickup_items:
            time += item.load_time
            capacity += item.demand
            if capacity > capacity_limit:
                return -1, 0
        v_loc = node.id
    return cost, redundancy


def select_vehicle_for_orders(vehicles, items, pre_planned, pickup_node, delivery_node, cost_list):
    best_planned_route = None
    best_vehicle = None
    best_cost = 2147483648000
    best_redundancy = 0
    perm = np.random.permutation(len(vehicles))
    for v in range(len(vehicles)):
        vehicle = vehicles[perm[v]]
        # vehicle = vehicles[v]
        # if vehicle.id in cost_list:
        #     continue
        per_cost = 2147483648000
        per_redundancy = 0
        per_route = []
        for i in range(len(pre_planned[vehicle.id])):
            # if vehicle.id in cost_list:
            #     route = [n for n in pre_planned[vehicle.id]]
            #     route.append(pickup_node)
            #     route.append(delivery_node)
            #     route = optimize_route(route)
            #     per_cost, per_redundancy = route_cost(vehicle, route)
            #     per_route = route
            #     break
            # route = copy.deepcopy(pre_planned[vehicle.id])
            route = [n for n in pre_planned[vehicle.id]]
            if len(pickup_node.pickup_items) > 0:
                route.insert(i + 1, pickup_node)
                route.insert(i + 2, delivery_node)
            route = optimize_route(route)
            cost, redundancy = route_cost(vehicle, route)
            if 0 <= cost < per_cost or (cost == per_cost and redundancy > per_redundancy):
                per_cost = cost
                per_redundancy = redundancy
                per_route = route
        if len(pre_planned[vehicle.id]) == 0:
            # route = copy.deepcopy(pre_planned[vehicle.id])
            route = [n for n in pre_planned[vehicle.id]]
            route.append(pickup_node)
            route.append(delivery_node)
            per_cost, per_redundancy = route_cost(vehicle, route)
            per_route = route
        if 0 <= per_cost < best_cost or (per_cost == best_cost and per_redundancy > best_redundancy):
            best_cost = per_cost
            best_redundancy = per_redundancy
            best_planned_route = per_route
            best_vehicle = vehicle

    return best_vehicle, best_planned_route, best_cost


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
    # delivery_factory_id = __get_delivery_factory_id(items)
    if len(pickup_factory_id) == 0:
        return None, None

    pickup_factory = id_to_factory.get(pickup_factory_id)
    # delivery_factory = id_to_factory.get(delivery_factory_id)
    pickup_node = Node(pickup_factory.id, pickup_factory.lng, pickup_factory.lat, copy.copy(items), [])
    nodelist = [pickup_node]
    # path = get_shortest_path(pickup_factory_id, delivery_factory_id)[0]
    # for i in range(len(path)):
    #     if i == 0 or i == len(path) - 1:
    #         continue
    #     nodelist.append(Node(path[i], world.id2factory[path[i]].lon, world.id2factory[path[i]].lat, [], []))
    d_list = []
    items_list = []
    last_index = len(items) - 1
    for i in range(len(items)):
        d_id = items[last_index - i].delivery_factory_id
        if d_id not in d_list:
            d_list.append(d_id)
            items_list.append([items[last_index - i]])
        else:
            if d_id == d_list[-1]:
                items_list[-1].append(items[last_index - i])
            else:
                d_list.append(d_id)
                items_list.append([items[last_index - i]])

    for i in range(len(d_list)):
        delivery_factory = id_to_factory[d_list[i]]
        nodelist.append(
            Node(delivery_factory.id, delivery_factory.lng, delivery_factory.lat, [], copy.copy(items_list[i])))
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

    # read the route map
    # code_to_route = get_route_map(Configs.route_info_file_path)
    # route_map = Map(code_to_route)

    # read the input json, you can design your own classes
    unallocated_order_items = read_json_from_file(Configs.algorithm_unallocated_order_items_input_path)
    id_to_unallocated_order_item = get_order_item_dict(unallocated_order_items, 'OrderItem')

    ongoing_order_items = read_json_from_file(Configs.algorithm_ongoing_order_items_input_path)
    id_to_ongoing_order_item = get_order_item_dict(ongoing_order_items, 'OrderItem')

    id_to_order_item = {**id_to_unallocated_order_item, **id_to_ongoing_order_item}

    vehicle_infos = read_json_from_file(Configs.algorithm_vehicle_input_info_path)
    id_to_vehicle = get_vehicle_instance_dict(vehicle_infos, id_to_order_item, id_to_factory)

    return id_to_factory, id_to_unallocated_order_item, id_to_ongoing_order_item, id_to_vehicle


def __output_json(vehicle_id_to_destination, vehicle_id_to_planned_route):
    write_json_to_file(Configs.algorithm_output_destination_path, convert_nodes_to_json(vehicle_id_to_destination))
    write_json_to_file(Configs.algorithm_output_planned_route_path, convert_nodes_to_json(vehicle_id_to_planned_route))
