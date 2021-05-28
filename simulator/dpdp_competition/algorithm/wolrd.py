import datetime
import logging
import os
import pandas as pd
from dateutil.parser import parse
import copy
import numpy as np


class Order(object):
    def __init__(self, id, standard, small, box, demand, start_time, end_time, load_time, unload_time, start_factory,
                 end_factory):
        self.id = id
        self.q = [standard, small, box]
        self.demand = demand
        assert self.demand == self.q[0] + self.q[1] * 0.5 + self.q[2] * 0.25
        self.start_time = parse(start_time)
        self.end_time = parse(end_time)
        self.load_time = load_time
        self.unload_time = unload_time
        self.start_factory = start_factory
        self.end_factory = end_factory
        self.vehicle = None
        if end_factory.id in self.start_factory.destinations:
            self.routes = self.start_factory.destinations[end_factory.id]
        else:
            logging.warning("START AND END FACTORY NOT CONNECTED! ORDER {}".format(id))
        # assert self.routes


class Vehicle(object):
    def __init__(self, id, capacity, operation_time, gps_id):
        self.id = id
        self.capacity = capacity
        self.operation_time = operation_time
        self.gps_id = gps_id
        self.order_id = None  # id of the order that assigned to this vehicle, None otherwise


class Factory(object):
    def __init__(self, id, lon, lat, port_num):
        self.id = id
        self.lon = lon
        self.lat = lat
        self.port_num = port_num
        self.destinations = {}  # routes that from this factory to other destinations, mapped by factory id
        self.origins = {}  # routes that from other origins to this factory, mapped by factory id
        self_route = Route("self-" + id, self, self, 0, 0)
        self.add_destination(self, self_route)
        self.add_origin(self, self_route)

    # According to the data, there is only one route a given pair of origin and destination
    def add_destination(self, end_factory, route):
        # if end_factory.id in self.destinations:
        #     self.destinations[end_factory.id].append((end_factory, distance, time))
        # else:
        self.destinations[end_factory.id] = route

    def add_origin(self, start_factory, route):
        # if start_factory.id in self.origins:
        #     self.origins[start_factory.id].append((start_factory, distance, time))
        # else:
        self.origins[start_factory.id] = route


# change the partial order to total order
def f(x, y):
    return x + 100 * y


class Route(object):
    def __init__(self, id, start, end, distance, time):
        self.id = id
        self.start = start
        self.end = end
        self.distance = np.float32(distance)
        self.time = time
        self.details = [start.id, end.id] if start.id != end.id else [start.id]
        self.start.add_destination(end, self)
        self.end.add_origin(start, self)


class World(object):
    """
    Maintain information
    """

    def __init__(self, map_loc, factory_loc):
        print("Building world from route map: {}, factory info: {}".format(
            map_loc, factory_loc))

        self.known_order_list = []
        self.known_orders = []
        self.pointer = 0

        self.started_orders = []
        self.running_orders = []

        self.current_time = datetime.datetime(2021, 1, 1, 0, 0, 0)
        self.timestamp = datetime.timedelta(0, 0, 0, 0, 10, 0)

        # Read files
        self.factory_list = pd.read_csv(factory_loc).values
        self.factories = []
        self.id2factory = {}
        for f in self.factory_list:
            fac = Factory(f[0], f[1], f[2], f[3])  # 0 id, 1 lon, 2 lat, 3 port num
            self.factories.append(fac)
            self.id2factory[f[0]] = fac

        self.route_list = pd.read_csv(map_loc).values
        self.routes = []
        for r in self.route_list:
            if r[1] in self.id2factory and r[2] in self.id2factory:
                rou = Route(r[0], self.id2factory[r[1]], self.id2factory[r[2]], r[3], r[4])
                # 0 id, 1 start factory, 2 end factory, 3 distance, 4 time
                self.routes.append(rou)

        self.vehicle_list = []
        self.vehicles = []
        self.id2vehicle = {}

    def add_orders(self, instance_dir):
        files = os.listdir(instance_dir)
        day = 0
        for file in files:
            if not os.path.isdir(file) and ".csv" in file:
                file_name = instance_dir + "/" + file
                if "vehicle_info" in file_name:
                    self.vehicle_list = pd.read_csv(file_name).values
                    for v in self.vehicle_list:
                        veh = Vehicle(v[0], v[1], v[2], v[3])  # 0 id, 1 capacity, 2 operation time, 3 gps id
                        self.vehicles.append(veh)
                        self.id2vehicle[v[0]] = veh
                else:
                    day += 1
                    order_list = pd.read_csv(file_name).values
                    sorted(order_list, key=lambda x: x[5])
                    self.known_order_list.extend(order_list)
                    for o in order_list:
                        if o[9] in self.id2factory and o[10] in self.id2factory:
                            start_time = "2021-01-{} ".format(day) + o[5]
                            if o[6] > o[5]:
                                end_time = "2021-01-{} ".format(day) + o[6]
                            else:
                                end_time = "2021-01-{} ".format(day + 1) + o[6]
                            ord = Order(o[0], o[1], o[2], o[3], o[4], start_time, end_time, o[7], o[8],
                                        self.id2factory[o[9]], self.id2factory[o[10]])
                            # 0 id, 1 standard, 2 small, 3 box, 4 demand, 5/6 start/end time,
                            # 7/8 load/unload time, 9/10 start/end factory
                            self.known_orders.append(ord)
                        else:
                            logging.warning("Factory ID NOT FOUND in ORDER {}".format(o[0]))

    def step(self):

        # TODO: interact with the simulator, update the started_orders and running_orders.

        self.current_time += self.timestamp
        for o in self.known_orders:
            if self.current_time > o.start_time >= self.current_time - self.timestamp:
                self.running_orders.append(o)
        pass

    # check triangle inequality
    def check(self):
        m = 0
        for i in self.factories:
            for j in self.factories:
                for k in self.factories:
                    # if i != j and j != k and k != i:
                    d = i.destinations[j.id].distance
                    d1 = i.destinations[k.id].distance
                    d2 = k.destinations[j.id].distance
                    t = i.destinations[j.id].time
                    t1 = i.destinations[k.id].time
                    t2 = k.destinations[j.id].time
                    if d1 + d2 < d and t1 + t2 < t:
                        # logging.warning("Triangle Distance Not Satisfied!")
                        m += 1
        logging.warning("Triangle distance not satisfied for {} times!".format(m))

    def find_shortest_path(self):
        for k in self.factories:
            for i in self.factories:
                for j in self.factories:
                    # if i != j and j != k and k != i:
                    d = i.destinations[j.id].distance
                    d1 = i.destinations[k.id].distance
                    d2 = k.destinations[j.id].distance
                    t = i.destinations[j.id].time
                    t1 = i.destinations[k.id].time
                    t2 = k.destinations[j.id].time
                    if f(d1 + d2, t1 + t2) < f(d, t):
                        i.destinations[j.id].distance = d1 + d2
                        i.destinations[j.id].time = t1 + t2
                        i.destinations[j.id].details = copy.deepcopy(i.destinations[k.id].details)
                        i.destinations[j.id].details.extend(k.destinations[j.id].details[1:])

    def output_shortest_path(self):
        n = len(self.factories)
        paths = np.zeros((n, n, n))
        factory2id = {}
        for i in range(n):
            factory2id[self.factories[i].id] = i
        for i in range(n):
            for j in range(n):
                path = self.factories[i].destinations[self.factories[j].id].details
                for k in range(len(path)):
                    paths[i][j][k] = factory2id[path[k]]
        np.save("shortest_path.npy", paths)
        np.save("factory2id.npy", np.array(list(factory2id.keys())))


if __name__ == '__main__':
    world = World("../benchmark/route_map.csv", "../benchmark/factory_info.csv")
    world.add_orders("../benchmark/instance_1/")
    # world.check()
    world.find_shortest_path()
    world.output_shortest_path()
    print("Hello World~~~")
