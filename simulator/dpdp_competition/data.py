import astroid
import numpy as np
import datetime
import pandas as pd
from algorithm.wolrd import World
import matplotlib.pyplot as plt

gap = 10


def process_data(index):
    world = World("benchmark/route_info.csv", "benchmark/factory_info.csv")
    world.add_orders("benchmark/instance_{}/".format(index))

    demands = np.zeros((len(world.factories), 1440 // gap))
    f2id = {}
    for i in range(len(world.factories)):
        f2id[world.factories[i]] = i
    for o in world.known_orders:
        start = f2id[o.start_factory]
        demand = o.demand
        time = (o.start_time.hour * 60 + o.start_time.minute) // gap
        demands[start][time] += demand
    return demands


if __name__ == '__main__':
    demands_list = []
    for i in range(64):
        print(i)
        demands_list.append(process_data(i + 1))
    idx = []
    idx_ = []
    for i in range(len(demands_list[0])):
        check = 1
        for j in range(64):
            if np.sum(demands_list[j][i]) > 0:
                check = 0
                break
        if check:
            idx.append(i)
        else:
            idx_.append(i)

    # dataframe = pd.DataFrame(demands2)
    # dataframe.to_csv("test.csv", index=False, sep=',')
    print(idx_)
    print("hello world!", len(idx_))
    for o in range(64):
        demands = demands_list[o]
        demands2 = np.delete(demands, idx, axis=0)
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        # 设置标题
        ax1.set_title('Order Instance {}'.format(o))
        # 设置X轴标签
        plt.xlabel('Time')
        # 设置Y轴标签
        plt.ylabel('Factory')
        # 画散点图
        plt.axis([-1, 96, -1, 25])
        cm = plt.cm.get_cmap('RdYlBu')
        maxN = np.max(demands2)

        X = []
        Y = []
        c = []
        # demands2 /= maxN * 10
        for i in range(len(demands2)):
            X += range(1440 // gap)
            Y += (np.ones_like(range(1440 // gap)) * i).tolist()
            c += demands2[i].tolist()

        ax1.scatter(x=X, y=Y, c=c, cmap='OrRd')
        # 设置图标
        # plt.legend('x1')
        # 显示所画的图
        # plt.show()
        plt.savefig("plots/order_{}.png".format(o))
