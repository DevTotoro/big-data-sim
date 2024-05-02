from random import seed, uniform
from pandas import DataFrame
from uxsim import World

from config import SIMULATION_TIME


def run_simulation(delta_n=5, t_max=SIMULATION_TIME, demand_interval=30, average_demand=2) -> DataFrame:
    nodes = [
        {'name': 'I1', 'x': 1, 'y': 0, 'signal': [20, 20]},
        {'name': 'I2', 'x': 2, 'y': 0, 'signal': [20, 20]},
        {'name': 'I3', 'x': 3, 'y': 0, 'signal': [20, 20]},
        {'name': 'I4', 'x': 4, 'y': 0, 'signal': [20, 20]},
        {'name': 'W1', 'x': 0, 'y': 0},
        {'name': 'E1', 'x': 5, 'y': 0},
        {'name': 'N1', 'x': 1, 'y': 1},
        {'name': 'N2', 'x': 2, 'y': 1},
        {'name': 'N3', 'x': 3, 'y': 1},
        {'name': 'N4', 'x': 4, 'y': 1},
        {'name': 'S1', 'x': 1, 'y': -1},
        {'name': 'S2', 'x': 2, 'y': -1},
        {'name': 'S3', 'x': 3, 'y': -1},
        {'name': 'S4', 'x': 4, 'y': -1},
    ]

    links = [
        {'name': 'W1I1', 'start_node': 'W1', 'end_node': 'I1', 'signal_group': 0},
        {'name': 'I1I2', 'start_node': 'I1', 'end_node': 'I2', 'signal_group': 0},
        {'name': 'I2I3', 'start_node': 'I2', 'end_node': 'I3', 'signal_group': 0},
        {'name': 'I3I4', 'start_node': 'I3', 'end_node': 'I4', 'signal_group': 0},
        {'name': 'I4E1', 'start_node': 'I4', 'end_node': 'E1', 'signal_group': 0},

        {'name': 'N1I1', 'start_node': 'N1', 'end_node': 'I1', 'signal_group': 1},
        {'name': 'I1S1', 'start_node': 'I1', 'end_node': 'S1', 'signal_group': 1},
        {'name': 'N3I3', 'start_node': 'N3', 'end_node': 'I3', 'signal_group': 1},
        {'name': 'I3S3', 'start_node': 'I3', 'end_node': 'S3', 'signal_group': 1},

        {'name': 'N2I2', 'start_node': 'N2', 'end_node': 'I2', 'signal_group': 2},
        {'name': 'I2S2', 'start_node': 'I2', 'end_node': 'S2', 'signal_group': 2},
        {'name': 'N4I4', 'start_node': 'N4', 'end_node': 'I4', 'signal_group': 2},
        {'name': 'I4S4', 'start_node': 'I4', 'end_node': 'S4', 'signal_group': 2},
    ]

    demands = [
        {'orig': 'N1', 'dest': 'S1', 'demand_multiplier': 0.25},
        {'orig': 'S2', 'dest': 'N2', 'demand_multiplier': 0.25},
        {'orig': 'N3', 'dest': 'S3', 'demand_multiplier': 0.25},
        {'orig': 'S4', 'dest': 'N4', 'demand_multiplier': 0.25},

        {'orig': 'E1', 'dest': 'W1', 'demand_multiplier': 0.75},
        {'orig': 'N1', 'dest': 'W1', 'demand_multiplier': 0.75},
        {'orig': 'S2', 'dest': 'W1', 'demand_multiplier': 0.75},
        {'orig': 'N3', 'dest': 'W1', 'demand_multiplier': 0.75},
        {'orig': 'S4', 'dest': 'W1', 'demand_multiplier': 0.75},
    ]

    world = World(name='', deltan=delta_n, tmax=t_max, save_mode=0)

    for node in nodes:
        node['signal'] = node['signal'] if 'signal' in node else [0]
        world.addNode(**node)

    for link in links:
        world.addLink(**link, length=500, free_flow_speed=50, jam_density=0.2, number_of_lanes=3)

    seed()
    for t in range(0, t_max, demand_interval):
        flow = uniform(0, average_demand)
        for demand in demands:
            orig = demand['orig']
            dest = demand['dest']
            multiplier = demand['demand_multiplier']
            world.adddemand(orig=orig, dest=dest, t_start=t, t_end=t + demand_interval, flow=flow * multiplier)

    world.exec_simulation()

    return world.analyzer.vehicles_to_pandas()


if __name__ == '__main__':
    run_simulation()
