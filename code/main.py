
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This program reads in an input, triples it and then writes the result
to a file. 
"""

import sys 
import os
from utils import dispatcher, server, \
    file_job_simulator, random_job_simulator, \
    load_config, server_depart, mean_response_time
from pprint import pprint

def do_test(case_num, config_path, out_path):
    mode = f"{config_path}/mode_{case_num}.txt"
    para = f"{config_path}/para_{case_num}.txt"
    workload = f"{config_path}/service_{case_num}.txt"
    interarrival = f"{config_path}/interarrival_{case_num}.txt"

    config = load_config(mode, para, workload, interarrival)
    print("Config is:")
    pprint(config)
    print('')

    svr_dep = []
    mrt = -1

    para = config['para']
    disp = dispatcher(slow_svr_rate=1, fast_svr_rate=para['f'], algo_ver=para['algo_ver'], d=para['d'])
    if config['mode'] == 'trace':
        sim = file_job_simulator(disp, config['workload'], config['interarrival'])
    else:
        sim = random_job_simulator(disp, config['workload'], config['interarrival'], config['time_end'])
    sim.start()

    print('')
    print("Arrive\tDepart")
    print("------\t------")
    for k, v in disp.job_assignment.items():
        print("Server", k + 1)
        ret = server_depart(v)
        print(ret)
        svr_dep.append(ret)
    
    mrt = str(mean_response_time(disp.job_assignment))
    print("---- mean response time:", mrt, "----")

    with open(f"{out_path}/mrt_{case_num}.txt", 'w') as f:
        f.write(mrt)

    for idx, sd in enumerate(svr_dep):
        with open(f"{out_path}/s{idx + 1}_dep_{case_num}.txt", 'w') as f:
            f.writelines(sd)


if __name__ == "__main__":
    do_test(sys.argv[1], 'config', 'output')