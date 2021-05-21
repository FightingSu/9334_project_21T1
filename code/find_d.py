import sys 
import os
import numpy as np

from utils import dispatcher, server, \
    file_job_simulator, random_job_simulator, \
    load_config, server_depart, steady_mean_response_time
from pprint import pprint
from matplotlib import pyplot as plt
from scipy import stats


def plot_mrt_setady(job_assignment):
    ''' invoke in do_test to check steady state '''
    rt = 0
    time_cnt = 0
    cnt = 0
    mean = []
    for v in job_assignment.values():
        next_depart = v[0].arriv_time
        for j in v:
            job_start = next_depart if next_depart > j.arriv_time else j.arriv_time
            next_depart = job_start + j.workload
            rt = next_depart - job_start
            time_cnt += rt
            cnt += 1
            mean.append(time_cnt / cnt)
    plt.plot([i for i in range(0, len(mean))], mean)
    plt.show()


def update_config(d, algo_ver, time_end):
    line = []
    with open('special/para_special.txt', 'r') as f:
        line.append(f.readline()[:-1])
    line.append(str(algo_ver))
    line.append(str(d))
    line.append(str(time_end))
    with open('special/para_special.txt', 'w') as f:
        f.writelines("\n".join(line))



def do_test(case_num, config_path, out_path):
    mode = f"{config_path}/mode_{case_num}.txt"
    para = f"{config_path}/para_{case_num}.txt"
    workload = f"{config_path}/service_{case_num}.txt"
    interarrival = f"{config_path}/interarrival_{case_num}.txt"

    config = load_config(mode, para, workload, interarrival)
    print("Config is:")
    pprint(config)

    svr_dep = []
    mrt = -1

    para = config['para']
    disp = dispatcher(slow_svr_rate=1, fast_svr_rate=para['f'], algo_ver=para['algo_ver'], d=para['d'])
    sim = random_job_simulator(disp, config['workload'], config['interarrival'], config['time_end'])
    sim.start()

    # plot_mrt_setady(disp.job_assignment)
    for k, v in disp.job_assignment.items():
        ret = server_depart(v)
        svr_dep.append(ret)
    
    mrt = str(steady_mean_response_time(disp.job_assignment, (0.2, 0.4)))
    print("---- mean response time:", mrt, "----")

    return float(mrt)


def test_with_args(algo_ver, reputation, alpha, start, end, interval, time_end):
    range_d = [d / (1 / interval) for d in range(start, int(end * (1 / interval)))]
    mrt = []
    for d in range_d:
        update_config(d, algo_ver, time_end)
        mrt.append([do_test("special", "special", "special/output") for x in range(0, reputation)])
    mrt = np.array(mrt)
    print(mrt, "----")
    mean_mrt = np.array([[np.mean(m) for m in mrt], [np.mean(m) for m in mrt]]).T
    std_mrt = np.array([[np.std(m) for m in mrt], [np.std(m) for m in mrt]]).T
    n = reputation
    mf = stats.t.ppf(1 - alpha / 2, n - 1) / np.sqrt(n)
    confidence_interval = mean_mrt + np.array([-1, 1]) * mf * std_mrt
    print(confidence_interval)

    plt.title(f"time_end = {time_end}, reputation = {reputation}")
    plt.xlabel("d")
    plt.ylabel("confidence interval")
    for low, high, mean, x in zip(confidence_interval[:, 0], confidence_interval[:, 1], mean_mrt.T[0], range_d):
        plt.plot((x, x), (low, high), color="blue")
        plt.plot((x - 0.01, x + 0.01), (mean, mean), color="blue")
    plt.show()


def compare_server(rep, time_end):
    config = load_config(
        "special/mode_special.txt", 
        "special/para_special.txt", 
        "special/services_special.txt", 
        "special/interarrival.txt")

    mrt1 = []
    for i in range(0, rep):
        disp1 = dispatcher(1, config["para"]["f"], 0.2, 1)
        sim = random_job_simulator(disp1, "special/service_special.txt", "special/interarrival_special.txt", time_end)
        sim.start()
        mrt1.append(steady_mean_response_time(disp1.job_assignment, (0.2, 0.4)))

    mrt2 = []
    for i in range(0, rep):
        disp2 = dispatcher(1, config["para"]["f"], 0.5, 2)
        sim = random_job_simulator(disp2, "special/service_special.txt", "special/interarrival_special.txt", time_end)
        sim.start()
        mrt2.append(steady_mean_response_time(disp2.job_assignment, (0.2, 0.4)))
    
    mrt = np.array(mrt1) - np.array(mrt2)
    mean_mrt = np.mean(mrt)# np.array([[np.mean(m) for m in mrt], [np.mean(m) for m in mrt]]).T
    std_mrt = np.std(mrt)#np.array([[np.std(m) for m in mrt], [np.std(m) for m in mrt]]).T
    n = rep
    alpha = 0.05
    mf = stats.t.ppf(1 - alpha / 2, n - 1) / np.sqrt(n)
    confidence_interval = mean_mrt + np.array([-1, 1]) * mf * std_mrt
    print(confidence_interval)


if __name__ == "__main__":
    # test_with_args(1, 3, 0.05, 0, 0.2, 0.1, 2000)
    compare_server(30, 10000)