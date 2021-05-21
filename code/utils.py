from math import inf, sqrt
from pprint import pprint
from itertools import accumulate
import numpy as np
import sys

class job(object):
    ''' job class stores jobs '''
    def __init__(self, arriv_time, workload):
        self.arriv_time = arriv_time
        self.workload = workload

    def __repr__(self):
        return f"({round(self.arriv_time, 4)}, {round(self.workload, 4)})"

# --------------------- may deleted after ---------------------
class event(object):
    ''' event for master clock to read '''
    def __init__(self, event_type, server_name, time_point):
        self.event_type = event_type
        self.time_point = time_point
        self.server_name = server_name
    
    def __str__(self):
        print(f"Master clock: {self.time_point}")
        print(f"Event type: {self.event_type}")
        print(f"Server: {self.server_name}")
        print("==============")
# --------------------- may deleted after ---------------------


class server(object):
    ''' defined a server '''
    def __init__(self, process_rate):
        self.process_rate = process_rate
        self.queue = []
        self.last_depart = 0
        self.next_depart = 0


    def update(self, time_point):
        ''' update queue with given time point '''
        to_delete = []
        idx = 0
        for j in self.queue:
            self.next_depart = max(self.last_depart, j.arriv_time) + j.workload
            if self.next_depart <= time_point:
                to_delete.append(idx)
                self.last_depart = self.next_depart
            else:
                break
            idx += 1
        
        self.queue = [j for i, j in enumerate(self.queue) if i not in to_delete]

    def num_of_jobs(self):
        ''' update when querying the number of jobs remains in server '''
        return len(self.queue)

    def next_departure(self):
        if len(self.queue) == 0:
            return inf
        else:
            return self.next_depart

    # --------------------- may deleted after ---------------------
    def status(self):
        is_idle = False
        if self.num_of_jobs() == 0:
            is_idle = True
            return "idle, inf, -"
        elif self.num_of_jobs() == 1:
            return f"busy, {self.queue[0].workload}, -"
        else:
            return f"busy, {self.queue[0].workload}, {self.queue[1:]}"
    # --------------------- may deleted after ---------------------

    def assign_job(self, arriv_job):
        self.queue.append(arriv_job)
    

class dispatcher(object):
    def __init__(self, slow_svr_rate, fast_svr_rate, d, algo_ver):
        ''' 
        slow_svr_rate: processing rate of slow server
        fast_svr_rate: processing rate of fast server
        d: the parameter for priority
        algo_ver: choose version 1 or version 2 are used for load balancing
        '''

        self.job_assignment = {0: [], 1: [], 2: []}
        self.svr = (
            server(slow_svr_rate),
            server(slow_svr_rate),
            server(fast_svr_rate),
        )
        self.d = d
        if algo_ver == 1:
            self.f = 1
        else:
            self.f = fast_svr_rate

    def next_departure(self):
        return min(
            self.svr[0].next_departure(), 
            self.svr[1].next_departure(),
            self.svr[2].next_departure())
    
    def on_dispatch(self, arrival, workload):
        ''' update servers' status '''
        for s in self.svr:
            s.update(arrival)

        ''' load balancing algo '''
        job_assign_to = None
        arriv_job_slow = job(arrival, workload)
        arriv_job_fast = job(arrival, workload / self.svr[2].process_rate)
        
        # algorithm describled in project spec
        n1 = self.svr[0].num_of_jobs()
        n2 = self.svr[1].num_of_jobs()
        n3 = self.svr[2].num_of_jobs()
        ns = min(n1, n2)
        if n3 == 0:
            job_assign_to = 2
        elif ns == 0 or ns <= (n3 / self.f) - self.d:
            if n1 == ns:
                job_assign_to = 0
            else:
                job_assign_to = 1
        else:
            job_assign_to = 2
        # end of algorithm

        ''' assign jobs '''
        arriv_job = None
        if job_assign_to == 2:
            arriv_job = arriv_job_fast
        else:
            arriv_job = arriv_job_slow

        # --print job assignment -- 
        # print(f"{n1}\t{n2}\t{n3}\tJob {arriv_job}\tassigned to {job_assign_to + 1}")
        self.svr[job_assign_to].assign_job(arriv_job)
        self.job_assignment[job_assign_to].append(arriv_job)

class job_simulator(object):
    def __init__(self, dispatcher, file_workload, file_interarrival):
        self.dispatcher = dispatcher
        self.file_workload = file_workload
        self.file_interarrival = file_interarrival

    ''' abstract base class, should implement concreate generater '''
    def start(self):
        ''' abstract generator '''
        raise NotImplementedError


class file_job_simulator(job_simulator):
    def __init__(self, dispatcher, file_workload, file_interarrival):
        super().__init__(dispatcher, file_workload, file_interarrival)
    def start(self):
        workloads = []
        intervals = []
        arrivals = []
        with open(self.file_workload, 'r') as f:
            workloads = [ round(float(t), 5) for t in f.readlines() ]
        with open(self.file_interarrival, 'r') as f:
            intervals = [ round(float(t), 5) for t in f.readlines() ]
        arrivals = [ round (t, 5) for t in list(accumulate(intervals)) ]

        print(f"Workloads are: {workloads}")
        print(f"Arrivals are: {list(arrivals)}")
        print(f"Intervals are: {intervals}\n")
        for w, a in zip(workloads, arrivals):
            self.dispatcher.on_dispatch(a, w)

class random_job_simulator(job_simulator):
    def __init__(self, dispatcher, file_workload, file_interarrival, time_end):
        super().__init__(dispatcher, file_workload, file_interarrival)
        self.time_end = time_end
    
    def service_time(self, alpha, beta):
        t = 0
        while 0 <= t and t <= alpha:
            val = np.random.rand()
            t = alpha * (val ** (1 / (1 - beta))) 
        return t


    def start(self):
        random_args = []
        alpha_beta = []
        with open(self.file_workload, 'r') as f:
            alpha_beta = [ round(float(t), 5) for t in f.readlines()]
        with open(self.file_interarrival, 'r') as f:
            random_args = [ round(float(t), 5) for t in f.readlines()]
        lamb = random_args[0]
        a2l = random_args[1]
        a2u = random_args[2]
        alpha = alpha_beta[0]
        beta = alpha_beta[1]

        print(f"\
            Lambda:\t{lamb}, a2l:\t{a2l}, a2u:\t{a2u}\n\
            alpha:\t{alpha}, beta:\t{beta}, time end: {self.time_end}\
        ")

        assigned_jobs = []
        master_clock = 0
        time_point = 0
        while master_clock <= self.time_end:
            # generate interarrivals
            a1k = np.random.exponential()
            a2k = np.random.uniform(low=a2l, high=a2u)
            ak = a1k * a2k / lamb
            time_point += ak
            gt = self.service_time(alpha, beta)
            # print(f"arrival time: {ak}, service time: {gt}")
            assigned_jobs.append(job(time_point, gt))
            self.dispatcher.on_dispatch(time_point, gt)
            master_clock = min(self.dispatcher.next_departure(), time_point)
        return assigned_jobs


def load_config(file_mode, file_para, file_workload, file_interarrival):
    config = {}
    config['workload'] = file_workload
    config['interarrival'] = file_interarrival
    with open(file_mode, 'r') as f:
        config['mode'] = f.readline()
    with open(file_para, 'r') as f:
        paras = [ float(p) for p in f.readlines() ]
        config['para'] = {}
        config['para']['f'] = float(paras[0])
        config['para']['algo_ver'] = int(paras[1])
        config['para']['d'] = float(paras[2])
        if config['mode'] == 'random':
            config['time_end'] = paras[3]
    
    return config


def server_depart(jobs):
    ret_str = ''
    next_depart = jobs[0].arriv_time
    for j in jobs:
        job_start = next_depart if next_depart > j.arriv_time else j.arriv_time
        next_depart = job_start + j.workload
        ret_str += "{:.4f}\t{:.4f}\n".format(j.arriv_time, next_depart)
    return ret_str


def mean_response_time(job_assignment):
    time_cnt = 0
    jobs_cnt = 0

    for k, v in job_assignment.items():
        next_depart = v[0].arriv_time
        for j in v:
            job_start = max(next_depart, j.arriv_time)
            next_depart = job_start + j.workload
            time_cnt += next_depart - j.arriv_time
            jobs_cnt += 1    
    return "{:.4f}".format(time_cnt / jobs_cnt)


def steady_mean_response_time(job_assignment, steady=(0.0, 1.0)):
    time_cnt = 0
    jobs_cnt = 0
    for k in job_assignment:
        start, end = int(len(job_assignment[k]) * (steady[0])), int(len(job_assignment[k]) * (steady[1]))
        job_assignment[k] = sorted(job_assignment[k], key=lambda a: a.arriv_time)[start:end]

    for k, v in job_assignment.items():
        next_depart = v[0].arriv_time
        for j in v:
            job_start = max(next_depart, j.arriv_time)
            next_depart = job_start + j.workload
            time_cnt += next_depart - j.arriv_time
            jobs_cnt += 1    
    return time_cnt / jobs_cnt