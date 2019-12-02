'''
10,000 nodes
700,000 jobs
20,000,000 task
-> ~30 tasks per job
average task duration: 260 seconds
avg task CPU: .022%
avg task mem: .020%
'''
import numpy as np
from collections import deque, Counter

from node_manager import NodeManager
from job_manager import JobManager, Task, Status
from cluster_manager import Tetris, FIFO
from resource import ResourceVec

def randomResource(maxVec):
    cpu, mem, disk, nw = maxVec.vec
    return ResourceVec(
        np.random.randint(1, cpu/2+1),
        np.random.randint(1, mem/2+1),
        np.random.randint(1, disk/2+1),
        np.random.randint(1, nw/2+1))

def simulate(cluster_manager):
    # NUM_NODES = 50
    NUM_NODES = 20
    # NUM_TASKS = 18000
    NUM_JOBS = 40
    JOB_ARRIVAL_DURATION = 1000
    MACHINE_SPEC = ResourceVec(16,64,3000,5)


    # a, m = 3., 2.  # shape and mode of distribution
    # durations = np.round((np.random.pareto(a, NUM_TASKS) + 1) * m)

    # Initialize cluster
    cluster = cluster_manager()

    # Add nodes
    for _ in range(NUM_NODES):
        cluster.addNode(MACHINE_SPEC)

    # Create jobs
    arrival_times = np.sort(np.round(np.random.uniform(
        low=0, high=JOB_ARRIVAL_DURATION, size=NUM_JOBS)))
    jobs = []
    for i in range(NUM_JOBS):
        duration = 260
        numTasks = 30
        taskResources = randomResource(MACHINE_SPEC)
        jobs.append(JobManager(duration, numTasks, taskResources))

    print("{0} {1} {0}".format("="*15, cluster.name))
    print("Starting simulation.")

    time = 0
    jobIdx = 0
    # Run simulation
    while cluster.hasUncompletedJobs() or jobIdx < len(jobs):
        while jobIdx < len(jobs) and arrival_times[jobIdx] == time:
            job = jobs[jobIdx]
            cluster.assignJob(job)
            jobIdx += 1

        if time % 100 == 0:
            print(f'{time}s\n{cluster.status()}')
        cluster.tick()
        time += 1

    print(f'All jobs completed in {time}s.')

    job_durations = [job.endTime - job.startTime for job in jobs]
    print(job_durations)
    print(sum(job_durations) / len(job_durations))

    return time



ITERATIONS = 1
tetris = []
fifo = []

for i in range(ITERATIONS):
    print("{0} Iteration {1} {0}".format("="*15, i))
    tetris.append(simulate(Tetris))
    fifo.append(simulate(FIFO))


print("Tetris average: {}s".format(sum(tetris) / ITERATIONS))
print("FIFO average: {}s".format(sum(fifo) / ITERATIONS))