"""
- Do you have any guidance on how I should begin implementing the scheduler? Are there any existing frameworks that I can build on top of?  
- You can just implement the key idea of the scheduler at first. Just try as simple as you can and use your most familiar programming language. For example, if you need to implement a job, you don't even need to run the job but should include every information the scheduler needs to know.

- How are you implementing the scheduler you are working on? 
- I made a simple simulation of my scheduler and compare it with other schedulers at first. Currently, I am implementing runnable jobs and distribute them to real nodes.
"""
from collections import deque, Counter
import numpy as np

from node_manager import NodeManager
from job_manager import Status

class ClusterManager:

    def __init__(self):
        self.nodes = []
        self.pending_jobs = set()
        self.numJobsCompleted = 0
        self.task_queue = deque([])
        self.name = "ClusterManager"
        self.time = 0

    def addNode(self, machineSpec):
        self.nodes.append(NodeManager(self, machineSpec))

    def ask(self, task):
        self.task_queue.append(task)

    def allocateTasks(self):
        queued = len(self.task_queue)
        # print(queued)
        # Allocate tasks that are in the queue
        while len(self.task_queue) > 0 and queued > 0:
            queued -= 1
            task = self.task_queue.popleft()
            node = self.findBestNode(task)

            if not node:
                self.task_queue.append(task)
            else:
                node.allocate(task)

    def assignJob(self, job):
        self.pending_jobs.add(job)
        job.start(self)

    def hasUncompletedJobs(self):
        return len(self.pending_jobs) > 0

    def status(self):
        avg_utilization = np.zeros(4)
        for node in self.nodes:
            avg_utilization += node.utilization()
        avg_utilization /= len(self.nodes)

        inProgress = 0
        completed = 0
        for node in self.nodes:
            inProgress += len(node.tasks)
            completed += node.numCompleted

        return '\tTasks:\t{} queued, {} in progress, and {} completed.\n\tJobs:\t{} in progress, {} completed.\n\tUtil.:\tcpu={:.2f}, mem={:.2f}, disk={:.2f}, nw={:.2f}'.format(
            len(self.task_queue), inProgress, completed, len(self.pending_jobs), self.numJobsCompleted, *avg_utilization)

    def tick(self):
        self.allocateTasks()

        for node in self.nodes:
            node.tick()

        completed = []
        for job in self.pending_jobs:
            if job.status == Status.COMPLETED:
                completed.append(job)
                job.endTime = self.time

        for job in completed:
            self.pending_jobs.remove(job)
            self.numJobsCompleted += 1

        self.time += 1

    def findBestNode(self, task):
        bestNode = None
        bestScore = 0
        for node in self.nodes:
            score = node.scoreTask(task)
            if score > bestScore and node.canAllocate(task):
                bestNode = node
                bestScore = score

        return bestNode


class Tetris(ClusterManager):
    def __init__(self):
        super().__init__()
        self.task_queue = set()
        self.name = "Tetris"

    def ask(self, task):
        self.task_queue.add(task)
    
    def allocateTasks(self):
        for node in self.nodes:
            canAllocate = True
            while canAllocate:
                bestTask = None
                bestScore = 0
                for task in self.task_queue:
                    score = node.scoreTask(task)
                    if score > bestScore and node.canAllocate(task):
                        bestTask = task
                        bestScore = score
                if bestTask:
                    node.allocate(task)
                    self.task_queue.remove(task)
                else:
                    canAllocate = False



class FIFO(ClusterManager):
    def __init__(self):
        super().__init__()
        self.name = "FIFO"
    
    def allocateTasks(self):
        for node in self.nodes:            
            while len(self.task_queue) > 0:
                task = self.task_queue[0]
                if node.canAllocate(task):
                    node.allocate(task)
                    self.task_queue.popleft()
                else:
                    break
