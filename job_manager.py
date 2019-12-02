'''
Tasks are significantly diverse in their requirements. Figure 2 shows the variation of resource requirements of tasks across re- sources. CPU usage varies all the way from only 2% of a core to over 6 cores. Similarly, memory usage ranges from 100MB to nearly 17GB. While some tasks are IO-intensive, others are less so. Overall, the minimum values of resource requirements are 5−10× lower than the median, which in turn is 20× lower than the maximum values. The coefficient-of-variations among tasks in their requirements for CPU, memory, disk and network bandwidths are high at 1.42, 1.26, 2.24 and 2.05, respectively.
'''

from collections import Counter
from enum import Enum

class Status(Enum):
    NOT_STARTED = 1
    IN_PROGRESS = 2
    PREEMPTED = 3
    COMPLETED = 4

class JobManager:

    def __init__(self, duration, numTasks, taskResources):
        # self.pending_tasks = set()
        self.numTasks = numTasks
        self.numCompleted = 0
        self.taskDuration = duration
        self.taskResources = taskResources
        self.status = Status.NOT_STARTED

        # for _ in range(numTasks):
            # self.pending_tasks.add(Task(self, taskDuration, taskResources))



    def start(self, cluster):
        self.status = Status.IN_PROGRESS
        self.startTime = cluster.time
        for _ in range(self.numTasks):
            cluster.ask(Task(self, self.taskDuration, self.taskResources))

    def endTask(self, task):
        self.numCompleted += 1
        if self.numCompleted == self.numTasks:
            self.status = Status.COMPLETED

    def progress(self):
        return self.numCompleted / self.numTasks


class Task:

    def __init__(self, job, duration, resources):
        self.job = job
        self.required = resources
        self.duration = duration
        self.time_remaining = duration
        self.status = Status.NOT_STARTED
        self.node = None

    def start(self, node):
        self.status = Status.IN_PROGRESS
        self.node = node

    def end(self):
        self.status = Status.COMPLETED
        self.job.endTask(self)

    def tick(self):
        if self.status != Status.IN_PROGRESS:
            return

        self.time_remaining -= 1
        if self.time_remaining <= 0:
            self.end()


'''
make sure that workload is the same


add job deadlines
- prioritize jobs that are near the deadline
- 
'''