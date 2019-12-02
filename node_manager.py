# import psutil # get available resources

from job_manager import Status

class NodeManager:

    def __init__(self, cluster, machineSpec):
        self.total = machineSpec
        self.available = machineSpec
        self.tasks = set()
        self.cluster = cluster
        self.numCompleted = 0

    def allocate(self, task):
        self.tasks.add(task)
        self.available = self.available.subtract(task.required)
        task.start(self)

    # def deallocate(self, task):
    #     self.tasks.remove(task)
    #     self.available = self.available.add(task.required)

    def scoreTask(self, task):
        return self.available.norm(self.total).dot(
            task.required.norm(self.total))

    def canAllocate(self, task):
        return not (task.required > self.available).any()

    def utilization(self):
        return (self.total.vec-self.available.vec) / self.total.vec

    def tick(self):
        completed = []
        for task in self.tasks:
            task.tick()
            if task.status == Status.COMPLETED:
                completed.append(task)
                self.numCompleted += 1
                self.available = self.available.add(task.required)

        for task in completed:
            self.tasks.remove(task)


    def __repr__(self):
        return f"Avail:\t{self.available}\nTotal:\t{self.total}"



