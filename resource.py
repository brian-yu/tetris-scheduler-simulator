import numpy as np


'''
When computing the dot product, Tetris normalizes the task requirements as well as available resources on the machine by the machine’s overall capacity. This ensures that the numerical range of a machine’s resource (e.g., 16 cores vs. 96GB of RAM) and tasks’ demands (e.g., 4 cores vs. 1 Gbps network) do not affect the alignment score. All the resources are weighed equally.


On the larger, 250 server, cluster each machine has 32 cores, 96GB of memory, 8 1TB 7200RPM disk drives, a 10Gbps NIC and runs Linux 2.6.32.
'''

class ResourceVec:

    def __init__(self, cpu, memory, disk, network):
        self.vec = np.array([cpu, memory, disk, network])

    def dot(self, other):
        return np.dot(self.vec, other.vec)

    def norm(self, other):
        return self.vec / other.vec

    def subtract(self, other):
        return ResourceVec(*(self.vec - other.vec))

    def add(self, other):
        return ResourceVec(*(self.vec + other.vec))

    def __str__(self):
        return "[cpu={}cores mem={}Gb disk={}Gb nw={}Gbps]".format(
            *self.vec)

    def __repr__(self):
        return str(self)

    def __lt__(self, other):
        return self.vec < other.vec

    def __gt__(self, other):
        return self.vec > other.vec

    def __truediv__(self, other):
        return ResourceVec(*(self.vec / other.vec))

    def __sub__(self, other):
        return ResourceVec(*(self.vec - other.vec))
