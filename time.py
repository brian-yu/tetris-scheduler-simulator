from resource import ResourceVec
import numpy as np
import time


def randomResource(maxVec):
    cpu, mem, disk, nw = maxVec.vec
    return ResourceVec(
        np.random.randint(cpu+1),
        np.random.randint(mem+1),
        np.random.randint(disk+1),
        np.random.randint(nw+1))


def listMachines(n, total):
    machines = []
    cpu, mem, disk, nw = total.vec
    for i in range(n):
        machines.append(randomResource(total))
    return machines


def findBestMachine(task, machines, total):
    taskNorm = task.norm(total)
    bestMachine = 0
    bestAlignment = 0
    start = time.clock()
    for i, machine in enumerate(machines):
        alignment = machine.norm(total).dot(taskNorm)
        if alignment > bestAlignment and not (task > machine).any():
            bestMachine = i
            bestAlignment = alignment
    end = time.clock()

    return bestMachine, bestAlignment, end-start

total = ResourceVec(32,96,8000,10)

machines = listMachines(10000, total)

task = randomResource(total)

bestMachine, bestAlignment, time = findBestMachine(task, machines, total)




print(f"Best machine ({bestMachine}, {machines[bestMachine]}) for task ({task}) with alignment {bestAlignment} found in {time * 1000}ms.")


