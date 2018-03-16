import asyncio
import json
import numpy as np
from random import random
from collections import defaultdict


statistic = defaultdict(list)


q = asyncio.Queue()

async def producer(probability=0.8):
    global q
    while True:
        maybe = random()
        print(f"Current probability: {maybe}")
        if maybe <= probability:
            print("Put in queue")
            await q.put(1)
            print(f"queue size: {q.qsize()}")
        else:
            print("None")
        print("Sleeping\n")
        await asyncio.sleep(2)

async def consumer(mu=7, sigma=1):
    global q
    await asyncio.sleep(3)
    while True:
        normal_time = np.random.normal(mu, sigma)
        print("Get object from queue")
        if q.empty() and len([task for task in asyncio.Task.all_tasks() if not task.done()]) > 3:
            print(f"Queue is empty. Consumer Deleted\n")
            break
        print("Handling...")
        item = await q.get()
        print("Handled !!!\n")
        await asyncio.sleep(normal_time)

async def control():
    global loop
    global q
    global statistic
    while True:
        if q.qsize() >= 20:
            loop.create_task(consumer())
            print("Queue has too many objects. Another consumer created")
        queue_size_per_second = q.qsize()
        amount_task_per_second = len([task for task in asyncio.Task.all_tasks() if not task.done()])
        statistic['amount_task_per_second'].append(amount_task_per_second)
        statistic['queue_size_per_second'].append(queue_size_per_second)
        print('Active tasks count: ', amount_task_per_second, '\n')
        await asyncio.sleep(1)

try:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(
            producer(),
            consumer(),
            control()
        )
    )
    loop.close()
except:
    print("input to file")
    with open("statistic.txt",'w') as f:
        f.write(json.dumps(statistic))
