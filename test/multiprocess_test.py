import multiprocessing
import time

start_time = time.time()

def count(name):
    for i in range(5001):
        print(name, " : ", i)