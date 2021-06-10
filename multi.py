import multiprocessing as mp
import os
from time import time

eTime = time()

def f(name):
    print 'hello', name

if __name__ == '__main__':
    pool = mp.Pool(5) #use all available cores, otherwise specify the number you want as an argument
    for i in xrange(0, 64):
        pool.apply_async(f, args=(i,))
    pool.close()
    pool.join()

sTime = time()
print "Parallel took", eTime-sTime, "seconds"

