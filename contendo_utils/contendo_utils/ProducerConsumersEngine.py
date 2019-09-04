
from datetime import datetime as dt, timedelta
import os
import multiprocessing
import time
import pandas as pd

class ProducerConsumersEngine():
    #
    # read in the configurations
    def __init__(self, producerFunc):
        #
        # get the initial configuration
        #self.consumerStatus = multiprocessing.Queue()
        self.sentinel = 'Done'
        self.producerFunc = producerFunc
        self.jobsQueue = multiprocessing.JoinableQueue()  # start a joinable queue to pass messages

    def main_consumer_func(self, i):
        #
        # execute a list of query jobs
        print('Start executor %d' % i)
        startTime = dt.now()
        try:
            for jobData in iter(self.jobsQueue.get, self.sentinel):
                #
                # to enforce the schema is correct, we first copy the empty table from the schema template
                # and then append the result to this empty table
                try:
                    jobData.handler(jobData.instructions, startTime)
                    self.jobsQueue.task_done()

                except Exception as e:
                    print('Error {} with Jobtype: {}, Instructions: {}'.format(e, jobData.type, jobData.instructions))

        except Exception as e:
            print("Error {} in self.jobsQueue.get".format(e))

    def main_producer_func(self, numExecutors, configurations=[]):
        startTime = dt.now()
        self.producerFunc(configurations, startTime)
        #
        # Set the sentinel for all processes.
        for i in range(numExecutors):
            self.jobsQueue.put(self.sentinel)  # indicate sentinel

    def run(self, configurations=[], numExecutors=0):
        #
        # main method

        startTime = dt.now()

        if numExecutors==0:
            numExecutors = multiprocessing.cpu_count() * 8

        producer = multiprocessing.Process(name='main_producer_func',
                                           target=self.main_producer_func,
                                           args=(numExecutors,),
                                           kwargs={'configurations': configurations})
        producer.start()
        self.jobsQueue.join()
        #
        # initate consumers
        # consumer will execute the job
        consumers = [multiprocessing.Process(name='main_consumer_func',
                                             target=self.main_consumer_func,
                                             args=(i, )) for i in range(numExecutors)]
        for c in consumers:
            c.start()

        while True:
            if any(c.is_alive() for c in consumers):
                time.sleep(1)
            else:
                print('Done')
                break
    #
    # define the job class
    class JobData():
        def __init__(self, handler, inst={}):
            self.handler = handler
            self.instructions = inst


def test():
    generator.run(configurations=['Baseball.GameStats.Last7Days'])

#test()