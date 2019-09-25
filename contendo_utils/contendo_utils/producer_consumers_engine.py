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
        self._sentinel = 'Done'
        self._producerFunc = producerFunc
        self._jobsQueue = multiprocessing.JoinableQueue()  # start a joinable queue to pass messages
        self._handlers = {}

    def register_handler(self, func):
        self._handlers[func.__name__] = func

    def main_consumer_func(self, i, **kwargs):
        #
        # execute a list of query jobs
        #print('Start executor %d' % i)
        try:
            for jobData in iter(self._jobsQueue.get, self._sentinel):
                #
                # to enforce the schema is correct, we first copy the empty table from the schema template
                # and then append the result to this empty table
                try:
                    assert jobData.handler in self._handlers
                    for key, value in kwargs.items():
                        jobData.instructions[key] = value
                    self._handlers[jobData.handler](**jobData.instructions)
                    self._jobsQueue.task_done()

                except Exception as e:
                    print('Error {} with Jobtype: {}, Instructions: {}'.format(e, jobData.handler, jobData.instructions))
                    raise e

        except Exception as e:
            print("Error {} in self.jobsQueue.get".format(e))
            raise e

    def main_producer_func(self, numExecutors, **kwargs):
        self._producerFunc(numExecutors, self._jobsQueue, **kwargs)
        #
        # Set the sentinel for all processes.
        for i in range(numExecutors):
            self._jobsQueue.put(self._sentinel)  # indicate sentinel

    def run(self, numExecutors=0, **kwargs):
        #
        # main method
        startTime = dt.now()

        if numExecutors==0:
            numExecutors = multiprocessing.cpu_count() * 8

        producer = multiprocessing.Process(name='main_producer_func',
                                           target=self.main_producer_func,
                                           args=(numExecutors,),
                                           kwargs=kwargs)
        producer.start()
        self._jobsQueue.join()
        #
        # initate consumers
        # consumer will execute the job
        consumers = [multiprocessing.Process(name='main_consumer_func',
                                             target=self.main_consumer_func,
                                             args=(i, ),
                                             kwargs=kwargs) for i in range(numExecutors)]
        for c in consumers:
            c.start()

        while True:
            if any(c.is_alive() for c in consumers):
                time.sleep(1)
            else:
                #print('Done')
                break
    #
    # define the job class
    class PCEngineJobData():
        def __init__(self, handler, inst={}):
            self.handler = handler.__name__
            self.instructions = inst


def test():
    pass

#test()