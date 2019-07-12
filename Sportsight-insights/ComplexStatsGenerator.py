#
# set the google credentials env. variable
from datetime import datetime as dt
import os
import multiprocessing
import time
import pandas as pd
import BigqueryUtils as bqu
import ProUtils as pu

targetDataset = 'Sportsight_Stats_v2'
targetTableFormat = 'Stat_{SportCode}_{StatSource}_{StatName}_{StatObjects}_{StatTimeframes}'

class ComplexStatsGenerator():
    #
    # read in the configurations
    def __init__(self):
        #
        # get the initial configuration
        self.configsheet_url = 'https://docs.google.com/spreadsheets/d/1gwtQlzk0iA4qyLzqaYEk5SggOqNZtJnSSfwnZYDNlAw/export?format=csv&gid={SheetId}&run=1'
        statsconfig_df = pd.read_csv(self.configsheet_url.replace('{SheetId}', '284194018')).fillna('')
        self.sourcesConfigDict = pu.ProUtils.pandas_df_to_dict(statsconfig_df, 'Configname')
        self.statsDefDict = {}
        self.sport_configs = {}
        for sourceConfig in self.sourcesConfigDict.values():
            self.get_source_configuration(sourceConfig['Configname'])
        # print(sourceConfig)

        self.consumerStatus = multiprocessing.Queue()
        self.sentinel = 'Done'
        self.bqUtils = bqu.BigqueryUtils()

    def get_source_configuration(self, configName):
        sourceConfig = self.sourcesConfigDict[configName]
        sheetId = sourceConfig['SportSheetId']
        #
        # read all relevant metrics
        if sheetId not in self.sport_configs.keys():
            self.sport_configs[sheetId] = pd.read_csv(
                self.configsheet_url.replace('{SheetId}', str(sourceConfig['SportSheetId']))).fillna('')
            #self.sport_configs[sheetId]['SportCode'] = sourceConfig['SportCode']
            self.statsDefDict.update(pu.ProUtils.pandas_df_to_dict(self.sport_configs[sheetId], 'StatName'))
        if 'query' not in sourceConfig.keys():
            sourceConfig['query'] = open('./Queries/' + sourceConfig['QueryFile'], 'r').read()

        self.sourcesConfigDict[configName] = sourceConfig
        return sourceConfig

    def queryExecutor(self, i, query_jobs):
        #
        # execute a list of query jobs
        print('Start executor %d' % i)
        startTime = dt.now()
        for queryJob in iter(query_jobs.get, self.sentinel):
            #
            # to enforce the schema is correct, we first copy the empty table from the schema template
            # and then append the result to this empty table
            try:
                nRows = self.bqUtils.execute_query_with_schema_and_target(**queryJob['params'])
                print('Returened for Statname: {} ({} rows), StatObject: {}, StatTimeframe: {}, Detlatime: {}'.format(
                    queryJob['StatName'],
                    nRows,
                    queryJob['StatObject'],
                    queryJob['StatTimeframe'],
                    dt.now() - startTime),
                    flush=True)
                query_jobs.task_done()
            except Exception as e:
                # print(queryJob['query'],flush=True)
                print('Error {} with Statname: {}, StatObject: {}, StatTimeframe: {}'.format(e,
                                                                                             queryJob['StatName'],
                                                                                             queryJob['StatObject'],
                                                                                             queryJob['StatTimeframe']),
                      flush=True)
        print('Consumer {} terminates, Deltatime: {}'.format(str(i), dt.now() - startTime), flush=True)

    def queriesGenerator(self, queriesQueue, numExecutors, all=False, sports=[], sources=[], stats=[]):

        startTime = dt.now()
        #
        # Make sure the target dataset exists
        self.bqUtils.create_dataset(targetDataset)
        #
        # create jobs for all relevant metrics.
        for statDef in self.statsDefDict.values():
            if all or statDef['SportCode'] in sports or statDef['StatSource'] in sources or statDef[
                'StatName'] in stats:
                sourceConfig = self.get_source_configuration(
                    '{}.{}'.format(statDef['SportCode'], statDef['StatSource']))
                sourceDefinitions = definitions[sourceConfig['Configname']]

                print('Metric: {}, Sport:{}, Delta time: {}'.format(statDef['StatName'],
                                                                    statDef['SportCode'],
                                                                    dt.now() - startTime),
                      flush=True)

                for statObject in statDef['StatObject'].split(','):
                    for statTimeframe in sourceConfig['Yaha'].split(','):
                        query = sourceConfig['query']
                        query = query.replace('{StatObject}', statObject)
                        query = query.replace('{StatTimeframe}', statTimeframe)
                        query = pu.ProUtils.format_string(query, sourceDefinitions['StatObject'][statObject])
                        # query=pu.ProUtils.format_string(query, sourceDefinitions['StatTimeframe'][statTimeframe])
                        query = pu.ProUtils.format_string(query, statDef)
                        query = pu.ProUtils.format_string(query, sourceConfig)
                        # print (query)
                        #
                        # define the destination table
                        instructions = statDef
                        instructions['StatObject'] = statDef['StatObject']
                        instructions['StatTimeframe'] = statDef['StatObject']
                        targetTable = pu.ProUtils.format_string(targetTableFormat, instructions).replace('.', '_')
                        jobDefinition = {
                            'params': {
                                'query': query,
                                'targetDataset': targetDataset,
                                'targetTable': targetTable,
                            },
                            'StatName': statDef['StatName'],
                            'StatObject': statObject,
                            'StatTimeframe': statTimeframe
                        }
                        queriesQueue.put(jobDefinition)
        for i in range(numExecutors):
            queriesQueue.put(self.sentinel)  # indicate sentinel

    def run(self, all=False, sports=[], sources=[], stats=[]):
        #
        # main method

        startTime = dt.now()
        queriesQueue = multiprocessing.JoinableQueue()  # start a joinable queue to pass messages

        numExecutors = multiprocessing.cpu_count() * 5
        if len(stats) > 0:
            numExecutors = len(stats) * 2
        producer = multiprocessing.Process(name='QueriesGenerator',
                                           target=self.queriesGenerator,
                                           args=(queriesQueue, numExecutors,),
                                           kwargs={'all': all, 'sports': sports, 'sources': sources, 'stats': stats})
        producer.start()
        queriesQueue.join()
        #
        # initate consumers
        # consumer will execute the job
        consumers = [multiprocessing.Process(name='QueriesExecutor',
                                             target=self.queryExecutor,
                                             args=(i, queriesQueue,)) for i in range(numExecutors)]
        for c in consumers:
            c.start()

        while True:
            if any(c.is_alive() for c in consumers):
                time.sleep(1)
            else:
                print('Done')
                break


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../sportsight-tests.json"
generator = SimpleStatsGenerator()  # 'Baseball.PlayerSeasonStats')
generator.run(all=True)
# 7generator.run(stats=['pitching.pitchesPerInning'])
