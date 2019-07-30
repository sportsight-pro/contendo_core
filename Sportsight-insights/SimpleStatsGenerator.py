from datetime import datetime as dt, timedelta
import os
import multiprocessing
import time
import pandas as pd
import BigqueryUtils as bqu
import ProUtils as pu

definitions = {
    'SeasonStats': {
        'StatObject': {
            'team': {'PlayerCode': '"N/A"'},
            'player': {'PlayerCode': 'player.id'},
        },
    },
    'GameStats': {
        'StatObject': {
            'team': {'PlayerCode': '"N/A"'},
            'player': {'PlayerCode': 'player.id'},
        },
    },
    'PBP': {
        'StatObject': {
            'team': {'PlayerProperty': '"N/A"'},
            'player': {},
        },
    },
}
targetDataset = 'Sportsight_Stats_v2'
targetTableFormat = 'Stat_{SportCode}_{StatSource}_{StatName}_{StatObject}_{StatTimeframe}'

class SimpleStatsGenerator():
    #
    # read in the configurations
    def __init__(self, root):
        #
        # get the initial configuration
        self.root = root
        self.configsheet_url = 'https://docs.google.com/spreadsheets/d/1gwtQlzk0iA4qyLzqaYEk5SggOqNZtJnSSfwnZYDNlAw/export?format=csv&gid={SheetId}&run=1'
        sourceConfigDF = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,'284194018')).fillna('')
        sourceConfigDF['enriched'] = False
        self.sourcesConfigDict = pu.ProUtils.pandas_df_to_dict(sourceConfigDF, 'Configname')
        self.sport_configs = {}
        self.TRUE = True

        #
        # read IMDB title definitions
        titleTypesDF = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,'1802180540')).fillna('')
        self.titletypesConfigDict = pu.ProUtils.pandas_df_to_dict(titleTypesDF, 'TitleType')

        #print(sourceConfig)

        self.consumerStatus = multiprocessing.Queue()
        self.sentinel = 'Done'
        self.bqUtils = bqu.BigqueryUtils()

    def get_source_configuration(self, configName):
        sourceConfig = self.sourcesConfigDict[configName]
        if sourceConfig['DoIT']!='y' or sourceConfig['enriched']==True:
            return sourceConfig

        sheetId = sourceConfig['SportSheetId']
        #
        # read all relevant metrics
        if sheetId not in self.sport_configs.keys():
            self.sport_configs[sheetId] = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,str(sourceConfig['SportSheetId']))).fillna('')
            self.sport_configs[sheetId]['SportCode'] = sourceConfig['SportCode']

        sourceConfig['StatsDefDict'] = pu.ProUtils.pandas_df_to_dict(self.sport_configs[sheetId], 'StatName')

        if 'query' not in sourceConfig.keys():
            sourceConfig['query'] = open(self.root + '/Queries/' + sourceConfig['QueryFile'], 'r').read()

        sourceConfig['enriched'] = True
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

    def queriesGenerator(self, queriesQueue, numExecutors, configurations=[]):
        startTime = dt.now()
        #
        # Make sure the target dataset exists
        self.bqUtils.create_dataset(targetDataset)
        #
        # if there are only partial list of configurations
        if len(configurations) == 0 :
            configurations = self.sourcesConfigDict.keys()
        #
        # loop over all configurations and generate
        print(configurations)
        for sourceConfigName in configurations:
            #
            # get the source configuration
            sourceConfig = self.get_source_configuration(sourceConfigName)
            #
            # make sure it is required.
            if sourceConfig['DoIT']!='y':
                continue
            #
            # call the relevant generation function.
            print ("running configuration {}".format(sourceConfigName))
            generatorFunc = eval('self.{}'.format(sourceConfig['generatorFunc']))
            generatorFunc(queriesQueue, sourceConfig, startTime)
        #
        # Set the sentinel for all processes.
        for i in range(numExecutors):
            queriesQueue.put(self.sentinel)  # indicate sentinel

    def imdbQueriesGenerator(self, queriesQueue, sourceConfig, startTime):

        #
        # create jobs for all relevant metrics.
        for statDef in sourceConfig['StatsDefDict'].values():

            if statDef['Doit']!='y':
                continue

            print('Metric: {}, Sport:{}, Delta time: {}'.format(statDef['StatName'],
                                                                statDef['SportCode'],
                                                                dt.now() - startTime),
                  flush=True)

            for titleType in statDef['TitleType'].split(','):
                titletypeConfig = self.titletypesConfigDict[titleType]
                if statDef['Genres']=='y':
                    genresList = titletypeConfig['GenresList'].split(',')
                else:
                    genresList = ['All']

                for genre in genresList:
                    _statDef = statDef.copy()
                    query = sourceConfig['query']
                    if genre=='All':
                        _statDef['StatCondition'] = ''
                    else:
                        _statDef['StatCondition'] = 'AND STRPOS(Genres, "{}")>0'.format(genre)
                        _statDef['StatName'] = '{}.{}'.format(statDef['StatName'], genre)
                    _statDef['TitleType'] = titleType
                    _statDef['Genre'] = genre
                    _statDef['StatObject'] = titleType
                    query=pu.ProUtils.format_string(query, _statDef)
                    query=pu.ProUtils.format_string(query, sourceConfig)
                    query=pu.ProUtils.format_string(query, titletypeConfig)
                    #print (query)
                    #a=b
                    #
                    # define the destination table
                    instructions = _statDef
                    instructions['StatTimeframe'] = sourceConfig['StatTimeframe']
                    instructions['StatSource'] = sourceConfig['StatSource']
                    targetTable = pu.ProUtils.format_string(targetTableFormat, instructions).replace('.', '_').replace('-', '_')
                    jobDefinition = {
                        'params': {
                            'query': query,
                            'targetDataset': targetDataset,
                            'targetTable': targetTable,
                        },
                        'StatName': _statDef['StatName'],
                        'StatObject': titleType,
                        'StatTimeframe': sourceConfig['StatTimeframe']
                    }
                    queriesQueue.put(jobDefinition)

    def imdbQuestionsDefGenerator(self):
        #
        # create jobs for all relevant metrics.
        questionsList=[]
        sourceConfig = self.get_source_configuration('Entertainmant.IMDB')

        for statDef in sourceConfig['StatsDefDict'].values():

            for titleType in statDef['TitleType'].split(','):
                titletypeConfig = self.titletypesConfigDict[titleType]
                if statDef['Genres']=='y':
                    genresList = titletypeConfig['GenresList'].split(',')
                else:
                    genresList = ['All']

                for genre in genresList:
                    questionDef = {}
                    questionDef['QuestionCode'] = '{}.{}'.format(titleType, statDef['StatName'])
                    questionDef['StatName'] = statDef['StatName']
                    questionDef['StatObject'] = titleType
                    questionDef['Genre'] = ''
                    questionDef['TitleType'] = titleType
                    questionDef['Level'] = 'Easy'
                    questionDef['Value1Template'] = statDef['Value1Template']
                    questionDef['Value2Template'] = statDef['Value2Template']
                    questionDef['ObjectDisplayName'] = titletypeConfig['ObjectDisplayName']

                    questionDef['QuestionNObjects'] = ''
                    if genre!='All':
                        questionDef['QuestionCode'] = '{}.{}'.format(questionDef['QuestionCode'], genre)
                        questionDef['StatName'] = '{}.{}'.format(questionDef['StatName'], genre)
                        questionDef['Genre'] = genre+' '

                    questionDef['Question2Objects'] = pu.ProUtils.format_string(statDef['Question2Objects'], questionDef)
                    questionsList.append(questionDef)

        keys = ['QuestionCode', 'StatName', 'Genre', 'Level', 'ObjectDisplayName', 'Question2Objects',
                'QuestionNObjects', 'StatObject', 'TitleType', 'Value1Template', 'Value2Template']
        questionsDF = pd.DataFrame(questionsList, columns=keys)
        questionsDF.to_csv('imdb_questionsList.csv')

    def days_range(self, interval, prev):
        instructions = {}
        startDate = (dt.today()-timedelta(days=interval+prev-1))
        endDate = (dt.today()-timedelta(days=prev))
        condTemplate = '{DateProperty} BETWEEN "{StartDate}" and "{EndDate}"'
        condInst = {'StartDate': startDate.strftime('%Y%m%d'), 'EndDate': endDate.strftime('%Y%m%d')}
        instructions['StatCondition'] = pu.ProUtils.format_string(condTemplate, condInst)
        instructions['DaysRange'] = '{}...{}'.format(startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'))
        return instructions

    def games_days_range(self, interval, prev):
        instructions = {}
        startDate = (dt.today()-timedelta(days=interval+prev-1))
        endDate = (dt.today()-timedelta(days=prev))
        condTemplate = '{DateProperty} BETWEEN "{StartDate}" and "{EndDate}"'
        condInst = {'StartDate': startDate.strftime('%Y%m%d'), 'EndDate': endDate.strftime('%Y%m%d')}
        instructions['StatCondition'] = pu.ProUtils.format_string(condTemplate, condInst)
        instructions['DaysRange'] = 'N/A'
        return instructions

    def sportsQueriesGenerator(self, queriesQueue, sourceConfig, startTime):
        #
        # create jobs for all relevant metrics.
        for statDef in sourceConfig['StatsDefDict'].values():

            if statDef['Doit']!='y':
                continue

            print('Metric: {}, Sport:{}, Delta time: {}'.format(statDef['StatName'],
                                                                statDef['SportCode'],
                                                                dt.now() - startTime),
                  flush=True)

            sourceDefinitions = definitions[sourceConfig['StatSource']]

            for statObject in statDef['StatObject'].split(','):
                for statTimeframe in sourceConfig['StatTimeframe'].split(','):
                    query = sourceConfig['query']
                    query = query.replace('{StatObject}', statObject)
                    query = query.replace('{StatTimeframe}', statTimeframe)
                    if sourceConfig['StatCondition'] != '':
                        query = pu.ProUtils.format_string(query, eval("self."+sourceConfig['StatCondition']))
                    else:
                        query = pu.ProUtils.format_string(query, {'StatCondition': True})

                    query = pu.ProUtils.format_string(query, sourceDefinitions['StatObject'][statObject])
                    query=pu.ProUtils.format_string(query, statDef)
                    query=pu.ProUtils.format_string(query, sourceConfig)
                    #print (query)
                    #
                    # define the destination table
                    instructions = statDef
                    instructions['StatObject'] = statObject
                    instructions['StatTimeframe'] = statTimeframe
                    instructions['StatSource'] = sourceConfig['StatSource']
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

    def run(self, configurations=[]):
        #
        # main method

        startTime = dt.now()
        queriesQueue = multiprocessing.JoinableQueue()  # start a joinable queue to pass messages

        numExecutors = multiprocessing.cpu_count() * 8

        producer = multiprocessing.Process(name='QueriesGenerator',
                                           target=self.queriesGenerator,
                                           args=(queriesQueue, numExecutors,),
                                           kwargs={'configurations': configurations})
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

def test():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../../sportsight-tests.json"
    root = os.getcwd() #+ '/sportsight-core/Sportsight-insights'
    generator = SimpleStatsGenerator(root)#'Baseball.PlayerSeasonStats')
    #generator.run()
    generator.run(configurations=['Baseball.PBP.Last7Days', 'Baseball.PBP.Last30Days', 'Baseball.PBP.Season'])
    #print(generator.days_range(30,1))
    #generator.imdbQuestionsDefGenerator()

#test()