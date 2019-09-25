import os
import multiprocessing
import pandas as pd
import random
import time
from datetime import datetime as dt
from datetime import timedelta

from contendo_utils import BigqueryUtils
from contendo_utils import ProUtils

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
definitions['PBPv2'] = definitions['PBP']
targetDataset = 'Sportsight_Stats_v2'
targetTableFormat = 'Stat_{SportCode}_{StatTimeframe}_{StatSource}_{StatName}_{StatObject}'

class SimpleStatsGenerator():
    #
    # read in the configurations
    def __init__(self, root):
        #
        # get the initial configuration
        self.root = root
        self.configsheet_url = 'https://docs.google.com/spreadsheets/d/1gwtQlzk0iA4qyLzqaYEk5SggOqNZtJnSSfwnZYDNlAw/export?format=csv&gid={SheetId}&run=1'
        sourceConfigDF = pd.read_csv(self.configsheet_url.format(SheetId='284194018')).fillna('')
        sourceConfigDF['enriched'] = False
        self.sourcesConfigDict = ProUtils.pandas_df_to_dict(sourceConfigDF, 'Configname')
        self.sport_configs = {}
        self.TRUE = True

        #
        # read IMDB title definitions
        #titleTypesDF = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,'1802180540')).fillna('')
        #self.titletypesConfigDict = ProUtils.pandas_df_to_dict(titleTypesDF, 'TitleType')

        #print(sourceConfig)

        self.consumerStatus = multiprocessing.Queue()
        self.sentinel = 'Done'
        self.bqUtils = BigqueryUtils()

    def get_source_configuration(self, configName):
        sourceConfig = self.sourcesConfigDict[configName]
        if sourceConfig['DoIT']!='y' or sourceConfig['enriched']==True:
            return sourceConfig

        sheetId = sourceConfig['SportSheetId']
        #
        # read all relevant metrics
        if sheetId not in self.sport_configs.keys():
            self.sport_configs[sheetId] = pd.read_csv(self.configsheet_url.format(SheetId = str(sourceConfig['SportSheetId']))).fillna('')
            self.sport_configs[sheetId]['SportCode'] = sourceConfig['SportCode']

        sourceConfig['StatsDefDict'] = ProUtils.pandas_df_to_dict(self.sport_configs[sheetId], 'StatName')

        if 'query' not in sourceConfig.keys():
            sourceConfig['query'] = open(self.root + '/Queries/' + sourceConfig['QueryFile'], 'r').read()

        sourceConfig['enriched'] = True
        self.sourcesConfigDict[configName] = sourceConfig
        return sourceConfig

    def queryExecutor(self, i, query_jobs):
        #
        # execute a list of query jobs
        #print('Start executor %d' % i)
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
                queryFile = 'results/queries/{}.sql'.format(queryJob['params']['targetTable'])
                f = open(queryFile, 'w')
                f.write(queryJob['params']['query'])
                f.close()
            except Exception as e:
                queryFile = 'errors/{}.sql'.format(queryJob['params']['targetTable'])
                f = open(queryFile, 'w')
                f.write(queryJob['params']['query'])
                f.close()
                # print(queryJob['query'],flush=True)
                print('Error {} with Statname: {}, StatObject: {}, StatTimeframe: {}'.format(e,
                                                                                       queryJob['StatName'],
                                                                                       queryJob['StatObject'],
                                                                                       queryJob['StatTimeframe']),
                                                                                       flush=True)
        #print('Consumer {} terminates, Deltatime: {}'.format(str(i), dt.now() - startTime), flush=True)

    def queriesGenerator(self, queriesQueue, numExecutors, configurations=None, stats=None):
        startTime = dt.now()
        #
        # Make sure the target dataset exists
        self.bqUtils.create_dataset(targetDataset)
        #
        # if there are only partial list of configurations
        if not configurations:
            configurations = self.sourcesConfigDict.keys()
        #
        # loop over all configurations and generate
        #print(configurations)
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
            generatorFunc(queriesQueue, sourceConfig, startTime, stats)
        #
        # Set the sentinel for all processes.
        for i in range(numExecutors):
            queriesQueue.put(self.sentinel)  # indicate sentinel

    def financeQueriesGenerator(self, queriesQueue, sourceConfig, startTime, stats=None):
        #
        # target table definitions
        financeTableFormat = 'Stat_Finance_{StatSource}_{StatName}_{StatObject}_Rolling_{RollingDays}'
        financeStatsDataset = 'Finance_Stats'
        self.bqUtils.create_dataset(financeStatsDataset)

        #
        # create jobs for all relevant metrics.
        for statDef in sourceConfig['StatsDefDict'].values():

            if statDef['Doit']!='y':
                continue

            #print('Metric: {}, Sport:{}, Delta time: {}'.format(statDef['StatName'], statDef['SportCode'], dt.now() - startTime), flush=True)

            for statObject in statDef['StatObject'].split(',')[:1]:
                for rollingDays in statDef['RollingDaysList'].split(','):
                    _statDef = statDef.copy()
                    _statDef['StatObject'] = statObject
                    rollingDaysInst = {'RollingDays': rollingDays}
                    query = sourceConfig['query']
                    query=ProUtils.format_string(query, _statDef)
                    query=ProUtils.format_string(query, sourceConfig)
                    query=ProUtils.format_string(query, rollingDaysInst)
                    #print (query)
                    #
                    # define the destination table
                    instructions = _statDef
                    instructions['StatTimeframe'] = sourceConfig['StatTimeframe']
                    instructions['StatSource'] = sourceConfig['StatSource']
                    instructions['RollingDays'] = rollingDays
                    targetTable = ProUtils.format_string(financeTableFormat, instructions).replace('.', '_').replace('-', '_')
                    jobDefinition = {
                        'params': {
                            'query': query,
                            'targetDataset': financeStatsDataset,
                            'targetTable': targetTable,
                        },
                        'StatName': _statDef['StatName'],
                        'StatObject': statObject,
                        'StatTimeframe': '{}_Rollingdays'.format(rollingDays)
                    }
                    queriesQueue.put(jobDefinition)

    def imdbQueriesGenerator(self, queriesQueue, sourceConfig, startTime, stats=None):

        #
        # create jobs for all relevant metrics.
        for statDef in sourceConfig['StatsDefDict'].values():

            if statDef['Doit']!='y':
                continue

            #print('Metric: {}, Sport:{}, Delta time: {}'.format(statDef['StatName'], statDef['SportCode'], dt.now() - startTime), flush=True)

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
                    query=ProUtils.format_string(query, _statDef)
                    query=ProUtils.format_string(query, sourceConfig)
                    query=ProUtils.format_string(query, titletypeConfig)
                    #print (query)
                    #
                    # define the destination table
                    instructions = _statDef
                    instructions['StatTimeframe'] = sourceConfig['StatTimeframe']
                    instructions['StatSource'] = sourceConfig['StatSource']
                    targetTable = ProUtils.format_string(targetTableFormat, instructions).replace('.', '_').replace('-', '_')
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

                    questionDef['Question2Objects'] = ProUtils.format_string(statDef['Question2Objects'], questionDef)
                    questionsList.append(questionDef)

        keys = ['QuestionCode', 'StatName', 'Genre', 'Level', 'ObjectDisplayName', 'Question2Objects',
                'QuestionNObjects', 'StatObject', 'TitleType', 'Value1Template', 'Value2Template']
        questionsDF = pd.DataFrame(questionsList, columns=keys)
        questionsDF.to_csv('imdb_questionsList.csv')

    def game_time(self):
        instructions = {}
        instructions['StatCondition'] = 'TRUE'
        instructions['DaysRange'] = 'CAST(game.startTime as STRING)'
        return instructions

    def days_range(self, interval, prev):
        instructions = {}
        startDate = (dt.today()-timedelta(days=interval+prev-1))
        endDate = (dt.today()-timedelta(days=prev))
        condTemplate = '{DateProperty} BETWEEN "{StartDate}" and "{EndDate}"'
        condInst = {'StartDate': startDate.strftime('%Y%m%d'), 'EndDate': endDate.strftime('%Y%m%d')}
        instructions['StatCondition'] = ProUtils.format_string(condTemplate, condInst)
        instructions['DaysRange'] = '"{}...{}"'.format(startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'))
        return instructions

    def games_days_range(self, interval, prev):
        instructions = {}
        startDate = (dt.today()-timedelta(days=interval+prev-1))
        endDate = (dt.today()-timedelta(days=prev))
        condTemplate = '{DateProperty} BETWEEN "{StartDate}" and "{EndDate}"'
        condInst = {'StartDate': startDate.strftime('%Y%m%d'), 'EndDate': endDate.strftime('%Y%m%d')}
        instructions['StatCondition'] = ProUtils.format_string(condTemplate, condInst)
        instructions['DaysRange'] = '"N/A"'
        return instructions

    def mlbpbpQueriesGenerator(self, queriesQueue, sourceConfig, startTime, stats=None):
        #
        # create jobs for all relevant metrics.

        #
        # Get the dimentions and player/team map definitions.
        dimentionsDF = pd.read_csv(self.configsheet_url.format(SheetId='550054182')).fillna('')
        dimentionsDict = ProUtils.pandas_df_to_dict(dimentionsDF, 'ConditionCode')
        ptmapDF = pd.read_csv(self.configsheet_url.format(SheetId='1185264127')).fillna('')
        ptmapDict = ProUtils.pandas_df_to_dict(ptmapDF, 'Object')

        for statDef in sourceConfig['StatsDefDict'].values():
            if stats:
                if statDef['StatName'] not in stats:
                    continue
            #
            # only produce if the condition is defined.
            if statDef['Condition']=='':
                continue

            if statDef['DoIT']!='y':
                continue

            sourceDefinitions = definitions[sourceConfig['StatSource']]
            statTimeframe = sourceConfig['StatTimeframe']
            for condCode, condDef in dimentionsDict.items():
                for object, objectDef in ptmapDict.items():
                    #
                    # only do if defined as 1
                    if condDef[object] != 1 or statDef[object]=='' or condDef['Condition']=='':
                        continue

                    statObject = objectDef['StatObject']

                    query = sourceConfig['query']
                    query = query.replace('{StatObject}', statObject)
                    query = query.replace('{StatTimeframe}', statTimeframe)
                    if sourceConfig['StatCondition'] != '':
                        query = ProUtils.format_string(query, eval("self."+sourceConfig['StatCondition']))
                    else:
                        query = ProUtils.format_string(query, {'StatCondition': True})

                    queryInst = {
                        'DaysRange':        sourceConfig['DateProperty'],
                        'PBPCondition':     '{} and {}'.format(condDef['Condition'], statDef['Condition']),
                        'StatName':         'pbp.{}.{}.{}'.format(statDef['StatName'], object, condCode),
                        'BaseStat':         statDef['StatName'],
                        'ObjectType':       object,
                        'ConditionCode':    condCode,
                        'TeamType':         objectDef['TeamType'],
                        'PlayerProperty':   statDef[object],
                        #'PropertyName':     '{}{}{}'.format(objectDef['Prefix'], statDef[object], objectDef['Suffix']),
                    }

                    query = ProUtils.format_string(query, sourceDefinitions['StatObject'][statObject])
                    query=ProUtils.format_string(query, queryInst)
                    query=ProUtils.format_string(query, statDef)
                    query=ProUtils.format_string(query, sourceConfig)
                    #
                    # define the destination table
                    instructions = statDef.copy()
                    instructions['StatName'] = queryInst['StatName']
                    instructions['StatObject'] = statObject
                    instructions['StatTimeframe'] = statTimeframe
                    instructions['StatSource'] = sourceConfig['StatSource']
                    instructions['DaysRange'] = instructions.get('DaysRange', 'N/A')
                    targetTable = ProUtils.format_string(targetTableFormat, instructions).replace('.', '_')
                    jobDefinition = {
                        'params': {
                            'query': query,
                            'targetDataset': targetDataset,
                            'targetTable': targetTable,
                        },
                        'StatName': queryInst['StatName'],
                        'StatObject': statObject,
                        'StatTimeframe': statTimeframe
                    }
                    queriesQueue.put(jobDefinition)

    def mlbpbpQuestionsDefGenerator(self):
        #
        # create jobs for all relevant metrics.
        questionsList=[]
        sourceConfig = self.get_source_configuration('Baseball.PBPv2.Season')

        #
        # Get the dimentions and player/team map definitions.
        dimentionsDF = pd.read_csv(self.configsheet_url.format(SheetId='550054182')).fillna('')
        dimentionsDict = ProUtils.pandas_df_to_dict(dimentionsDF, 'ConditionCode')
        ptmapDF = pd.read_csv(self.configsheet_url.format(SheetId='1185264127')).fillna('')
        ptmapDict = ProUtils.pandas_df_to_dict(ptmapDF, 'Object')

        for statDef in sourceConfig['StatsDefDict'].values():
            #
            # only produce if the condition is defined.
            if statDef['Condition']=='':
                continue

            timeFrameTexts = sourceConfig['Timeframe'].split(',')

            sourceDefinitions = definitions[sourceConfig['StatSource']]
            statTimeframe = sourceConfig['StatTimeframe']
            for condCode, condDef in dimentionsDict.items():
                for object, objectDef in ptmapDict.items():
                    #
                    # only do if defined as 1
                    if condDef[object] != 1 or statDef[object]=='' or condDef['Condition']=='':
                        continue
                    #
                    # build the statname
                    statName = 'pbp.{}.{}.{}'.format(statDef['StatName'], object, condCode)
                    #
                    # calculate the question string
                    if statDef['Owner'] != objectDef['TeamType']:
                        allowedText = statDef['AllowedText']
                    else:
                        allowedText = ''
                    questionInst = {
                        'Conditiontext': condDef['Conditiontext'],
                        'ObjectType': objectDef['ObjectType'],
                        'Allowed': allowedText,
                        'Timeframe': timeFrameTexts[random.randint(0,len(timeFrameTexts)-1)],
                    }
                    question = ProUtils.format_string(statDef['Question2AnswersTemplate'], questionInst)
                    while question.find('  ')  > -1:
                        question = question.replace('  ', ' ')
                    #
                    # set the question definition.
                    questionDef = {
                        'QuestionCode': statName,
                        'StatName': statName,
                        'BaseStat': statDef['StatName'],
                        'StatObject': objectDef['StatObject'],
                        'Level': '',
                        'Value1Template': 'INT',
                        'Value2Template': 'INT',
                        'Question2Objects': question,
                    }
                    questionsList.append(questionDef)

        keys = questionDef.keys()
        questionsDF = pd.DataFrame(questionsList, columns=keys)
        questionsDF.to_csv('mlb-pbp-questionsList.csv')

    def sportsQueriesGenerator(self, queriesQueue, sourceConfig, startTime, stats=None):
        #
        # create jobs for all relevant metrics.
        for statDef in sourceConfig['StatsDefDict'].values():

            if statDef['Doit']!='y':
                continue

            #print('Metric: {}, Sport:{}, Delta time: {}'.format(statDef['StatName'], statDef['SportCode'], dt.now() - startTime), flush=True)

            sourceDefinitions = definitions[sourceConfig['StatSource']]

            for statObject in statDef['StatObject'].split(','):
                for statTimeframe in sourceConfig['StatTimeframe'].split(','):
                    query = sourceConfig['query']
                    query = query.replace('{StatObject}', statObject)
                    query = query.replace('{StatTimeframe}', statTimeframe)
                    if sourceConfig['StatCondition'] != '':
                        query = ProUtils.format_string(query, eval("self."+sourceConfig['StatCondition']))
                    else:
                        query = ProUtils.format_string(query, {'StatCondition': True})

                    query = ProUtils.format_string(query, sourceDefinitions['StatObject'][statObject])
                    query=ProUtils.format_string(query, statDef)
                    query=ProUtils.format_string(query, sourceConfig)
                    #print (query)
                    #
                    # define the destination table
                    instructions = statDef
                    instructions['StatObject'] = statObject
                    instructions['StatTimeframe'] = statTimeframe
                    instructions['StatSource'] = sourceConfig['StatSource']
                    instructions['DaysRange'] = instructions.get('DaysRange', 'N/A')
                    targetTable = ProUtils.format_string(targetTableFormat, instructions).replace('.', '_')
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

    def complexQueriesGenerator(self, queriesQueue, sourceConfig, startTime, stats=None):
        #
        # create jobs for all relevant metrics.
        for statDef in sourceConfig['StatsDefDict'].values():

            if statDef['Doit']!='y':
                continue

            #print('Metric: {}, Sport:{}, Delta time: {}'.format(statDef['StatName'], statDef['SportCode'], dt.now() - startTime), flush=True)
            inst={}
            inst ['StatTimeframes'] = ProUtils.commastring_to_liststring(statDef['StatTimeframes'])
            inst['StatObjects'] = ProUtils.commastring_to_liststring(statDef['StatObjects'])
            inst['NumeratorStatNames'] = ProUtils.commastring_to_liststring(statDef['NumeratorStatNames'])
            inst['DenominatorStatNames'] = ProUtils.commastring_to_liststring(statDef['DenominatorStatNames'])
            query = sourceConfig['query']
            query=ProUtils.format_string(query, inst)
            query=ProUtils.format_string(query, statDef)
            query=ProUtils.format_string(query, sourceConfig)
            #print (query)
            #
            # define the destination table
            instructions = statDef
            instructions['StatObject'] = statDef['StatObjects'].replace(',', '_')
            instructions['StatTimeframe'] = statDef['StatTimeframes'].replace(',', '_')
            instructions['StatSource'] = sourceConfig['StatSource']
            targetTable = ProUtils.format_string(targetTableFormat, instructions).replace('.', '_')
            jobDefinition = {
                'params': {
                    'query': query,
                    'targetDataset': targetDataset,
                    'targetTable': targetTable,
                },
                'StatName': statDef['StatName'],
                'StatObject': instructions['StatObject'],
                'StatTimeframe': instructions['StatTimeframe']
            }
            queriesQueue.put(jobDefinition)

    def run(self, configurations=None, stats=None, numExecutors=0):
        #
        # main method

        startTime = dt.now()
        queriesQueue = multiprocessing.JoinableQueue()  # start a joinable queue to pass messages

        if numExecutors==0:
            numExecutors = multiprocessing.cpu_count() * 8
        producer = multiprocessing.Process(name='QueriesGenerator',
                                           target=self.queriesGenerator,
                                           args=(queriesQueue, numExecutors,),
                                           kwargs={
                                               'configurations': configurations,
                                               'stats': stats,
                                           }
                                           )
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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
    root = os.getcwd() #+ '/sportsight-core/Sportsight-insights'
    os.chdir('{}/tmp/'.format(os.environ["HOME"]))
    generator = SimpleStatsGenerator(root)#'Baseball.PlayerSeasonStats')
    #generator.run()
    generator.run(configurations=['Finance.EOD'])
    #print(generator.days_range(30,1))
    #generator.imdbQuestionsDefGenerator()

if __name__ == '__main__':
    #print(globals())
    test()