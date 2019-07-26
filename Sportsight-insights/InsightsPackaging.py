from datetime import datetime as dt
import ProUtils as pu
import BigqueryUtils as bqu
import InsightsConfigurationManager as icm
import pandas as pd
import random

class InsightsPackaging:
    def __init__(self, root='.'):
        self.icm = icm.InsightsConfigurationManager()
        self.bqUtils = bqu.BigqueryUtils()
        self.questionsReaderQuery = open(root + '/Queries/QuestionsReaderQuery.sql', 'r').read()

    def two_answers_reader(self, contentConfigCode):
        configDef = self.icm.get_content_config(contentConfigCode)
        #
        # read the questions
        query = pu.ProUtils.format_string(self.questionsReaderQuery, configDef)
        questionsDF = self.bqUtils.execute_query_to_df(query)
        #
        # find all metrics within slot
        nSlots = configDef['NumSlots']
        slotStatGroups = {}
        slotStatGroupKeys = {}
        for i in range(1, nSlots+1):
            slotDF = questionsDF.query('slotNum == %d' % i)
            slotStatGroups[i] = slotDF.groupby(['QuestionCode', 'StatObject']).groups
            slotStatGroupKeys[i]= set(slotStatGroups[i].keys())

        return questionsDF, slotStatGroups, slotStatGroupKeys

    def two_answers_question_generator(self, questionDict, configDef):
        #print(questionDict)
        stat1 = questionDict['Stat1']
        stat2 = questionDict['Stat2']
        questionTemplate = stat1['Question2Objects']
        questionInstructions = stat1
        questionInstructions['Timeframe'] = configDef['TimeframeText']
        questionText = pu.ProUtils.format_string(questionTemplate, questionInstructions)
        templateDict = self.icm.templateDefsDict

        outQuestion = {
            'QuestionText': questionText,
            'Answer1': stat1['StatObjectName'],
            'Answer2': stat2['StatObjectName'],
            'Value1': str(eval(templateDict[stat1['Value1Template']]['Template'].replace('{value}', "stat1['StatValue']"))),
            'Value2': str(eval(templateDict[stat2['Value1Template']]['Template'].replace('{value}', "stat2['StatValue']"))),
        }
        questionKeys=[
            'ContentDefCode', 
            'SportCode',
            'StatSource',
            'slotNum',
            'rankDiff', 
            'StatObject', 
            'StatTimeframe', 
            'LeagueCode', 
            'SeasonCode', 
            'CompetitionStageCode', 
            'MatchStageCode', 
            'QuestionCode',
            'StatCode',
            'Description',
            'numRanks',
            'rankItemsCount',
            'valueRange',
            'internalDenseRank',
            'objectsCount',
            'minValue',
            'maxValue'
        ]
        statKeys = [
            'StatObjectName',
            'StatFunction',
            'MatchCode',
            'TeamCode',
            'PlayerCode', 
            'StatValue',
            'Count', 
            'DenseRank',
            'TeamName', 
            'PlayerName'
        ]
        pu.ProUtils.update_dict(outQuestion, stat1, questionKeys)
        pu.ProUtils.update_dict(outQuestion, questionDict, questionKeys)
        pu.ProUtils.update_dict(outQuestion, stat1, statKeys, '1')
        pu.ProUtils.update_dict(outQuestion, stat2, statKeys, '2')

        return outQuestion

    def two_answers_package_generator(self, contentConfigCode):
        configDef = self.icm.get_content_config(contentConfigCode)
        numPackages = configDef['NumPackages']
        numSlots = configDef['NumSlots']
        outputDF = pd.DataFrame()
        questionsDF, slotStatGroups, slotStatGroupKeys = self.two_answers_reader(contentConfigCode)

        for packageNo in range(1, numPackages+1):
            selectedStats = set()
            package = []
            packageId = '{}-{}-{}'.format(contentConfigCode, packageNo, int(dt.timestamp(dt.now()) * 1000))
            for slotNo in range(1, numSlots+1):
                while True:
                    try:
                        remainingStatCombinations = slotStatGroupKeys[slotNo] - selectedStats
                        statComb = random.sample(remainingStatCombinations, 1)[0]
                        break

                    except ValueError:
                        selectedStats.clear()
                        continue

                    except Exception as e:
                        print ("Error selecting a new stat in slot #{}: {}, {}".format(slotNo, e, type(e)))

                selectedStats.add(statComb)
                questionGroup = slotStatGroups[slotNo][statComb]
                questionIndex = questionGroup[random.randint(0,len(questionGroup)-1)]
                questionDict = dict(questionsDF.iloc[questionIndex])
                newQuestion = self.two_answers_question_generator(questionDict, configDef)
                newQuestion['PackageId'] = packageId
                newQuestion['Timestamp'] = dt.now()
                package.append(newQuestion)
            #print(package)
            packageDF = pd.DataFrame(package)
            #print(packageDF)
            outputDF = outputDF.append(packageDF)

        #
        # write to BigQuery
        #print(outputDF)
        tableId = 'Sportsight_Packages.two_answers_all_V3'
        outputDF.to_gbq(
            tableId,
            if_exists='append'
        )
