from datetime import datetime as dt
import os, time
import ProUtils as pu
import BigqueryUtils as bqu

import InsightsConfigurationManager as icm


class InsightsGenerator:
    def __init__(self):
        self.icm = icm.InsightsConfigurationManager()
        self.statsPrepQuery = open('./Queries/StatsPrepQuery.sql', 'r').read()
        self.twoAnswersQuestionQuery = open('./Queries/TwoAnswersQuestionQuery.sql', 'r').read()
        self.questionsReaderQuery = open('./Queries/QuestionsReaderQuery.sql', 'r').read()
        self.bqUtils = bqu.BigqueryUtils()

    def get_dataset_and_table(self, contentConfigCode):
        return 'temp', 'questions_'+contentConfigCode

    def two_answers_generator(self, contentConfigCode):
        #
        # Save the insights configuration to BQ
        configTableId = self.icm.save_configuration_to_bigquery(contentConfigCode)
        #
        # read the query, configure and run it.
        instructions = self.icm.get_content_config(contentConfigCode)
        instructions['InsightsConfigurationTable'] =  configTableId
        query ='{},\n{}\nSELECT * from twoQuestionsFinal'.format(self.statsPrepQuery, self.twoAnswersQuestionQuery)
        query = pu.ProUtils.format_string(query, instructions)
        #print("Running query:\n" + query, flush=True)
        #
        # Execute the query.
        dataset_id, table_id = self.get_dataset_and_table(contentConfigCode)
        nQuestions = self.bqUtils.execute_query_with_schema_and_target(query, dataset_id, table_id)
        return nQuestions

    def two_answers_reader(self, contentConfigCode, nQuestionsToSelect):
        #
        # read the questions
        instructions = {
            'ContentConfigCode': contentConfigCode
            'nQuestionsToSelect': nQuestionsToSelect,
        }
        query =


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../sportsight-tests.json"
startTime = dt.now()
ig = InsightsGenerator()
print('Created insightsGenerator, delta time: {}'.format(dt.now()-startTime))
for configCode in ig.icm.contentConfigDict.keys():
    print('Starting: ' + configCode)
    nQuestions = ig.two_questions_generator(configCode)
    print('Done, created {} questions. delta time: {}'.format(nQuestions, dt.now()-startTime))
