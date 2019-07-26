import ProUtils as pu
import BigqueryUtils as bqu

import InsightsConfigurationManager as icm


class InsightsGenerator:
    def __init__(self, root='.'):
        self.root = root
        self.icm = icm.InsightsConfigurationManager()
        self.statsPrepQuery = open(root + '/Queries/StatsPrepQuery.sql', 'r').read()
        self.twoAnswersQuestionQuery = open(root + '/Queries/TwoAnswersQuestionQuery.sql', 'r').read()
        self.questionsReaderQuery = open(root + '/Queries/QuestionsReaderQuery.sql', 'r').read()
        self.bqUtils = bqu.BigqueryUtils()
        self.TRUE=True

    def get_dataset_and_table(self, contentConfigCode):
        return 'temp', 'questions_'+contentConfigCode

    def one_team_filter(self, teamCode):
        return '"{}" in (stat1.TeamCode, stat2.TeamCode)'.format(teamCode)

    def compare_teams_filter(self, team1, team2):
        return '"{}" in (stat1.TeamCode, stat2.TeamCode) AND "{}" in (stat1.TeamCode, stat2.TeamCode)'.format(team1, team2)

    def one_player_filter(self, playerCode):
        return '"{}" in (stat1.PlayerCode, stat2.PlayerCode)'.format(playerCode)

    def property_compare(self, property, value):
        return '{} = "{}"'.format(property, value)

    def condition(self, cond):
        return cond

    def calc_filter(self, filter):
        if filter==True:
            retFilter = filter
        else:
            try:
                execStr = 'self.'+filter
                retFilter = eval(execStr)
            except Exception as e:
                print("Error while evaluating '{}', error: {}".format(execStr, e))
                retFilter=True

        return retFilter

    def two_answers_generator(self, contentConfigCode):
        #
        # Save the insights configuration to BQ
        configTableId = self.icm.save_configuration_to_bigquery(contentConfigCode)
        #
        # read the query, configure and run it.
        instructions = self.icm.get_content_config(contentConfigCode)
        instructions['InsightsConfigurationTable'] =  configTableId
        instructions['StatFilter'] =  self.calc_filter(instructions['StatFilter'])
        instructions['QuestionsFilter'] =  self.calc_filter(instructions['QuestionsFilter'])
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
            'ContentConfigCode': contentConfigCode,
            'nQuestionsToSelect': nQuestionsToSelect,
        }
        #query =


