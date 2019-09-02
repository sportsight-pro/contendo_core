from datetime import datetime as dt
from contendo_utils import BigqueryUtils
from contendo_utils import ProUtils

import InsightsConfigurationManager as icm

class InsightsGenerator:
    def __init__(self, root='.'):
        self.root = root
        self.icm = icm.InsightsConfigurationManager()
        self.queryDict = {}
        self.bqu = BigqueryUtils()
        self.TRUE=True

    def get_twoanswers_query(self, domain):
        queryKey='{}.TwoAnswersQuery'.format(domain)
        if queryKey not in self.queryDict:
            statsPrepQuery = open(self.root + '/Queries/{StatsPrepQuery}'.format(**self.icm.domainsDict[domain]), 'r').read()
            twoAnswersQuestionQuery = open(self.root + '/Queries/{TwoAnswersQuestionQuery}'.format(**self.icm.domainsDict[domain]), 'r').read()
            self.queryDict[queryKey] = '{},\n{}\nSELECT * from twoQuestionsFinal'.format(statsPrepQuery, twoAnswersQuestionQuery)
        #print(self.queryDict[domain])
        return self.queryDict[queryKey]

    def get_lists_query(self, domain):
        queryKey='{}.ListsQuery'.format(domain)
        if queryKey not in self.queryDict:
            listsQuery = open(self.root + '/Queries/{ListsQuery}'.format(**self.icm.domainsDict[domain]), 'r').read()
            self.queryDict[queryKey] = listsQuery
        #print(self.queryDict[domain])
        return self.queryDict[queryKey]

    def get_onelist_query(self, domain):
        queryKey='{}.OneListQuery'.format(domain)
        if queryKey not in self.queryDict:
            listsQuery = open(self.root + '/Queries/{OneListQuery}'.format(**self.icm.domainsDict[domain]), 'r').read()
            self.queryDict[queryKey] = listsQuery
        #print(self.queryDict[domain])
        return self.queryDict[queryKey]

    def get_twoquestions_dataset_and_table(self, contentConfigCode):
        return 'temp', 'finance_questions_'+contentConfigCode

    def get_lists_dataset_and_table(self, contentConfigCode):
        return 'temp', 'finance_lists_'+contentConfigCode

    def trend_teams_filter(self, top=30, minTrend=0):
        query = 'SELECT TeamId, Trend FROM `sportsight-tests.Baseball1.teams_trend` where Trend>{} order by trend desc limit {}'.format(minTrend,top)
        teamsDF = self.bqu.execute_query_to_df(query)
        teamsList = list(teamsDF['TeamId'])
        inst={}
        inst['teamIDs'] = str(teamsList).replace('[', '(').replace(']', ')')
        #return 'stat1.TeamCode in {teamIDs} or stat2.TeamCode in {teamIDs}'.format(**inst)
        return 'TeamCode in {teamIDs}'.format(**inst)

    def filter(self, cond):
        return cond

    def one_team_filter(self, teamCode):
        return '"{}" in (stat1.TeamCode, stat2.TeamCode)'.format(teamCode)

    def compare_teams_filter(self, team1, team2):
        return '"{}" in (stat1.TeamCode, stat2.TeamCode) AND "{}" in (stat1.TeamCode, stat2.TeamCode)'.format(team1, team2)

    def one_player_filter(self, playerCode):
        return '"{}" in (stat1.PlayerCode, stat2.PlayerCode)'.format(playerCode)

    def property_compare(self, property, value):
        return '{} = "{}"'.format(property, value)

    def marketcap_between(self, min, max):
        return 'MarketCap between {} and {}'.format(min, max)

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
        query = self.get_twoanswers_query(instructions['SportCode'])
        query = ProUtils.format_string(query, instructions)
        #print("Running query:\n" + query, flush=True)
        #
        # Execute the query.
        dataset_id, table_id = self.get_twoquestions_dataset_and_table(contentConfigCode)
        queryFile = 'results/queries/{}.sql'.format(table_id)
        f = open(queryFile, 'w')
        f.write(query)
        f.close()
        nQuestions = self.bqu.execute_query_with_schema_and_target(query, dataset_id, table_id)
        return nQuestions

    def lists_generator(self, contentConfigCode):
        #
        # Save the insights configuration to BQ
        configTableId = self.icm.save_configuration_to_bigquery(contentConfigCode)
        #
        # read the query, configure and run it.
        instructions = self.icm.get_content_config(contentConfigCode)
        instructions['InsightsConfigurationTable'] =  configTableId
        instructions['StatFilter'] =  self.calc_filter(instructions['StatFilter'])
        instructions['QuestionsFilter'] =  self.calc_filter(instructions['QuestionsFilter'])
        query = self.get_lists_query(instructions['SportCode'])
        query = ProUtils.format_string(query, instructions)
        #print("Running query:\n" + query, flush=True)
        #
        # Execute the query.
        dataset_id, table_id = self.get_lists_dataset_and_table(contentConfigCode)
        queryFile = 'results/queries/{}.sql'.format(table_id)
        f = open(queryFile, 'w')
        f.write(query)
        f.close()
        nItems = self.bqu.execute_query_with_schema_and_target(query, dataset_id, table_id)
        return nItems


def test():
    import os
    from datetime import datetime as dt
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ysherman/Documents/GitHub/sportsight-tests.json"
    startTime = dt.now()
    root = os.getcwd()
    ig = InsightsGenerator(root)
    #print(ig.trend_teams_filter(10,1))
    #return
    os.chdir('/Users/ysherman/Documents/GitHub/')

    print('Created insightsGenerator, delta time: {}'.format(dt.now()-startTime))
    #for configCode in ['Finance_100B', 'Finance_10to100B', 'Finance_Technology', 'Finance_All']: #ig.icm.contentConfigDict.keys():
    #for configCode in ['Finance_All']: #ig.icm.contentConfigDict.keys():
    keys = ig.icm.contentConfigDict.keys()
    # keys = ['MLB_2018_Reg_Merrifield']
    for configCode in keys:
        try:
            configCode.index('Finance_')
        except:
            continue
        try:
            print('Starting: ' + configCode)
            #nitems = ig.two_answers_generator(configCode)
            nitems = ig.lists_generator(configCode)
            print('Done, created {} items. delta time: {}'.format(nitems, dt.now() - startTime))
        except Exception as e:
            print("Error: {}".format(e))
            continue

if __name__ == '__main__':
    test()
