from datetime import datetime as dt
import time
import json, os
import pandas as pd
import BigqueryUtils as bqu
import pandas_gbq
from pytrends.request import TrendReq

class MLBTrends:

    def __init__(self):
        apikey = '98de7b49-a696-4ed7-8efa-94b28a'
        self.bqu = bqu.BigqueryUtils()

    def get_teams_trend(self, roll=3):
        teamQuery = 'SELECT format("%s %s", City, TeamName) as teamFullName, id as TeamId FROM `sportsight-tests.Baseball1.teams_view`'
        teams_df = self.bqu.execute_query_to_df(teamQuery)
        teams_list = list(teams_df['teamFullName'])


        firstTime = True
        # for team in teams_list:
        pytrends = TrendReq(hl='en-US', tz=360)
        for team in teams_list:
            # for i in [0,5,10,15,20,25]:
            time.sleep(3)
            print(team)
            pytrends.build_payload([team],  # teams_list[i:i+5],
                                   cat=259,
                                   timeframe='today 3-m',
                                   geo='US',
                                   gprop='')

            team_trend = pytrends.interest_over_time()
            if firstTime:
                trend_table = team_trend.iloc[:, :-1]
                firstTime = False
            else:
                trend_table = pd.merge(trend_table,
                                       team_trend.iloc[:, :-1],
                                       left_on='date',
                                       right_on='date',
                                       how='outer')
        summary_table = pd.DataFrame({
            "Avg":trend_table.mean(axis=0),
            "Std":trend_table.std(),
            "Last":trend_table[-roll:].mean()
        })
        summary_table["Trend"] = (summary_table["Last"] - summary_table["Avg"])/summary_table["Std"]
        summary_table = summary_table.sort_values(by=['Trend'], ascending=False)
        summary_table['teamFullName'] = summary_table.index
        summary_table = summary_table.join(teams_df.set_index('teamFullName')).sort_values(by=['Trend'], ascending=False)
        tableName = 'Baseball1.mlb_teams_trend_{}'.format(dt.now().strftime('%Y%m%d'))  # _{}'.format()
        summary_table.to_gbq(
            tableName,
            project_id='sportsight-tests',
            if_exists='replace'
        )


def test():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
    mt = MLBTrends()
    mt.get_teams_trend()

if __name__ == '__main__':
    test()