import os
import pandas as pd
import datetime
from datetime import datetime as dt
from datetime import timedelta
from datetime import date

from contendo_utils import ProUtils
from contendo_utils import BigqueryUtils

# get list of stock action from date to date

class GetBaseballData:

    def __init__(self):
        self.bqu = BigqueryUtils()
        self.statsDF = None
        self.teamsDF = None
        self.gamesDF = None
        self.playersDF = None
        self.resourceDir = 'resource'
        self.statsDataFileName = '{}/stats_{}.csv'.format(self.resourceDir, date.today())
        self.teamsDataFileName = '{}/teams_{}.csv'.format(self.resourceDir, date.today())
        self.playersDataFileName = '{}/players_{}.csv'.format(self.resourceDir, date.today())
        self.gamesDataFileName = '{}/games_{}.csv'.format(self.resourceDir, date.today())

    def get_stat_data(self, statobject='team', statname=None, teamcode=None, playercode=None, gamecode=None):
        #
        # get updated company data
        if self.statsDF is None:
            if os.path.exists(self.statsDataFileName):
                self.statsDF = pd.read_csv(self.statsDataFileName)
            else:
                statsQuery = """SELECT * FROM `sportsight-tests.Sportsight_Stats.all_gamelevel_stats_baseball` order by GameTime"""
                self.statsDF = self.bqu.execute_query_to_df(statsQuery)
                if not os.path.exists(self.resourceDir):
                    os.mkdir(self.resourceDir)
                self.statsDF.to_csv(self.statsDataFileName)

        assert statobject in ['team', 'player']
        retDF = self.statsDF.query('StatObject=="{statobject}"'.format(statobject=statobject))
        if teamcode:
            retDF = retDF.query('TeamCode=="{teamcode}"'.format(teamcode=teamcode))
        if playercode:
            retDF = retDF.query('PlayerCode=="{playercode}"'.format(playercode=playercode))
        if gamecode:
            retDF = retDF.query('GameCode=="{gamecode}"'.format(gamecode=gamecode))
        if statname:
            retDF = retDF.query('StatName=="{statname}"'.format(statname=statname))

        return retDF

    
    def get_teams_data(self):
        #
        # get updated company data
        if self.teamsDF is None:
            if os.path.exists(self.teamsDataFileName):
                self.teamsDF = pd.read_csv(self.teamsDataFileName)
            else:
                teamsQuery = """SELECT * FROM `sportsight-tests.Baseball1.teams_view`"""
                self.teamsDF = self.bqu.execute_query_to_df(teamsQuery)
                if not os.path.exists(self.resourceDir):
                    os.mkdir(self.resourceDir)
                self.teamsDF.to_csv(self.teamsDataFileName)
    
        return self.teamsDF
    
    def get_players_data(self):
        #
        # get updated company data
        if self.playersDF is None:
            if os.path.exists(self.playersDataFileName):
                self.playersDF = pd.read_csv(self.playersDataFileName)
            else:
                playersQuery = """SELECT * FROM `sportsight-tests.Baseball1.players_view`"""
                self.playersDF = self.bqu.execute_query_to_df(playersQuery)
                if not os.path.exists(self.resourceDir):
                    os.mkdir(self.resourceDir)
                self.playersDF.to_csv(self.playersDataFileName)
    
        return self.playersDF

    def get_games_data(self):
        #
        # get updated company data
        if self.gamesDF is None:
            if os.path.exists(self.gamesDataFileName):
                self.gamesDF = pd.read_csv(self.gamesDataFileName)
            else:
                gamesQuery = """SELECT * FROM `sportsight-tests.Baseball1.games_view`"""
                self.gamesDF = self.bqu.execute_query_to_df(gamesQuery)
                if not os.path.exists(self.resourceDir):
                    os.mkdir(self.resourceDir)
                self.gamesDF.to_csv(self.gamesDataFileName)
    
        return self.gamesDF
    

if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
    # Test
    startTime = dt.now()
    getdata = GetBaseballData()
    print('GetBaseballData initiated', dt.now() - startTime)
    teamStatsDF = getdata.get_stat_data()
    print('finished reading teams', teamStatsDF.shape, dt.now() - startTime)
    playerStatsDF = getdata.get_stat_data(statobject='player')
    print('finished reading players', playerStatsDF.shape, dt.now()- startTime)
    playerStatsDF = getdata.get_stat_data(statobject='player', statname='pbp.batting.rhp')
    print('finished reading players with statname', playerStatsDF.shape, dt.now()- startTime)

    #
    # getting teams, players and game data
    retDF = getdata.get_teams_data()
    print('finished reading teams data', retDF.shape, dt.now() - startTime)
    retDF = getdata.get_players_data()
    print('finished reading players data', retDF.shape, dt.now() - startTime)
    retDF = getdata.get_games_data()
    print('finished reading games data', retDF.shape, dt.now() - startTime)
    print(retDF.columns, getdata.gamesDF.columns)
    retDF.drop('Season', axis=1, inplace=True)
    print(retDF.columns, getdata.gamesDF.columns)

