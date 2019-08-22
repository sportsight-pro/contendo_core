from datetime import datetime as dt
import time
from ohmysportsfeedspy import MySportsFeeds
import json, os
from google.cloud import bigquery
import pandas as pd
import BigqueryUtils as bqu


class MsfImportMlb:

    def __init__(self):
        self.seasons = ['2019-regular', '2017-regular', '2017-playoff', '2018-regular', '2018-playoff']
        #self.seasons = ['2019-regular']
        apikey = '98de7b49-a696-4ed7-8efa-94b28a'
        self.msf = MySportsFeeds(version="2.0")
        self.msf.authenticate(apikey, "MYSPORTSFEEDS")
        self.bqu = bqu.BigqueryUtils()

    def get_seasonal_stats(self):
        start_time=dt.now()
        for feed in ['seasonal_games', 'seasonal_team_stats', 'seasonal_player_stats']:
            outfile_json = 'results/MLB/msf-mlb-{}-{}.json'.format(feed, dt.now().strftime('%Y%m%dT%H%M%S'))
            with open(outfile_json, 'w') as jsonfile:
                for season in self.seasons:
                    params = {
                        'league': 'mlb',
                        'season': season,
                        'feed': feed,
                        'format': 'json',
                    }
                    print('Starting msf {}-{}, delta-time: {}'.format(season, feed, dt.now()-start_time))
                    seasondata = self.msf.msf_get_data(**params)
                    outjson = json.dumps({'Season': season, 'Seasondata': seasondata})
                    jsonfile.write(outjson)
                    jsonfile.write('\n')
                    #delete_query = 'delete from `Baseball1.{}` where season="{}"'.format(feed, season)
                    #self.bqu.execute_query(delete_query)

            jsonfile.close()
            print('Starting upload of file {}, delta-time: {}'.format(outfile_json, dt.now() - start_time))
            uri = self.bqu.upload_file_to_gcp('sport-uploads', outfile_json, outfile_json)
            print('Starting table creation, delta-time: {}'.format(dt.now() - start_time))
            ret = self.bqu.create_table_from_gcp_file(uri, 'Baseball1', feed, 'WRITE_TRUNCATE')


    def get_game_days_stats(self):
        start_time=dt.now()
        for statObject in ['team', 'player']:
            feed = 'daily_{}_gamelogs'.format(statObject)
            #
            # get the missing game-days
            query = 'SELECT season,gameDay FROM `Baseball1.missing_{}_gamelogs` group by 1,2'.format(statObject)
            games_df = self.bqu.execute_query_to_df(query)
            print(games_df.shape)
            if (games_df.shape[0] == 0):
                return
            #
            # loop over missing game days
            for i,game in games_df.iterrows():
                #
                # open the main file.
                mainfile_name = 'results/MLB/msf-mlb-dayfeeds-{}-{}.json'.format(feed, dt.now().strftime('%Y%m%d'))
                mainfile = open(mainfile_name, 'w')

                params = {
                    'league': 'mlb',
                    'date': game['gameDay'],
                    'season': game['season'],
                    'feed': feed,
                    'format': 'json',
                }
                outfile_json = 'results/MLB/dayfeeds/msf-mlb-{feed}-{season}-{date}.json'.format(**params)
                if (not os.path.exists(outfile_json) or True): # and (os.path.getsize(outfile_json)>0):
                    print('Getting msf #{}, {}, delta-time: {}'.format(i, outfile_json, dt.now()-start_time))
                    jsonfile = open(outfile_json, 'w')
                    #
                    # Getting the data from MySportsFeeds
                    try:
                        seasondata = self.msf.msf_get_data(**params)
                    except Exception as e:
                        print('msf_get_data returned with error {}'.format(e))
                        continue
                    except Warning as w:
                        print('msf_get_data returned with warning {}'.format(w))
                        continue

                    jsonfile.write(json.dumps(seasondata))
                    jsonfile.close()
                else:
                    print('Reading msf #{}, {}, delta-time: {}'.format(i, outfile_json, dt.now()-start_time))
                    #
                    # loading the JSON from already existing file.
                    try:
                        jsonfile = open(outfile_json,'r')
                        seasondata = json.load(jsonfile)
                    except Exception as e:
                        print('Error loading JSON from file {}'.format(e))
                        continue

                dayfeed = {
                    'gamelogs': seasondata['gamelogs'],
                    'lastUpdatedOn': seasondata['lastUpdatedOn'],
                    'season': params['season']
                }
                mainfile.write(json.dumps(dayfeed)+'\n')
                mainfile.close()
                #
                # upload file and update table.
                try:
                    print('Starting upload of file {}, delta-time: {}'.format(outfile_json, dt.now() - start_time))
                    uri = self.bqu.upload_file_to_gcp('sport-uploads', mainfile_name, outfile_json + dt.now().strftime('.%Y%m%dT%H%M%S'))
                    print('Starting table creation from {}, delta-time: {}'.format(uri, dt.now() - start_time))
                    ret = self.bqu.create_table_from_gcp_file(uri, 'Baseball1', '{}_{}'.format(feed, game['gameDay']), 'WRITE_TRUNCATE')
                except Exception as e:
                    print('Error while uploading table {}'.format(e))
                    continue

    def get_game_pbp(self):
        start_time = dt.now()
        query='SELECT * FROM `sportsight-tests.Baseball1.missing_pbp_bydate`'
        datesDF = self.bqu.execute_query_to_df(query)
        print(datesDF.shape)
        if (datesDF.shape[0]==0):
            return

        feed = 'game_playbyplay'
        pbpFilePattern = '/Users/ysherman/Documents/GitHub/results/MLB/pbp/msf-pbp-{}-{}.json'
        for i,dayGames in datesDF.iterrows():
            dayGames = dict(dayGames)
            games = dayGames['games']
            date = dayGames['date'].strftime('%Y-%m-%d')
            outfile_json='results/MLB/temp/{}-{}.json'.format(feed, date)
            jsonfile = open(outfile_json, 'w')
            for game in games:
                pbpFileName = pbpFilePattern.format(game['id'], date)
                print(pbpFileName)
                if (not os.path.exists(pbpFileName) or True):
                    params = {}
                    params['season'] = dayGames['season']
                    params['matchname'] = game['matchname']
                    params['game'] = game['id']
                    params['format'] = 'json'
                    params['league'] = 'mlb'
                    params['feed']=feed
                    while True:
                        try:
                            print('Getting for day {}, game-id: {}, {}, season: {}, feed: {}, delta-time: {}'.format(
                                i,
                                game['id'],
                                game['matchname'],
                                dayGames['season'],
                                feed,
                                dt.now() - start_time))
                            seasondata = self.msf.msf_get_data(**params)
                            break
                        except Exception as e:
                            print("Error: {}".format(e))
                        except Warning as w:
                            print("Error - Warning: {}".format(w))
                        except:
                            print("Unknow Error")
                        time.sleep(10)
                    outfile = open(pbpFileName, 'w')
                    outfile.write(json.dumps(seasondata))
                    outfile.close()

                else:
                    try:
                        print('Reading file {}'.format(pbpFileName))
                        pbpfile = open(pbpFileName,'r')
                        seasondata = json.load(pbpfile)
                        pbpfile.close()
                    except Exception as e:
                        print('Error loading JSON from file {}'.format(e))
                        continue

                seasondata = self.pbp_to_bigqery_form(seasondata)
                seasondata['season'] = dayGames['season']
                seasondata['gameid'] = game['id']
                seasondata['gamename'] = game['matchname']
                outjson = json.dumps(seasondata)
                jsonfile.write(outjson)
                jsonfile.write('\n')
            #
            # uploading file for the day
            jsonfile.close()
            try:
                print('Starting upload of file {}, delta-time: {}'.format(outfile_json, dt.now() - start_time))
                uri = self.bqu.upload_file_to_gcp('sport-uploads', outfile_json, outfile_json)
                print('Starting table creation, delta-time: {}'.format(dt.now() - start_time))
                ret = self.bqu.create_table_from_gcp_file(uri, 'Baseball1', '{}_{}'.format(feed, date.replace('-','')), 'WRITE_TRUNCATE')
            except Exception as e:
                print('Error while uploading table {}'.format(e))
                continue

    def pbp_atbatsubplay_new(self, atBatSubPlay):
        newAtBatSubPlay = []
        for key,value in atBatSubPlay.items():
            if value is None:
                continue
                #newAtBatSubPlay.append({'key': key,'value': '', 'type': 'NULL'})
            elif key in ['retrievedAtLocation', 'pitchedLocation']:
                newAtBatSubPlay.append({'key': key+'-x','value': str(value['x']), 'type': 'INTEGER'})
                newAtBatSubPlay.append({'key': key+'-y','value': str(value['y']), 'type': 'INTEGER'})
            elif type(value)==dict:
                newAtBatSubPlay.append({'key': key,'value': str(value['id']), 'type': 'PlayerId'})
            else:
                newAtBatSubPlay.append({'key': key,'value': str(value), 'type': type(value).__name__})
        return newAtBatSubPlay

    def pbp_atbatplaystatus_new(self, atBatPlayStatus):
        playerRoles =  ['batter', 'catcher', 'centerFielder', 'firstBaseman', 'firstBaseRunner', 'leftFielder', 'outFielder', 'pitcher', 'rightFielder', 'secondBaseman', 'secondBaseRunner', 'shortStop', 'thirdBaseman', 'thirdBaseRunner']
        runnerRoles =  ['firstBaseRunner', 'secondBaseRunner', 'thirdBaseRunner']
        nRunners = 0
        newAtBatPlayStatus = {}
        for key,value in atBatPlayStatus.items():
            if key in runnerRoles:
                if value is not None:
                    nRunners+=1
            if value is None:
                value={'id': -1}
            if key in playerRoles:
                newAtBatPlayStatus[key] = value['id']
            else:
                newAtBatPlayStatus[key] = value
        newAtBatPlayStatus['numRunners'] = nRunners
        return newAtBatPlayStatus

    def pbp_to_bigqery_form(self, pbpDict):
        newAtBats = []
        atBatCounter=0
        playCounter=0
        for atBat in pbpDict['atBats']:
            newAtBatPlays = []
            atBatPlayCounter = 0
            for atBatPlay in atBat['atBatPlay']:
                newAtBatPlay={}
                try:
                    if type(atBatPlay)!=dict:
                        continue
                    for key, value in atBatPlay.items():
                        if key=='description':
                            newAtBatPlay[key]=value
                        elif key == 'playStatus':
                            newAtBatPlay[key] = self.pbp_atbatplaystatus_new(value)
                        else:
                            newAtBatPlay['atBatSubPlay'] = {'name': key, 'properties': self.pbp_atbatsubplay_new(value)}
                except Exception as e:
                    print('Error {} with atBatCounter = {}, atBatPlayCounter={}, key={}, atbatsubplay={}, atBatPlay={}'.format(e,atBatCounter,atBatPlayCounter,key,value, atBatPlay))
                atBatPlayCounter+=1
                playCounter+=1
                newAtBatPlay['index']=atBatPlayCounter
                newAtBatPlay['playindex']=playCounter
                newAtBatPlays.append(newAtBatPlay)
            atBatCounter += 1
            atBat['index'] = atBatCounter
            atBat['atBatPlay'] = newAtBatPlays
            newAtBats.append(atBat)
        pbpDict['atBats'] = newAtBats
        return pbpDict




import os
def test():
    os.chdir('../../')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="sportsight-tests.json"
    mi = MsfImportMlb()
    #mi.get_seasonal_stats()
    #mi.get_game_pbp()
    mi.get_game_days_stats()
    print('Done')

#test()
