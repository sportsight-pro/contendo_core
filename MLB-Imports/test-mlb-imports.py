import M

#feeds = ['seasonal_games', 'daily_games', 'weekly_games', 'seasonal_dfs', 'daily_dfs', 'weekly_dfs', 'seasonal_player_gamelogs', 'daily_player_gamelogs', 'weekly_player_gamelogs', 'seasonal_team_gamelogs', 'daily_team_gamelogs', 'weekly_team_gamelogs', 'game_boxscore', 'game_playbyplay', 'game_lineup', 'current_season', 'player_injuries', 'latest_updates', 'seasonal_team_stats', 'seasonal_player_stats', 'seasonal_venues', 'players', 'seasonal_standings']
#print (feeds)
mi = MsfImportMlb()

def test_one():
    with open('results/game_playbyplay-mlb-2017-regular.json', 'r') as infile:
        pbp = json.load(infile)
        newPbp = mi.pbp_to_bigqery_form(pbp)
        with open('results/newpbp.json', 'w') as outfile:
            json.dump(newPbp, outfile)
            outfile.close()
        infile.close()

#test_one()
mi.get_seasonal_stats()
#mi.get_game_pbp()
print('Done')
