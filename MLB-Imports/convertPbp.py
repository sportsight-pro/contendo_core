import os, json

def converPbp():
    pbpFile = open('/Users/ysherman/Downloads/msf-game_playbyplay.json', 'r')
    outFilePattern = '/Users/ysherman/Documents/GitHub/results/pbp/msf-pbp-{}-{}.json'
    count = 1
    for pbpLine in pbpFile: ## how to check that end is reached?
        pbpJson = json.loads(pbpLine)
        gameId = pbpJson['game']['id']
        gameDate = pbpJson['game']['strtTime'].split('T')[0]
        print(count, gameId, gameDate)
        count+=1
        outfile=open(outFilePattern.format(gameId, gameDate), 'w')
        outfile.write(json.dumps(pbpJson))
        outfile.close()
    pbpFile.close()

#
# Test
if __name__ == '__main__':
    converPbp()
