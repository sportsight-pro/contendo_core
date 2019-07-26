from datetime import datetime as dt
import os, time
import InsightsGenerator, InsightsPackaging, SimpleStatsGenerator

#print (os.getcwd())
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../../sportsight-tests.json"
startTime = dt.now()
root = os.getcwd() #+ '/sportsight-core/Sportsight-insights'

#generator = SimpleStatsGenerator(root)#'Baseball.PlayerSeasonStats')
#generator.run(configurations=['Entertainmant.IMDB'])
#generator.run()

ig = InsightsGenerator.InsightsGenerator(root=root)
ip = InsightsPackaging.InsightsPackaging(root=root)

print('Created insightsGenerator, delta time: {}'.format(dt.now()-startTime))
for configCode in ig.icm.contentConfigDict.keys():
    #if configCode not in ['IMDB_Drama', 'IMDB_movies', 'IMDB_tvSeries', 'IMDB_tvMiniSeries', 'IMDB_All', ]:
    if configCode.find('MLB_')==-1: # not in ['IMDB_History']:
        continue
    print('Starting: ' + configCode)
    nQuestions = ig.two_answers_generator(configCode)
    print('Done questions generation, created {} questions. delta time: {}'.format(nQuestions, dt.now()-startTime))
    ip.two_answers_package_generator(configCode)
    print('Done packaging, delta time: {}'.format(dt.now() - startTime))
