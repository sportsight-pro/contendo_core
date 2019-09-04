from datetime import datetime as dt
import os, time
import InsightsGenerator, InsightsPackaging, SimpleStatsGenerator, MsfImportMlbFeeds

def import_and_generate_stats():
    mi = MsfImportMlbFeeds.MsfImportMlb()
    print('Created MSF imports & Stats generator, delta time: {}'.format(dt.now() - startTime))
    mi.get_seasonal_stats()
    print('Done get_seasonal_stats(), delta time: {}'.format(dt.now() - startTime))
    mi.get_game_days_stats()
    print('Done get_game_days_stats(), delta time: {}'.format(dt.now() - startTime))
    mi.get_game_pbp()
    print('Done get_game_pbp(), delta time: {}'.format(dt.now() - startTime))

    si = SimpleStatsGenerator.SimpleStatsGenerator(root=root)
    si.run()
    print('Done Stats-generation(), delta time: {}'.format(dt.now() - startTime))

def questions_generation():
    ig = InsightsGenerator.InsightsGenerator(root=root)
    ip = InsightsPackaging.InsightsPackaging(root=root)
    print('Created insightsGenerator & InsightsPackaging, delta time: {}'.format(dt.now() - startTime))
    keys = ig.icm.contentConfigDict.keys()
    #keys = ['MLB_2018_Reg_Merrifield']
    for configCode in keys:
        try:
          configCode.index('MLB_')
        except:
            continue
        try:
            print('Starting: ' + configCode)
            nQuestions = ig.two_answers_generator(configCode)
            print('Done questions generation, created {} questions. delta time: {}'.format(nQuestions, dt.now()-startTime))
            #ip.two_answers_package_generator(configCode)
            print('Done packaging, delta time: {}'.format(dt.now() - startTime))
        except Exception as e:
            print("Error: {}".format(e))
            continue

if __name__ == '__main__':
    startTime = dt.now()
    root = os.getcwd()  # + '/sportsight-core/Sportsight-insights'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])

    import_and_generate_stats()
    questions_generation()
