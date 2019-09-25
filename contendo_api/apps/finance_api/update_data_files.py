import os
from datetime import datetime as dt
from datetime import date

from contendo_utils import ContendoConfigurationManager
from contendo_utils import ProUtils
from get_stocks_data import GetStocksData

if 'CONTENDO_ON_CLOUD' not in os.environ:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
#
# generate the lists configuration file
startTime = dt.now()
print('Start generating the lists configuration', dt.now()-startTime)
ccm = ContendoConfigurationManager()
configDict = ccm.get_configuration_dict('Finance.Stocks', 1287256531, 'QuestionCode')
if not os.path.exists('resource'):
    os.mkdir('resource')
ProUtils.save_dict_to_jsonfile('resource/lists_config.json', configDict)
getstocks = GetStocksData()
print('Start updating fundamentals company data', dt.now()-startTime)
getstocks.update_companies_data()
print('Start updating stocks trading data', dt.now()-startTime)
getstocks.update_stock_data()
print('Done', dt.now()-startTime)
