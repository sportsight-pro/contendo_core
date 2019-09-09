import os
from datetime import datetime as dt

from InsightsConfigurationManager import InsightsConfigurationManager
from contendo_utils import ProUtils

startTime = dt.now()
#
# generate the lists configuration file
print('Start generating the lists configuration', dt.now()-startTime)
icm = InsightsConfigurationManager()
configDict = icm.get_configuration_dict('Finance_All')
ProUtils.save_dict_to_jsonfile('finance_Api/lists_config.json', configDict)
#
# deploy the function to google cloud
os.chdir('finance_api')
print('Start building finance_api', dt.now()-startTime)
os.system('gcloud builds submit --tag gcr.io/sportsight-tests/finance_api')
print('Start deploying finance_api', dt.now()-startTime)
os.system('gcloud beta run deploy contendo-finance-api --image gcr.io/sportsight-tests/finance_api --platform managed --memory=2Gi --timeout=15m')
print('Done deploying the function', dt.now()-startTime)