from datetime import datetime as dt
import os
import json
from InsightsConfigurationManager import InsightsConfigurationManager
from contendo_utils import ProUtils

startTime = dt.now()
#
# generate the lists configuration file
print('Start generating the lists configuration', dt.now()-startTime)
icm = InsightsConfigurationManager()
configDict = icm.get_configuration_dict('Finance_All')
ProUtils.save_dict_to_jsonfile('get_top_lists/lists_config.json', configDict)
#
# deploy the function to google cloud
#print(os.getcwd())
print('Start deploying the function', dt.now()-startTime)
os.chdir('get_top_lists')
os.system('gcloud beta functions deploy get_top_lists  --runtime python37  --trigger-http --entry-point=get_top_lists --memory=2048MB ')
print('Done deploying the function', dt.now()-startTime)
