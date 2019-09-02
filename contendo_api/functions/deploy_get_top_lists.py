import os
import json
from InsightsConfigurationManager import InsightsConfigurationManager
from contendo_utils import ProUtils
#
# generate the lists configuration file
icm = InsightsConfigurationManager()
configDict = icm.get_configuration_dict('Finance_All')
ProUtils.save_dict_to_jsonfile('get_top_lists/lists_config.json', configDict)
#
# deploy the function to google cloud
os.system('gcloud beta functions deploy get_top_lists  --runtime python37  --trigger-http --entry-point=get_top_lists')