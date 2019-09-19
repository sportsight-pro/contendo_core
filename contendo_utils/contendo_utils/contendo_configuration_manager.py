import os
import pandas as pd
from datetime import datetime as dt

from contendo_utils import ProUtils

class ContendoConfigurationManager:

    def __init__(self):
        self.configsheet_url_template = 'https://docs.google.com/spreadsheets/d/{documentId}/export?format=csv&gid={sheetId}&run=1'
        __main_configuration_doc_id = '1OHAkPPMtURUWu1d8BlZqcTX1iVnPeuZ1SdFLsGrgIDY'

        _domains_df = pd.read_csv(self.configsheet_url_template.format(documentId = __main_configuration_doc_id, sheetId='0')).fillna('')
        self.domainsDict = ProUtils.pandas_df_to_dict(_domains_df, 'Domain')
        _templates_df = pd.read_csv(self.configsheet_url_template.format(documentId = __main_configuration_doc_id, sheetId='1672371224')).fillna('')
        self.templateDefsDict = ProUtils.pandas_df_to_dict(_templates_df, 'TemplateName')

        return

    def get_configuration_dict(self, domain, sheetId, key):
        assert (domain in self.domainsDict)
        _config_df = pd.read_csv(self.configsheet_url_template.format(documentId = self.domainsDict[domain]['DocumentId'], sheetId=sheetId)).fillna('')
        _config_dict =  ProUtils.pandas_df_to_dict(_config_df, key)
        return _config_dict

def test():
    startTime = dt.now()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
    ccm = ContendoConfigurationManager()
    configDict = ccm.get_configuration_dict('Football.NFL', '1564699495', 'StatName')
    print(configDict, len(configDict.keys()))

if __name__ == '__main__':
    test()