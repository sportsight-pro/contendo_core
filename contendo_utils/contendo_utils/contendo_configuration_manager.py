import os
import pandas as pd
from datetime import datetime as dt

from contendo_utils import ProUtils

class ContendoConfigurationManager:

    def __init__(self):
        self.configsheet_url_template = 'https://docs.google.com/spreadsheets/d/{sheetId}/export?format=csv&gid={gid}&run=1'
        __main_configuration_doc_id = '1OHAkPPMtURUWu1d8BlZqcTX1iVnPeuZ1SdFLsGrgIDY'
        self._cacheConfigs = dict()

        _domains_df = self.get_configuration_pd(__main_configuration_doc_id, 0)
        self.domainsDict = ProUtils.pandas_df_to_dict(_domains_df, 'Domain')
        _templates_df = self.get_configuration_pd(__main_configuration_doc_id, 1672371224)
        self.templateDefsDict = ProUtils.pandas_df_to_dict(_templates_df, 'TemplateName')


        return

    def get_configuration_pd(self, documentId, gid):
        if (documentId, gid) not in self._cacheConfigs:
            try:
                _url = self.configsheet_url_template.format(sheetId = documentId, gid=gid)
                _config_df = pd.read_csv(_url).fillna('')
            except Exception as e:
                print('Error reading document {}, exception: {}'.format(_url, e))
                raise e

            self._cacheConfigs[(documentId, gid)] = _config_df

        return self._cacheConfigs[(documentId, gid)]


    def get_configuration_dict(self, domain, gid, key):
        assert (domain in self.domainsDict)
        _config_df = self.get_configuration_pd(self.domainsDict[domain]['DocumentId'], gid)
        _config_dict = ProUtils.pandas_df_to_dict(_config_df, key)

        return _config_dict

    def get_domain_docid(self, domain):
        return self.domainsDict[domain]['DocumentId']


def test():
    startTime = dt.now()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
    ccm = ContendoConfigurationManager()
    configDict = ccm.get_configuration_dict('Football.NFL', '1564699495', 'StatName')
    print(configDict, len(configDict.keys()))
    print (ccm.get_domain_docid('Football.NFL'))


if __name__ == '__main__':
    test()