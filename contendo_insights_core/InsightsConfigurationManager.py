import pandas as pd
import os
from contendo_utils import ProUtils

class InsightsConfigurationManager:

    def __init__(self):
        self.configsheet_url = 'https://docs.google.com/spreadsheets/d/1hsTL7TzdtwPTBe5ZXs4PN1McQ4h5-NctW_KnV9sZQfo/export?format=csv&gid={SheetId}&run=1'
        contentConfig_df = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,'742083111')).fillna('')
        self.contentConfigDict = ProUtils.pandas_df_to_dict(contentConfig_df, 'ContentDefCode')
        domainsDF = pd.read_csv(self.configsheet_url.format(SheetId='103209122')).fillna('')
        self.domainsDict = ProUtils.pandas_df_to_dict(domainsDF, 'Domain')
        templates_df = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,'2085630088')).fillna('')
        self.templateDefsDict = ProUtils.pandas_df_to_dict(templates_df, 'TemplateName')

        return

    def get_content_config(self, contentConfigCode):
        return self.contentConfigDict[contentConfigCode]

    def get_configuration_dict(self, contentConfigCode):
        contentConfig = self.contentConfigDict[contentConfigCode]
        #
        # Getting the insight-configuration data, setting common configurations and writing to BQ..
        config_url = self.configsheet_url.format(**contentConfig)
        insightConfig_df = pd.read_csv(config_url).fillna('')
        return ProUtils.pandas_df_to_dict(insightConfig_df, 'QuestionCode')

    def save_configuration_to_bigquery(self, contentConfigCode):
        contentConfig = self.contentConfigDict[contentConfigCode]
        #
        # Getting the insight-configuration data, setting common configurations and writing to BQ..
        config_url = self.configsheet_url.format(**contentConfig)
        insightConfig_df = pd.read_csv(config_url).fillna('')
        #print (ProUtils.pandas_df_to_dict(insightConfig_df, 'QuestionCode'))
        configKeys = ['SeasonCode', 'SportCode', 'NumSlots', 'StatTimeframes', 'ListDescription']
        for key in configKeys:
            if key in contentConfig.keys():
                insightConfig_df[key] = contentConfig[key]
        insightConfig_df['StartSlot'] = 1
        #
        # Writing to BQ.
        contentDefinitionTableId = 'temp.configuration_definition_{}'.format(contentConfigCode)
        insightConfig_df.to_gbq(
            contentDefinitionTableId,
            if_exists='replace'
        )

        return contentDefinitionTableId

def test():
    print("Starting... cwd=", os.getcwd())
    startTime = dt.now()
    root = os.getcwd()  # + '/sportsight-core/Sportsight-insights'
    os.chdir('../../')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
    icm = InsightsConfigurationManager()
    tableId = icm.save_configuration_to_bigquery('Finance_All')
    print('Done ' + tableId)
    #tableId = icm.save_configuration_to_bigquery('IMDB_All')
    #print('Done ' + tableId)

if __name__ == '__main__':
    test()