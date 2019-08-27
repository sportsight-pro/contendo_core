import pandas as pd
import os
import ProUtils as pu

class InsightsConfigurationManager:

    def __init__(self):
        self.configsheet_url = 'https://docs.google.com/spreadsheets/d/1hsTL7TzdtwPTBe5ZXs4PN1McQ4h5-NctW_KnV9sZQfo/export?format=csv&gid={SheetId}&run=1'
        contentConfig_df = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,'742083111')).fillna('')
        self.contentConfigDict = pu.ProUtils.pandas_df_to_dict(contentConfig_df, 'ContentDefCode')
        domainsDF = pd.read_csv(self.configsheet_url.format(SheetId='103209122')).fillna('')
        self.domainsDict = pu.ProUtils.pandas_df_to_dict(domainsDF, 'Domain')
        templates_df = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,'2085630088')).fillna('')
        self.templateDefsDict = pu.ProUtils.pandas_df_to_dict(templates_df, 'TemplateName')

        return

    def get_content_config(self, contentConfigCode):
        return self.contentConfigDict[contentConfigCode]

    def save_configuration_to_bigquery(self, contentConfigCode):
        contentConfig = self.contentConfigDict[contentConfigCode]
        #
        # Getting the insight-configuration data, setting common configurations and writing to BQ..
        config_url = self.configsheet_url.format(**contentConfig)
        #print (config_url)
        insightConfig_df = pd.read_csv(config_url).fillna('')
        configKeys = ['SeasonCodes', 'SportCode', 'NumSlots', 'StatTimeframes']
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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ysherman/Documents/GitHub/sportsight-tests.json"
    icm = InsightsConfigurationManager()
    tableId = icm.save_configuration_to_bigquery('Finance_All')
    print('Done ' + tableId)
    #tableId = icm.save_configuration_to_bigquery('IMDB_All')
    #print('Done ' + tableId)

#test()