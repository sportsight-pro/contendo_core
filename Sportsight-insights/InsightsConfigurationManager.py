import pandas as pd
import os
import ProUtils as pu

class InsightsConfigurationManager:

    def __init__(self):
        self.configsheet_url = 'https://docs.google.com/spreadsheets/d/1hsTL7TzdtwPTBe5ZXs4PN1McQ4h5-NctW_KnV9sZQfo/export?format=csv&gid={SheetId}&run=1'
        contentConfig_df = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,'742083111')).fillna('')
        self.contentConfigDict = pu.ProUtils.pandas_df_to_dict(contentConfig_df, 'ContentDefCode')
        templates_df = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,'2085630088')).fillna('')
        self.templateDefsDict = pu.ProUtils.pandas_df_to_dict(templates_df, 'TemplateName')

        return

    def get_content_config(self, contentConfigCode):
        return self.contentConfigDict[contentConfigCode]

    def save_configuration_to_bigquery(self, contentConfigCode):
        #
        # Getting the insight-configuration data, setting common configurations and writing to BQ..
        contentConfig = self.contentConfigDict[contentConfigCode]
        insightConfig_df = pd.read_csv(self.configsheet_url.replace('{SheetId}' ,str(contentConfig['SheetId']))).fillna('')
        insightConfig_df['SeasonCodes'] = contentConfig['SeasonCode']
        insightConfig_df['SportCode'] = contentConfig['SportCode']
        insightConfig_df['NumSlots'] = contentConfig['NumSlots']
        insightConfig_df['StartSlot'] = 1
        #
        # Writing to BQ.
        contentDefinitionTableId = 'temp.configuration_definition_{}'.format(contentConfigCode)
        insightConfig_df.to_gbq(
            contentDefinitionTableId,
            if_exists='replace'
        )

        return contentDefinitionTableId

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../sportsight-tests.json"
#icm = InsightsConfigurationManager()
#tableId = icm.save_configuration_to_bigquery('MLB_2019_Reg')
#print('Done ' + tableId)
#tableId = icm.save_configuration_to_bigquery('MLB_2018_Playoff')
#print('Done ' + tableId)
