import json
from datetime import date, datetime as dt
import time
from pytrends.request import TrendReq
import pandas as pd

class GoogleTrendImport:
    def __init__(self):
        self.pytrends = TrendReq(hl='en-US', tz=360)

    def get_trend_for_list(self, itemList, key, category, categoryName):
        startTime = dt.now()
        filename = 'trends_{}_{}_{}.json'.format(categoryName, category, startTime.strftime('%Y%m%dT5H%M%S'))
        outfile = open(filename, 'w')
        # pytrends = TrendReq(hl='en-US', tz=360, proxies=['https://34.203.233.13:80', 'https://35.201.123.31:880'])

        first_ind = True
        itemCount = 0
        for itemDict in itemList:
            item = itemDict[key]
            sleeptime = 2
            itemCount += 1
            print(itemCount, item, dt.now() - startTime)
            while True:
                try:
                    time.sleep(sleeptime)
                    self.pytrends.build_payload([item],
                                           cat=category,
                                           timeframe='today 3-m',
                                           geo='US',
                                           gprop='')
                    itemTrend = self.pytrends.interest_over_time()
                    break
                except Exception as e:
                    print('Error reading trend {}, sleeptime: {}, delta-time: {}, error: {}'.format(item,
                                                                                                    sleeptime,
                                                                                                    dt.now() - startTime,
                                                                                                    e))
                    if str(e).find('429') >= 0:
                        sleeptime += 60

            if itemTrend.shape[0] != 90:
                display('No trend for player')
                continue

            itemTrend['Date'] = itemTrend.index
            trends = []
            for i, row in itemTrend.iterrows():
                trend = {}
                trend['Trend'] = row[item]
                trend['Date'] = row['Date'].strftime('%Y-%m-%d')
                trends.append(trend)
            trendsDict = {'ItemTrend': trends, 'CategoryId': category, 'CategoryName': categoryName}
            trendsDict.update(itemDict)
            trendsDict.pop('count')
            #print(trendsDict)
            outfile.write(json.dumps(trendsDict)+'\n')
            #break
        outfile.close()
        return filename

import BigqueryUtils
import os
def test():
    os.chdir('/Users/ysherman/Documents/GitHub/results/trends')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ysherman/Documents/GitHub/sportsight-tests.json"
    query = 'SELECT Code, Name, Sector, count(*) count FROM `sportsight-tests.Finance_Data.indices_company_list` left join unnest(Components) group by 1,2,3 having count>0 order by count desc, name'
    bqu = BigqueryUtils.BigqueryUtils()

    gtrend = GoogleTrendImport()
    itemsDict = bqu.execute_query_to_dict(query)
    print('Getting {} items for finance'.format(itemsDict['nRows']))
    filename = gtrend.get_trend_for_list(itemsDict['Rows'], 'Code', 7, 'Finance')
    #return
    datasetId = 'Trends_Data'
    bqu.create_dataset(datasetId)
    bqu.create_table_from_local_file(filename, datasetId, 'trends_v1', writeDisposition='WRITE_TRUNCATE')
    'Done'

test()