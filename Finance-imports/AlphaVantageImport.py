import pandas as pd
import os
from datetime import datetime as dt, timedelta
import time
import re
import pandas_gbq
from google.cloud import bigquery

os.chdir('../..')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="sportsight-tests.json"

api_key = 'NN4P0527XD25VT1Q'
alpha_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={Symbol}&outputsize=full&apikey={api_key}&datatype=csv'.replace('{api_key}', api_key)

startTime = dt.now()

__bigquery_client = bigquery.Client()
query_job = __bigquery_client.query('SELECT * FROM `sportsight-tests.Finance_Data.missing_companies`')
comp_df = query_job.result().to_dataframe().fillna('')

pattern = re.compile("[A-Z]+$")
count = 0
if_exists = 'append'
print('getting {} companies, delta time: {} '.format(comp_df.shape[0], dt.now() - startTime))
stockValuesDF = pd.DataFrame()
for i, comp in comp_df.iterrows():
    sym = comp['Symbol'].strip()
    comp['Symbol'] = sym
    if not bool(pattern.match(sym)):
        print(i+1, '. skipping: ', sym)
        continue
    csv_file = 'results/Finance/daily-quotes/{Symbol}.csv'.format(**comp)
    if (not os.path.exists(csv_file)):  # and (os.path.getsize(outfile_json)>0):
        url = alpha_url.format(**comp)
        print('{}. Getting {}, delta time: {}'.format(i+1, sym, dt.now() - startTime))
        try:
            for retry in range(0, 5):
                svDF = pd.read_csv(url).fillna(0)
                print(svDF.shape)
                if (svDF.shape[1] != 9):
                    print(url, svDF)
                    time.sleep(10)
                    continue
                else:
                    svDF['Symbol'] = sym
                    svDF.to_csv(csv_file)
                    break
        except Exception as e:
            print ('Error {}, url: {}'.format(e, url))
            continue
    else:
        svDF = pd.read_csv(csv_file)[['timestamp', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume', 'dividend_amount', 'split_coefficient', 'Symbol']]

    #stockValuesDF = stockValuesDF.append(svDF)
    count += 1
    if count % 20000 == 0:
        tableName = 'Finance_Data.daily_stock_history'
        stockValuesDF.to_gbq(
            tableName,
            project_id='sportsight-tests',
            if_exists=if_exists,

        )
        if_exists = 'append'
        print('End Write to Bigquery table {},shape: {}, delta-time: {}'.format(tableName, stockValuesDF.shape,
                                                                                  dt.now() - startTime))
        stockValuesDF = pd.DataFrame()

