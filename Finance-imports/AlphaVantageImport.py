import pandas as pd
import os
from datetime import datetime as dt, timedelta
import time
import re
import pandas_gbq
from google.cloud import bigquery
import glob
import csv
import BigqueryUtils


def import_daily_quotes():
    os.chdir(main_dir)
    startTime = dt.now()

    __bigquery_client = bigquery.Client()
    query_job = __bigquery_client.query('SELECT Symbol FROM `sportsight-tests.Finance_Data.companies_data` order by MarketCap desc')
    comp_df = query_job.result().to_dataframe().fillna('')

    pattern = re.compile("[A-Z]+$")
    count = 0
    if_exists = 'append'
    csv_dir = '{}/'.format(startTime.strftime('%Y-%m-%d'))
    try:
        os.mkdir(csv_dir)
    except Exception as e:
        a=True

    print('getting {} companies, delta time: {} '.format(comp_df.shape[0], dt.now() - startTime))
    stockValuesDF = pd.DataFrame()
    for i, comp in comp_df.iterrows():
        sym = comp['Symbol'].strip()
        comp['Symbol'] = sym
        comp['api_key'] = api_key
        comp['AnalyticFunction'] = 'TIME_SERIES_DAILY_ADJUSTED'
        if not bool(pattern.match(sym)):
            print(i+1, '. skipping: ', sym)
            continue
        csv_file = csv_dir+'{Symbol}.csv'.format(**comp)
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


def create_dated_quote_files(dirdate):
    os.chdir(main_dir)
    extension = 'csv'
    all_filenames = [i for i in glob.glob('{}/*.{}'.format(dirdate, extension))]
    print(len(all_filenames))
    outfiles = {}

    mydialect = csv.Dialect
    mydialect.lineterminator = '\n'
    mydialect.quoting = csv.QUOTE_MINIMAL
    mydialect.quotechar = '|'

    count = 0
    main_start_time = dt.now()
    for csvFileName in all_filenames:
        infile = open(csvFileName, 'r')
        linereader = csv.reader(infile, delimiter=',')
        firstrow = True
        for line in linereader:
            if firstrow:
                firstrow = False
                topLine = line
                continue
            date = line[1]
            if date not in outfiles:
                outfile = open('dated-files/{}.csv'.format(date), 'w')
                outFileWriter = csv.writer(outfile, delimiter=',', dialect=mydialect)
                outFileWriter.writerow(topLine[1:])
                outfiles[date] = {'outfile': outfile, 'outFileWriter': outFileWriter}
            else:
                outFileWriter = outfiles[date]['outFileWriter']
            outFileWriter.writerow(line[1:])
            #break
        count += 1
        infile.close()
        if count % 1000 == 0:
            print('Done reading: {} of {} Total-delta: {}\n'.format(count, len(all_filenames), dt.now() - main_start_time))



def upload_dated_quote_files(startdate):
    os.chdir(main_dir)
    extension = 'csv'
    all_filenames = [i for i in glob.glob('dated-files/2019-08*.{}'.format(extension))]
    print(all_filenames)
    bqu = BigqueryUtils.BigqueryUtils()
    main_start_time = dt.now()
    count = 0
    for csvFileName in all_filenames:
        datadate = csvFileName.split('/')[1].split('.')[0].replace('-', '')
        if (datadate < startdate.replace('-', '')):
            continue
        tableId = 'daily_stock_history_{}'.format(datadate)
        print(tableId, dt.now() - main_start_time)
        csvFile = open(csvFileName, 'rb')
        bqu.create_table_from_local_file(csvFile, 'Finance_Data', tableId)
        csvFile.close()
        count += 1
        if count % 20 == 0:
            print('Done reading: {} of {} Total-delta: {}\n'.format(count, len(all_filenames),
                                                                    dt.now() - main_start_time))


main_dir = '/Users/ysherman/Documents/GitHub/results/Finance/daily-quotes/'
api_key = 'NN4P0527XD25VT1Q'
WTD_apikey = 'kDxr9tfB8fYVUV0wnkNzZN4W3IQZO48hLOpFKJ2NIiHbHSgKsTyMt4jzW3Cm'
EODHD_apikey='5d5d0173c318e9.78603830'
alpha_url = 'https://www.alphavantage.co/query?function={AnalyticFunction}&symbol={Symbol}&outputsize=compact&apikey={api_key}&datatype=csv'.replace(
    '{api_key}', api_key)

def test():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ysherman/Documents/GitHub/sportsight-tests.json"

    date = '2019-08-20'
    import_daily_quotes()
    create_dated_quote_files('2019-08-21')
    upload_dated_quote_files('20190820')

test()