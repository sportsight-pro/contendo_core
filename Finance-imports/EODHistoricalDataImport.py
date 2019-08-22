import os
import pandas as pd
from datetime import datetime as dt, timedelta
import time
import re
import glob
import csv
import BigqueryUtils
import eod_historical_data as eod
import ProducerConsumersEngine

class EODHistoricalDataImport(ProducerConsumersEngine.ProducerConsumersEngine):
    def __init__(self):
        ProducerConsumersEngine.ProducerConsumersEngine.__init__(self, self.import_daily_quotes)
        self.bqu = BigqueryUtils.BigqueryUtils()
        self.main_dir = '/Users/ysherman/Documents/GitHub/results/Finance/EODHistoricalData/daily-quotes/'
        #self.AV_api_key = 'NN4P0527XD25VT1Q'
        #self.WTD_apikey = 'kDxr9tfB8fYVUV0wnkNzZN4W3IQZO48hLOpFKJ2NIiHbHSgKsTyMt4jzW3Cm'
        self.EODHD_apikey = '5d5d1d7259ef23.41685254'
        #self.alpha_url = 'https://www.alphavantage.co/query?function={AnalyticFunction}&symbol={Symbol}&outputsize=compact&apikey={api_key}&datatype=csv'.replace('{api_key}', api_key)
        self.EOD_Query = "SELECT Code, Exchange, Type FROM `sportsight-tests.Finance_Data.eod_exchange_symbols_list` where Exchange in ('COMM', 'NYSE', 'NASDAQ') or Code in ('GSPC', 'VIX', 'DJI') order by exchange desc"
        self.EOD_DAYBULK_URL = 'https://eodhistoricaldata.com/api/eod-bulk-last-day/{}?api_token=5d5d1d7259ef23.41685254&filter=extended&date={}'

    def get_eod_daily_bulk(self, startTime):
        csv_dir = self.main_dir + 'dated-files/'
        if not os.path.exists(csv_dir):
            os.mkdir(csv_dir)
        datesPD = self.bqu.execute_query_to_df ("SELECT distinct format_date('%Y-%m-%d', timestamp) as Date FROM `sportsight-tests.Finance_Data.daily_stock_history_*` where timestamp<=date_sub(current_date(), interval 2 day) order by Date desc limit 300")
        datesList = list(datesPD['Date'])
        for date in datesList:
            dailyDF = pd.DataFrame()
            for exchange in ['COMM', 'INDX', 'NASDAQ', 'NYSE']:
                url = self.EOD_DAYBULK_URL.format(exchange, date)
                print(url, dt.now()-startTime)
                try:
                    stockDF = pd.read_csv(url).fillna(0)[['Code', 'Name', 'Date', 'MarketCapitalization', 'Open', 'High', 'Low', 'Close', 'Adjusted_close', 'Volume', 'EMA_50', 'EMA_200', 'High_250', 'Low_250', 'Prev_close', 'Change']][:-1]
                    #print(stockDF.shape, stockDF.columns)
                    stockDF['Exchange'] = exchange
                    stockDF.to_csv(csv_dir+'{}-{}.csv'.format(date, exchange))
                    dailyDF = dailyDF.append(stockDF)
                except Exception as e:
                    print("Error {}".format(e))
                #break
            tableId = 'Finance_Data.eod_history_data_{}'.format(date.replace('-', ''))
            print('Writing table {}, size {}, delta time {}'.format(tableId, dailyDF.shape, dt.now()-startTime))
            dailyDF.to_gbq(
                tableId,
                if_exists='replace'
            )

            #break
        print('Done', dt.now()-startTime)

    def get_eod_quote(self, comp, startTime):
        stockCode = '{Code}-{Exchange}-{Type}'.format(**comp)
        csv_file = comp['CSVDir'] + '{}.csv'.format(stockCode)
        if (not os.path.exists(csv_file)):  # and (os.path.getsize(outfile_json)>0):
            print('{}. Getting {}, delta time: {}'.format(comp['i'] + 1, stockCode, dt.now() - startTime))
            try:
                symbol = comp['Code']
                exchange = comp['Exchange']
                if exchange in ['NYSE', 'NASDAQ']:
                    exchange='US'
                svDF = eod.get_eod_data(symbol, exchange, api_key=self.EODHD_apikey)
                print(svDF.shape)
                svDF['Symbol'] = comp['Code']
                svDF['Exchange'] = comp['Exchange']
                # svDF['Date'] = svDF.index
                svDF.to_csv(csv_file)
                return True

            except Exception as e:
                print('Error {}, Stock: {}'.format(e, stockCode))
        return False

    def import_daily_quotes(self, configurations, startTime):
        print("Starting import_daily_quotes")
        comp_df = self.bqu.execute_query_to_df(self.EOD_Query)

        csv_dir = self.main_dir + '{}/'.format(startTime.strftime('%Y-%m-%d'))
        if not os.path.exists(csv_dir):
            os.mkdir(csv_dir)

        print('getting {} companies, delta time: {} '.format(comp_df.shape[0], dt.now() - startTime))
        for i, comp in comp_df.iterrows():
            comp['CSVDir'] = csv_dir
            comp['i'] = i
            ret = self.get_eod_quote(comp, startTime)
            if ret:
                continue
            continue

            jobData = self.JobData(self.get_eod_quote, dict(comp))
            print(jobData.instructions)
            try:
                continue
                self.jobsQueue.put(jobData)
            except Exception as e:
                print("Error {} in queue.put".format(e))
                break
            break

    def create_dated_quote_files(self, dirdate):
        os.chdir(self.main_dir)
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



    def upload_dated_quote_files(self, startdate):
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


def test():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ysherman/Documents/GitHub/sportsight-tests.json"
    startTime = dt.now()

    ehd = EODHistoricalDataImport()
    #ehd.run(numExecutors=2)
    #ehd.import_daily_quotes([], startTime)
    #date = '2019-08-20'
    ehd.get_eod_daily_bulk(startTime)
    #ehd.create_dated_quote_files('2019-08-21')
    #ehd.upload_dated_quote_files('20190820')

test()