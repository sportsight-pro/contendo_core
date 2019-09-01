import os
import pandas as pd
from datetime import datetime as dt, timedelta, date
import time
import re
import glob
import csv
import json
import BigqueryUtils
import eod_historical_data_extended as eod
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
        self.EOD_Symbols_Query = "SELECT Code, Exchange, Type FROM `sportsight-tests.Finance_Data.eod_exchange_symbols_list` where Exchange in ('COMM', 'NYSE', 'NASDAQ', 'INDX') order by exchange desc"
        self.FUNDAMENTALS_Query = "SELECT Code, Exchange, Type FROM `sportsight-tests.Finance_Data.eod_exchange_symbols_list` where Exchange in ('NYSE', 'NASDAQ') AND type='Common Stock' order by exchange desc"
        self.EOD_DAYBULK_URL = 'https://eodhistoricaldata.com/api/eod-bulk-last-day/{}?api_token=5d5d1d7259ef23.41685254&filter=extended&date={}'

    def get_eod_daily_bulk(self, startTime):
        csv_dir = self.main_dir + 'dated-files/'
        if not os.path.exists(csv_dir):
            os.mkdir(csv_dir)
        #datesPD = self.bqu.execute_query_to_df ("SELECT distinct format_date('%Y-%m-%d', timestamp) as Date FROM `sportsight-tests.Finance_Data.daily_stock_history_*` where timestamp<=parse_date('%x', '09/25/18') order by Date desc limit 230")
        #datesList = list(datesPD['Date'])
        datesList = ['2019-08-30'] #list(datesPD['Date'])
        for stockDate in datesList:
            dailyDF = pd.DataFrame()
            for exchange in ['COMM', 'INDX', 'NASDAQ', 'NYSE']:
                url = self.EOD_DAYBULK_URL.format(exchange, stockDate)
                print(url, dt.now()-startTime)
                try:
                    stockDF = pd.read_csv(url).fillna(0)[['Date', 'Open', 'High', 'Low', 'Close', 'Adjusted_close', 'Volume', 'Code']][:-1]
                    #print(stockDF.shape, stockDF.columns)
                    stockDF.rename(columns={'Code': 'Symbol'}, inplace = True)
                    stockDF['Exchange'] = exchange
                    #dtd = dt.strptime(stockDate, '%Y-%m-%d')
                    #print(dtd, type(dtd))
                    #finalDate = dtd.date()
                    #print(finalDate, type(finalDate))
                    #builtDate = dt.date(dtd.year, dtd.month, dtd.day)
                    #print(builtDate, type(builtDate))
                    #stockDF['Date'] = date(2019,8,25)
                    stockDF['Volume'] = stockDF['Volume'].astype(int)
                    stockDF.to_csv(csv_dir+'{}-{}.csv'.format(stockDate, exchange), index=False)
                    dailyDF = dailyDF.append(stockDF)
                except Exception as e:
                    print("Error {}".format(e))
                #break
            #tableId = 'Finance_Data.eod_history_data_{}'.format(date.replace('-', ''))
            if dailyDF.shape[0]>0:
                datasetId = 'Finance_Data'
                tableId = 'eod_daily_history_1year'
                delQuery = "delete from `{}.{}` where Date=PARSE_DATE('%Y-%m-%d', '{}')".format(datasetId, tableId, stockDate)
                #print(delQuery)
                #print(schema)
                self.bqu.execute_query(delQuery)
                print('Writing table {}, size {}, delta time {}'.format(tableId, dailyDF.shape, dt.now() - startTime))
                schema = self.bqu.get_table_schema(datasetId, tableId)
                dailyDF.to_gbq(
                    '{}.{}'.format(datasetId, tableId),
                    table_schema=schema,
                    if_exists='append'
                )

            #break
        print('Done', dt.now()-startTime)

    def get_fundamentals_data(self, startTime):
        fundamentals_dir = self.main_dir + 'fundamentals/'
        if not os.path.exists(fundamentals_dir):
            os.mkdir(fundamentals_dir)
        jsonFileName = 'fundamentals-{}.json'.format(dt.now().strftime('%Y-%m-%dT%H%M%S'))
        outfileName = fundamentals_dir + jsonFileName
        outfile = open(outfileName, 'w')

        stocksDict = self.bqu.execute_query_to_dict (self.FUNDAMENTALS_Query)
        print('Getting {} stocks data'.format(stocksDict['nRows']))
        count=0
        successCount=0
        for stock in stocksDict['Rows']:
            count += 1
            print(count, stock, dt.now()-startTime)
            try:
                retDict = eod.get_fundamental_data(stock['Code'], 'US', api_key=self.EODHD_apikey)
                relevantKeys = ['General', 'Highlights', 'Valuation', 'SharesStats']
                stockData={x: retDict[x] for x in relevantKeys if x in retDict}
                stockData['Time']=dt.now().strftime('%Y-%m-%dT%H:%M:%S')
                technicalsDict={}
                for key, value in retDict['Technicals'].items():
                    if key[0] in '0123456789':
                        technicalsDict['T'+key] = value
                    else:
                        technicalsDict[key] = value
                stockData['Technicals'] = technicalsDict
                json.dump(stockData,outfile)
                outfile.write('\n')
                successCount += 1

            except Exception as e:
                print("Error {}".format(e))
                #break
            #break
        outfile.close()
        if successCount>0:
            datasetId = 'Finance_Data'
            tableId = 'fundamentals_daily_{}'.format(startTime.strftime('%Y%m%d'))
            self.bqu.create_dataset(datasetId)
            uri = self.bqu.upload_file_to_gcp('sport-uploads', outfileName, 'Finance/EOD/Fundamentals/{}'.format(jsonFileName))
            ret = self.bqu.create_table_from_gcp_file(uri, datasetId, tableId, 'WRITE_TRUNCATE')

        print('Done', successCount, dt.now()-startTime)

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
        comp_df = self.bqu.execute_query_to_df(self.EOD_Symbols_Query)

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
    ehd.get_fundamentals_data(startTime)
    #ehd.create_dated_quote_files('2019-08-21')
    #ehd.upload_dated_quote_files('20190820')
    'Done'

test()