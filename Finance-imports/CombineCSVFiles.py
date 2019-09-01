import os
import glob
import pandas as pd
import csv
from datetime import datetime as dt,timedelta


def combine_files():
    os.chdir("/Users/ysherman/Documents/GitHub/results/Finance/EODHistoricalData/daily-quotes/2019-08-25")
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

    mydialect = csv.Dialect
    mydialect.lineterminator = '\n'
    mydialect.quoting = csv.QUOTE_MINIMAL
    mydialect.quotechar = '|'

    count = 0
    lineCount=0
    volumeDict = {}
    main_start_time = dt.now()
    outfile = open('../daily-quotes-agg.csv', 'w')
    outFileWriter = csv.writer(outfile, delimiter=',', dialect=mydialect)
    firstTime = True
    for csvFileName in all_filenames:
        infile = open(csvFileName, 'r')
        linereader = csv.reader(infile, delimiter=',')
        firstrow = True
        for line in linereader:
            if firstrow and not firstTime:
                firstrow = False
                continue
            if line[6].find('.')>=0:
                line[6] = int(float(line[6]))
                symbol = line[7]
                if symbol in volumeDict.keys():
                    volumeDict[symbol] +=1
                else:
                    volumeDict[symbol] = 1
                    print(symbol)
            outFileWriter.writerow(line)
            lineCount+=1
        count += 1
        firstTime = False
        if count % 1000 == 0:
            print('Done writing: {} of {}, lines: {}, Total-delta: {}\n'.format(count, len(all_filenames), lineCount,
                                                                    dt.now() - main_start_time))
    print('Done {}, lines: {}, {}'.format(count, lineCount, volumeDict))

def files_by_date():
    os.chdir("/Users/ysherman/Documents/GitHub/results/Finance/daily-quotes")
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
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
        if count % 100 == 0:
            print('Done reading: {} of {} Total-delta: {}\n'.format(count, len(all_filenames),
                                                                    dt.now() - main_start_time))

import BigqueryUtils

def upload_files():
    os.chdir("/Users/ysherman/Documents/GitHub/results/Finance/daily-quotes/dated-files")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ysherman/Documents/GitHub/sportsight-tests.json"
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    print(all_filenames[0:10])
    bqu = BigqueryUtils.BigqueryUtils()
    main_start_time = dt.now()
    count=0
    for csvFileName in all_filenames:
        date = csvFileName.split('.')[0].replace('-', '')
        if (date<'20190101'):
            continue
        tableId  = 'daily_stock_history_{}'.format(date)
        print (tableId, dt.now() - main_start_time)
        csvFile = open(csvFileName, 'rb')
        bqu.create_table_from_local_file(csvFile, 'Finance_Data', tableId)
        csvFile.close()
        count += 1
        if count % 20 == 0:
            print('Done reading: {} of {} Total-delta: {}\n'.format(count, len(all_filenames),
                                                                    dt.now() - main_start_time))


def test():
    #combine_files()
    import EODHistoricalDataImport
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ysherman/Documents/GitHub/sportsight-tests.json"
    startTime = dt.now()

    ehd = EODHistoricalDataImport.EODHistoricalDataImport()
    #ehd.run(numExecutors=2)
    #ehd.import_daily_quotes([], startTime)
    #date = '2019-08-20'
    ehd.get_eod_daily_bulk(startTime)
    #ehd.get_fundamentals_data(startTime)
    #ehd.create_dated_quote_files('2019-08-21')
    #ehd.upload_dated_quote_files('20190820')
    'Done'


test()