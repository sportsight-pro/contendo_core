import pandas as pd
import os
import datetime
from datetime import datetime as dt, timedelta, date
from contendo_utils import BigqueryUtils, ProUtils

# get list of stock action from date to date

class GetStocksData:

    def __init__(self):
        self.bqu = BigqueryUtils()
        self.companiesDF = None
        self.stocksDF = None
        self.resourceDir = 'resource'
        self.companiesDataFileName = '{}/companies_{}.csv'.format(self.resourceDir, date.today())
        self.stocksDataFileName = '{}/stocks_{}.csv'.format(self.resourceDir, date.today())
        self.companiesURL = 'gs://sport-uploads/Finance/companies_fundamentals.csv'
        self.stocksURL = 'gs://sport-uploads/Finance/eod_stocks_data.csv'

    def get_stockdata_by_dates(self, stocklist, from_date, to_date):
        #
        # get updated company data
        if self.stocksDF is None:
            if os.path.exists(self.stocksDataFileName):
                self.stocksDF = pd.read_csv(self.stocksDataFileName)
            else:
                stocksQuery = """SELECT * FROM `sportsight-tests.Finance_Data.eod_daily_history_1year` order by Symbol, Date"""
                self.stocksDF = self.bqu.execute_query_to_df(stocksQuery)
                if not os.path.exists(self.resourceDir):
                    os.mkdir(self.resourceDir)
                self.stocksDF.to_csv(self.stocksDataFileName)
            self.stocksDF['Date1'] = self.stocksDF['Date'].astype(str)
                #url = self.bqu.upload_file_to_gcp('sport-uploads', self.stocksDataFileName, self.stocksURL.replace('gs://sport-uploads/', ''))

        if len(stocklist)>0:
            symbol_condition = 'Symbol in {tickersString} and '.format(tickersString=str(stocklist))
        else:
            symbol_condition = ''

        stocksQuery = '{symbol_condition} Date1 >= "{from_date}" and Date1 <= "{to_date}"'.format(symbol_condition=symbol_condition, from_date=from_date, to_date=to_date)
        stockDataDF = self.stocksDF.query(stocksQuery)
        stockDataDF.index = pd.to_datetime(stockDataDF['Date'])
        stockDataDF.rename_axis("date", axis='index', inplace=True)
        return stockDataDF

    # get list of stock action x days to date
    def get_stockdata_by_cal_days(self, stocklist, numdays, to_date):
        from_date = to_date - datetime.timedelta(days=numdays-1)
        return self.get_stockdata_by_dates(stocklist, from_date, to_date)

    def get_stock_fundamentals(self, stocklist=None, index=None, exchange=None):
        #
        # get updated company data
        if self.companiesDF is None:
            if os.path.exists(self.companiesDataFileName):
                self.companiesDF = pd.read_csv(self.companiesDataFileName)
            else:
                companiesQuery = """SELECT * FROM `sportsight-tests.Finance_Data.all_company_data` WHERE MarketCapitalizationMln > 1000"""
                self.companiesDF = self.bqu.execute_query_to_df(companiesQuery, fillna=0)
                if not os.path.exists(self.resourceDir):
                    os.mkdir(self.resourceDir)
                self.companiesDF.to_csv(self.companiesDataFileName)
                #url = self.bqu.upload_file_to_gcp('sport-uploads', self.companiesDataFileName, self.companiesURL.replace('gs://sport-uploads/', ''))

        if stocklist is not None:
            where_condition = 'Symbol in {tickersString}'.format(tickersString=str(stocklist))
        elif index in ['DJI', 'SNP']:
            where_condition = 'is{index}'.format(index=index)
        elif exchange in ['NASDAQ', 'NYSE']:
            where_condition = 'Exchange=="{exchange}"'.format(exchange=exchange)
        else:
            return self.companiesDF

        return self.companiesDF.query(where_condition)


if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
    # Test
    startTime = dt.now()
    getstocks = GetStocksData()
    companiesDF = getstocks.get_stock_fundamentals(exchange='NYSE')
    #companiesDF = getstocks.get_stock_fundamentals(['MSFT', 'AAPL'])
    symbolList = list(companiesDF['Symbol'])
    print(symbolList, len(symbolList))
    print(dt.now()- startTime)
    #a = get_stockdata_by_cal_days(["AAPL","IBM"],90,datetime.date.today())
    a = getstocks.get_stockdata_by_cal_days([], 365, datetime.date.today())
    print(dt.now()- startTime)
    print(a.shape)
    #a.to_csv('../../../../results/stocks.csv')