import numpy as np
import pandas as pd
from google.cloud import bigquery
import datetime
from datetime import datetime as dt, timedelta, date

import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../../sportsight-tests.json"
__bigquery_client = bigquery.Client()

# get list of stock action from date to date
def get_stockdata_by_dates(stocklist, from_date, to_date):
    if len(stocklist)>0:
        tickersString = str(stocklist).replace('[', '').replace(']', '')
        symbol_condition = 'Symbol IN ({tickersString})'.format(tickersString=tickersString)
    else:
        symbol_condition = 'TRUE'

    to_str = "'{}'".format(str(to_date)).replace("-","")
    from_str = "'{}'".format(str(from_date)).replace("-","")
    stockDataQuery = """SELECT * FROM `sportsight-tests.Finance_Data.eod_daily_history_1year` WHERE {symbol_condition} AND FORMAT_DATE('%Y%m%d', Date) BETWEEN {from_str} AND {to_str} and Exchange in ('NYSE', 'NASDAQ') ORDER BY Symbol, Date""".format(symbol_condition=symbol_condition,from_str=from_str,to_str=to_str)
    #print (stockDataQuery)
    query_job = __bigquery_client.query(stockDataQuery)
    stockDataDF = query_job.result().to_dataframe().fillna('')
    stockDataDF.index =pd.to_datetime(stockDataDF['Date'])
    stockDataDF.rename_axis("date", axis='index', inplace=True)
    return stockDataDF

# get list of stock action x days to date
def get_stockdata_by_cal_days(stocklist,numdays,to_date):
    from_date = to_date - datetime.timedelta(days=numdays-1)
    return get_stockdata_by_dates(stocklist, from_date, to_date)

def get_stock_fundamentals(stocklist=[], index=None):
    if len(stocklist)>0:
        tickersString = str(stocklist).replace('[', '').replace(']', '')
        where_condition = 'Symbol IN ({tickersString})'.format(tickersString=tickersString)
    elif index in ['DJI', 'SNP']:
        where_condition = 'is{index}'.format(index=index)
    else:
        where_condition = 'TRUE'
    stockDataQuery = """SELECT * FROM `sportsight-tests.Finance_Data.all_company_data` WHERE {where_condition} ORDER BY Symbol""".format(where_condition=where_condition)
    query_job = __bigquery_client.query(stockDataQuery)
    stockDataDF = query_job.result().to_dataframe().fillna('')
    return stockDataDF


if __name__ == '__main__':
    # Test
    startTime = dt.now()
    companiesDF = get_stock_fundamentals(index='SNP')
    symbolList = list(companiesDF['Symbol'])
    print(symbolList)
    print(dt.now()- startTime)
    #a = get_stockdata_by_cal_days(["AAPL","IBM"],90,datetime.date.today())
    a = get_stockdata_by_cal_days(symbolList,365,datetime.date.today())
    print(dt.now()- startTime)
    print(a.shape)
    a.to_csv('stocks.csv')