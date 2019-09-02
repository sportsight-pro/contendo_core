# get data and stats on a stock list
import datetime
import pandas as pd
import numpy as np
from get_stocks_data import get_stock_fundamentals, get_stockdata_by_dates, get_stockdata_by_cal_days


class StockMetricsCalculator:
# Stocklist will have all the tickers to be loaded
# startdate should be a tuple (yyyy,mm,dd)
# timeframe_name will show up in the timeframe section of texts    
  def __init__(self, stocklist, startdate, 
               timeframe_name = "3 months",
               listname = "S&P 500"):
    self.stocklist = stocklist
    self.startdate = startdate
    self.timeframe_name = timeframe_name
    self.listname = listname
    self.rawdata = get_stockdata_by_dates(stocklist,
                                     datetime.date(startdate[0],
                                                   startdate[1], 
                                                   startdate[2]),
                                     datetime.date.today())
    self.fundamentalData = get_stock_fundamentals(stocklist)
  
  def get_interesting_statements(self,ticker):
      selflist = [method_name for method_name in dir(self)
                  if callable(getattr(self, method_name))]
      selflist = [k for k in selflist if k[0] == "_" and k[1] != "_"]
      result = pd.DataFrame(columns=["Score","Statement"])
      for func in selflist:
          score,statement = getattr(self, func)(ticker)
          result.loc[-1] = [round(score,4),statement]
          result.index += 1
      return result.sort_values('Score',ascending=False)

  def _PEratio(self,ticker):
      filter = self.fundamentalData["PERatio"] != ""
      fund_no_null =  self.fundamentalData[filter]
      fund_no_null = fund_no_null.sort_values("PERatio").reset_index(drop=True)
      i = int(fund_no_null[fund_no_null['Symbol']==ticker].index[0])
      if i < 10:
          statement =  "{ticker} is number {i} on the list of best PE ratios in the {list}".format(ticker=ticker,i=i,list=self.listname)
      elif i > len(fund_no_null)-10:
          statement =  "{ticker} is number {i} on the list of worst PE ratios in the {list}".format(ticker=ticker,i=len(fund_no_null)-i,list=self.listname)
      else:
          top_pct = (1-i/len(fund_no_null))*100
          if top_pct > 50:
             adj = "top"
             pct = 100-top_pct
          else:
             adj = "bottom"
             pct = top_pct
          statement =  "{ticker}'s PE ratio is in the {adj} {pct}% in the {list}".format(ticker=ticker,adj=adj,pct=round(pct),list=self.listname)  
      interest = abs(i/len(fund_no_null)-0.5)*2
      return interest , statement

  def _52Wrange(self,ticker):
      low52 = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["T52WeekLow"].values[0]
      high52 = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["T52WeekHigh"].values[0]
      lastclose = self.rawdata[self.rawdata["Symbol"] == ticker].iloc[-1:].Close.values[0]
      relpos = (lastclose-low52)/(high52 - low52)
      statement =  "{ticker}'s last close was at {relpct}% of its 52 week range".format(ticker=ticker,relpct = round(relpos*100))
      interest = abs(relpos-0.5)*2
      return interest , statement      






