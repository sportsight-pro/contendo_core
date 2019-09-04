# get data and stats on a stock list
import datetime
import pandas as pd
import numpy as np
from get_stocks_data import GetStocksData
from ta import *

class StockMetricsCalculator:
# Stocklist will have all the tickers to be loaded
# startdate should be a tuple (yyyy,mm,dd)
# timeframe_name will show up in the timeframe section of texts
    def __init__(self, stocklist, startdate,
                 timeframe_name="3 months",
                 listname="S&P 500"):
        self.getstocks = GetStocksData()
        self.stocklist = stocklist
        self.startdate = startdate
        self.timeframe_name = timeframe_name
        self.listname = listname
        self.rawdata = self.getstocks.get_stockdata_by_dates(stocklist,
                                                        datetime.date(startdate[0],
                                                                      startdate[1],
                                                                      startdate[2]),
                                                        datetime.date.today())
        self.fundamentalData = self.getstocks.get_stock_fundamentals(stocklist)


    def get_interesting_statements(self, ticker):
        selflist = [method_name for method_name in dir(self)
                    if callable(getattr(self, method_name))]
        selflist = [k for k in selflist if k[0] == "_" and k[1] != "_"]
        result = pd.DataFrame(columns=["Score", "Statement"])
        for func in selflist:
            score, statement = getattr(self, func)(ticker)
            result.loc[-1] = [round(score, 4), statement]
            result.index += 1
        result = result.sort_values('Score', ascending=False)
        return result[result["Statement"] != ""]


    def _PEratio(self, ticker):
        if self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["PERatio"].values[0] == "":
            return 0, ""
        filter = self.fundamentalData["PERatio"] != 0
        fund_no_null = self.fundamentalData[filter]
        fund_no_null = fund_no_null.sort_values("PERatio").reset_index(drop=True)
        i = int(fund_no_null[fund_no_null['Symbol'] == ticker].index[0])
        if i < 10:
            statement = "{ticker} is number {i} on the list of best(lowest) PE ratios in the {list}".format(ticker=ticker,
                                                                                                            i=i,
                                                                                                            list=self.listname)
        elif i > len(fund_no_null) - 10:
            statement = "{ticker} is number {i} on the list of worst(highest) PE ratios in the {list}".format(ticker=ticker,
                                                                                                              i=len(
                                                                                                                  fund_no_null) - i,
                                                                                                              list=self.listname)
        else:
            top_pct = (1 - i / len(fund_no_null)) * 100
            if top_pct > 50:
                adj = "top"
                pct = 100 - top_pct
            else:
                adj = "bottom"
                pct = top_pct
            statement = "{ticker}'s PE ratio is in the {adj} {pct}% in the {list}".format(ticker=ticker, adj=adj,
                                                                                          pct=round(pct),
                                                                                          list=self.listname)
        interest = abs(i / len(fund_no_null) - 0.5) * 2
        return interest, statement


    def _52Wrange(self, ticker):
        low52 = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["T52WeekLow"].values[0]
        high52 = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["T52WeekHigh"].values[0]
        lastclose = self.rawdata[self.rawdata["Symbol"] == ticker].iloc[-1:].Close.values[0]
        relpos = (lastclose - low52) / (high52 - low52)
        statement = "{ticker}'s last close was at {relpct}% of its 52 week range".format(ticker=ticker,
                                                                                         relpct=round(relpos * 100))
        interest = (abs(relpos - 0.5) * 2) ** 2
        return interest, statement


    def _dayrange(self, ticker):
        lastday = self.rawdata.loc[self.rawdata['Symbol'] == ticker][-1:]
        relpos = (lastday.Close.values[0] - lastday.Low.values[0]) / (lastday.High.values[0] - lastday.Low.values[0])
        statement = "{ticker}'s last close was at {relpct}% of its daily range".format(ticker=ticker,
                                                                                       relpct=round(relpos * 100))
        interest = (abs(relpos - 0.5) * 2) ** 3
        return interest, statement


    def _volincrease(self, ticker):
        lastvolume = self.rawdata.loc[self.rawdata['Symbol'] == ticker][-1:]["Volume"].values[0]
        avg10volume = np.mean(self.rawdata.loc[self.rawdata['Symbol'] == ticker][-11:-1]["Volume"].values[0])
        volchange = lastvolume / avg10volume
        interest = np.min([np.log10(volchange), 1])
        if volchange < 1:
            interest = 0
            statement = "{ticker}'s volume is lower or in line with its 10-day average".format(ticker=ticker)
            return interest, statement
        if volchange < 2:
            statement = "The volume of {ticker} in the last session was {relpct}% higher than its 10 day average".format(
                ticker=ticker, relpct=round((volchange - 1) * 100))
            return interest, statement
        statement = "The volume of {ticker} in the last session was {volchange} times higher than its 10 day average".format(
            ticker=ticker, volchange=round(volchange, 1))
        return interest, statement


    def _consecutioveUps(self, ticker):
        degree = 30
        y = self.rawdata.loc[self.rawdata['Symbol'] == ticker].Close.diff() > 0
        condays = y * (y.groupby((y != y.shift()).cumsum()).cumcount() + 1)
        interest = ((len(condays) - sum(condays >= condays[-1])) / len(condays)) ** degree
        if condays[-1] >= 2:
            statement = "{ticker} is up {condays} days in a row".format(ticker=ticker, condays=condays[-1])
        else:
            statement = ""
        return interest, statement


    def _consecutioveDowns(self, ticker):
        degree = 30
        y = self.rawdata.loc[self.rawdata['Symbol'] == ticker].Close.diff() < 0
        condays = y * (y.groupby((y != y.shift()).cumsum()).cumcount() + 1)
        interest = ((len(condays) - sum(condays >= condays[-1])) / len(condays)) ** degree
        if condays[-1] >= 2:
            statement = "{ticker} is down {condays} days in a row".format(ticker=ticker, condays=condays[-1])
        else:
            statement = ""
        return interest, statement

    def _RSI14(self,ticker):
      dailydata = self.rawdata.loc[self.rawdata['Symbol'] == ticker]
      rsi = momentum.rsi(dailydata.Close, n=14, fillna=True)
      if rsi[-1] < 40 or rsi[-1] > 60:
          interest = abs(((rsi[-1]/100 - 0.5)*2))**(1/3)
      else:
          return 0,""
      if abs(rsi[-1]-50) > 20:
          adj = "Strongly"
      else:
          adj = "moderately"
      if rsi[-1] > 50:
          state = "overbought"
      else:
          state = "oversold"
      statement = "{ticker}'s 14 day RSI is indicating that it is {adj} {state}".format(ticker=ticker,adj=adj,state=state)
      return interest , statement