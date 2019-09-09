# get data and stats on a stock list
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from get_stocks_data import GetStocksData
from ta import momentum
import statsmodels.api as sm
from statsmodels import regression


class StockMetricsCalculator:
    # Stocklist will have all the tickers to be loaded
    # startdate should be a tuple (yyyy,mm,dd)
    # timeframe_name will show up in the timeframe section of texts
    def __init__(self, stocklist, listname="S&P 500"):
        self.getstocks = GetStocksData()
        self.stocklist = stocklist
        self.listname = listname
        self.sectdata = self.getstocks.get_stockdata_by_dates(
            ["XLE", "XLB", "XLI", "XLY", "XLP", "XLV", "XLF", "XLK", "XTL", "XLU", "XLRE"],
            datetime.date.today() - relativedelta(years=1),
            datetime.date.today())
        self.listdata = self.getstocks.get_stockdata_by_dates(["NYA", "DJI", "GSPC", "IXIC"],
                                                              datetime.date.today() - relativedelta(years=1),
                                                              datetime.date.today())
        self.rawdata = self.getstocks.get_stockdata_by_dates(stocklist,
                                                             datetime.date.today() - relativedelta(years=1),
                                                             datetime.date.today())
        self.fundamentalData = self.getstocks.get_stock_fundamentals(stocklist)

        self.sectorETFDict = {'Consumer Cyclical': "XLY",
                              'Consumer Defensive': "XLP",
                              'Technology': "XLK",
                              'Healthcare': "XLV",
                              'Industrials': "XLI",
                              'Industrial Goods': "XLI",
                              'Utilities': "XLU",
                              'Basic Materials': "XLB",
                              'Energy': "XLE",
                              'Financial Services': "XLF",
                              'Communication Services': "XLC",
                              'Real Estate': "XLRE"}

    def get_interesting_statements(self, ticker):
        selflist = [method_name for method_name in dir(self)
                    if callable(getattr(self, method_name))]
        selflist = [k for k in selflist if k[0] == "S" and k[1] == "_"]
        result = pd.DataFrame(columns=["Score", "Statement"])
        for func in selflist:
            try:
                score, statement = getattr(self, func)(ticker)
                result.loc[-1] = [round(score, 4), statement]
                result.index += 1
            except Exception as e:
                print ('Error {} in with get_interesting_statements function {} for ticker {}'.format(e, func, ticker))
                continue
        result = result.sort_values('Score', ascending=False)
        return result[result["Statement"] != ""]

    def S_PEratio(self, ticker):
        if self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["PERatio"].values[0] == 0:
            return 0, ""
        filter = self.fundamentalData["PERatio"] != 0
        fund_no_null = self.fundamentalData[filter]
        fund_no_null = fund_no_null.sort_values("PERatio").reset_index(drop=True)
        i = int(fund_no_null[fund_no_null['Symbol'] == ticker].index[0])
        if i < 10:
            statement = "{ticker} is number {i} on the list of best(lowest) PE ratios in the {list}".format(
                ticker=ticker,
                i=i,
                list=self.listname)
        elif i > len(fund_no_null) - 10:
            statement = "{ticker} is number {i} on the list of worst(highest) PE ratios in the {list}".format(
                ticker=ticker,
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

    def S_52Wrange(self, ticker):
        low52 = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["T52WeekLow"].values[0]
        high52 = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["T52WeekHigh"].values[0]
        lastclose = self.rawdata[self.rawdata["Symbol"] == ticker].iloc[-1:].Close.values[0]
        relpos = (lastclose - low52) / (high52 - low52)
        statement = "{ticker}'s last close was at {relpct}% of its 52 week range".format(ticker=ticker,
                                                                                         relpct=round(relpos * 100))
        interest = (abs(relpos - 0.5) * 2) ** 2
        return interest, statement

    def S_dayrange(self, ticker):
        lastday = self.rawdata.loc[self.rawdata['Symbol'] == ticker][-1:]
        relpos = (lastday.Close.values[0] - lastday.Low.values[0]) / (lastday.High.values[0] - lastday.Low.values[0])
        statement = "{ticker}'s last close was at {relpct}% of its daily range".format(ticker=ticker,
                                                                                       relpct=round(relpos * 100))
        interest = (abs(relpos - 0.5) * 2) ** 3
        return interest, statement

    def S_volincrease(self, ticker):
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

    def S_consecutioveUps(self, ticker):
        degree = 30
        y = self.rawdata.loc[self.rawdata['Symbol'] == ticker].Close.diff() > 0
        condays = y * (y.groupby((y != y.shift()).cumsum()).cumcount() + 1)
        interest = ((len(condays) - sum(condays >= condays[-1])) / len(condays)) ** degree
        if condays[-1] >= 2:
            statement = "{ticker} is up {condays} days in a row".format(ticker=ticker, condays=condays[-1])
        else:
            statement = ""
        return interest, statement

    def S_consecutioveDowns(self, ticker):
        degree = 30
        y = self.rawdata.loc[self.rawdata['Symbol'] == ticker].Close.diff() < 0
        condays = y * (y.groupby((y != y.shift()).cumsum()).cumcount() + 1)
        interest = ((len(condays) - sum(condays >= condays[-1])) / len(condays)) ** degree
        if condays[-1] >= 2:
            statement = "{ticker} is down {condays} days in a row".format(ticker=ticker, condays=condays[-1])
        else:
            statement = ""
        return interest, statement

    def S_RSI14(self, ticker):
        dailydata = self.rawdata.loc[self.rawdata['Symbol'] == ticker]
        rsi = momentum.rsi(dailydata.Close, n=14, fillna=True)
        if rsi[-1] < 40 or rsi[-1] > 60:
            interest = abs(((rsi[-1] / 100 - 0.5) * 2)) ** (1 / 3)
        else:
            return 0, ""
        if abs(rsi[-1] - 50) > 20:
            adj = "Strongly"
        else:
            adj = "moderately"
        if rsi[-1] > 50:
            state = "overbought"
        else:
            state = "oversold"
        statement = "{ticker}'s 14 day RSI is indicating that it is {adj} {state}".format(ticker=ticker, adj=adj,
                                                                                          state=state)
        return interest, statement

    def S_rel_volatility(self, ticker):
        isSNP = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["isSNP"].values[0]
        isDJI = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["isDJI"].values[0]
        isNASDAQ = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["Exchange"].values[0] == "NASDAQ"

        benchtick = "NYA"
        benchname = "New York Stock Exchange"

        if isNASDAQ:
            benchtick = "IXIC"
            benchname = "NASDAQ"
        if isSNP:
            benchtick = "GSPC"
            benchname = "S&P 500"
        if isDJI:
            benchtick = "DJI"
            benchname = "Dow Jones Industrial"

        benchret = self.listdata.Close[self.listdata.Symbol == benchtick].pct_change()[1:].values
        stockret = self.rawdata.Close[self.rawdata.Symbol == ticker].pct_change()[1:].values
        alpha, beta = self._linreg(benchret, stockret)

        if beta > 1:
            adj = "volatile"
        else:
            adj = "stable"

        pctdiff = round(abs(beta - 1) * 100, 1)
        statement = "{ticker} is {pctdiff}% more {adj} then the {benchname} benchmark".format(ticker=ticker,
                                                                                              pctdiff=pctdiff, adj=adj,
                                                                                              benchname=benchname)
        interest = np.min([np.abs(beta - 1) ** 0.5, 1])
        return interest, statement

    def S_sector_daily_return(self, ticker):
        sector = self.fundamentalData[self.fundamentalData["Symbol"] == ticker]["Sector"].values[0]
        if sector not in self.sectorETFDict.keys():
            return 0, ""
        sec_ticker = self.sectorETFDict[sector]
        sec_last_week = self.sectdata.loc[self.sectdata['Symbol'] == sec_ticker]["Close"][-5]
        sec_last = self.sectdata.loc[self.sectdata['Symbol'] == sec_ticker]["Close"][-1]
        stock_last_week = self.rawdata.loc[self.rawdata['Symbol'] == ticker]["Close"][-5]
        stock_last = self.rawdata.loc[self.rawdata['Symbol'] == ticker]["Close"][-1]
        sec_ret = (sec_last - sec_last_week) / sec_last_week * 100
        stock_ret = (stock_last - stock_last_week) / stock_last_week * 100
        sec_direction = ["up", "Down"][sec_ret < 0]
        stock_direction = ["up", "Down"][stock_ret < 0]
        statement = "During the last week {ticker} is {stock_direction} {stock_ret}% versus the entire {sector} sector that is {sec_direction} {sec_ret}%".format(
            ticker=ticker,
            stock_direction=stock_direction,
            sec_direction=sec_direction,
            stock_ret=abs(round(stock_ret, 2)),
            sec_ret=abs(round(sec_ret, 2)),
            sector=sector)
        interest = min([abs((stock_ret - sec_ret) / 8), 1])
        return interest, statement

    #   def _volatility_bata(self, ticker):
    def _linreg(self, x, y):
        x = sm.add_constant(x)
        model = regression.linear_model.OLS(y, x).fit()
        # remove the constant
        x = x[:, 1]
        return model.params[0], model.params[1]

