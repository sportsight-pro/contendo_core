import os
import json
from datetime import datetime as dt

from flask import Flask
from flask import escape
from flask import request
from werkzeug.exceptions import NotFound
from werkzeug.exceptions import BadRequest

from contendo_utils import BigqueryUtils
from contendo_utils import ProUtils
from StockMetricsCalculator import StockMetricsCalculator
from get_stocks_data import GetStocksData
from GetStockNews import GetStockNews

app = Flask(__name__)

def one_list_generator(listName, listConfigDict, startTime=dt.now()):
    listsDefDict = ProUtils.get_dict_from_jsonfile('lists_config.json')
    finquery = ProUtils.get_string_from_file('queries/top_lists_query.sql')
    #
    # read the query, configure and run it.
    instructions={}
    if listName in listsDefDict.keys():
        listConfig = listsDefDict[listName]
    else:
        raise NotFound('List {} does not exists'.format(listName))

    instructions['StatName']=listConfig['StatName']
    instructions['RollingDaysCondition'] = 'StatRollingDays="{}"'.format(listConfig['RollingDays'])

    if 'Sector' in listConfigDict:
        instructions['SectorCondition']='Sector="{}"'.format(listConfigDict['Sector'])
    else:
        instructions['SectorCondition']='TRUE'

    if listConfigDict.get('Index', '') in ['DJI', 'SNP']:
        instructions['IndexCondition'] = 'is'+listConfigDict['Index']
    else:
        instructions['IndexCondition'] = 'isSNP'

    minMarketCap = listConfigDict.get('MarketCapMin', 100)
    maxMarketCap = listConfigDict.get('MarketCapMax', 1000000000)
    instructions['MarketCapCondition'] = 'MarketCap BETWEEN {} AND {}'.format(minMarketCap, maxMarketCap)
    instructions['ListSize'] = min(listConfigDict.get('ListSize', 5), 10)

    #query = self.get_onelist_query(listConfigDict['Domain'])
    query = ProUtils.format_string(finquery, instructions)
    #print("Running query:\n" + query, flush=True)
    #return
    #
    # Execute the query.
    print('Starting get-top-list for {} query execution'.format(instructions), dt.now()-startTime)
    bqu = BigqueryUtils()
    listDF = bqu.execute_query_to_df(query)
    print(list(listDF['Symbol']))
    #listDF = listDF.query('TopBottom=="TOP"')
    #print (listDF.columns, listDF.shape, dt.now()-startTime)
    listDict = ProUtils.pandas_df_to_dict(listDF, 'TopRank')

    #
    # getting additional info
    print('Starting get_stock_fundamentals for {}'.format('SNP'), dt.now()-startTime)
    getstocks = GetStocksData()
    companiesDF = getstocks.get_stock_fundamentals(index='SNP')
    symbolList = list(companiesDF['Symbol'])
    print('Starting StockMetricsCalculator for {}, {} companies'.format(symbolList, len(symbolList)), dt.now()-startTime)
    smc = StockMetricsCalculator(symbolList)
    print('Done StockMetricsCalculator', dt.now()-startTime)
    gsn = GetStockNews()
    for key, stockDict in listDict.items():
        stockDict['InterestingStatements'] = get_statements_for_ticker(stockDict['Symbol'], smc)
        stockDict['RelevantNews'] = gsn.get_stocknews_byticker(stockDict['Symbol'])

    listDict['Description'] = listConfig['QuestionDescription']
    print(listDict, dt.now()-startTime )
    return json.dumps(listDict)

def get_statements_for_ticker(ticker, smc):
    interestingStatements = []
    try:
        interestingStatementsDF = smc.get_interesting_statements(ticker)
        for i, statement in interestingStatementsDF.iterrows():
            interestingStatements.append(dict(statement))
    except Exception as e:
        print("Exception {} while getting statements for {}".format(e, ticker), e.__traceback__)
        raise e

    return interestingStatements

def ticker_generator(ticker, startTime=dt.now()):
    #
    # get basic stock data
    getstocks = GetStocksData()
    companiesDF = getstocks.get_stock_fundamentals([ticker])
    if companiesDF.shape[0] == 0:
        raise NotFound('Ticker {} does not exists'.format(ticker))

    stockDict = dict(companiesDF[['Symbol', 'Industry', 'Sector', 'Name', 'Exchange', 'LastClose', 'T52WeekLow', 'T52WeekHigh', 'MarketCapitalizationMln', 'PERatio']].iloc[0])
    print(stockDict)
    #
    # get interestinf statements for the stock.
    companiesDF = getstocks.get_stock_fundamentals(index='SNP')
    symbolList = list(companiesDF['Symbol'])
    #print('Starting StockMetricsCalculator for {}, {} companies'.format(symbolList, len(symbolList)), dt.now()-startTime)
    smc = StockMetricsCalculator(symbolList)
    stockDict['InterestingStatements'] = get_statements_for_ticker(stockDict['Symbol'], smc)
    #
    # get the stock news for the ticker
    gsn = GetStockNews()
    stockDict['RelevantNews'] = gsn.get_stocknews_byticker(stockDict['Symbol'])
    return json.dumps(stockDict)

@app.route('/')
def index():
  return 'Service contendo-finance-api is up and running!'


@app.route('/finance/v1/get_ticker_data/<ticker>')
def get_ticker_data(ticker):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    #try:
    return ticker_generator(ticker)

@app.route('/finance/v1/get_top_lists/<listname>', methods=['GET','POST'])
def get_top_lists(listname):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    #try:
    if request.method == 'POST':
        # check user details from db
        request_json = request.get_json(silent=True)
        ret = one_list_generator(listname, request_json)
        print (type(ret), ret)
        return ret

    elif request.method == 'GET':
        # serve login page
        request_args = request.args
        print('args:', request_args)
        return one_list_generator(listname, request_args)

    else:
        raise BadRequest('Bad request: {}'.format(escape(request.data)))

    #except Exception as e:
    #    return 'Exception with request {}'.format(e)

if __name__ == "__main__":
    print(os.getcwd())
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'sportsight-tests.json'
    app.run(debug=True,host='0.0.0.0',port=int(os.environ.get('PORT', 8080)))
    #app.run(port=int(os.environ.get('PORT', 8080)))
