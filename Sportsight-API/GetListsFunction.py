listsDefDict = {'Variance10': {'QuestionDescription': 'variance 10 days', 'QuestionCode': 'Variance10', 'StatName': 'Finance.Variance', 'StatObject': 'Stock', 'RollingDays': 10, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher variance 10 days Over the last {RollingDays} days?', 'SentenceRegardingList': 'Highest variance over the last {RollingDays} days', 'Doit': 'y'}, 'CMF10': {'QuestionDescription': 'Chikin Money Flow index 10 days', 'QuestionCode': 'CMF10', 'StatName': 'Finance.CMF', 'StatObject': 'Stock', 'RollingDays': 10, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher Chikin Money Flow index 10 days Over the last {RollingDays} days?', 'SentenceRegardingList': 'Highest CMF over the last {RollingDays} days', 'Doit': 'y'}, 'Volatility10': {'QuestionDescription': 'volatility 10 days', 'QuestionCode': 'Volatility10', 'StatName': 'Finance.dailySwing', 'StatObject': 'Stock', 'RollingDays': 10, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher volatility 10 days Over the last {RollingDays} days?', 'SentenceRegardingList': 'Highest volatility over the last {RollingDays} days', 'Doit': 'y'}, 'DailyGain': {'QuestionDescription': 'Daily Gain %', 'QuestionCode': 'DailyGain', 'StatName': 'Finance.changePercentage', 'StatObject': 'Stock', 'RollingDays': 1, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher Daily Gain % Over the last {RollingDays} days?', 'SentenceRegardingList': 'Highest daily gain', 'Doit': 'y'}, 'WeeklyGain': {'QuestionDescription': 'Weekly Gain', 'QuestionCode': 'WeeklyGain', 'StatName': 'Finance.changePercentage', 'StatObject': 'Stock', 'RollingDays': 5, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher Weekly Gain Over the last {RollingDays} days?', 'SentenceRegardingList': 'Highest weekly gain', 'Doit': 'y'}, 'MonthlyGain': {'QuestionDescription': 'Monthly Gain', 'QuestionCode': 'MonthlyGain', 'StatName': 'Finance.OneMonthChangePercentage', 'StatObject': 'Stock', 'RollingDays': 0, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher %Monthly Gain ?', 'SentenceRegardingList': 'Highest monthly gain', 'Doit': 'y'}, 'QuarterlyGain': {'QuestionDescription': 'Quarterly Gain', 'QuestionCode': 'QuarterlyGain', 'StatName': 'Finance.ThreeMonthChangePercentage', 'StatObject': 'Stock', 'RollingDays': 0, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher %Quarterly Gain ?', 'SentenceRegardingList': 'Highest quarterly gain', 'Doit': 'y'}, 'YTDGain': {'QuestionDescription': 'YTD Gain', 'QuestionCode': 'YTDGain', 'StatName': 'Finance.YTDChangePercentage', 'StatObject': 'Stock', 'RollingDays': 0, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher %YTD Gain ?', 'SentenceRegardingList': 'Highest gain YTD', 'Doit': 'y'}, 'OneYearGain': {'QuestionDescription': '1 Year Gain', 'QuestionCode': 'OneYearGain', 'StatName': 'Finance.OneYearChangePercentage', 'StatObject': 'Stock', 'RollingDays': 0, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher %1 Year Gain ?', 'SentenceRegardingList': 'Highest gain over one year', 'Doit': 'y'}, 'belowMA50': {'QuestionDescription': 'Close value below MA 50 days', 'QuestionCode': 'belowMA50', 'StatName': 'Finance.belowMA', 'StatObject': 'Stock', 'RollingDays': 50, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher Close value below MA 50 days Over the last {RollingDays} days?', 'SentenceRegardingList': 'Highest close-value below moving average over the last {RollingDays} days', 'Doit': 'y'}, 'aboveMA50': {'QuestionDescription': 'Close value above MA 50 days', 'QuestionCode': 'aboveMA50', 'StatName': 'Finance.aboveMA', 'StatObject': 'Stock', 'RollingDays': 50, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher Close value above MA 50 days Over the last {RollingDays} days?', 'SentenceRegardingList': 'Highest close-value above moving average over the last {RollingDays} days', 'Doit': 'y'}, 'DollarVolume': {'QuestionDescription': 'Dollar Volume', 'QuestionCode': 'DollarVolume', 'StatName': 'Finance.dollarVolume', 'StatObject': 'Stock', 'RollingDays': 0, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher Dollar Volume on {DayOfWeek}?', 'SentenceRegardingList': 'Highest dollar volume', 'Doit': 'y'}, 'VolumePercentage': {'QuestionDescription': 'Volume % of company', 'QuestionCode': 'VolumePercentage', 'StatName': 'Finance.volumePercentage', 'StatObject': 'Stock', 'RollingDays': 0, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher Volume % of company on {DayOfWeek}?', 'SentenceRegardingList': 'Highest dollar volume percentage from company value', 'Doit': 'y'}, 'MarketCapMil': {'QuestionDescription': 'Market cap in millions', 'QuestionCode': 'MarketCapMil', 'StatName': 'Finance.marketCap', 'StatObject': 'Stock', 'RollingDays': 0, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher Market cap in millions on {DayOfWeek}?', 'SentenceRegardingList': 'Highest market cap in millions', 'Doit': 'y'}, 'HypeChange': {'QuestionDescription': 'Google finance search trend', 'QuestionCode': 'HypeChange', 'StatName': 'Finance.FinanceTrend', 'StatObject': 'Stock', 'RollingDays': 0, 'SortDirection': 'DESC', 'Value1Template': 'FLOAT', 'Value2Template': 'FLOAT', 'Question2Objects': 'Which {StatObject} had higher Google finance search trend on {DayOfWeek}?', 'SentenceRegardingList': 'Highest google search trend', 'Doit': 'y'}}

finquery="""
WITH
  insightStats AS (
  SELECT
    DENSE_RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName ORDER BY StatValue DESC) AS highDenseRank,
    RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName ORDER BY StatValue DESC) AS TopRank,
    DENSE_RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName ORDER BY StatValue ASC) AS lowDenseRank,
    RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName ORDER BY StatValue ASC) AS BottomRank,
    COUNT(*) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName) AS objectsCount,
    MIN(StatValue) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName) AS minValue,
    MAX(StatValue) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName) AS maxValue,
    MAX(StatDate) OVER (PARTITION BY StatObject, StatRollingDays, StatName) AS maxDate,
    CASE
      WHEN StatObject='Sector' THEN Sector
      WHEN StatObject='Stock' THEN FORMAT("%s (%s)", Name, Symbol)
    END
    AS StatObjectName,
    stats.*
  FROM
    `Finance_Data.all_stats_finance` stats
  WHERE
    StatName = '{StatName}'
    AND {SectorCondition}
    AND {MarketCapCondition}
    AND ({IndexCondition})
    #AND {StatObjectCondition}
    AND {RollingDaysCondition}
  ),
  insightStatsFinal AS (
  SELECT
    if (TopRank<={ListSize}, 'TOP', 'BOTTOM') as TopBottom,
    *
  FROM
    insightStats
  WHERE
    (TopRank<={ListSize} or BottomRank<={ListSize})
    AND StatDate=maxDate
  )
SELECT
  #StatName,
  Symbol,
  Name,
  Sector,
  Exchange,
  isDJI,
  isSNP,
  MarketCap,
  format_date('%Y-%m-%d', StatDate) as StatDate,
  StatValue,
  TopBottom,
  TopRank,
  BottomRank
FROM
  insightStatsFinal
ORDER BY
  TopBottom DESC,
  TopRank
"""
from google.cloud import bigquery
from datetime import datetime as dt
import json
import sys
from flask import escape

def pandas_df_to_dict(in_df, key):
    ret_dict = {}
    for i, config in in_df.iterrows():
        ret_dict[config[key]] = dict(config)

    return ret_dict

def format_string(string, instructions):
    if type(string)!=str:
        return ''
    outstring = string
    for key,value in instructions.items():
        outstring=outstring.replace('{'+key+'}', str(value))

    return outstring

def execute_query_to_df(query, fillna=''):
    __bigquery_client = bigquery.Client()
    query_job = __bigquery_client.query(query)
    ret_df = query_job.result().to_dataframe().fillna(fillna)
    return ret_df

def one_list_generator(listConfigDict, startTime=dt.now()):
    #
    # read the query, configure and run it.
    instructions={}
    if 'Listname' in listConfigDict:
        listName=listConfigDict['Listname']
    else:
        return {'Error': '"Listname" parameter must be defined'}

    if listName in listsDefDict.keys():
        listConfig = listsDefDict[listName]
    else:
        return {'Error': 'StatName parameter illegal'}

    instructions['StatName']=listConfig['StatName']
    instructions['RollingDaysCondition'] = 'StatRollingDays="{}"'.format(listConfig['RollingDays'])

    if 'Sector' in listConfigDict:
        instructions['SectorCondition']='Sector="{}"'.format(listConfigDict['Sector'])
    else:
        instructions['SectorCondition']='TRUE'

    if listConfigDict['Index'] in ['DJI', 'SNP']:
        instructions['IndexCondition'] = 'is'+listConfigDict['Index']
    else:
        instructions['IndexCondition'] = 'TRUE'

    minMarketCap = listConfigDict.get('MarketCapMin', 1000)
    maxMarketCap = listConfigDict.get('MarketCapMax', 1000000000)
    instructions['MarketCapCondition'] = 'MarketCap BETWEEN {} AND {}'.format(minMarketCap, maxMarketCap)
    instructions['ListSize'] = min(listConfigDict.get('ListSize', 5), 10)

    #query = self.get_onelist_query(listConfigDict['Domain'])
    query = format_string(finquery, instructions)
    #print("Running query:\n" + query, flush=True)
    #return
    #
    # Execute the query.
    print('Starting query execution', dt.now()-startTime)
    listDF = execute_query_to_df(query)
    print (listDF.columns, listDF.shape, dt.now()-startTime)
    listDict = pandas_df_to_dict(listDF, 'TopRank')
    listDict['Description'] = listConfig['QuestionDescription']
    return json.dumps(listDict)

def get_top_lists(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json:
        return one_list_generator(request_json)
    elif request_args:
        print('args:', request_args)
        return one_list_generator(request_args)
    else:
        return 'Error with request {}'.format(escape(request.data))
