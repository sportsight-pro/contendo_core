import sys
from flask import escape
from datetime import datetime as dt
import json
from contendo_utils import BigqueryUtils
from contendo_utils import ProUtils

def one_list_generator(listConfigDict, startTime=dt.now()):
    listsDefDict = ProUtils.get_dict_from_jsonfile('lists_config.json')
    finquery = ProUtils.get_string_from_file('queries/top_lists_query.sql')
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
    query = ProUtils.format_string(finquery, instructions)
    #print("Running query:\n" + query, flush=True)
    #return
    #
    # Execute the query.
    print('Starting query execution', dt.now()-startTime)
    bqu = BigqueryUtils()
    listDF = bqu.execute_query_to_df(query)
    listDF = listDF.query('TopBottom=="TOP"')
    #print (listDF.columns, listDF.shape, dt.now()-startTime)
    listDict = ProUtils.pandas_df_to_dict(listDF, 'TopRank')
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

    try:
        request_json = request.get_json(silent=True)
        request_args = request.args

        if request_json:
            return one_list_generator(request_json)
        elif request_args:
            print('args:', request_args)
            return one_list_generator(request_args)
        else:
            return 'Error with request {}'.format(escape(request.data))

    except Exception as e:
        return 'Exception with request {}'.format(e)

