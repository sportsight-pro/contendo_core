#import flask
#import pytest
from unittest.mock import Mock

def test_getlists_args():
    testDict = {}
    testDict['Listname'] = 'YTDGain'
    #testDict['Ticker'] = 'AMD'

    #testDict['Index'] = 'SNP'
    #testDict['Sector'] = 'Technology'
    #testDict['MarketCapMin'] = 1000
    #testDict['MarketCapMax'] = 10000000

    req = Mock(get_json=Mock(return_value=testDict), args=testDict)

    res = get_top_lists(req)
    print(res)
    assert 'Error' not in res

def test_getlists_json():
    testDict = {}
    testDict['Listname'] = 'DailyGain'

    testDict['Index'] = 'SNP'
    # testDict['Sector'] = 'Technology'
    testDict['MarketCapMin'] = 1000
    testDict['MarketCapMax'] = 10000000

    req = Mock(get_json=Mock(return_value=testDict), json=testDict)

    res = get_top_lists(req)
    #print(res)

    assert 'Error' not in res

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
print(os.getcwd())
#os.chdir('functions/get_top_lists')
from functions.get_top_lists.main import get_top_lists
test_getlists_args()
#test_getlists_json()
