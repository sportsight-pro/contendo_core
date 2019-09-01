import flask
import pytest
from unittest.mock import Mock
import GetListsFunction

# Create a fake "app" for generating test request contexts.
@pytest.fixture(scope="module")
def app():
    return flask.Flask(__name__)

def test_getlists_args():
    testDict = {}
    testDict['Listname'] = 'DollarVolume'
    testDict['Index'] = 'SNP'
    testDict['Sector'] = 'Technology'
    testDict['MarketCapMin'] = 1000
    testDict['MarketCapMax'] = 10000000

    req = Mock(get_json=Mock(return_value=testDict), args=testDict)

    res = GetListsFunction.get_top_lists(req)
    print(res)
    assert 'Error' not in res

def test_getlists_json():
    testDict = {}
    testDict['Listname'] = 'DollarVolume'
    testDict['Index'] = 'SNP'
    # testDict['Sector'] = 'Technology'
    testDict['MarketCapMin'] = 1000
    testDict['MarketCapMax'] = 10000000

    req = Mock(get_json=Mock(return_value=testDict), json=testDict)

    res = GetListsFunction.get_top_lists(req)
    print(res)
    assert 'Error' not in res

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ysherman/Documents/GitHub/sportsight-tests.json"
test_getlists_args()
#test_getlists_json()