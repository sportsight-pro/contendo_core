import os
import requests
import json
import datetime
from datetime import datetime as dt, date

import nltk
from newspaper import Article

from contendo_utils import BigqueryUtils
from contendo_utils import ProUtils

class GetStockNews:
    def __init__(self):
        stocknews_apikey='oywghpku7talnwtde1k4h5eqonrgze6i1v6fzmcq'
        self.stocknews_url_template = "https://stocknewsapi.com/api/v1?tickers={ticker}&items={nitems}&date={fromdate_MMDDYYYY}-today&sortby={sortby}&token={stocknews_apikey}".replace('{stocknews_apikey}', stocknews_apikey)
        self.bqu = BigqueryUtils()
        self.bucketName = 'sport-uploads'
        #nltk.download('punkt')  # 1 time download of the sentence tokenizer

    def get_stocknews_byticker(self, tickersList, nitems=50, daysback=30, sortby='trending'):
        assert(sortby in ['trending', 'algo'])

        tickers = str(tickersList).replace('[', '').replace(']', '').replace("'", '').replace(' ', '')
        urlInstructions={
            'ticker': tickers,
            'nitems': nitems,
            'fromdate_MMDDYYYY': (date.today() - datetime.timedelta(days=daysback)).strftime('%m%d%Y') ,
            'sortby': sortby,
            'today': date.today(),
        }
        outfileName = 'Finance/temp/{ticker}-{nitems}-{fromdate_MMDDYYYY}-{sortby}-{today}.json'.format(**urlInstructions)

        text = self.bqu.read_string_from_gcp(self.bucketName, outfileName)
        if text is None:
            url = self.stocknews_url_template.format(**urlInstructions)
            print(url)
            response = requests.request("GET", url)
            text = response.text
            self.bqu.upload_string_to_gcp(response.text, self.bucketName, outfileName)

        data = json.loads(text)

        if not 'data' in data:
            print ('Error getting news for tickers {}, return={}'.format(tickers, data))
            return {}
        newsDict = data['data']

        sentimentDict = {'Count': 0, 'Negative': 0, 'Positive': 0, 'Neutral': 0, 'Weighted': 0}
        sentimentWeight = {'Negative': -1, 'Positive': 1, 'Neutral': 0}
        count=0
        newsFeed = []
        startTime=dt.utcnow()
        for newsItem in newsDict:
            count+=1
            newItem = {key: newsItem[key] for key in ['title', 'text', 'sentiment', 'source_name', 'topics', 'news_url', 'image_url']}
            newItem['index'] = count
            itemDate = dt.strptime(newsItem['date'], '%a, %d %b %Y %H:%M:%S %z')
            delta = startTime.date() - itemDate.date()
            if delta.days <= 3 or count <= 3:
                newItem['date'] = str(itemDate.date())
                if False: # suspend getting the summary
                    article = Article(newItem['news_url'])
                    # Do some NLP
                    try:
                        article.download()  # Downloads the link’s HTML content
                        article.parse()  # Parse the article
                        article.nlp()  # Keyword extraction wrapper
                        newItem['Summary'] = article.summary.replace('\n', '\n')
                    except Exception as e:
                        print ('Error occured:', e)
                        newItem['Summary'] = "<...>"

                #print(newItem['Summary'])
                newsFeed.append(newItem)
            if delta.days<=3:
                deltaWeight=1
            elif delta.days<=7:
                deltaWeight=0.5
            elif delta.days<=14:
                deltaWeight=0.25
            elif delta.days<=30:
                deltaWeight=0.125
            else:
                deltaWeight=0.05

            sentiment = newsItem['sentiment']
            sentimentDict[sentiment]+=1
            sentimentDict['Count']+=1
            sentimentDict['Weighted'] += sentimentWeight[sentiment]*deltaWeight
        retDict = {
            'NumItems': len(newsFeed),
            'Sentiment': sentimentDict,
            'Newsfeed': newsFeed,
        }

        return retDict


if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
    startTime = dt.now()
    gsn = GetStockNews()
    print(gsn.get_stocknews_byticker(['AMD']))