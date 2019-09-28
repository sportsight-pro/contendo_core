import os
from datetime import datetime as dt
from datetime import date

from contendo_utils import ProUtils
from get_stocks_data import GetStocksData

os.chdir('finance_api')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/sportsight-tests.json".format(os.environ["HOME"])
startTime = dt.now()
#
# deploy the function to google cloud
imageName = 'gcr.io/sportsight-tests/finance_api'
serviceAccount = 'remote-user@sportsight-tests.iam.gserviceaccount.com'
buildCMD = 'gcloud builds submit --tag {image}'.format(image=imageName)
deployCMD = 'gcloud beta run deploy contendo-finance-api --image {image} --platform managed --memory=2Gi --timeout=15m --service-account={account}'.format(
    image=imageName,
    account=serviceAccount
)
print('Start building finance_api', buildCMD, dt.now()-startTime)
os.system(buildCMD)
print('Start deploying finance_api', deployCMD, dt.now()-startTime)
os.system(deployCMD)
print('Done deploying the service', dt.now()-startTime)
