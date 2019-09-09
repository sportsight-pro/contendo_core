import os
os.system('PORT=8080 && docker run -p 8080:${PORT} -e PORT=${PORT} gcr.io/sportsight-tests/finance_api')