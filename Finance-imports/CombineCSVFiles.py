import os
import glob
import pandas as pd
import csv
from datetime import datetime as dt,timedelta

os.chdir("/Users/ysherman/Documents/GitHub/results/Finance/daily-quotes")
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

mydialect = csv.Dialect
mydialect.lineterminator='\n'
mydialect.quoting=csv.QUOTE_MINIMAL
mydialect.quotechar='|'

count=0
main_start_time=dt.now()
outfile = open('../daily-quotes-agg.csv', 'w')
outFileWriter = csv.writer(outfile, delimiter=',', dialect=mydialect)
firstTime = True
for csvFileName in all_filenames:
    infile = open(csvFileName,'r')
    linereader = csv.reader(infile, delimiter=',')
    firstrow=True
    for line in linereader:
        if firstrow and not firstTime:
            firstrow=False
            continue
        outFileWriter.writerow(line[1:])
    count+=1
    firstTime = False
    if count % 100 == 0:
        print ('Done writing: {} of {} Total-delta: {}\n'.format(count, len(all_filenames), dt.now()-main_start_time))
