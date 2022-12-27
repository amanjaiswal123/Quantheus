from data_scripts.get_data import alphavantage_nyse_nasdaq_download
from datetime import datetime
from datetime import timedelta
import time as Time
from source.Commons import notify
from source.AWS import *

from source.Commons import upload_to_rds_table

firstrun = True
first = False
# This is used to update data everyday from Alpha Vantage
os.system('sudo timedatectl set-timezone America/New_York')
while True:
    # Defining Today's Date
    Today = str(datetime.today().date())
    # Date that data will be available. Data is will not be available for today because all data is delayed by 1 day so
    # to reflect this discrepancy accurately we must also subtract 1 day from the current day and label all the data
    # with that date. For example if we are pulling data today it is not the data for the current date rather the data
    # for yesterday
    # Check if Today is a Trading Day by looking if it is in a list of all Trading Days until 2024 that is imported
    if Today in TradingDays and not firstrun:
        print('Starting Data Pull for', Today)
        while True:
            try:
                if first == True:
                    todaydata = alphavantage_nyse_nasdaq_download()
                    pass
                else:
                    todaydata = alphavantage_nyse_nasdaq_download()  # Get today's Data from Alpha Vantage
                    pass
                break
            except Exception as e:
                print("Could not download data from alpha vantage on"+Today)
                notify("Could not download data from alpha vantage on"+Today)
                raise e
        todaydata = todaydata[todaydata['date'] == Today]
        upload_to_rds_table(todaydata,'alpha_vantage',row_by_row=True,save_errors=True)
    now = datetime.now()
    # Get the time we want to re-run the program at
    if Today == str(datetime.today().date()) and not firstrun:
        runAt = (datetime.now() + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
    elif datetime.now() < (datetime.now() + timedelta(days=0)).replace(hour=21, minute=30, second=0, microsecond=0) or firstrun:
        runAt = (datetime.now() + timedelta(days=1)).replace(hour=21, minute=30, second=0, microsecond=0)
    else:
        runAt = (datetime.now() + timedelta(seconds=5))
    # Get the difference between the time we want to sleep at and the time right now
    delta = (runAt - now).total_seconds()
    print('\nFinished Task for', Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60, 2)) + ' hours.\n')
    # Sleep for the difference between the time now and the time we want to run at again where it will resume and go to
    # Start of the while loop again
    firstrun = False
    if not firstrun:
        first = False
    Time.sleep(delta)

