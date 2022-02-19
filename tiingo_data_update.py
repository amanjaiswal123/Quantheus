from data_scripts.get_data import AHistroy
from datetime import datetime
from datetime import timedelta
import time as Time
from source.Commons import notify
from source.AWS import *
from source.Commons import upload_to_rds_table
from config.config import data_logs_path
firstrun = True
first = False
# This is used to update data everyday from tiingo
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
        print('Starting Data Pull from tiingo on', Today)
        while True:
            try:
                if first == True:
                    todaydata = AHistroy(['all'], 'tiingo', End_Day=Today, Days=365*60)
                    pass
                else:
                    todaydata = AHistroy(['all'], 'tiingo', Start_Day=Today,End_Day=Today)  # Get today's Data from tiingo
                    pass
                break
            except Exception as e:
                todaydata.to_csv(data_logs_path+'todaydata_tiingo_'+Today+'.csv')
                print("Could not download data from tiingo on"+Today)
                notify("Could not download data from tiingo on"+Today)
                raise e
        todaydata.to_csv(data_logs_path+'todaydata_tiingo_' + Today + '.csv')
        upload_to_rds_table(todaydata,'tiingo',row_by_row=True,save_errors=True)
    now = datetime.now()
    # Get the time we want to re-run the program at
    if Today == str(datetime.today().date()) and firstrun == False:
        runAt = (datetime.now() + timedelta(days=1)).replace(hour=20, minute=30, second=0, microsecond=0)
    elif datetime.now() < (datetime.now() + timedelta(days=0)).replace(hour=20, minute=30, second=0, microsecond=0):
        runAt = (datetime.now() + timedelta(days=0)).replace(hour=20, minute=30, second=0, microsecond=0)
    else:
        runAt = (datetime.now() + timedelta(seconds=5))
    # Get the difference between the time we want to sleep at and the time right now
    delta = (runAt - now).total_seconds()
    print('\nFinished Task for', Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60, 2)) + ' hours.\n')
    # Sleep for the difference between the time now and the time we want to run at again where it will resume and go to
    # Start of the while loop again
    firstrun = False
    first = False
    Time.sleep(delta)

