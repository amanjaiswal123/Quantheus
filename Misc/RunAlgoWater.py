from config import backtestdatapath
from datetime import datetime
from Algos.PeakTrough import UpTrend
from source.Realtime_v1 import Realtime
from source.Commons import notify, TradingDays, _datadate
from time import sleep
from source.IB import TestWrapper, TestClient
from threading import Thread
from pgrep import pgrep
import os
import signal
from datetime import timedelta
class TestApp(TestWrapper,TestClient):
    #When called it will automaticly connect to tws
    def __init__(self):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)
while True:
    Today = str(datetime.today().date()) #Getting Todays Date will run everyday at 6am Timezone is location of the machine running this script
    first = False
    if Today in TradingDays and first is False: #Check if today is a trading day
        #Check if Data for today was downloaded locally
        print('Starting Tasks for',Today)
        datadate = _datadate(Today)
        tfirst = True
        print('Checking for data file')
        datacheckfirst = True
        while True:
            if os.path.isfile(backtestdatapath+'/BacktestData'+datadate+'.csv'):  # Checking if the data file was updated
                if datacheckfirst == False:
                    notify('Found updated data on water')
                break
            else:
                if datacheckfirst:
                    notify('Data not updated on water trying again in 20 minutes')
                    print('Data not updated on water trying again in 20 minutes')
                    datacheckfirst = False
                sleep(60*20)
        print('Starting IBGateway')
        if len(pgrep('Xvfb')) != 1:
           os.system('Xvfb :20 &')
        os.system('export DISPLAY=:20; /opt/IBController/IBControllerGatewayStart.sh')
        sleep(60*2)
        connected = False  # The connection check is False unless the checks are passed
        while connected is False: #Loop until you are connected or dont want to try to connect any more
            connected = False  # The connection check is False unless the checks are passed
            trys = 0 #Set trys to 0
            while trys <= 10: #Try to connect 10 times until asking user to fix
                sleep(30) #Wait 30 seconds between connection attempts
                trys += 1 #Add 1 to try
                app = TestApp()  # Creating an instance of TestApp
                try:
                    # Trying to connect
                    app.connect("127.0.0.1", 7497, 10) #Trying to connect using TestApp instance
                except Exception as e:
                    print(e) #Print exception if unable to connect
                thread = Thread(target=app.run)  # IDK what this does just saw it online
                thread.start()  # Start thread
                test = app.get_time(testing=True) #Test connection by requesting time from TWS
                if test is not None: #If test is not None then there was successful communtion of the time signifying a sucessful connection
                    connected = True #Setting connection to True
                    break #Breaking try loop
                else:
                    app.disconnect() #Close any possible connection to TWS
            if connected is False:
                notify('Could not connect to TWS on water')
                print('Could not connect to TWS')  # Warning user that we were unable to connect to tws
                input('we are not able to connect to TWS press enter to retry') #If the file is not found wait for the user to fix problem and retry on user command
                trys = 0
            app.disconnect() #Close connection to TWS
        print('Running Realtime Script')
        try:
            Realtime(UpTrend,365*5)
            pass
        except Exception as e:
            notify('Realtime script on water ran into an error')
            print('Realtime script ran into an error: \n',e)
        gatewaypid = pgrep('xterm')
        if len(gatewaypid) == 1:
            os.kill(gatewaypid[0], signal.SIGTERM)
        else:
            notify('Could not close IB Gateway on water')
            print('Could not close IB Gateway on water')
    now = datetime.now()  # Get exzact time and date
    runAt = (datetime.now() + timedelta(days=1)).replace(hour=8, minute=0, second=0,microsecond=0)  # Set time to re-run as tommorow at 6 am
    delta = (runAt - now).total_seconds()  # Get diffrence in seconds between that re-run time and the time now
    print('\nFinished Task for', Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60,2)) + ' hours.\n')  # Tell user tasks for today have been finished and how long it will wait to re-run the codes
    sleep(delta)  # Sleep for the diffrence between the re-run time and current time
    first = False