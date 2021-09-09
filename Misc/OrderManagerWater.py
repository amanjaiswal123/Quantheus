import pandas
from datetime import datetime
from source.Commons import notify, TradingDays, _datadate
from time import sleep,
from source.IB import TestWrapper, TestClient
from threading import Thread
from pgrep import pgrep
import os
import signal
from datetime import timedelta
from source.IB import TestApp as TestApp1
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
                notify('Could not connect to TWS on water for order manager')
                print('Could not connect to TWS')  # Warning user that we were unable to connect to tws
                input('we are not able to connect to TWS press enter to retry') #If the file is not found wait for the user to fix problem and retry on user command
                trys = 0
            app.disconnect() #Close connection to TWS
        print('Running Order Manager')
        app = TestApp1("127.0.0.1", 7497, 10) #Connection to TWS
        OpenOrdersBeforeOpen = app.get_openordersall()
        OpenOrdersBeforeOpen['Action'] = None
        for x in OpenOrdersBeforeOpen.index:
            OpenOrdersBeforeOpen['Action'][x] = getattr(OpenOrdersBeforeOpen['order'][x], 'action')
        now = datetime.now() #Get exzact time and date
        runAt = (datetime.now() + timedelta(days=0)).replace(hour=9, minute=29, second=0, microsecond=0) #Set time to re-run as tommorow at 6 am
        delta = (runAt - now).total_seconds() #Get diffrence in seconds between that re-run time and the time now
        sleep(delta) #Sleep for the diffrence between the re-run time and current time
        if len(OpenOrdersBeforeOpen.index) > 0:
            OpenOrdersAfterOpen = app.get_openordersall()
            start = datetime.now()
            time = (datetime.now()-start).seconds
            while len(OpenOrdersAfterOpen.index) > 0 and time < 660:
                sleep(30)
                OpenOrdersAfterOpen = app.get_openordersall()
                time = (datetime.now()-start).seconds
            networth = app.get_account_details('NetLiquidation')
            Positions = app.get_positions(networth)
            NotInAfterOpen = ~OpenOrdersBeforeOpen.index.isin(OpenOrdersAfterOpen.index)
            NotinPortfolio = ~OpenOrdersBeforeOpen.index.isin(Positions.index)
            CancledOrders = OpenOrdersBeforeOpen[(NotinPortfolio & NotInAfterOpen & (OpenOrdersBeforeOpen['Action'] == 'BUY')) | (~NotinPortfolio & NotInAfterOpen & (OpenOrdersBeforeOpen['Action'] == 'SELL'))]
        else:
            CancledOrders = pandas.DataFrame()
        for x in CancledOrders.index:
            contract = CancledOrders['contract'][x]
            order = CancledOrders['order'][x]
            order.tif = ''
            try:
                app.placeOrder(app.get_order_ID(), contract, order)
            except:
                pass
        sleep(60)
        gatewaypid = pgrep('xterm')
        if len(gatewaypid) == 1:
            os.kill(gatewaypid[0], signal.SIGTERM)
        else:
            notify('Could not close IB Gateway on water')
            print('Could not close IB Gateway water')
    now = datetime.now()  # Get exzact time and date
    runAt = (datetime.now() + timedelta(days=1)).replace(hour=9, minute=20, second=0,microsecond=0)  # Set time to re-run as tommorow at 6 am
    delta = (runAt - now).total_seconds()  # Get diffrence in seconds between that re-run time and the time now
    print('\nFinished Task for', Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60,2)) + ' hours.\n')  # Tell user tasks for today have been finished and how long it will wait to re-run the codes
    sleep(delta)  # Sleep for the diffrence between the re-run time and current time
    first = False