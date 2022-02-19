import numpy
from data_scripts.get_data import AHistroy
from source.IB import *
from source.Commons import NearestTradingDay,_datadate,datadateslice,TradingDays
import pandas

pandas.set_option('mode.chained_assignment', None)
from os import remove
from os.path import exists
from config.config import PortfolioPath, subtradespath, Signalspath, openedpositionspath, subsellpath, closedpositionspath, ranpath

StartTime = datetime.now()
def Realtime(func,Days):
    Today = str(datetime.today().date())
    datadate = _datadate(NearestTradingDay(Today)) #Get the data for which data should be available
    Fields = AHistroy(['adj_close','close','volume'],'livealpha',Days=Days,End_Day=datadate)  #Getting Data
    app = TestApp("127.0.0.1", 7497, 10) #Connection to TWS
    #Getting networth that will be used in getting your portfolio since it is need to calculate allocations
    while True: #Will loop indefintely until we get the networth or the user decides to end the script
        try: #Trying to get nettworth
            networth = app.get_account_details('NetLiquidation') #Getting networth
        except ConnectionAbortedError: #If the connection is for some reason aborted
            networth = None #If there was an excpetion networth will be none
        if networth is None: #if networth is None then try again after the user fixes the connection
            warnings.warn('Could not retrieve Net Worth. This is a Fatal Error.') #Warning user about networth
            while True: #Loop indefinetly until the user inputs a valid response
                retry = str(input('\nShould I try to retrieve this accounts Net Worth again? (Y or N): ')).lower() #Asking for user input
                if retry != 'y' and retry != 'n': #Checking for valid response
                    print('Please enter Y or N') #If he did not enter a valid response ask him to enter a valid one
                else: #If he did input a valid input then break the loop
                    break #Breaking the loop
            if retry == 'n': #If the user said no break the loop and retry will be no this will keep networth as none and end the script
                break #break
            else: #if you do want to retry it will disconnect and reconnect to IB and repeat the loop
                app.disconnect() #Disconnect from IB
                app = TestApp("127.0.0.1", 7497, 10) #Reconnect to IB
        else:
            break #If we succesful got our networth then break the loop
    if networth != None: #If we got networth we will get our portfolio
        while True: #Loop indefinetly until we get the portfolio or the user decides to end the script
            try: #Try getting the portfolio
                Portfolio = app.get_positions(networth) #Get portfolio
            except ConnectionAbortedError: #If the connection was interrupted portfolio will be None
                Portfolio = None #Setting portfolio to None
            if Portfolio is None: #If portfolio is None ask the user if he wants to retry getting it
                warnings.warn('Could not retrieve your Portfolio. This is a Fatal Error.') #Warn user that we were not able to retrieve the portfolio
                while True: #Loop indecently till the user inputs a valid response
                    retry = str(input('\nShould I try to retrieve your Portfolio again? (Y or N): ')).lower() #Get user inputs
                    if retry != 'y' and retry != 'n': #if the user did not enter a valid response then tell him
                        print('Please enter Y or N') #Telling user he did not enter a valid response
                    else: #If he did enter a valid response break the loop
                        break #Breaking the loop
                if retry == 'n': #If the user entered n for no then it will stop trying to get the portfolio by breaking the loop. This will end the script
                    break #Breaking loop
                else: #If the user does want to retry getting retrieve the portfolio then it will disconnect and reconnect to IB and restart the loop
                    app.disconnect() #Disconecting from IB
                    app = TestApp("127.0.0.1", 7497, 10) #Reconnecting to IB
            else: #If we got the networth succesful move on by breaking the loop
                break #Breaking the loop
    else: #If we could not get the networth then the portfolio is also going to be None
        Portfolio = None #Setting portfolio to None
    if Portfolio is not None: #If we got the portfolio the last thing we need to proceed is the amount of money we have left so we can figure out how much money we have to spend
        while True:# Loop indefinitely until we get the amount of cash we have or the user decides to end the script
            try: #Try Getting cash
                Cash = app.get_account_details('AvailableFunds') #Getting Cash
            except ConnectionAbortedError: #If the connection was aborted then cash will be None
                Cash = None #Setting Cash to None
            if Cash is None: #If the cash is None then ask the user to retry
                warnings.warn('Could not retrieve your Cash. This is a Fatal Error.')#Warning user we were not able to get cash
                while True: #Loop until user gives a valid response
                    retry = str(input('\nShould I try to retrieve your Cash again? (Y or N): ')).lower() #Ask the user whether he wants to retry getting cash
                    if retry != 'y' and retry != 'n': #Check if there is a valid response
                        print('Please enter Y or N') #Tell user he did not enter a valid response
                    else: #If he did enter a valid response break the loop
                        break #breaking loop
                if retry == 'n': #If he does not wants to retry then set retry to no and this will end the script
                    break #break the loop
                else: #If he does want to retry disconect and reconnect then restart the loop
                    app.disconnect() #Disconnecting
                    app = TestApp("127.0.0.1", 7497, 10) #Reconnecting
            else: #If we sucessfuly got cash move on by breaking the loop
                break #Breaking the loop
    else: #If we do not have portfolio cash will be None
        Cash = None #Setting Cash to None
    if Cash is not None and Portfolio is not None: #If we have the portfolio and cash then proceed
        #Here we are getting yestedays values that are needed to trade since the values will not be in memory because this script does not run continually
        Portfolio['Opened Position on'] = Today #Setting opened position today as defualt for stocks we cannot find in yesterday portfolio
        Portfolio['Expiry'] = None #Create a expiry column
        Portfolio['Stop Limit Percent'] = None #Creating a stop limit% column
        Portfolio['Holding Period'] = None #creating a holding period column
        if 'Action' not in Portfolio.columns.values: #If Action is not a column then we are making it
            Portfolio['Action'] = None #Making action column
        try: #Trying to get yesterdays portfolio from a file
            YPortfolio = pandas.DataFrame.from_csv(PortfolioPath + TradingDays[TradingDays.index(Today) - 1] + '.csv') #Getting portfolio
            YPortfolio.reset_index(inplace=True) #Reseting Index
            YPortfolio.set_index(['ticker','exchange'],inplace=True) #Setting Index
        except FileNotFoundError: #If the file is not found we will create a empty dataframe so we can grab values for old positions and also see compare changes to todays values
            YPortfolio = pandas.DataFrame(columns=['ticker','exchange','Opened Position on','Expiry','Holding Period','Stop Limit Percent','Contract','Buy Price','Allocation','Amount Holding']) #If we cannot get portfolio then we create a empty dataframe as portfolio because we need it later on
            YPortfolio.set_index(['ticker','exchange'],inplace=True) #Setting the index
        try: #Trying to get the submitted buy orders from yesterday so we can grab values from it for new positions we opened
            Ysubbuy = pandas.DataFrame.from_csv(subtradespath + TradingDays[TradingDays.index(Today) - 1] + '.csv')  #Getting yesterdays submitted sell orders
            if 'ticker' in Ysubbuy.reset_index().columns.values: #We are checking if ticker is in it because the dataframe may be empty and if it is empty then we cannot set the index
                Ysubbuy.reset_index(inplace=True) #resetting index
                Ysubbuy.set_index(['ticker','exchange'],inplace=True) #set index to ticker and exchange
            else: #If it does not have ticker as a column create a empty dataframe
                Ysubbuy = pandas.DataFrame(columns=['ticker', 'exchange', 'Opened Position on','Expiry','Holding Period','Stop Limit Percent','Allocation']) #Create a empty replica of portfolio
        except FileNotFoundError: #If the file is not found it will create a empty replica of portfolio
            Ysubbuy = pandas.DataFrame(columns=['ticker', 'exchange', 'Opened Position on','Expiry','Holding Period','Stop Limit Percent','Allocation']) #Create a empty replica of portfolio
            Ysubbuy.set_index(['ticker', 'exchange'], inplace=True) #Set the index
        Portfolio['Opened Position on'][Portfolio.index.isin(YPortfolio.index)] = YPortfolio['Opened Position on'][YPortfolio.index.isin(Portfolio.index)] #Get the date you opened the position on from yesterdays portfolio
        if len(Portfolio[Portfolio.index.isin(YPortfolio.index)]) > 0:  # If the length of the values in todays portfolio and yesterdays portfolio are greater than 0 then get the data from it
                for x in YPortfolio.drop(columns=['Buy Price','Allocation','Amount Holding','Contract']).columns.values: #Get these values from yesterdays files
                    Portfolio[x] = None
                    Portfolio[x][Portfolio.index.isin(YPortfolio.index)] = YPortfolio[x][YPortfolio.index.isin(Portfolio.index)] #Get values from yesterday's file
        if len(Portfolio[Portfolio.index.isin(Ysubbuy.index)]) > 0:
            for x in Ysubbuy.drop(columns=['Allocation']).columns.values:
                Portfolio[x] = None
                if len(Portfolio[Portfolio.index.isin(Ysubbuy.index)]) > 0:#If the length of the values in todays portfolio and yesterdays submitted buy orders are greater than 0 then get the data from it
                    Portfolio[x][Portfolio.index.isin(Ysubbuy.index)] = Ysubbuy[x][Ysubbuy.index.isin(Portfolio.index)] #Get values from yesterdays portfolio
        #Calculating some indicators such as Moving averages requires previous calculations to calculate this will get yesterdays calculations from a file previously saved so we can use it.
        try: #Try getting yesterdays calcualtions
            YSignals = pandas.read_csv(Signalspath + datadate + '.csv') #Getting yesterdays calculations
            if exists(Signalspath + TradingDays[TradingDays.index(Today) - 2] + '.csv'): #Checking if the file exists
                #remove('/home/ubuntu/Quantheus/Data/Signals'+TradingDays[TradingDays.index(Today)-2]+'.csv') #Deleting the file containing signals from 2 days as we do not want these to pile up because they take up alot of space. We always keep 2 days worth if signals on hand for debugging purposes
                pass
            YSignals.set_index(['ticker','exchange'],inplace=True) #Setting the index
            firstcalc = False #We use this variable to tell your calculating function that it can calculate it differently by using yesterdays signals. When making a calculating function it must have a place to pass this variable whether you use it or not.
        except FileNotFoundError: #If we cannot find the file then tell the calculating function to calculate it differently
            firstcalc = True #Setting variable that tells your function that we do not have yesterdays data
        if firstcalc: #If we do not have yesterdays calculations we do not pass YSignals
            Signals = func(Fields, Portfolio, datadate, firstcalc) #Calling your function and not passing YSignals by defualt it is None if you do not pass it
        else: #If we do not have yesterdays signals then pass it through
            Signals = func(Fields, Portfolio, datadate, firstcalc, YSignals=YSignals) #Calling your function and passing yesterdays signals
        Signals.to_csv(Signalspath + Today + '.csv') #Saving the signals you just generated so it can be used tomorrow
        if exists(Signalspath + TradingDays[TradingDays.index(datadate)] + '.csv'):
            remove(Signalspath + TradingDays[TradingDays.index(datadate)] + '.csv')
            pass
        Portfolio['Action'][(~Portfolio.index.isin(YPortfolio.index)) & (~Portfolio.index.isin(Ysubbuy.index)) | Portfolio['Expiry'].isna()] = 'Absolute Sell' #If we cannot find integral data such as holding period or The stop limit for stocks in your portfolio then we will sell them no matter what
        NewPositions = Portfolio[~Portfolio.index.isin(YPortfolio.index)] #Getting new positions by checking for the stocks that are not in yesterdays portfolio and in todays
        SoldPositions = YPortfolio[~YPortfolio.index.isin(Portfolio.index)] #Getting Sold positions by checking for stocks that are in yesterdays portfolio but not in Todays
        if "Today's Close" not in Signals.columns.values: #If Today's close is not in the portfolio then add it as we will need it to see what stocks we need to sell
            TodaysClose = Fields['adj_close'][Fields['adj_close'].index.get_level_values('dat') == datadate].reset_index('dat').drop(columns='dat') #Getting Todays close
            Signals["Today's Adjusted Close"] = TodaysClose[TodaysClose.index.isin(Signals.index)] #Adding it to Signals and converiting to a dataframe
        AllocationRemaining = 1-Portfolio['Allocation'].sum() #Getting Allocation remainging
        avgvolume = datadateslice(Fields['volume'],Days=90).groupby(['ticker','exchange']).mean() #Getting average volume for the best 90 Days. We will use this because in live trading stocks that have low volumes are hard to sell so we will only buy stocks with a avg volume over 100000 to avoid liquidty problems
        Signals['Avg Volume'] = avgvolume #Adding average volume to signals
        Buy = Signals[(Signals['Action'] == 'Buy') & (~Signals.index.isin(Portfolio.index)) & (Signals['Avg Volume'] >= 50000)].copy()
        if 'Stop Limit Percent' in Buy.columns.values:
            if 'Stop Limit' in Buy.columns.values:
                StopLimitNa = Buy['Stop Limit'].isna()
                StopLimitPNa = Buy['Stop Limit Percent'].isna()
                Buy['Stop Limit'][StopLimitNa] = (Buy["Today's Adjusted Close"][StopLimitNa] * Buy['Stop Limit Percent'][StopLimitNa]).apply(pandas.to_numeric, errors='coerce').round(2)
                Buy['Stop Limit Percent'][StopLimitPNa] = -((Buy["Today's Adjusted Close"][StopLimitPNa] - Buy['Stop Limit'][StopLimitPNa]) /Buy["Today's Adjusted Close"][StopLimitPNa]).apply(pandas.to_numeric, errors='coerce').round(2)
            else:
                Buy['Stop Limit'] = (Buy["Today's Adjusted Close"] * Buy['Stop Limit Percent']).apply(pandas.to_numeric, errors='coerce').round(2)
                Signals['Stop Limit'] = 0
        else:
            if 'Stop Limit' in Buy.columns.values:
                Buy['Stop Limit Percent'] = -((Buy["Today's Adjusted Close"] - Buy['Stop Limit']) / Buy["Today's Adjusted Close"]).apply(pandas.to_numeric, errors='coerce').round(2)
                Signals['Stop Limit Percent'] = -1
            else:
                Buy['Stop Limit Percent'] = -1
                Buy['Stop Limit'] = 0
                Signals['Stop Limit Percent'] = -1
                Signals['Stop Limit'] = 0
        Buy['Stop Limit Percent'] = Buy['Stop Limit Percent'].round(2)
        Buy['Stop Limit'] = Buy['Stop Limit'].round(2)
        Buy['contract'] = numpy.nan
        Buy['Quantity'] = numpy.nan
        for x in Signals.drop(columns=['Allocation','Stop Limit Percent','Stop Limit','Holding Period']).columns.values: #Update your portfolio with the newly generated calculations except the static variables that it should have gotten from yesterdays portffolio or submitted buy orders
            Portfolio[x] = Signals[x][Signals.index.isin(Portfolio.index)] #Repalcing the values
        for x in Buy.drop(columns=['Action','Stop Limit Percent','Stop Limit','Holding Period']).columns.values:
                Buy[x+' on Buy'] = Buy[x]
        tfirst = True
        Bought = pandas.DataFrame()
        for x in Buy.index:
            if AllocationRemaining - Buy['Allocation'][x] >= 0:
                AllocationRemaining -= Buy['Allocation'][x]
                Buy_ = True
            else:
                Buy.drop(x, inplace=True)
                Buy_= False
            if Buy_:
                contract = None
                try:
                    contractL = app.get_contract_by_ticker(x[0])
                except:
                    contractL = None
                if contractL is not None:
                    for y in contractL:
                        contract = getattr(y, 'contract')
                        exchange = getattr(contract, 'primaryExchange')
                        ticker = getattr(contract, 'symbol')
                        if set(list(x[1])).issubset(list(exchange)) and ticker == x[0] and ~numpy.isnan(Buy['Allocation'][x]) and ~numpy.isnan(Buy["Today's Close"][x]) and ~numpy.isnan(Buy['Stop Limit Percent'][x]):
                            Buy['Quantity'][x] = int((Cash * Buy['Allocation'][x]) / (Buy["Today's Close"][x] * 1.02))
                            contract.exchange = 'SMART'
                            order = Order()
                            order.action = "BUY"
                            order.totalQuantity = Buy['Quantity'][x]
                            order.orderType = "STP"
                            order.auxPrice = Buy['Stop Limit'][x]
                            app.placeOrder(app.get_order_ID(), contract, order)
                            temp = Buy[(Buy.index.get_level_values('ticker') == x[0]) & (Buy.index.get_level_values('exchange') == x[1])]
                            if tfirst == True:
                                Bought = temp
                                Bought['Expiry'] = None
                            else:
                                Bought = Bought.append(temp)
                            if Bought['Holding Period'][x] != 'Infinite':
                                Bought['Expiry'][x] = str(datetime.strptime(Today,'%Y-%m-%d')+timedelta(int(Bought['Holding Period'][x])))[0:10]
                            else:
                                Bought['Expiry'][x] = None
                            tfirst = False
        if len(Portfolio.index) > 0:
            Sold_Today = Portfolio[(Portfolio['Action'] == 'Sell') | (Portfolio['Expiry'] > Today) | (Portfolio['Action'] == 'Absolute Sell')]
        else:
            Sold_Today = pandas.DataFrame()
        for x in Sold_Today.index:
            contract = Sold_Today['Contract'][x]
            contract.exchange = 'SMART'
            order = Order()
            order.action = "SELL"
            order.tif = "OPG"
            order.orderType = "MKT"
            order.totalQuantity = Sold_Today['Amount Holding'][x]
            if ~numpy.isnan(Sold_Today["Today's Close"][x]):
                order.lmtPrice = Sold_Today["Today's Close"][x]*.99
            try:
                pass
                app.placeOrder(app.get_order_ID(), contract, order)
            except Exception as e:
                pass
        if 'Contract' in NewPositions.columns.values:
            NewPositions.drop(columns='Contract',inplace=True)
        if 'Contract' in SoldPositions.columns.values:
            SoldPositions.drop(columns='Contract',inplace=True)
        print('Completed Buy Orders for '+datadate,NewPositions)
        print('Completed Sell Orders for '+datadate,SoldPositions)
        NewPositions.to_csv(openedpositionspath + datadate + '.csv')
        SoldPositions.to_csv(closedpositionspath + datadate + '.csv')
        if 'contract' in Portfolio.columns.values:
            Portfolio.drop(columns='contract',inplace=True)
        if 'contract' in Bought.columns.values:
            Bought.drop(columns='contract',inplace=True)
        if 'contract' in Sold_Today.columns.values:
            Sold_Today.drop(columns='contract',inplace=True)
        print('Portfolio on '+Today,'\n',Portfolio)
        print('Submitted Buy Orders on '+Today,'\n',Bought)
        print('Submitted Sell Orders on '+Today,'\n',Sold_Today)
        Portfolio.to_csv(PortfolioPath+Today+'.csv')
        Bought.to_csv(subtradespath + Today + '.csv')
        Sold_Today.to_csv(subsellpath+Today+'.csv')
        file = open(ranpath, 'w')
        file.close()
    app.disconnect()