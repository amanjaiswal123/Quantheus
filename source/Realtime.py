import numpy
from data_scripts.get_data import AHistroy
#from source.IB import *
from source.Commons import NearestTradingDay,_datadate, TradingDays
import pandas
from datetime import datetime
pandas.set_option('mode.chained_assignment', None)
from os import remove
from os.path import exists
from config.config import PortfolioPath, subtradespath, Signalspath, ranpath,InitialMargReq


StartTime = datetime.now()
def Realtime(func,Days,MargCallProtect=True,IntialMarginRequirment=.5,MaintenceMarginRequirment=.3):
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
        Portfolio['Stop Price Percent'] = None #Creating a stop limit% column
        Portfolio['Holding Period'] = None #creating a holding period column
        if 'Action' not in Portfolio.columns.values: #If Action is not a column then we are making it
            Portfolio['Action'] = None #Making action column
        try: #Trying to get yesterdays portfolio from a file
            YPortfolio = pandas.DataFrame.from_csv(PortfolioPath + TradingDays[TradingDays.index(Today) - 1] + '.csv') #Getting portfolio
            YPortfolio.reset_index(inplace=True) #Reseting Index
            YPortfolio.set_index(['ticker','exchange'],inplace=True) #Setting Index
        except FileNotFoundError: #If the file is not found we will create a empty dataframe so we can grab values for old positions and also see compare changes to todays values
            YPortfolio = pandas.DataFrame(columns=['ticker','exchange','Opened Position on','Expiry','Holding Period','Stop Price Percent','Contract','Buy Price','Allocation','Quantity']) #If we cannot get portfolio then we create a empty dataframe as portfolio because we need it later on
            YPortfolio.set_index(['ticker','exchange'],inplace=True) #Setting the index
        try: #Trying to get the submitted buy orders from yesterday so we can grab values from it for new positions we opened
            Ysubtrades = pandas.DataFrame.from_csv(subtradespath + TradingDays[TradingDays.index(Today) - 1] + '.csv')  #Getting yesterdays submitted sell orders
            if 'ticker' in Ysubtrades.reset_index().columns.values: #We are checking if ticker is in it because the dataframe may be empty and if it is empty then we cannot set the index
                Ysubtrades.reset_index(inplace=True) #resetting index
                Ysubtrades.set_index(['ticker','exchange'],inplace=True) #set index to ticker and exchange
            else: #If it does not have ticker as a column create a empty dataframe
                Ysubtrades = pandas.DataFrame(columns=['ticker', 'exchange', 'Opened Position on','Expiry','Holding Period','Stop Price Percent','Allocation','Quantity']) #Create a empty replica of portfolio
        except FileNotFoundError: #If the file is not found it will create a empty replica of portfolio
            Ysubtrades = pandas.DataFrame(columns=['ticker', 'exchange', 'Opened Position on','Expiry','Holding Period','Stop Price Percent','Allocation','Quantity']) #Create a empty replica of portfolio
            Ysubtrades.set_index(['ticker', 'exchange'], inplace=True) #Set the index
        Portfolio['Opened Position on'][Portfolio.index.isin(YPortfolio.index)] = YPortfolio['Opened Position on'][YPortfolio.index.isin(Portfolio.index)] #Get the date you opened the position on from yesterdays portfolio
        if len(Portfolio[Portfolio.index.isin(YPortfolio.index)]) > 0:  # If the length of the values in todays portfolio and yesterdays portfolio are greater than 0 then get the data from it
                for x in YPortfolio.drop(columns=['Buy Price','Allocation','Quantity','Contract']).columns.values: #Get these values from yesterdays files
                    Portfolio[x] = numpy.nan
                    Portfolio[x][Portfolio.index.isin(YPortfolio.index)] = YPortfolio[x][YPortfolio.index.isin(Portfolio.index)] #Get values from yesterday's file
        OpenedPositions = Portfolio[~Portfolio.index.isin(YPortfolio.index)] #Getting new positions by checking for the stocks that are not in yesterdays portfolio and in todays
        if len(Portfolio[(Portfolio.index.isin(Ysubtrades.index)) & (Portfolio.index.isin(OpenedPositions.index))]) > 0:
            for x in Ysubtrades[(Ysubtrades.index.isin(Portfolio.index)) & Ysubtrades.index.isin(OpenedPositions.index)].drop(columns=['Allocation','Quantity']).columns.values:
                Portfolio[x] = numpy.nan
                Portfolio[x][(Portfolio.index.isin(Ysubtrades.index)) & (Portfolio.index.isin(OpenedPositions.index))] = Ysubtrades[x][(Ysubtrades.index.isin(Portfolio.index)) & (Ysubtrades.index.isin(OpenedPositions.index))] #Get values from yesterdays portfolio
        #Calculating some indicators such as Moving averages requires previous calculations to calculate this will get yesterdays calculations from a file previously saved so we can use it.
        try: #Try getting yesterdays calculations
            YSignals = pandas.read_csv(Signalspath + datadate + '.csv') #Getting yesterdays calculations
            if exists(Signalspath + TradingDays[TradingDays.index(Today) - 2] + '.csv'): #Checking if the file exists
                #remove('/home/ubuntu/Quantheus/Data/Signals'+TradingDays[TradingDays.index(Today)-2]+'.csv') #Deleting the file containing signals from 2 days as we do not want these to pile up because they take up alot of space. We always keep 2 days worth if signals on hand for debugging purposes
                pass
            YSignals.set_index(['ticker','exchange'],inplace=True) #Setting the index
            firstcalc = False #We use this variable to tell your calculating function that it can calculate it differently by using yesterdays signals. When making a calculating function it must have a place to pass this variable whether you use it or not.
        except FileNotFoundError: #If we cannot find the file then tell the calculating function to calculate it differently
            firstcalc = True #Setting variable that tells your function that we do not have yesterdays data
        if firstcalc: #If we do not have yesterdays calculations we do not pass YSignals
            Signals = func(Fields, Portfolio, Cash, networth, datadate, firstcalc) #Calling your function and not passing YSignals by defualt it is None if you do not pass it
        else: #If we do not have yesterdays signals then pass it through
            Signals = func(Fields, Portfolio, Cash, networth, datadate, firstcalc, YSignals=YSignals) #Calling your function and passing yesterdays signals
        Signals.to_csv(Signalspath + Today + '.csv') #Saving the signals you just generated so it can be used tomorrow
        if exists(Signalspath + TradingDays[TradingDays.index(datadate)] + '.csv'):
            remove(Signalspath + TradingDays[TradingDays.index(datadate)] + '.csv')
            pass
        for x in Signals.drop(columns=['Allocation','Quantity','Stop Limit Percent','Stop Limit','Holding Period']).columns.values: #Update your portfolio with the newly generated calculations except the static variables that it should have gotten from yesterdays portffolio or submitted buy orders
            Portfolio[x] = numpy.nan
            Portfolio[x][Portfolio.index.isin(Signals.index)] = Signals[x][Signals.index.isin(Portfolio.index)] #Repalcing the values
        ClosedPositions = YPortfolio[~YPortfolio.index.isin(Portfolio.index)] #Getting Sold positions by checking for stocks that are in yesterdays portfolio but not in Todays
        if "Today's Adjusted Close" not in Signals.columns.values: #If Today's close is not in the portfolio then add it as we will need it to see what stocks we need to sell
            TodaysAdjClose = Fields['adj_close'][Fields['adj_close'].index.get_level_values('dat') == datadate].reset_index('dat').drop(columns='dat') #Getting Todays close
            Signals["Today's Adjusted Close"] = TodaysAdjClose[TodaysAdjClose.index.isin(Signals.index)] #Adding it to Signals and converiting to a dataframe
        if "Today's Close" not in Signals.columns.values: #If Today's close is not in the portfolio then add it as we will need it to see what stocks we need to sell
            TodaysClose = Fields['close'][Fields['close'].index.get_level_values('dat') == datadate].reset_index('dat').drop(columns='dat') #Getting Todays close
            Signals["Today's Close"] = TodaysClose[TodaysClose.index.isin(Signals.index)] #Adding it to Signals and converiting to a dataframe
        AllocationRemaining = 1-Portfolio['Allocation'].sum() #Getting Allocation remaining
        Signals = Signals.append(Portfolio[~Portfolio.index.isin(Signals.index)]) #Adding stocks that are not in Signals but are in Portfolio. To mitigate the error when indexing Signals with stocks in Portfolio.
        if 'Quantity' not in Signals.columns.values: #Setting NaN values if Quantity and Expiry are not defined because we will be indexing these columns later on.
            Signals['Quantity'] = numpy.nan
        if 'Expiry' not in Signals.columns.values:
            Signals['Expiry'] = numpy.nan
        Portfolio['Expiry'][Portfolio['Expiry'] != 'Infinite'] = pandas.to_datetime(Portfolio['Expiry'][Portfolio['Expiry'] != 'Infinite']) #Converting expiry dates to datetime objects so we may compare them later.
        Signals['Expiry'][Signals.index.isin(Portfolio.index)] = Portfolio['Expiry'][Portfolio.index.isin(Signals.index)] #Updating the expiry dates in our portfolio with the ones from signals that we just converted to datetime objects.
        #Closing Bad Data Positions when it is not yesterdays portfolio or subtrades or the position has passed its expiry date or the expiry is na
        Signals['Quantity'][(~Signals.index.isin(YPortfolio.index)) & (~Signals.index.isin(Ysubtrades.index)) & (Signals.index.isin(Portfolio.index)) | Signals['Expiry'].isna() & (Signals.index.isin(Portfolio.index)) | (Signals['Expiry'] >= datetime.strptime(Today,'%Y-%m-%d')) & (Signals['Expiry'].str.lower() != 'infinite') & (Signals.index.isin(Portfolio.index))] = Portfolio['Quantity'][(~Portfolio.index.isin(YPortfolio.index)) & (~Portfolio.index.isin(Ysubtrades.index)) & (Portfolio.index.isin(Signals.index)) | Portfolio['Expiry'].isna() & (Portfolio.index.isin(Signals.index)) | (Portfolio['Expiry'] >= datetime.strptime(Today,'%Y-%m-%d')) & (Portfolio['Expiry'].str.lower() != 'infinite') & (Portfolio.index.isin(Signals.index))] #Setting the quantity in signals as the quantity in our portfolio as that is how much we will need to close the position
        Signals['Action'][(~Signals.index.isin(YPortfolio.index)) & (~Signals.index.isin(Ysubtrades.index)) & (Signals.index.isin(Portfolio.index)) & (Signals['Quantity'] > 0) | Signals['Expiry'].isna() & (Signals.index.isin(Portfolio.index)) & (Portfolio['Quantity'] > 0) | (Portfolio['Expiry'] >= datetime.strptime(Today,'%Y-%m-%d')) & (Signals['Expiry'].str.lower() != 'infinite') & (Signals.index.isin(Portfolio.index)) & (Portfolio['Quantity'] > 0)] = 'SELL' #Setting the Action to Sell for all long positions so that they closed
        Signals['Action'][(~Signals.index.isin(YPortfolio.index)) & (~Signals.index.isin(Ysubtrades.index)) & (Signals.index.isin(Portfolio.index)) & (Signals['Quantity'] < 0) | Signals['Expiry'].isna() & (Signals.index.isin(Portfolio.index)) & (Portfolio['Quantity'] < 0) | (Portfolio['Expiry'] >= datetime.strptime(Today,'%Y-%m-%d')) & (Signals['Expiry'].str.lower() != 'infinite') & (Signals.index.isin(Portfolio.index)) & (Portfolio['Quantity'] < 0)] = 'Buy' #Setting Action for all short positions to buy so they will closed
        #End of closing Bad Data Positions
        Signals[(Signals['Quantity'] < 0) & (Signals.index.isin(Portfolio.index))] = -1*Signals[(Signals['Quantity'] < 0) & (Signals.index.isin(Portfolio.index))] #Making short orders in our portfolio to negative because there will be no action column in portfolio to tell if they are long or short positions.
        Orders = Signals[(Signals['Action'].str.lower() == 'buy') | (Signals['Action'].str.lower() == 'sell')] #Getting Orders that need to be placed.
        isSTP = (Orders['Order Type'].str.upper() == 'STP')  #Isolating Orders with Stop Limits
        #Checking if the stop orders have a stop price or stop price percent column. Then we are trying to calculate then fill based on the available information. If none are defined we will create both columns and fill it with NA values.
        if len(Orders[isSTP]) > 0:
            if 'Stop Price Percent' in Orders.columns.values: #If Stop Price Percent is a defined column then we will use it to Calculate then fill missing Stop Price value.
                if 'Stop Price' in Orders.columns.values: #If Stop Price is also a defined column we will use it to Calculate then fill missing Stop Price Percent Values
                    StopNa = Orders['Stop Price'].isna() #Find missing Stop Price Values
                    StopPercentNa = Orders['Stop Price Percent'].isna() #Finding missing Stop Price Percent Values
                    Orders['Stop Price'][StopNa & isSTP] = (Orders["Today's Adjusted Close"][StopNa & isSTP] + (Orders["Today's Adjusted Close"][StopNa & isSTP]*Orders['Stop Price Percent'][StopNa & isSTP])).apply(pandas.to_numeric, errors='coerce').round(2) #Calculating Missing Stop Price Values using Stop Price Percent and then filling the missing values
                    Orders['Stop Price Percent'][StopPercentNa & isSTP] = -((Orders["Today's Adjusted Close"][StopPercentNa & isSTP] - Orders['Stop Price'][StopPercentNa & isSTP]) /Orders["Today's Adjusted Close"][StopPercentNa & isSTP]).apply(pandas.to_numeric, errors='coerce').round(2) #Calculating Missing Stop Price Percent Values using Stop Price and then filling the missing values
                else: #If Stop Price is not a defined column we will create it and calculate using Stop Price Percent Values then fill the column with the calculated values.
                    Orders['Stop Price'] = numpy.nan #Creating Stop Price Column and filling with NA values
                    Orders['Stop Price'][isSTP] = (Orders["Today's Adjusted Close"][isSTP] + (Orders["Today's Adjusted Close"][isSTP]*Orders['Stop Price Percent'][isSTP])).apply(pandas.to_numeric, errors='coerce').round(2) #Calculating Stop Price using Stop Price Percent then filling the column with the calculated values.
                    Signals['Stop Price'][isSTP] = numpy.nan #Creating a Stop Price Column in Signals and filling with NA values in case we need to index it later.
            else: #If Stop Price Percent is not a defined we will use Stop Price to calculate and fill the values in Stop Price Percent. If neither column is defined Both Stop Price and Stop Price Percent columns will be created and filled with NA values
                if 'Stop Price' in Orders.columns.values: ##If Stop Price is defined we will use it to calculate and fill the values in Stop Price Percent
                    Orders['Stop Price Percent'] = numpy.nan #Creating a Stop Price Percent Column
                    Orders['Stop Price Percent'][isSTP] = -((Orders["Today's Adjusted Close"][isSTP] - Orders['Stop Price'][isSTP]) / Orders["Today's Adjusted Close"][isSTP]).apply(pandas.to_numeric, errors='coerce').round(2) #Calculating Stop Price Percent using Stop Price and then filling the values.
                    Signals['Stop Price Percent'][isSTP] = numpy.nan #Creating Stop Price Percent Column in Signals and filling with NA values in case we need to index it later.
                else: #If Neither Column Exzsits we are creating both columns and filling them entirely with NA values.
                    Orders['Stop Price Percent'] = numpy.nan
                    Orders['Stop Price'] = numpy.nan
                    Signals['Stop Price Percent'] = numpy.nan
                    Signals['Stop Price'] = numpy.nan
            # Rounding all Stop Order Limits to 2 decimals as this is what is allowed by IB
            Orders['Stop Price Percent'][isSTP] = Orders['Stop Price Percent'][isSTP].round(2) #Rounding Stop Price Percent to 2 decimals
            Orders['Stop Price'][isSTP] = Orders['Stop Price'][isSTP].round(2) #Rounding Stop Price to 2 decimals as it must be no more than 2 decimals or else IB will throw an error.
            Orders['Stop Price'][Orders.index.isin(Portfolio.index)] = Portfolio['Stop Price'][Portfolio.index.isin(Orders.index)] #You cannot set different stop losses for orders you already have in your portfolio, therefore I'am overwriting the stop loss you set with the stop loss of your existing position
        #Checking if Quantity or Allocation is defined. If one is defined we will use it to calculate the other. If neither is defined we will the value of both columns to nans
        if 'Quantity' in Orders.columns: #Checking for the quantity column
            if 'Allocation' in Orders.columns: #If the Allocation column is defined then use both columns to fill the missing values in the other column
                QuantityNA = Orders['Quantity'].isna() #Finding na values in Quantity
                AllocNA = Orders['Allocation'].isna() #Finding na values in Allocation
                Orders['Quantity'][QuantityNA] = int((networth * Orders['Allocation'][QuantityNA]) / (Orders["Today's Close"][QuantityNA] * 1.02)) #Calculating Quantity using Allocation
                Orders['Allocation'][AllocNA] = (Orders['Quantity'][AllocNA]*Orders["Today's Close"][AllocNA])/networth #Calculate Allocation using Quantity
            else: #If Allocation is not defined but Quantity is then Calculate it using the values from quantity
                Orders['Allocation'] = (Orders['Quantity']*Orders["Today's Close"])/networth #Calculting Allocation using Quantity
        else: #if Quantity is not defined
            if 'Allocation' in Orders.columns: #If Allocation is defined but Quantity is not then fill use the Allocation Column to full the Quantity column
                Orders['Quantity'] = int((networth * Orders['Allocation']) / (Orders["Today's Close"] * 1.02)) #Filling Quantity Column with allocation
            else: #If both columns are not defined the set the values for both to na
                Orders['Quantity'] = numpy.nan #Setting Quantity to NA
                Orders['Allocation'] = numpy.nan #Setting Allocation to NA
        #Checking if Holding Period or Expiry is defined. If one is defined we will use it to calculate the other. If neither is defined we will the value of both columns to nans
        if 'Holding Period' in Orders.columns: #Checking if the holding period is defined
            if 'Expiry' in Orders.columns: #If the Expiry column is also defined then use both columns to fill the missing values in the other column
                HoldingNA = Orders['Holding Period'].isna() #Finding na Holding Period Values
                ExpiryNA = Orders['Expiry'].isna() #Finding NA expiry values
                Orders['Expiry'] = pandas.to_datetime(Orders['Expiry']) #converting expiry to a datetime so we can use it to calculate
                Orders['Holding Period'][HoldingNA] = int(Orders['Expiry']-datetime.strptime(Today, '%Y-%m-%d').day) #Using Expiry to Calculating Holding Period Expiry Date - Todays Date = Holding Period
                for x in Orders[ExpiryNA].index: #Iterating through the na expiry rows because there is no pandas command to convert to datetime then add
                    Orders['Expiry'][x] = str(datetime.strptime(Today,'%Y-%m-%d')+timedelta(int(Orders['Holding Period'][x])))[0:10] #Getting Expiry using Holding Period by today + holding period = expiry
            else: #if Expiry is not defined
                Orders['Expiry'] = numpy.nan #Create the expiry column
                for x in Orders.index: #Iterating through the na expiry rows because there is no pandas command to convert to datetime then add
                    Orders['Expiry'][x] = str(datetime.strptime(Today,'%Y-%m-%d')+timedelta(int(Orders['Holding Period'][x])))[0:10] #Getting Expiry using Holding Period by today + holding period = expiry
        else: #if Holding period is not defined
            if 'Expiry' in Orders.columns: #If Expiry is defined but holding period is not
                Orders['Expiry'] = pandas.to_datetime(Orders['Expiry']) #Convert Expiry to datetime so we can perform calculations on it
                Orders['Holding Period'] = int(Orders['Expiry']-datetime.strptime(Today, '%Y-%m-%d').day) #Get Holding Period from expiry by Expiry Date - Todays Date = Holding Period
            else: #if neither column exists create it
                Orders['Holding Period'] = 'Infinite' #Creating and Setting Holding Period to Infinite
                Orders['Expiry'] = numpy.nan #Creating Expiry and setting it to a nan

        #Converting Limit Price to Limit Percent and Limit Percent to Limit Price. Also filling in nan values for both columns where one column is not nan#
        Sell = Orders[Orders['Action'].str.lower == 'sell']
        NewShort = Sell & ~Orders.index.isin(Portfolio.index)
 #       if 'Limit Price' in Orders.columns.values:
 #           Orders['Limit Price'][(Orders.to_numeric(Orders['Limit Price'], errors='coerce').notnull()) & isNewShort] = \
 #           Orders["Today's Close"][(Orders.to_numeric(Orders['Limit Price'],
 #                                                      errors='coerce').notnull()) & NewShort] * .98  # Chaning short orders whose Limit Prices are not numeric(Float or int) to 2%
 #           if 'Limit Percent' in Orders.columns.values:
 #       else:
 #           Orders['Limit Price'] = numpy.nan  # Dropping All short orders if there is no limitPrice defined
 #           Orders['Limit Price'] =



        #When trying to see what conditions(Chaning values like rsi, macd,ect...) we opened the positions on, we would need see the day we opened it and then check that csv for that data. This can be cumbersome for every position so we create a on open column that will display the conditions when we opened the position
        for x in Orders[~Orders.index.isin(Portfolio.index)].drop(columns=['Action','Stop Limit Percent','Stop Limit','Holding Period']).columns.values:
            Orders[x + ' on Open'] = numpy.nan #Adding on Open to the column
            Orders[x+' on Open'][~Orders.index.isin(Portfolio.index)] = Orders[x][~Orders.index.isin(Portfolio.index)] #Setting the values equal to the original column
        #To place an order on interactive brokers the order needs to have a contract.
        Orders['Contract'] = numpy.nan #Creating a contract column
        Orders['Contract'][Orders.index.isin(Portfolio.index)] = Portfolio['Contract'][Portfolio.index.isin(Orders.index)] #Getting Contracts for all orders that are in our portfolio. Since we already have them.
        for x in Orders[Orders['Contract'].isna()].index: #Getting Contracts for all orders not in portfolio
            try: #Since we are now interfacing with IB we may get unpredictable errors so we must a try and except loop for the exception handling
                contractL = app.get_contract_by_ticker(x[0])#Getting the contract by ticker
            except:
                contractL = None #If a error is raised the contract will be set to none
            if contractL is not None: #If the contract is not None then it has been received
                for y in contractL: #A single ticker may be traded on many different indexes so we must iterate through each contract to find the one on the index we want
                    contract = getattr(y, 'contract') #Get the contract from the object
                    exchange = getattr(contract, 'primaryExchange') #Get the primary exchange from the contract object
                    ticker = getattr(contract, 'symbol') #get the symbol from the contract object
                    if set(list(x[1])).issubset(list(exchange)) and ticker == x[0]: #Check if they match the desired symbol and exchange
                        contract.exchange = 'SMART' #If they match then set the exchange to smart not sure why this is done?????
                        Orders['Contract'][x] = contract #Put the order contract in the contract column and corresponding row
        if MargCallProtect == True: #These measures ensures that the account will never face a margin call
            #Defualt Values
            DefLMT = .02 #The default limit order if one is not set must be a decimal
            #Setting Limit Orders for all orders if not already defined
            Sell = Orders['Action'].str.lower == 'sell' #Isolating all sell signals
            #Finding Long Orders
            Buy = Orders['Action'].str.lower == 'buy' #Isolating all Buy signals
            #Calculating the amount of shares we will have after the trade is placed
            Orders['Amount Holding After Trade'] = numpy.nan # Creating empty column
            Orders['Amount Holding After Trade'][Sell & ~(Orders.index.isin(Portfolio.index))] = Orders['Quantity'][(Orders['Action'].str.lower() == 'sell') & ~(Orders.index.isin(Portfolio.index))]*-1 #If the order does not modify any existing positions and it is a sell order the amount we will have after the trade is -1*The Quantity of shares we want to sell
            Orders['Amount Holding After Trade'][Sell & ~(Orders.index.isin(Portfolio.index))] = Orders['Quantity'][(Orders['Action'].str.lower() == 'buy') & ~(Orders.index.isin(Portfolio.index))]#If the order does not modify any existing positions and it is a buy order the amount we will have after the trade is the Quantity of shares we want to buy
            Orders['Amount Holding After Trade'][Orders.index.isin(Portfolio.index)] = Portfolio['Quantity'][Portfolio.index.isin(Orders.index)] #The shares we currently have are being put into orders
            Orders['Amount Holding After Trade'][Sell & (Orders.index.isin(Portfolio.index))] = Orders['Amount Holding'][(Orders.index.isin(Portfolio.index)) & Sell]-Orders['Quantity'][(Orders.index.isin(Portfolio.index)) & Sell]#If the sell order position is in your portfolio, The Amount Holding After Trade equals the difference between Amount Holding and Quantity respectively
            Orders['Amount Holding After Trade'][Buy & (Orders.index.isin(Portfolio.index))] = Orders['Amount Holding'][(Orders.index.isin(Portfolio.index)) & Buy]+Orders['Quantity'][(Orders.index.isin(Portfolio.index)) & Buy]#If the buy order position is in your portfolio, The Amount Holding After Trade equals the sum between Amount Holding and Quantity

            #Setting Default limit orders to orders whom didn't have limits already
            if 'Limit Price' in Orders.columns.values:
                #If limit orders are already defined fill the na ones
                Orders['Limit Price'][(Orders.to_numeric(Orders['Limit Price'], errors='coerce').null()) & Buy] = Orders["Today's Close"][(Orders.to_numeric(Orders['Limit Price'], errors='coerce').null()) & Buy]*(1+DefLMT) #Chaning long orders whose Limit Prices are not numeric(Float or int) to default limit price
                Orders['Limit Price'][(Orders.to_numeric(Orders['Limit Price'], errors='coerce').null()) & Sell] = Orders["Today's Close"][(Orders.to_numeric(Orders['Limit Price'], errors='coerce').null()) & Sell]*(1-DefLMT) #Chaning long orders whose Limit Prices are not numeric(Float or int) to the default limit price
            else:  #If limit orders is not a defined column create it
                Orders['Limit Price'] = numpy.nan #Creating the Limit Price Column
                Orders['Limit Price'][Buy] = Orders["Today's Close"][Buy]*(1+DefLMT) #Setting default limit price to Buy orders
                Orders['Limit Price'][Sell] = Orders["Today's Close"][Sell]*(1-DefLMT) #Setting default limit price to Sell orders




                if firstcalc:
                    warnings.warn('When Margin Call Protection is active you need to define a Limit Price for orders or else the default limit price will be set at'+str(DefLMT)+'%. To turn this off set the MargCallProtect parameter to False') #Warn user about dropping orders
            #Droping Orders that don't have the required values to place the order
            PortQMOrderQ = (Portfolio[Portfolio.index.isin(Orders[Sell].index)]['Quantity'] - Orders[Orders.index.isin(Portfolio.index) & Sell]['Quantity']) #Subtracting the Amount of shares in your portfolio minus the amount shares we want to sell
            PortQMOrderQN = Orders[Orders.index.isin(Portfolio.index) & Orders.index.isin(PortQMOrderQ[PortQMOrderQ < 0].index)] #If it is in our portfolio and the diffrence above is less than 0 its a new short position
            isNewShort = Sell & ~Orders.index.isin(Portfolio.index) | Sell & PortQMOrderQN & (Orders.index.isin(Portfolio.index)) #If we are are selling a position that is not in our portfolio or filter the above it is a short
            if len(Orders[isNewShort] > 0): #Checking for new short positions
                if "Stop Price" in Orders.columns.values: #Checking if stop price is a defined column
                    if len(Orders[(Orders.to_numeric(Orders['Stop Price'], errors='coerce').notnull()) & isNewShort]) > 0: #Checking for null stop price values
                        warnings.warn('Some of your short orders were not placed because Margin Call Protection is active. When Margin Call Protection is true you need to define a Stop Price for short orders or else they will not be placed, to turn this off set the MargCallProtect parameter to False')  # Warning user about dropping orders
                    Orders = Orders.drop(Orders[(Orders.to_numeric(Orders['Stop Price'], errors='coerce').null()) & isNewShort].index) #Getting rid of short orders whose Limit Prices are not numeric(Float or int)
                else:
                    Orders.drop(Orders[isNewShort].index,inplace=True)  # Dropping All short orders if there is no limitPrice defined
                    warnings.warn('Your short orders were not placed because Margin Call Protection is active. When Margin Call Protection is true you need to define a Stop Price for short orders or else they will not be placed, to turn this off set the MargCallProtect parameter to False')  # Warning user about dropping orders

            #Calculating the effect on our buying power or Cost of each order:
            Orders['Cost'] = numpy.nan
            Sell = Orders[Orders['Action'].str.lower == 'sell'] #Isolating all sell signals
            Buy = Orders[Orders['Action'].str.lower == 'buy'] #Isolating all Buy
            PortQMOrderQ = (Portfolio[Portfolio.index.isin(Orders[Sell].index)]['Quantity'] - Orders[Orders.index.isin(Portfolio.index) & Sell]['Quantity']) #Subtracting the Amount of shares in your portfolio minus the amount shares we want to sell
            PortQMOrderQN = Orders[Orders.index.isin(Portfolio.index) & Orders.index.isin(PortQMOrderQ[PortQMOrderQ < 0].index)] #If it is in our portfolio and the diffrence above is less than 0 its a new short position
            PortQMOrderQZ = Orders[Orders.index.isin(Portfolio.index) & Orders.index.isin(PortQMOrderQ[PortQMOrderQ == 0].index)] #If it is in our portfolio and the diffrence above is equal to zero then we closed the position
            PortQPOrderQ = (Portfolio[Portfolio.index.isin(Orders[Buy].index)]['Quantity'] + Orders[Orders.index.isin(Portfolio.index) & Buy]['Quantity']) #Adding the amount of shares in your portfolio to the amount shares we want to buy
            PortQPOrderQP = Orders[Orders.index.isin(Portfolio.index) & Orders.index.isin(PortQPOrderQ[PortQPOrderQ > 0].index)] #If the sum above is positive then the position is long
            PortQPOrderQZ = Orders[Orders.index.isin(Portfolio.index) & Orders.index.isin(PortQPOrderQ[PortQPOrderQ == 0].index)]  # If the sum above is zero then the position is closed

            #Various scenarios alter the effect of orders on the buying power
            NewShort = Sell & ~Orders.index.isin(Portfolio.index) #new short position
            ExShort = Sell & (Orders.index.isin(Portfolio.index)) & PortQMOrderQN & (Orders.index.isin(Portfolio[Portfolio['Quantity'] < 0].index)) #selling more of a existing short position
            CShort = Buy & (Orders.index.isin(Portfolio.index)) & PortQPOrderQZ & (Orders.index.isin(Portfolio[Portfolio['Quantity'] < 0].index)) #closing a existing short position
            LongShort = Sell & (Orders.index.isin(Portfolio.index)) & PortQMOrderQN & (Orders.index.isin(Portfolio[Portfolio['Quantity'] > 0].index)) #existing long positions that is being changed to short

            NewLong = Buy & ~Orders.index.isin(Portfolio.index) # New Long Position
            ExLong = Buy & Orders.index.isin(Portfolio.index) & PortQPOrderQP & (Orders.index.isin(Portfolio[Portfolio['Quantity'] > 0].index)) #Buying more of a existing long position
            CLong = Sell & Orders.index.isin(Portfolio.index) & PortQMOrderQZ & (Orders.index.isin(Portfolio[Portfolio['Quantity'] > 0].index))#Closing a Long Position
            ShortLong = Buy & Orders.index.isin(Portfolio.index) & PortQPOrderQP & (Orders.index.isin(Portfolio[Portfolio['Quantity'] < 0].index))#Going from a short to a long position

            #Calculating the effect on our buying power if it is negative that means it is taking away money if it is positive then it is adding
            Orders['Cost'] = numpy.nan
            Orders['Cost'][NewShort | ExShort] = -1*Orders["Limit Price"][NewShort | ExShort]*Orders['Quantity'][NewShort | ExShort]*IntialMarginRequirment #The cost of a new short position or cost of selling more of a existing short position(Initial Buying Requirement*Price of Stocks)
            Orders['Cost'][LongShort] = ((Orders['Quantity'][LongShort]+Orders['Amount Holding After Trade'][LongShort])*Orders['Limit Price'][LongShort])-(Orders['Amount Holding After Trade'][LongShort]*Orders['Limit Price'][LongShort]*InitialMargReq)#The cost of a existing long positions that is being changed to short
            Orders['Cost'][CShort] = -1*Orders['Quantity'][CShort]*Orders['Limit Price'][CShort] #The cost of closing a existing short position
            Orders['Cost'][NewLong or ExLong] = -1*Orders["Limit Price"][NewLong or ExLong]*Orders['Quantity'][NewLong or ExLong] #The cost of a new long position or cost of buying more of an existing long position
            Orders['Cost'][ShortLong] = ((Orders['Amount Holding After Trade'][ShortLong]-Orders['Quantity'][ShortLong])*Orders['Limit Price'][ShortLong])+(Orders['Amount Holding After Trade'][ShortLong]*Orders['Limit Price'][ShortLong])#The cost of a short position that is being changed to a long
            Orders['Cost'][CLong] = Orders['Quantity'][CLong]*Orders['Limit Price'][CLong]#The Cost of closing a long position

            #calculating the commissions
            Orders['Commission'] = (Orders['Quantity']*.005) #Commissions are .005 per trade
            Orders['Commission'][Orders['Commission'] < 1] = 1 #If the commissions are less than $1 than commission is $1
            Orders['Cost'] = Orders['Cost']+Orders['Commission'] #Adding the Commissions from cost
            MaxMargin = ((Portfolio['Quantity'][Portfolio['Quantity'] < 0]*-1)*(1+Portfolio['Stop Price Percent'][Portfolio['Quantity'] < 0])*Portfolio['Buy Price'][Portfolio['Quantity'] < 0]).sum()*1+MaintenceMarginRequirment #Amount on Margin if short positions hit stop limits

            #Calculating Lowest Possible Equity not including cash for the portfolio before trades are placed
            MinPosEquity = (Portfolio['Quantity'][Portfolio['Quantity'] > 0]*Portfolio['Buy Price'][Portfolio['Quantity'] > 0]*(1-(Portfolio['Stop Limit'][Portfolio['Quantity'] > 0]))).sum() #The Max Drawdown for the whole portfolio

            #If our minimum equity excluding cash is less than the maximum margin amount then subtract the diffrence of our max margin and our min equity from our cash to make them equal
            if MaxMargin > MinPosEquity:
                Cash -= MaxMargin-MinPosEquity
            Orders['Cost CumSum Negative'] = numpy.nan
            Orders['Cost CumSum Negative'][Orders['Cost'] < 0] = Orders['Cost'][Orders['Cost'] < 0].cumsum()*-1
            Orders = Orders.drop(Orders[(Orders['Cost CumSum'] > Cash) & (Orders['Cost'] < 0)].index)



        tfirst = True
        Trades = pandas.DataFrame()
        if 'Cost' not in Orders.columns.values:
            Orders['Cost'] = numpy.nan
        for x in Orders.index: #Placing Orders
            Cost = Orders['Cost'][x]
            if Cash > Cost and MargCallProtect:
                if Cost != numpy.nan:
                    Cash += Cost
                elif ~MargCallProtect:
                    order = Orders()
                    order.action = Orders['Action'][x].str.upper()
                    order.totalQuantity = int(Orders['Quantity'][x])
                    order.orderType = Orders['Order Type'][x].str.upper()
                    if Orders['Order Type'][x].str.upper() == 'STP':
                        order.auxPrice = Orders['Stop Price'][x]
                    try:
                        app.placeOrder(app.get_order_ID(), Orders['Contract'][x], order)
                        if Orders['Action'][x].str.upper() == 'BUY':
                            Cash -= Cost
                        if (Orders['Action'][x].str.upper() == 'SELL') & x not in Portfolio.index:
                            Cash -= Cost*.5
                    except Exception as e:
                        pass
                    temp = Orders[(Orders.index.get_level_values('ticker') == x[0]) & (Orders.index.get_level_values('exchange') == x[1])]
                    if tfirst == True:
                        Trades = temp
                        tfirst = False
                    else:
                        Trades = Trades.append(temp)
            else:
                Orders.drop(x,inplace=True)
        if 'Contract' in Trades.columns.values:
            Trades.drop(columns='Contract',inplace=True)
        print('Completed Opened Positions for '+datadate, OpenedPositions)
        print('Completed Closed Positions for '+datadate, ClosedPositions)
        OpenedPositions.to_csv(OpenedPositions + datadate + '.csv')
        ClosedPositions.to_csv(ClosedPositions + datadate + '.csv')
        if 'contract' in Portfolio.columns.values:
            Portfolio.drop(columns='contract',inplace=True)
        if 'contract' in Trades.columns.values:
            Trades.drop(columns='contract',inplace=True)
        print('Portfolio on '+Today,'\n',Portfolio)
        print('Submitted Orders on '+Today,'\n',Trades)
        Portfolio.to_csv(PortfolioPath+Today+'.csv')
        Trades.to_csv(subtradespath + Today + '.csv')
        file = open(ranpath, 'w')
        file.close()
    app.disconnect()