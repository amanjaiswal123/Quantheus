import pandas
from data_scripts.get_data import AHistroy
from source.Commons import _datadate
from datetime import datetime,timedelta
from source.Commons import TradingDays
from source.Commons import GetTradingDays
import numpy
from source.Commons import NearestTradingDay, notify
pandas.set_option('mode.chained_assignment', None)
pandas.set_option('display.width', 320)
numpy.set_printoptions(linewidth=320)
def Backtest(func,WData=['all'],End_Date=_datadate(), Start_Date=None, Days=None,StartingOffset=0,EndOffset=0,SC = 6000,BacktestTitle='Backtest'):
    try:
        start = datetime.now() #Getting the start time for the backtest. We will use this to calculate the total amount of
        #Checking if you provided the necessary data to proceed
        if Days is None and Start_Date is None:
            raise Exception('You must define either the Start Date or an amount of Days to run the backtest for')
        if Days is not None and Start_Date is not None:
            raise Exception('You can only specify an amount of days or a set a start date not both')
        if type(WData) != list:
            raise Exception('WData must be a list')
        #time it took to complete the backtest
        #If you specified Days and not start day the program will calculate the start date based on the amount of days you specified
        End_Date = NearestTradingDay(End_Date)
        if Start_Date == None:
            Start_Date = NearestTradingDay(str(datetime.strptime(End_Date,'%Y-%m-%d') - timedelta(days=Days+StartingOffset))[0:10])
        else:
            Start_Date = NearestTradingDay(str(datetime.strptime(Start_Date,'%Y-%m-%d') - timedelta(days=StartingOffset))[0:10])
#        notify('The ' + BacktestTitle + '_' + Start_Date + '-' + End_Date + ' backtest has started')
        Data = AHistroy(WData, 'livealpha', Start_Day=Start_Date, End_Day=End_Date) #Getting Data for the backtest
        DatesinData = sorted(list(Data[Data.columns.values[0]][Data.index.get_level_values('ticker').values[0]].index.get_level_values('dat').values))
        if str(datetime.strptime(End_Date, '%Y-%m-%d') - timedelta(days=1))[0:10] not in DatesinData:
            raise Exception('We do not have marketdata after',End_Date, 'we only have data before',DatesinData[len(DatesinData)-1],'please adjust your End_Date argument accordingly')
        if Start_Date not in DatesinData:
            raise Exception('We do not have marketdata before',Start_Date, 'we only have data after',DatesinData[0],'please adjust your Start_Date argument accordingly. Your starting offset will also effect your start date as the actual start date will be the startdate-starting offset')
        #Getting all the dates that should be in the Data by filtering trading days between the start and end dates
        Dates = GetTradingDays(Start_Date,End_Date)
        Start_Date = str(datetime.strptime(Start_Date, '%Y-%m-%d') + timedelta(days=StartingOffset))[0:10]
        Start_Date = sorted(numpy.array(Dates)[Start_Date <= numpy.array(Dates)])[0]
        if Start_Date not in DatesinData:
            raise Exception('We do not have marketdata before',Start_Date, 'we only have data after',DatesinData[0],'please adjust your Start_Date argument accordingly. Your starting offset will also effect your start date as the actual start date will be the startdate-starting offset')
        NetWorth = SC # The balanace of your portfolio
        AvalCash = SC
        print('\nStarting Backtest on', Start_Date,'with','$'+str(SC))
        Daily_Balances = pandas.DataFrame(columns=['Net Worth','Exposure','Date']).set_index('Date')
        TradingDaysCount = Dates.index(Start_Date)+1 #The total number of trading days that have passed in the backtest including today so on day 1 it will be 1 day 2 it is 2 ect...
        YearOverYearBacktestSummary = pandas.DataFrame({'Date':[Dates[TradingDaysCount+1]],'Hit Rate':[0],'Net Worth':[SC],'Max DrawDown':[NetWorth],'Yearly Gains':[0]}).set_index('Date')
        Transactions = pandas.DataFrame() #Dataframe containing all the transactions of a stock
        Portfolio = pandas.DataFrame(columns=['Allocation','Stop Limit Percent','Holding Period','Buy Price','Opened Position On','ticker','exchange']).set_index(['ticker','exchange']) #Your Portfolio updated Daily after the interval is passed
        TradeNumber = 0 #Every Trade will have a trade # that is assigned after it is sold
        first = True
        for Today in Dates[TradingDaysCount+1:len(Dates)-EndOffset-1]:     #Starting Iteration of Days in Back Test
            TradingDaysCount += 1
            Start = datetime.now() #Getting Start time for each day so we can print the total time it took at the end of each
            #day
            print('\n'+Today,'\n')
            Fields = Data.loc[Data.index.get_level_values('dat') < Today] #This is the sliced Data or the data available for
            #calculations. It will only have data up till that day
            datadate = TradingDays[TradingDays.index(Today)-1]
            NetWorth = AvalCash
            for x in Portfolio.index:
                try:
                    NetWorth += Data['adj_open'][x][Today]*Portfolio['Quantity'][x]
                except:
                    pass
            AllocationRemaining = AvalCash/NetWorth
            #This is everything you must define within your signals dataframe to function properly
                #A Data Frame with the index being ticker and exchange and the following columns:
                #Action - Must be a Buy,Sell or nothing.
                #Allocation - The amount to allocate to the signal must be a less than 1 but greater than 0. Basically a percent but in a decimal form(Only needed for Buy Signals)
                #Stop Limit Percent - The Percent Stop Limit for the signal in decimal form
                #Stop Limit - The Stop Limit Price for the signal
                    # If Neither Stop Limit or Stop Limit Percent are defined both will automaticly be set to 0 and -1(-100%) respectively.
                #Holding Period - Max amount of trading to hold the stock for.(Only needed for Buy Signals) if there is no holding period set it to Infinite
                #All the indicators you used
            #Calling your function it should be able to accept three arguments
                #Fields: The Data
                #Signals: Yesterdays Signals. This is used so you can get yesterdays values and use those if you are calculating a moving average
                #Today: Today's Date
                #first: Whether or not it is the first run or not.
            #After the first run we pass the previous days calculations incase the user needs yesterdays data to calculate a moving average
            SignalsStartTime = datetime.now()
            if first == True: #If it is the first run we don't have yesterdays calculations so we wont pass it
                Signals = func(Fields,Portfolio,datadate,first,None) #Getting calculations
            else: #After the first run we will pass yesterdays calculations
                Signals = func(Fields,Portfolio,datadate,first,Signals) #Getting calculations and passing yesterdays Data
            SignalsTime = datetime.now()-SignalsStartTime
            Signals['Opened Position On'] = Today #Adding todays date to the signals
                #Today: The Current Day
                #first: Whether it is the first iteration or not
            Signals['Integrity'] = True #Integrity is used to ensure that when you buy a stock it has Data in the future avalaible
            Signals['Buy Price'] = None #Setting Buy Prices for stocks
            IntegCheckStartTime = datetime.now()
            for x in Signals[(Signals['Action'] == 'Buy') & (~Signals.index.isin(Portfolio.index))].index: #This loop
                #will check if all the buy signals have atleast 30 days worth if data if it does not then it will not
                #buy the stock and if it does set a buy price aswell
                if Signals['Holding Period'][x] != 'Infinite':
                    for y in Data.columns: #Iterating though the columns
                        try:
                            DatesinData = list(Data[y][x][Dates[TradingDaysCount-1:TradingDaysCount + Signals['Holding Period'][x]+1]].dropna().index.get_level_values('dat'))
                            EndDate = TradingDays[TradingDays.index(Today) + Signals['Holding Period'][x]]
                            if EndDate > Dates[len(Dates)-EndOffset-1]:
                                EndDate = Dates[len(Dates)-EndOffset-1]
                            ActualDates = GetTradingDays(datadate,EndDate)
                            if DatesinData != ActualDates:  # Checking Data
                                Signals['Integrity'][x] = False  # if it hits this except statement that means the data was not there and the stock will not be bought
                                break
                        except:
                            Signals['Integrity'][x] = False  # if it hits this except statement that means the data was not there and the stock will not be bought
                            break
                if Signals['Integrity'][x] == True:
                    try:
                        Signals['Buy Price'][x] = Data['adj_open'][x][Today]  # Then setting the buy price for todays open
                    except:
                        Signals['Integrity'][x] = False  # if it hits this except statement that means the data was not there and the stock will not be bought
            IntegCheckEndTime = datetime.now()-IntegCheckStartTime
            Bought_Today = Signals[(Signals['Integrity'] == True) & (Signals['Action'] == 'Buy') & (~Signals.index.isin(Portfolio.index)) & (Signals["Today's Close"] == Signals["Today's Adjusted Close"])].drop(columns=['Integrity']).copy() # Getting the buy signals that passed the integrity check and are not already in the portfolio
            Bought_Today['Commission'] = None
            Bought_Today['Expiry'] = None
            Bought_Today['Quantity'] = None
            Bought_Today['Max Drawdown'] = 0
            if AvalCash > 0:
                for x in Bought_Today.index: #This loop sees if we have enough money to buy the stock if we do it will calculate the commisions based on interactive brokrs
                    AllocatedAmount = NetWorth*Bought_Today['Allocation'][x]
                    Quantity = round((AllocatedAmount/Bought_Today['Buy Price'][x])-.5)
                    Commisions = Quantity*.005 #Calculating Commissions
                    CostOfPurchase = (Bought_Today['Buy Price'][x]*Quantity)+Commisions
                    if CostOfPurchase <= AvalCash and Quantity > 0 and x != ('NHTC','NASDAQ') and x != ('USAU','NASDAQ') and x != ('MBOT','NASDAQ') and x != ('WCN','NYSE') and x != ('PTC','NASDAQ'): #Checking if we have enough money to buy the stock
                        AvalCash -= CostOfPurchase
                        AllocationRemaining = AvalCash/NetWorth
                        Bought_Today['Commission'][x] = Commisions
                        Bought_Today['Quantity'][x] = Quantity
                        try:#This handles the exception that when you have a holding period longer than the dates in the
                            #backtest. If this exception is raised the expiry will be set to the last date available
                            if Bought_Today['Holding Period'][x] != 'Infinite':
                                Bought_Today['Expiry'][x] = Dates[Dates.index(Today)+Bought_Today['Holding Period'][x]] #Trying to set expiry
                            else:
                                Bought_Today['Expiry'][x] = 'Infinite'
                        except IndexError: #If the date is not there
                            Bought_Today['Expiry'][x] = Dates[len(Dates)-1] #Set it to the last available
                    else:
                        Bought_Today.drop(x,inplace=True)#If there is not enough money then it gets dropped
                if 'Stop Limit Percent' in Bought_Today.columns.values:
                    if 'Stop Limit' in Bought_Today.columns.values:
                        StopLimitNa = Bought_Today['Stop Limit'].isna()
                        StopLimitPNa = Bought_Today['Stop Limit Percent'].isna()
                        Bought_Today['Stop Limit'][StopLimitNa] = (Bought_Today['Buy Price'][StopLimitNa] * Bought_Today['Stop Limit Percent'][StopLimitNa]).apply(pandas.to_numeric, errors='coerce').round(2)
                        Bought_Today['Stop Limit Percent'][StopLimitPNa] = -((Bought_Today['Buy Price'][StopLimitPNa]-Bought_Today['Stop Limit'][StopLimitPNa])/Bought_Today['Buy Price'][StopLimitPNa]).apply(pandas.to_numeric, errors='coerce').round(2)
                    else:
                        Bought_Today['Stop Limit'] = (Bought_Today['Buy Price']*Bought_Today['Stop Limit Percent']).apply(pandas.to_numeric, errors='coerce').round(2)
                        Signals['Stop Limit'] = 0
                else:
                    if 'Stop Limit' in Bought_Today.columns.values:
                        Bought_Today['Stop Limit Percent'] = -((Bought_Today['Buy Price']-Bought_Today['Stop Limit'])/Bought_Today['Buy Price']).apply(pandas.to_numeric, errors='coerce').round(2)
                        Signals['Stop Limit Percent'] = -1
                    else:
                        Bought_Today['Stop Limit Percent'] = -1
                        Bought_Today['Stop Limit'] = 0
                        Signals['Stop Limit Percent'] = -1
                        Signals['Stop Limit'] = 0
                for x in Bought_Today.drop(columns=['Action','Commission','Expiry','Stop Limit Percent','Stop Limit','Holding Period','Buy Price','Opened Position On']).columns.values:
                    Bought_Today[x+' on Buy'] = Bought_Today[x]
                #Appending Portoflio with stocks that were bought today
                Portfolio = Portfolio.append(Bought_Today.copy()) #Adding the stocks that were bought to your portfolio
            for x in Signals.drop(columns=['Integrity','Allocation','Total Size','Stop Limit Percent','Stop Limit','Holding Period','Buy Price','Opened Position On']).columns.values: #This updates the values in your Portfolio everyday. ALthough some of the values are static so they should not be updated
                Portfolio[x] = Signals[x][Signals.index.isin(Portfolio.index)] #Updating the portfolio values
            if len(Portfolio.index) > 0: #After everything is bought we start to sell.
                TodaysAdjClose = Fields['adj_close'][Fields['adj_close'].index.get_level_values('dat') == datadate].reset_index('dat')['adj_close']
                Portfolio["Today's Adjusted Close"] = TodaysAdjClose[TodaysAdjClose.index.isin(Portfolio.index)]
                Portfolio['Total Change'] = ((Portfolio["Today's Adjusted Close"]-Portfolio['Buy Price'])/Portfolio['Buy Price'])
                Portfolio['Max Drawdown'][(Portfolio['Total Change'] < Portfolio['Max Drawdown'])] = Portfolio['Total Change'][(Portfolio['Total Change'] < Portfolio['Max Drawdown'])]
                # Allocation Size
                #Creating the Sell filters
                Portfolio['Stop Loss'] = False #Setting Stop loss for every stock to false
                Portfolio['Stop Loss'][(Portfolio["Today's Adjusted Close"] <= Portfolio['Stop Limit'])] = True
                Portfolio['Expired'] = False #Setting Expurd to false for evert stock
                Portfolio['Expired'][(Portfolio['Expiry'] <= Today) & (Portfolio['Expiry'] != 'Infinite')] = True #If it passed its the expiry it will be sold
                Sold_Today = Portfolio[(Portfolio['Action'] == 'Sell') | (Portfolio['Expired']) | (Portfolio['Stop Loss'])].copy() #Getting stocks that must be sold that are in your portfolio
                Portfolio.drop(index=Sold_Today.index,inplace=True)#Removing the sold stocks from your portfolio
                Portfolio = Portfolio.drop(columns=['Stop Loss','Expired'])#Removing The filters from portfolio so when it is printed it is not shown
                #Adding Sell Price,Calculating Gains on the transaction and multiplying Gains to the Balance of the Portfolio
                Sold_Today['Sell Price'] = None
                Sold_Today['Gains'] = None
                Sold_Today['ID'] = None
                Sold_Today['Trade #'] = None #Every Stock has a number.
                Sold_Today["Gains Relative to Net-Worth"] = None #The change selling the stock made to your portfolio
                for x in Sold_Today.index: #Calculating Gains and Sell Price
                    try: #This handles the excpetion if we cannot get the sell price for the stock it will drop it and it will not effect the portfolio
                        Sold_Today['Sell Price'][x] = Data['adj_open'][x][Today] #Getting the sell price as tommorows open price
                    except:
                        Sold_Today['Sell Price'][x] = False #If we could not the sell price it is set as false and later dropping it
                    if Sold_Today['Sell Price'][x] is not False: #Calcuating the gains and modifying the portfolio accordingly
                        TradeNumber += 1  # The first trade is #1 and then the second is #2 and so on
                        Gains = ((Sold_Today['Sell Price'][x]-Sold_Today['Buy Price'][x])/Sold_Today['Buy Price'][x]) #Calculting Gains
                        if Gains < Sold_Today['Stop Limit Percent'][x]: #Checking if it exceeds the stop limit and if it did then set the losses to the stop limit
                            Gains = Sold_Today['Stop Limit Percent'][x] #Setting gains to the stop limit
                        if Gains == None or Gains == numpy.nan or Gains > 2: #If the gains turned out to numpy or None set it to 0
                            Gains = 0 #Set Gains to 0
                            Sold_Today.drop(index=x, inplace=True)
                        DollarValueGains = (Sold_Today['Buy Price'][x]*Sold_Today['Quantity'][x]*Gains)-(Sold_Today['Quantity'][x]*.005)
                        AvalCash += Sold_Today['Sell Price'][x]*Sold_Today['Quantity'][x]
                        Sold_Today["Gains"][x] = Gains #Setting Gains
                        Sold_Today["Gains Relative to Net-Worth"][x] = DollarValueGains/NetWorth #Setting Absolute Gains
                        try:#Creating the ID for the transactions
                            Sold_Today['ID'][x] = ''.join(x) + Portfolio['Opened Position On'][x]
                        except:
                            pass
                        Sold_Today['Trade #'][x] = TradeNumber #Setting that trade with its corresponding trade number
                        Sold_Today['Date Sold'] = Today #Setting the sold Date
                        Sold_Today['ID'] = x[0]+x[1]+Sold_Today['Opened Position On'][x]
                    else:
                        Sold_Today.drop(index=x, inplace=True)
                AllocationRemaining = AvalCash / NetWorth
                TodaysBalances = pandas.DataFrame({'Net Worth':[NetWorth],'Exposure':[str((1-AllocationRemaining)*100)+'%'],'Date':[Today]}).set_index('Date')
                Daily_Balances = Daily_Balances.append(TodaysBalances)
                Transactions = Transactions.append(Sold_Today)  # Adding that trade to the stocks
            else:
                Sold_Today = None
            first = False
            print('\nBought Today:\n',Bought_Today) #Printing the stocks you bought today
            print('\nSold Today:\n',Sold_Today) #Printing the stocks that we sold today
            #print(TempPipe)
            #print(Transactions)
            print('\n',Portfolio,'Portfolio with',len(Portfolio.index),'positions using %'+str(round((1-AllocationRemaining)*100,0)), 'of the portfolio and a Balance of $'+str(NetWorth)+':\n') #printing your portfolio
            print('Total Time',datetime.now()-Start)#Printing the time it took to start
            print('Integ Check:',IntegCheckEndTime,'Signals:',SignalsTime)
            PrevDate = YearOverYearBacktestSummary.index.get_level_values('Date')[len(YearOverYearBacktestSummary.index.get_level_values('Date')) - 1]
            if datetime.strptime(Today,'%Y-%m-%d')-datetime.strptime(PrevDate,'%Y-%m-%d') == 365:
                MaxDrawDown = Daily_Balances[(Daily_Balances.index.get_level_values('Date') >= PrevDate) & (Daily_Balances.index.get_level_values('Date') <= Today)]['Net Worth'].sort_values().values[0]
                YearlyGains = str(round(((NetWorth-YearOverYearBacktestSummary['Net Worth'][PrevDate])/YearOverYearBacktestSummary['Net Worth'][PrevDate])*100,2))+'%'
                HitRate = str(round((len(Transactions[(Transactions['Date Sold'] >= PrevDate) & (Transactions['Date Sold'] <= Today) & (Transactions['Gains'] > 0)])/len(Transactions))*100,2))+'%'
                YearSummary = pandas.DataFrame({'Date':[Today],'Net Worth':[NetWorth],'Max DrawDown':[MaxDrawDown],'Yearly Gains':[YearlyGains],'Hit Rate':[HitRate]}).set_index('Date')
                YearOverYearBacktestSummary = YearOverYearBacktestSummary.append(YearSummary)
    except Exception as e:
        #Daily_Values.to_csv(BacktestTitle+'_'+Start_Date+'-'+End_Date+'_Daily_Values.csv')
        print('\nBackTest Failed on',Dates[len(Dates)-1],'with','$'+str(NetWorth))
        print('\nTotal Gain:',str(((NetWorth-SC)/SC)*100)+'%')
#        notify('The '+BacktestTitle+'_'+Start_Date+'-'+End_Date+' backtest has failed')
        print(e)
        Transactions.to_csv(BacktestTitle+'_'+Start_Date+'-'+End_Date+'_Transactions.csv')
        Daily_Balances.to_csv(BacktestTitle+'_'+Start_Date+'-'+End_Date+'_DailyBalances.csv')
        raise e
    Transactions.to_csv(BacktestTitle + '_' + Start_Date + '-' + End_Date + '_Transactions.csv')
    Daily_Balances.to_csv(BacktestTitle + '_' + Start_Date + '-' + End_Date + '_DailyBalances.csv')
    YearOverYearBacktestSummary.to_csv(BacktestTitle + '_' + Start_Date + '-' + End_Date + '_Summary.csv')
    #Daily_Values.to_csv(BacktestTitle + '_' + Start_Date + '-' + End_Date + '_Daily_Values.csv')
    print('\nBackTest Completed on', Dates[len(Dates) - 1], 'with', '$' + str(NetWorth))
    print('\nTotal Gain:', str(((NetWorth - SC) / SC) * 100) + '%')
#    notify('The '+BacktestTitle+'_'+Start_Date+'-'+End_Date+' backtest has finished')
