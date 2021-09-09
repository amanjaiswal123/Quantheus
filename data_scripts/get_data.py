#This is used to get data
import warnings
from data_scripts.AlphaVantageDataStream import get_data_a_v
from data_scripts.tiingo_data_stream import get_data_tiingo
import numpy
from datetime import datetime,timedelta
from time import sleep
import os
from source.Commons import NearestTradingDay
from source.Commons import TradingDays
from source.config import backtestdatapath, livedatapath
import pandas
import psycopg2

#When calling Ahistory you must a start_day or an amount of days and a source of data
def AHistroy(Field:[],Source:str,Start_Day=None,End_Day=NearestTradingDay(str(datetime.now().date())),Days=None):
    Source = Source.lower()
    Today = str(datetime.today().date()) #Getting Todays Date will run everyday at 6am Timezone is location of the machine running this script
    datadate = End_Day
    if Start_Day == None:
        Start_Day = str(datetime.strptime(End_Day,'%Y-%m-%d') - timedelta(days=Days))[0:10]
    #Data will not be avalaible for this day becuase the data is updated at 11 pm so only yesterdays data will be available and this variable reflects the date which data will be available
    #Checking which source to pull data from if a valid one was not entered it will ask you to re-enter a valid one
    while True:
        if Source not in ['livehdfs','alpha','backtest','livealpha','rds','tiingo']:
            warnings.warn(Source + ' is not a valid data Source. Valid data sources are backtest,live and alpha,livealpha,rds.')
            Source = str(input('Enter a valid data source: '))
        else:
            break
    #Getting the data from the specified source
    if Source == 'livehdfs':
        #Creating Spark conf and setting master and app name because to create spark context you need to pass configuration
        conf = SparkConf().setAppName("TestAppAman").setMaster('local')
        #Creating SparkConext with the conf we created which will be used to create a spark session
        sc = SparkContext(conf=conf)
        #Creating a Spark Session to query and read hdfs
        ss = SparkSession(sc)
        #Creating Empty list that will contain all the queries that pyspark will use to query hdfs for data
        Queries = []
        #Getting Todays Date to pull data
        #Using this function we get the closests trading day that past. This is done because files only exzists for Trading Days
        ClosestTradingDay = NearestTradingDay(End_Day)
        #This handles the exception if you are running this on a Trading Day or not. The exception is that if it is run
        #on a holiday or weekend there will be data for the most recent trading day but if you are trading on a
        #trading day odds will be the data is not going to be there for that data because data is updated the next day at
        #5 am. So to ensure you get x number days of data and the most recent data available you must adjust the
        #start, end and check dates for the queries
        if Today in TradingDays:
            #Adjusting Start to a 1 to account for the fact that the data for today will not be available
            start = 1
            #Adjusting Check date to previous trading day because today data will not be available until tommorow
            Check = TradingDays[TradingDays.index(ClosestTradingDay)-1]
        else:
            #if today is not a trading day you want the data for the most recent trading day should be available
            start = 0
            #When you adjust start to have one more day you need to counter that by knocking one day off of the end so that you will always get the x number of data that you asked for which is what this does
            Days -= 1
            #Closest Trading Day will be check what it is because it will be available
            Check = ClosestTradingDay
        #This is where we check if the data for the most recent day is available if it is not the program does not proceed until it is
        #This is going to break up the each character into a separate string and put them into a list
        Check = list(Check)
        #This while loop removes '-' from the list because data files on hdfs are labeled with '-'
        while '-' in Check:
            Check.remove('-')
        Check = ''.join(Check)
        #Here is where we are checking if the data file for the most recent trading day exists
        try:
            ss.read.format("csv").option("header", "true").csv("hdfs://water.cattinc.com:9000/stockdata/eodprices/" + Check + '.txt').toPandas()
        except AnalysisException:
            print('Data not available retrying in 20 min')
            sleep(1200)
        #If it does it exist this where the queries that will be passed to spark that will query hdfs are created
        for x in range(start, Days + 1):
            #Creating a date to insert into the file name for each date
            tempday = TradingDays[TradingDays.index(ClosestTradingDay) - x]
            #Break up date into a list and remove '-' from it exactly how it is done above
            ltempday = list(tempday)
            while '-' in ltempday:
                ltempday.remove('-')
            tempday = ''.join(ltempday)
            #This is inserting the date into the pre defined query as all files are named in a pattern
            Query = "hdfs://water.cattinc.com:9000/stockdata/eodprices/" + tempday + '.txt'
            #Adding the completed Query into the list of Queries
            Queries.append(Query)
        #This is command to actually request all the files by passing the list of all the diffrent queries
        while True:
            try:
                df = ss.read.option("header", "true").csv(Queries).toPandas()
                break
            except Exception as e:
                print('Failed to get Data retrying')
        #This is formatting the data into how it should be by reseting the index and changing the column names to the
        #generic names this is done so no matter what source you get the data from it will always be the same
        #From this point on we have the data and we are just cleaning it up
        df = df.reset_index().rename(columns={'Symbol': 'ticker','Date': 'dat','AdjClose':'adj_close','AdjVol':'adj_vol'}).drop(columns=['index']).fillna(value=numpy.nan).replace('null', numpy.nan).replace('NASD','NASDAQ')
        #This changes all capital column names to lower case that can be changed
        for x in df.columns.values:
            try:
                df.rename(columns={x: x.lower()}, inplace=True)
            except:
                pass
        #This is handling the exception where data does not have a exchange. This is necessary because exchange is a
        #primary key and it is necessary in differentiating stocks and placing orders
        #This is done by checking whether it is a list of NYSE or NASDAQ symbols
        #Getting NYSE Symbols
        NYSEsymbols = pandas.read_csv('https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download')['Symbol'].values
        #Getting NASDAQ symbols
        NASDAQsymbols = pandas.read_csv('https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download')['Symbol'].values
        #Checking for na values
        df['exchange'][df['exchange'].isna() & (df['ticker'].isin(NYSEsymbols))] = 'NYSE'
        df['exchange'][df['exchange'].isna() & (df['ticker'].isin(NASDAQsymbols))] = 'NASDAQ'
        df['exchange'][df['exchange'].isna() & (df['ticker'].isin(NYSEsymbols)) & (df['ticker'].isin(NASDAQsymbols))] = 'unknown'
        df['exchange'][df['exchange'].isna() & (~df['ticker'].isin(NYSEsymbols)) & (~df['ticker'].isin(NASDAQsymbols))] = 'unknown'
        Data = df
        ss.stop()
    elif Source == 'alpha':
        #Get Data directly from Alpha Vantage
        Fetch = get_data_a_v()
        Data = Fetch.NYSEandNASDAQData()
        Data['adj_open'] = (Data['adj_close'] / Data['close']) * Data['open']
    elif Source == 'tiingo':
        # Get Data directly from Alpha Vantage
        Fetch = get_data_tiingo()
        Data = Fetch.NYSEandNASDAQData(Start_Day,End_Day)
        if 'adj_open' not in Data.columns.values:
            Data['adj_open'] = (Data['adj_close'] / Data['close']) * Data['open']
        if "date" in Data.columns.values:
            try:
                Data = Data.rename(columns={'date':'dat'})
            except:
                pass
    elif Source == 'backtest':
        #Get static EOD Data file for the last 20 years
        if os.path.exists(backtestdatapath):
            if True in [set(list('Backtest')).issubset(list(x)) for x in os.listdir(backtestdatapath)]:
                for x in os.listdir(backtestdatapath):
                    if set(list('BacktestData')).issubset(list(x)):
                        Data = pandas.read_csv(backtestdatapath+'/'+x)
                        break
            else:
                raise Exception('Backtest Market Data File not Found')
        else:
            raise Exception('Backtest Market Data File not Found')
        for column in Data.columns.values:
            if 'Unnamed' in column:
                Data.drop(columns=column,inplace=True)
    elif Source =='livealpha':
        #Get dynamic file that is sourced from Alpha Vantage and updated daily
        #checks if the data for the day we want is there if it is not it waits until it is there. This is possible because all data is
        #named in a pattern. The pattern is livedata+(The most recent day of data it has) so if the file
        #livedata+(The most recent day of data it has) does not exists the data does not exists.
        while os.path.isfile(livedatapath+datadate+'.csv') is False:
            print('Could not find live data for '+datadate+'.retrying in 20 min')
            sleep(1200)
        #Retrieving the Data
        Data = pandas.read_csv(livedatapath+ datadate + '.csv')
        try:
            Data['dat'] = pandas.to_datetime(Data['dat'], format='%m/%d/%Y')
        except:
            pass
    elif Source == "rds":
        try:
            conn = psycopg2.connect(dbname='postgres', user='qtheus',host='qtheus-dev.cxd1dlbrmydp.us-east-2.rds.amazonaws.com', password='ycoWwypi2')
        except:
            raise Exception("Unable to connect to the database")
        Data = pandas.read_sql("""SELECT * FROM alpha_vantage WHERE date >= %s and date <= %s""",conn,params=(Start_Day,End_Day))
        Data["date"] = Data["date"].astype(str)
        Data = Data.rename(columns={'date':'dat'})
        Data = Data.sort_values('dat')
        Data.set_index(['ticker', 'exchange', 'dat'], inplace=True)
    print('Retrieved '+str(Field)+' Data')
    #Slicing Data so you get the data between the dates you asked for
    if Source != 'rds':
        Data = Data[(Data['dat'] >= Start_Day) & (Data['dat'] <= str(End_Day))]
        #Setting Index and organizing Data
        Data = Data.sort_values(['ticker','exchange','dat'])
        Data.set_index(['ticker','exchange','dat'],inplace=True)
        #Replacing blank values with nans
    Data.replace('', numpy.nan, inplace=True)
    Data.replace(' ', numpy.nan, inplace=True)
    #Slicing the specified data fields if the desired fields are not valid it will ask you to re-enter valid ones
    if 'all' not in Field:
        while True:
            try:
                #Trying to slice the data
                Data = Data[Field]
                #Break out of loop is slice was succesful
                break
                #If it does not happen
            except KeyError or IndexError:
                for x in Field:
                    if x not in Data.columns.values:
                        validF = list(Data.columns)
                        validF.remove('index')
                        warnings.warn(x+' is not a valid Field.'+' Valid Fields are '+str(validF))
                        Field = input('\nPlease enter valid Fields with a space as the delimter: ').split()
                        break
    Data = Data[~Data.index.duplicated(keep='first')]
    Data = Data.apply(pandas.to_numeric, errors='coerce')
    print('Returning Data\n')
    return Data

