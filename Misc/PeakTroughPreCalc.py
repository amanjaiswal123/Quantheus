from data_scripts.get_data import AHistroy
from source.Commons import NearestTradingDay,GetTradingDays,datadateslice,_datadate,notify
from datetime import datetime,timedelta
from source.Indicators import _PeakTroughs
import numpy

try:
    End_Date = '2019-01-01'
    StartingOffset = 365*5
    Start_Date = '2005-01-01'
    Start_Date = NearestTradingDay(str(datetime.strptime(Start_Date,'%Y-%m-%d') - timedelta(days=StartingOffset))[0:10])
    Data = AHistroy('adj_close', 'backtest', Start_Day=Start_Date, End_Day=End_Date)  # Getting Data for the backtest
    Dates = GetTradingDays(Start_Date, End_Date)
    first = True
    EndOffset = 0
    Start_Date = str(datetime.strptime(Start_Date, '%Y-%m-%d') + timedelta(days=StartingOffset))[0:10]
    Start_Date = sorted(numpy.array(Dates)[Start_Date <= numpy.array(Dates)])[0]
    TradingDaysCount = Dates.index(Start_Date)+1
    for Today in Dates[TradingDaysCount + 1:len(Dates) - EndOffset - 1]:  # Starting Iteration of Days in Back Test
        print(Today)
        Start = datetime.now()  # Getting Start time for each day so we can print the total time it took at the end of each
        datadate = _datadate(Today)
        Data5 = datadateslice(Data,End_Date=Today,Days=365*5)
        PeaksTroughs = _PeakTroughs(Data5)
        PeaksTroughs['dat'] = Today
        PeaksTroughs = PeaksTroughs.reset_index().set_index(['ticker','exchange','dat'])
        if first:
            OverallPeakTroughs = PeaksTroughs
        else:
            OverallPeakTroughs = OverallPeakTroughs.append(PeaksTroughs)
        first = False
        print('Total Time', datetime.now() - Start)  # Printing the time it took to start
    OverallPeakTroughs.to_csv('PeaksTroughValues1year.csv')
    OverallPeakTroughs.to_csv('/home/hduser/Quantheus/NewPeaksTroughValues5year.csv')
except Exception as e:
    print(e)
    notify('Peak Trough Generation has failed')
    raise e
notify('Peak Trough Gen has Finished')