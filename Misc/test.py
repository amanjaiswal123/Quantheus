import pandas
from source.Commons import upload_to_rds_table
import numpy

def find_support_resistance(df):
  # Calculate the rolling mean with a window of 20 days
  df['rolling_mean'] = df['Close'].rolling(20).mean()

  # Calculate the rolling standard deviation with a window of 20 days
  df['rolling_std'] = df['Close'].rolling(20).std()

  # Calculate the upper and lower bounds for the Bollinger bands
  df['upper_band'] = df['rolling_mean'] + df['rolling_std'] * 2
  df['lower_band'] = df['rolling_mean'] - df['rolling_std'] * 2

  # Set the support and resistance levels to the upper and lower bounds of the Bollinger bands
  support = df['lower_band'].min()
  resistance = df['upper_band'].max()

  return support, resistance

import numpy as np

def calculate_sharpe_ratio(returns, risk_free_rate):
  # Calculate the mean of the returns
  mean_return = np.mean(returns)

  # Calculate the standard deviation of the returns
  std_dev = np.std(returns)

  # Calculate the Sharpe ratio
  sharpe_ratio = (mean_return - risk_free_rate) / std_dev

  return sharpe_ratio
def calculate_slippage(bid, ask, midpoint):
  """
Calculate slippage given level 2 market data
  Args:
    bid (float): Bid price
    ask (float): Ask price
    midpoint (float): Midpoint of bid and ask
  Returns:
    float: slippage
  """
  return midpoint - (bid + ask) / 2

headers = pandas.read_csv('C:/Users/Aman Jaiswal/PycharmProjects/Quantheus/Data/MarketData/BacktestData_2022-12-24.csv',nrows=1)
Data = pandas.read_csv('C:/Users/Aman Jaiswal/PycharmProjects/Quantheus/Data/MarketData/BacktestData_2022-12-24.csv',skiprows=5310984)
Data.columns = headers.columns
Data['adj_open'] = (Data['adj_close'] / Data['close']) * Data['open']
#    conn = psycopg2.connect(dbname='postgres', user=qtheus_rds['user'],host=qtheus_rds['host'], password=qtheus_rds['password'])
 #   Data = pandas.read_sql("""SELECT * FROM alpha_vantage WHERE date >= %s and date <= %s""",conn,params=(Start_Day,End_Day))
 #   Data["date"] = Data["dat"].astype(str)
   # Data = Data.rename(columns={'date':'dat'})
Data = Data.sort_values('dat')
#Data = Data[Data['dat'] > '2020-07-23']
Data = Data.rename(columns={'dat':'date'})
#Slicing Data so you get the data between the dates you asked for
Data.replace('', numpy.nan, inplace=True)
Data.replace(' ', numpy.nan, inplace=True)
#Slicing the specified data fields if the desired fields are not valid it will ask you to re-enter valid ones
upload_to_rds_table(Data,'alpha_vantage',save_errors=True,rm_duplicate_index=True,remove_duplicate_rows=False,chunks=1000)


