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